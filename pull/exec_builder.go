package pull

import (
	"errors"
	"fmt"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/types"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/sess"

	"github.com/pingcap/tidb/planner/core"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/pull/exec"
)

func (p *PullEngine) buildPullQueryExecutionFromQuery(session *sess.Session, query string, prepare bool, remote bool) (queryDAG exec.PullExecutor, err error) {
	ast, err := session.PullPlanner().Parse(query)
	if err != nil {
		return nil, err
	}
	return p.buildPullQueryExecutionFromAst(session, ast, prepare, remote)
}

func (p *PullEngine) buildPullQueryExecutionFromAst(session *sess.Session, ast parplan.AstHandle, prepare bool, remote bool) (queryDAG exec.PullExecutor, err error) {

	// Build the physical plan
	physicalPlan, logicalSort, err := session.PullPlanner().BuildPhysicalPlan(ast, prepare)
	if err != nil {
		return nil, err
	}
	// Build initial dag from the plan
	dag, err := p.buildPullDAG(session, physicalPlan, session.Schema, remote)
	if err != nil {
		return nil, err
	}
	if logicalSort != nil {
		_, hasPhysicalSort := dag.(*exec.PullSort)
		if !hasPhysicalSort {
			// The TiDB planner assumes range partitioning and therefore sometimes elides the physical sort operator
			// from the physical plan duriung the optimisation process if the order by is on the primary key of the table
			// In this case it thinks the table is already ordered and we fan out to remote nodes using range scans
			// so the iteration order of the partial results when joined together gives us ordered results as require.
			// However, we use hash partitioning, so we always need to implement the sort after all partial results
			// are returned from nodes.
			// So... if the logical plan has a sort, but the physical plan doesn't we need to add a sort executor
			// in manually, which is what we're doing here

			desc, sortByExprs := p.byItemsToDescAndSortExpression(logicalSort.ByItems)
			sortExec := exec.NewPullSort(dag.ColNames(), dag.ColTypes(), desc, sortByExprs)
			exec.ConnectPullExecutors([]exec.PullExecutor{dag}, sortExec)
			dag = sortExec
		}
	}

	return dag, nil
}

// TODO: extract functions and break apart giant switch
// nolint: gocyclo
func (p *PullEngine) buildPullDAG(session *sess.Session, plan core.PhysicalPlan, schema *common.Schema, remote bool) (exec.PullExecutor, error) {
	cols := plan.Schema().Columns
	colTypes := make([]common.ColumnType, 0, len(cols))
	colNames := make([]string, 0, len(cols))
	for _, col := range cols {
		colType := col.GetType()
		pranaType := common.ConvertTiDBTypeToPranaType(colType)
		colTypes = append(colTypes, pranaType)
		colNames = append(colNames, col.OrigName)
	}
	var executor exec.PullExecutor
	var err error
	switch op := plan.(type) {
	case *core.PhysicalProjection:
		var exprs []*common.Expression
		for _, expr := range op.Exprs {
			exprs = append(exprs, common.NewExpression(expr))
		}
		executor, err = exec.NewPullProjection(colNames, colTypes, exprs)
		if err != nil {
			return nil, err
		}
	case *core.PhysicalSelection:
		var exprs []*common.Expression
		for _, expr := range op.Conditions {
			exprs = append(exprs, common.NewExpression(expr))
		}
		executor = exec.NewPullSelect(colNames, colTypes, exprs)
		if err != nil {
			return nil, err
		}
	case *core.PhysicalHashAgg:
		// TODO
		println("%v", op)
	case *core.PhysicalTableReader:
		// The Physical tab reader may have a a list of plans which can be sent to remote nodes
		// So we need to take each one of those and assemble into a dag, then add the dag to
		// RemoteExecutor pull executor
		// We only do this when executing a remote query
		var remoteDag exec.PullExecutor
		if remote {
			remotePlan := op.GetTablePlan()
			remoteDag, err = p.buildPullDAG(session, remotePlan, schema, remote)
			if err != nil {
				return nil, err
			}
		}
		executor = exec.NewRemoteExecutor(remoteDag, session.QueryInfo, colTypes, schema.Name, p.cluster)
	case *core.PhysicalTableScan:
		if !remote {
			panic("table scans only used on remote queries")
		}
		tableName := op.Table.Name.L
		tbl, ok := schema.GetTable(tableName)
		if !ok {
			return nil, fmt.Errorf("unknown source or materialized view %s", tableName)
		}
		if len(op.Ranges) > 1 {
			return nil, errors.New("only one range supported")
		}
		var scanRange *exec.ScanRange
		if len(op.Ranges) == 1 {
			rng := op.Ranges[0]
			if !rng.IsFullRange() {
				if len(rng.LowVal) != 1 {
					return nil, errors.New("composite ranges not supported")
				}
				lowD := rng.LowVal[0]
				highD := rng.HighVal[0]
				if lowD.Kind() != types.KindInt64 {
					return nil, errors.New("only int64 range supported")
				}
				scanRange = &exec.ScanRange{
					LowVal:   lowD.GetInt64(),
					HighVal:  highD.GetInt64(),
					LowExcl:  rng.LowExclude,
					HighExcl: rng.HighExclude,
				}
			}
		}
		executor, err = exec.NewPullTableScan(tbl.GetTableInfo(), p.cluster, session.QueryInfo.ShardID, scanRange)
		if err != nil {
			return nil, err
		}
	case *core.PhysicalSort:
		desc, sortByExprs := p.byItemsToDescAndSortExpression(op.ByItems)
		executor = exec.NewPullSort(colNames, colTypes, desc, sortByExprs)
	default:
		return nil, fmt.Errorf("unexpected plan type %T", plan)
	}

	var childExecutors []exec.PullExecutor
	for _, child := range plan.Children() {
		childExecutor, err := p.buildPullDAG(session, child, schema, remote)
		if err != nil {
			return nil, err
		}
		if childExecutor != nil {
			childExecutors = append(childExecutors, childExecutor)
		}
	}
	exec.ConnectPullExecutors(childExecutors, executor)

	return executor, nil
}

func (p *PullEngine) byItemsToDescAndSortExpression(byItems []*util.ByItems) ([]bool, []*common.Expression) {
	lbi := len(byItems)
	desc := make([]bool, lbi)
	sortByExprs := make([]*common.Expression, lbi)
	for i, byitem := range byItems {
		desc[i] = byitem.Desc
		sortByExprs[i] = common.NewExpression(byitem.Expr)
	}
	return desc, sortByExprs
}
