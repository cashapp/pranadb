package pull

import (
	"github.com/pingcap/parser/model"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/sess"
	"github.com/squareup/pranadb/tidb/planner"
	"github.com/squareup/pranadb/tidb/planner/util"
	"github.com/squareup/pranadb/tidb/util/ranger"
	"strings"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/pull/exec"
)

func (p *Engine) buildPullQueryExecutionFromQuery(session *sess.Session, query string, prepare bool) (queryDAG exec.PullExecutor, err error) {

	if strings.HasPrefix(query, "select * from customer_balances where customer_token=") {
		log.Println("foo")
	}

	ast, err := session.PullPlanner().Parse(query)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return p.buildPullQueryExecutionFromAst(session, ast, prepare)
}

func (p *Engine) buildPullQueryExecutionFromAst(session *sess.Session, ast parplan.AstHandle, prepare bool) (queryDAG exec.PullExecutor, err error) {

	// Build the physical plan
	physicalPlan, logicalPlan, err := session.PullPlanner().BuildPhysicalPlan(ast, prepare)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Build initial dag from the plan
	dag, err := p.buildPullDAG(session, physicalPlan, session.Schema, false)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	logicalSort, ok := logicalPlan.(*planner.LogicalSort)
	if ok {
		_, hasPhysicalSort := dag.(*exec.PullSort)
		if !hasPhysicalSort {
			// The TiDB planner assumes range partitioning and therefore sometimes elides the physical sort operator
			// from the physical plan during the optimisation process if the order by is on the primary key of the table
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
func (p *Engine) buildPullDAG(session *sess.Session, plan planner.PhysicalPlan, schema *common.Schema, remote bool) (exec.PullExecutor, error) {
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
	case *planner.PhysicalProjection:
		var exprs []*common.Expression
		for _, expr := range op.Exprs {
			exprs = append(exprs, common.NewExpression(expr))
		}
		executor, err = exec.NewPullProjection(colNames, colTypes, exprs)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	case *planner.PhysicalSelection:
		var exprs []*common.Expression
		for _, expr := range op.Conditions {
			exprs = append(exprs, common.NewExpression(expr))
		}
		executor = exec.NewPullSelect(colNames, colTypes, exprs)
		if err != nil {
			return nil, errors.WithStack(err)
		}

	case *planner.PhysicalTableScan:
		if remote {
			tableName := op.Table.Name.L
			executor, err = p.createPullTableScan(schema, tableName, op.Ranges, op.Columns, session.QueryInfo.ShardID)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		} else {
			remoteDag, err := p.buildPullDAG(session, op, schema, true)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			// TODO should be done in transformation rule, not here!
			executor = exec.NewRemoteExecutor(remoteDag, session.QueryInfo, colNames, colTypes, schema.Name, p.cluster)
		}
	case *planner.PhysicalIndexScan:
		if remote {
			tableName := op.Table.Name.L
			executor, err = p.createPullTableScan(schema, tableName, op.Ranges, op.Columns, session.QueryInfo.ShardID)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		} else {
			remoteDag, err := p.buildPullDAG(session, op, schema, true)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			// TODO should be done in transformation rule, not here!
			executor = exec.NewRemoteExecutor(remoteDag, session.QueryInfo, colNames, colTypes, schema.Name, p.cluster)
		}
	case *planner.PhysicalSort:
		desc, sortByExprs := p.byItemsToDescAndSortExpression(op.ByItems)
		executor = exec.NewPullSort(colNames, colTypes, desc, sortByExprs)
	default:
		return nil, errors.Errorf("unexpected plan type %T", plan)
	}

	var childExecutors []exec.PullExecutor
	for _, child := range plan.Children() {
		childExecutor, err := p.buildPullDAG(session, child, schema, remote)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if childExecutor != nil {
			childExecutors = append(childExecutors, childExecutor)
		}
	}
	exec.ConnectPullExecutors(childExecutors, executor)

	return executor, nil
}

func (p *Engine) createPullTableScan(schema *common.Schema, tableName string, ranges []*ranger.Range, columns []*model.ColumnInfo, shardID uint64) (exec.PullExecutor, error) {
	tbl, ok := schema.GetTable(tableName)
	if !ok {
		return nil, errors.Errorf("unknown source or materialized view %s", tableName)
	}
	if len(ranges) > 1 {
		return nil, errors.Error("only one range supported")
	}
	var scanRange *exec.ScanRange
	if len(ranges) == 1 {
		rng := ranges[0]
		if !rng.IsFullRange() {
			if len(rng.LowVal) != 1 {
				return nil, errors.Error("composite ranges not supported")
			}
			lowD := rng.LowVal[0]
			highD := rng.HighVal[0]
			scanRange = &exec.ScanRange{
				LowVal:   common.TiDBValueToPranaValue(lowD.GetValue()),
				HighVal:  common.TiDBValueToPranaValue(highD.GetValue()),
				LowExcl:  rng.LowExclude,
				HighExcl: rng.HighExclude,
			}
		}
	}
	var colIndexes []int
	for _, col := range columns {
		colIndexes = append(colIndexes, col.Offset)
	}
	return exec.NewPullTableScan(tbl.GetTableInfo(), colIndexes, p.cluster, shardID, scanRange)
}

func (p *Engine) byItemsToDescAndSortExpression(byItems []*util.ByItems) ([]bool, []*common.Expression) {
	lbi := len(byItems)
	desc := make([]bool, lbi)
	sortByExprs := make([]*common.Expression, lbi)
	for i, byitem := range byItems {
		desc[i] = byitem.Desc
		sortByExprs[i] = common.NewExpression(byitem.Expr)
	}
	return desc, sortByExprs
}
