package pull

import (
	"fmt"
	"github.com/pingcap/tidb/planner/util"
	"log"

	"github.com/pingcap/tidb/planner/core"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/pull/exec"
)

func (p *PullEngine) buildPullQueryExecution(schema *common.Schema, query string, queryID string, remote bool, shardID uint64) (queryDAG exec.PullExecutor, err error) {
	// TODO The parser is not thread safe so we lock exclusively
	// Instead, we should maintain a separate parser/planner per client session
	p.queryLock.Lock()
	defer p.queryLock.Unlock()

	// Build the physical plan
	log.Printf("Executing query %s", query)
	physicalPlan, logicalSort, err := p.planner.QueryToPlan(schema, query, true)
	if err != nil {
		log.Printf("Query got error %v", err)
		return nil, err
	}
	// Build initial dag from the plan
	dag, err := p.buildPullDAG(physicalPlan, schema, query, queryID, remote, shardID)
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
func (p *PullEngine) buildPullDAG(plan core.PhysicalPlan, schema *common.Schema, query string, queryID string,
	remote bool, shardID uint64) (exec.PullExecutor, error) {
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
			remoteDag, err = p.buildPullDAG(remotePlan, schema, query, queryID, remote, shardID)
			if err != nil {
				return nil, err
			}
		}
		executor = exec.NewRemoteExecutor(remoteDag, colTypes, schema.Name, query, queryID, p.cluster)
	case *core.PhysicalTableScan:
		if !remote {
			panic("table scans only used on remote queries")
		}
		tableName := op.Table.Name.L
		var t *common.TableInfo
		mv, ok := schema.Mvs[tableName]
		if !ok {
			source, ok := schema.Sources[tableName]
			if !ok {
				return nil, fmt.Errorf("unknown source or materialized view %s", tableName)
			}
			t = source.TableInfo
		} else {
			t = mv.TableInfo
		}
		executor = exec.NewPullTableScan(t, p.cluster, shardID)
	case *core.PhysicalSort:
		desc, sortByExprs := p.byItemsToDescAndSortExpression(op.ByItems)
		executor = exec.NewPullSort(colNames, colTypes, desc, sortByExprs)
	default:
		return nil, fmt.Errorf("unexpected plan type %T", plan)
	}

	var childExecutors []exec.PullExecutor
	for _, child := range plan.Children() {
		childExecutor, err := p.buildPullDAG(child, schema, query, queryID, remote, shardID)
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
