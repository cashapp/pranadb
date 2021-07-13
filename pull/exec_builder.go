package pull

import (
	"fmt"
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
	physicalPlan, err := p.planner.QueryToPlan(schema, query, true)
	if err != nil {
		log.Printf("Query got error %v", err)
		return nil, err
	}
	// Build initial dag from the plan
	dag, err := p.buildPullDAG(physicalPlan, schema, query, queryID, remote, shardID)
	if err != nil {
		return nil, err
	}

	// TODO TODO
	// We need to create out own remote call executor which corresponds to the TableReader
	// Based on the ranges in the table reader we can figure out what remote nodes we need to call
	// In the case of a point lookup it will be a single node, otherwise it will be all nodes.
	// In the first execution of getRows on the remote call executor we make a gRPC call to the
	// node(s) and in the case of a non point lookup we pass the serialized dag fragment to the node
	// when that is received, it is instantiated and a reference stored in a "PullQueryManager" and
	// a unique id returned.
	// The next call to getRows just needs to pass the unique id of the query. When the query is complete
	// the remote node can automatically unregister the query. Queries should also be unregistered after
	// a no activity timeout.

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
		executor = exec.NewRemoteExecutor(colTypes, remoteDag, schema.Name, query, queryID, p.cluster)
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
		executor = exec.NewPullTableScan(colTypes, t, p.cluster, shardID)
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
