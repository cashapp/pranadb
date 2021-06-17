package pranadb

import (
	"errors"
	"fmt"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/core"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/exec"
	planner2 "github.com/squareup/pranadb/parplan"
)

func BuildPushQueryExecution(schema *Schema, is infoschema.InfoSchema, query string,
	planner planner2.Planner, remoteConsumers map[uint64]*remoteConsumer) (queryDAG exec.PushExecutor, err error) {
	// Build the physical plan
	physicalPlan, err := planner.QueryToPlan(query, is)
	if err != nil {
		return nil, err
	}
	// Build initial dag from the plan
	dag, err := buildPushDAG(physicalPlan)
	if err != nil {
		return nil, err
	}
	// Update schemas to the form we need
	err = updateSchemas(dag, schema, remoteConsumers)
	if err != nil {
		return nil, err
	}
	// Split the dag at any aggregations
	splitAtAggregations(dag)
	return dag, nil
}

func buildPushDAG(plan core.PhysicalPlan) (exec.PushExecutor, error) {
	cols := plan.Schema().Columns
	colTypes := make([]common.ColumnType, 0, len(cols))
	colNames := make([]string, 0, len(cols))
	for _, col := range cols {
		colType := col.GetType()
		pranaType, err := common.ConvertTiDBTypeToPranaType(colType)
		if err != nil {
			return nil, err
		}
		colTypes = append(colTypes, pranaType)
		colNames = append(colNames, col.OrigName)
	}
	var executor exec.PushExecutor
	var executorToConnect exec.PushExecutor
	var err error
	switch plan.(type) {
	case *core.PhysicalProjection:
		physProj := plan.(*core.PhysicalProjection)
		var exprs []*common.Expression
		for _, expr := range physProj.Exprs {
			exprs = append(exprs, common.NewExpression(expr))
		}
		executor, err = exec.NewPushProjection(colNames, colTypes, exprs)
		if err != nil {
			return nil, err
		}
		executorToConnect = executor
	case *core.PhysicalSelection:
		physSel := plan.(*core.PhysicalSelection)
		var exprs []*common.Expression
		for _, expr := range physSel.Conditions {
			exprs = append(exprs, common.NewExpression(expr))
		}
		executor, err = exec.NewPushSelect(colNames, colTypes, exprs)
		if err != nil {
			return nil, err
		}
		executorToConnect = executor
	case *core.PhysicalHashAgg:
		physAgg := plan.(*core.PhysicalHashAgg)
		var aggFuncs []common.AggFunction
		// TODO agg functions
		var exprs []*common.Expression
		for _, expr := range physAgg.GroupByItems {
			exprs = append(exprs, common.NewExpression(expr))
		}
		// We build two executors here - a partitioner and an aggregator
		// Then we'll split those later into multiple DAGs
		partitioner, err := exec.NewAggPartitioner(colNames, colTypes, aggFuncs, exprs, nil, 0)
		if err != nil {
			return nil, err
		}
		aggregator, err := exec.NewAggregator(colNames, colTypes, aggFuncs, nil, nil, 0)
		if err != nil {
			return nil, err
		}
		exec.ConnectExecutors([]exec.PushExecutor{partitioner}, aggregator)
		executor = aggregator
		executorToConnect = partitioner
	case *core.PhysicalTableReader:
		physTabReader := plan.(*core.PhysicalTableReader)
		if len(physTabReader.TablePlans) != 1 {
			panic("expected one table plan")
		}
		tabPlan := physTabReader.TablePlans[0]
		physTableScan, ok := tabPlan.(*core.PhysicalTableScan)
		if !ok {
			return nil, errors.New("expected PhysicalTableScan")
		}
		tableName := physTableScan.Table.Name
		executor, err = exec.NewTableScan(colTypes, tableName.L)
		if err != nil {
			return nil, err
		}
		executorToConnect = executor
	default:
		return nil, fmt.Errorf("unexpected plan type %T", plan)
	}

	var childExecutors []exec.PushExecutor
	for _, child := range plan.Children() {
		childExecutor, err := buildPushDAG(child)
		if err != nil {
			return nil, err
		}
		if childExecutor != nil {
			childExecutors = append(childExecutors, childExecutor)
		}
	}
	exec.ConnectExecutors(childExecutors, executorToConnect)

	return executor, nil
}

// TODO - do we need the schema information provided from the planner at all?? We could not bother setting it
// The schema provided by the planner may not be the ones we need. We need to provide information
// on key cols, which the planner does not provide, also we need to propagate keys through
// projections which don't include the key columns. These are needed when subsequently
// identifying a row when it changes
// We also connect up any executors which consumer data from sources, materialized views, or remote receivers
// to their feeders
func updateSchemas(executor exec.PushExecutor, schema *Schema, remoteConsumers map[uint64]*remoteConsumer) error {
	for _, child := range executor.GetChildren() {
		err := updateSchemas(child, schema, remoteConsumers)
		if err != nil {
			return err
		}
	}
	switch executor.(type) {
	case *exec.TableScan:
		tableScan := executor.(*exec.TableScan)
		tableName := tableScan.TableName
		var tableInfo *common.TableInfo
		mv, ok := schema.Mvs[tableName]
		if !ok {
			source, ok := schema.Sources[tableName]
			if !ok {
				return fmt.Errorf("unknown source or materialized view %s", tableName)
			}
			tableInfo = source.Table.Info()
			source.AddConsumingExecutor(executor)
		} else {
			tableInfo = mv.Table.Info()
			mv.AddConsumingExecutor(executor)
		}
		tableScan.SetSchema(tableInfo.ColumnNames, tableInfo.ColumnTypes, tableInfo.PrimaryKeyCols)
	case *exec.Aggregator:
		aggregator := executor.(*exec.Aggregator)
		rf, err := common.NewRowsFactory(aggregator.ColTypes())
		if err != nil {
			return err
		}
		rc := &remoteConsumer{
			rowsFactory: rf,
			colTypes:    aggregator.ColTypes(),
			rowsHandler: aggregator,
		}
		remoteConsumers[aggregator.TableID] = rc
	default:
		executor.ReCalcSchemaFromChildren()
	}
	return nil
}

// TODO do we need the return value here?
// Traverse the dag and wherever we find an [aggregate partitioner, aggregate] split the dag
// at that point, and return all the dags that have been split at the partitioner
func splitAtAggregations(executor exec.PushExecutor) []exec.PushExecutor {
	var dags []exec.PushExecutor
	for _, child := range executor.GetChildren() {
		dags = append(dags, splitAtAggregations(child)...)
	}
	_, ok := executor.(*exec.Aggregator)
	if ok {
		execChildren := executor.GetChildren()
		if len(execChildren) != 1 {
			panic("expected one child")
		}
		aggPartitioner := executor.GetChildren()[0]
		if _, ok := aggPartitioner.(*exec.AggPartitioner); !ok {
			panic("expected an aggregate partitioner")
		}
		// break the link between partitioner and aggregate
		aggPartitioner.SetParent(nil)
		executor.ClearChildren()
		dags = append(dags, aggPartitioner)
	}
	return dags
}
