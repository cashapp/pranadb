package pranadb

import (
	"errors"
	"fmt"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/core"
	"github.com/squareup/pranadb/aggfuncs"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/exec"
	planner2 "github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/storage"
	"github.com/squareup/pranadb/table"
)

func BuildPushQueryExecution(schema *Schema, is infoschema.InfoSchema, query string,
	planner planner2.Planner, remoteConsumers map[uint64]*remoteConsumer, tableIDGenerator TableIDGenerator,
	queryName string, store storage.Storage, sharder common.Sharder) (queryDAG exec.PushExecutor, err error) {
	// Build the physical plan
	physicalPlan, err := planner.QueryToPlan(query, is)
	if err != nil {
		return nil, err
	}
	// Build initial dag from the plan
	dag, err := buildPushDAG(physicalPlan, tableIDGenerator, 0, queryName, store, sharder)
	if err != nil {
		return nil, err
	}
	// Update schemas to the form we need
	err = updateSchemas(dag, schema, remoteConsumers)
	if err != nil {
		return nil, err
	}
	return dag, nil
}

func buildPushDAG(plan core.PhysicalPlan, tableIDGenerator TableIDGenerator, aggSequence int,
	queryName string, store storage.Storage, sharder common.Sharder) (exec.PushExecutor, error) {
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

		var aggFuncs []*exec.AggregateFunctionInfo

		firstRowFuncs := 0
		for _, aggFunc := range physAgg.AggFuncs {
			argExprs := aggFunc.Args
			if len(argExprs) > 1 {
				return nil, errors.New("more than one aggregate function arg")
			}
			var argExpr *common.Expression
			if len(argExprs) == 1 {
				argExpr = common.NewExpression(argExprs[0])
			}

			var funcType aggfuncs.AggFunctionType
			switch aggFunc.Name {
			case "sum":
				funcType = aggfuncs.SumAggregateFunctionType
			case "avg":
				funcType = aggfuncs.AverageAggregateFunctionType
			case "count":
				funcType = aggfuncs.CountAggregateFunctionType
			case "max":
				funcType = aggfuncs.MaxAggregateFunctionType
			case "min":
				funcType = aggfuncs.MinAggregateFunctionType
			case "firstrow":
				funcType = aggfuncs.FirstRowAggregateFunctionType
				firstRowFuncs++
			default:
				return nil, fmt.Errorf("unexpected aggregate function %s", aggFunc.Name)
			}
			af := &exec.AggregateFunctionInfo{
				FuncType: funcType,
				Distinct: aggFunc.HasDistinct,
				ArgExpr:  argExpr,
			}
			aggFuncs = append(aggFuncs, af)
		}

		nonFirstRowFuncs := len(aggFuncs) - firstRowFuncs

		pkCols := make([]int, len(physAgg.GroupByItems))
		groupByCols := make([]int, len(physAgg.GroupByItems))
		for i, expr := range physAgg.GroupByItems {
			col, ok := expr.(*expression.Column)
			if !ok {
				return nil, errors.New("group by expression not a column")
			}
			groupByCols[i] = col.Index
			pkCols[i] = col.Index + nonFirstRowFuncs
		}

		tableID, err := tableIDGenerator()
		if err != nil {
			return nil, err
		}

		tableName := fmt.Sprintf("%s-aggtable-%d", queryName, aggSequence)
		aggSequence++

		tableInfo := &common.TableInfo{
			ID:             tableID,
			TableName:      tableName,
			PrimaryKeyCols: pkCols,
			ColumnNames:    colNames,
			ColumnTypes:    colTypes,
			IndexInfos:     nil, // TODO
		}

		aggTable, err := table.NewTable(store, tableInfo)
		if err != nil {
			return nil, err
		}

		// We build two executors here - a partitioner and an aggregator
		// Then we'll split those later into multiple DAGs
		partitioner, err := exec.NewAggPartitioner(colNames, colTypes, pkCols, tableID, groupByCols, sharder)
		if err != nil {
			return nil, err
		}
		aggregator, err := exec.NewAggregator(colNames, colTypes, pkCols, aggFuncs, aggTable, groupByCols)
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
		childExecutor, err := buildPushDAG(child, tableIDGenerator, aggSequence, queryName, store, sharder)
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
		// The col types for decoding from a remote receive before passing to the aggregator are
		// from the agg partitioner not the aggregator
		colTypes := aggregator.GetChildren()[0].ColTypes()
		rf, err := common.NewRowsFactory(colTypes)
		if err != nil {
			return err
		}
		rc := &remoteConsumer{
			rowsFactory: rf,
			colTypes:    colTypes,
			rowsHandler: aggregator,
		}
		remoteConsumers[aggregator.Table.Info().ID] = rc
	default:
		executor.ReCalcSchemaFromChildren()
	}
	return nil
}
