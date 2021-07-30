package push

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/core"
	"github.com/squareup/pranadb/aggfuncs"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/push/exec"
)

func (p *PushEngine) buildPushQueryExecution(pl *parplan.Planner, schema *common.Schema, query string, mvName string, seqGenerator common.SeqGenerator) (queryDAG exec.PushExecutor, err error) {
	// Build the physical plan
	physicalPlan, _, err := pl.QueryToPlan(query, false)
	if err != nil {
		return nil, err
	}
	// Build initial dag from the plan
	dag, err := p.buildPushDAG(physicalPlan, 0, schema.Name, mvName, seqGenerator)
	if err != nil {
		return nil, err
	}
	// Update schemas to the form we need
	err = p.updateSchemas(dag, schema)
	if err != nil {
		return nil, err
	}
	return dag, nil
}

// TODO: extract functions and break apart giant switch
// nolint: gocyclo
func (p *PushEngine) buildPushDAG(plan core.PhysicalPlan, aggSequence int, queryName string, schemaName string, seqGenerator common.SeqGenerator) (exec.PushExecutor, error) {
	cols := plan.Schema().Columns
	colTypes := make([]common.ColumnType, 0, len(cols))
	colNames := make([]string, 0, len(cols))
	for _, col := range cols {
		colType := col.GetType()
		pranaType := common.ConvertTiDBTypeToPranaType(colType)
		colTypes = append(colTypes, pranaType)
		colNames = append(colNames, col.OrigName)
	}
	var executor exec.PushExecutor
	var err error
	switch op := plan.(type) {
	case *core.PhysicalProjection:
		var exprs []*common.Expression
		for _, expr := range op.Exprs {
			exprs = append(exprs, common.NewExpression(expr))
		}
		executor = exec.NewPushProjection(colNames, colTypes, exprs)
		if err != nil {
			return nil, err
		}
	case *core.PhysicalSelection:
		var exprs []*common.Expression
		for _, expr := range op.Conditions {
			exprs = append(exprs, common.NewExpression(expr))
		}
		executor = exec.NewPushSelect(colNames, colTypes, exprs)
		if err != nil {
			return nil, err
		}
	case *core.PhysicalHashAgg:
		var aggFuncs []*exec.AggregateFunctionInfo

		firstRowFuncs := 0
		for _, aggFunc := range op.AggFuncs {
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

		pkCols := make([]int, len(op.GroupByItems))
		groupByCols := make([]int, len(op.GroupByItems))
		for i, expr := range op.GroupByItems {
			col, ok := expr.(*expression.Column)
			if !ok {
				return nil, errors.New("group by expression not a column")
			}
			groupByCols[i] = col.Index
			pkCols[i] = col.Index + nonFirstRowFuncs
		}

		tableID := seqGenerator.GenerateSequence()

		tableName := fmt.Sprintf("%s-aggtable-%d", queryName, aggSequence)
		aggSequence++

		tableInfo := &common.TableInfo{
			ID:             tableID,
			SchemaName:     schemaName,
			Name:           tableName,
			PrimaryKeyCols: pkCols,
			ColumnNames:    colNames,
			ColumnTypes:    colTypes,
			IndexInfos:     nil, // TODO
		}
		aggInfo := &common.InternalTableInfo{
			TableInfo:            tableInfo,
			MaterializedViewName: queryName,
		}
		if err := p.meta.RegisterInternalTable(aggInfo, false); err != nil {
			return nil, err
		}
		executor, err = exec.NewAggregator(colNames, colTypes, pkCols, aggFuncs, tableInfo, groupByCols, p.cluster, p.sharder)
		if err != nil {
			return nil, err
		}
	case *core.PhysicalTableReader:
		if len(op.TablePlans) != 1 {
			panic("expected one table plan")
		}
		tabPlan := op.TablePlans[0]
		physTableScan, ok := tabPlan.(*core.PhysicalTableScan)
		if !ok {
			return nil, errors.New("expected PhysicalTableScan")
		}
		tableName := physTableScan.Table.Name
		executor, err = exec.NewTableScan(colTypes, tableName.L)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unexpected plan type %T", plan)
	}

	var childExecutors []exec.PushExecutor
	for _, child := range plan.Children() {
		childExecutor, err := p.buildPushDAG(child, aggSequence, schemaName, queryName, seqGenerator)
		if err != nil {
			return nil, err
		}
		if childExecutor != nil {
			childExecutors = append(childExecutors, childExecutor)
		}
	}
	exec.ConnectPushExecutors(childExecutors, executor)
	return executor, nil
}

// TODO - do we need the schema information provided from the planner at all?? We could not bother setting it
// The schema provided by the planner may not be the ones we need. We need to provide information
// on key cols, which the planner does not provide, also we need to propagate keys through
// projections which don't include the key columns. These are needed when subsequently
// identifying a row when it changes
// We also connect up any executors which consumer data from sources, materialized views, or remote receivers
// to their feeders
func (p *PushEngine) updateSchemas(executor exec.PushExecutor, schema *common.Schema) error {
	for _, child := range executor.GetChildren() {
		err := p.updateSchemas(child, schema)
		if err != nil {
			return err
		}
	}
	switch op := executor.(type) {
	case *exec.TableScan:
		tableName := op.TableName
		tbl, ok := schema.GetTable(tableName)
		if !ok {
			return fmt.Errorf("unknown source or materialized view %s", tableName)
		}
		switch tbl := tbl.(type) {
		case *common.SourceInfo:
			source := p.sources[tbl.GetTableInfo().ID]
			source.addConsumingExecutor(executor)
		case *common.MaterializedViewInfo:
			mv := p.materializedViews[tbl.GetTableInfo().ID]
			mv.addConsumingExecutor(executor)
		default:
			return fmt.Errorf("table scan on %s is not supported", reflect.TypeOf(tbl))
		}
		tableInfo := tbl.GetTableInfo()
		op.SetSchema(tableInfo.ColumnNames, tableInfo.ColumnTypes, tableInfo.PrimaryKeyCols)
	case *exec.Aggregator:
		colTypes := op.GetChildren()[0].ColTypes()
		rf := common.NewRowsFactory(colTypes)
		rc := &remoteConsumer{
			RowsFactory: rf,
			ColTypes:    colTypes,
			RowsHandler: op,
		}
		p.remoteConsumers[op.AggTableInfo.ID] = rc
	default:
		executor.ReCalcSchemaFromChildren()
	}
	return nil
}
