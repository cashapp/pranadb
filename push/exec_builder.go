package push

import (
	"fmt"
	"github.com/squareup/pranadb/tidb/planner"

	"github.com/squareup/pranadb/errors"

	"github.com/squareup/pranadb/aggfuncs"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/push/exec"
	"github.com/squareup/pranadb/tidb/expression"
)

// Builds the push DAG but does not register anything in memory
func (m *MaterializedView) buildPushQueryExecution(pl *parplan.Planner, schema *common.Schema, query string, mvName string,
	seqGenerator common.SeqGenerator) (exec.PushExecutor, []*common.InternalTableInfo, error) {

	// Build the physical plan
	physicalPlan, logicalPlan, _, err := pl.QueryToPlan(query, false, false)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	// Build initial dag from the plan
	dag, internalTables, err := m.buildPushDAG(physicalPlan, 0, schema, mvName, seqGenerator)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	// Update schemas to the form we need
	err = m.updateSchemas(dag, schema)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	// We get the final column names from the logical plan - they're not always present on th physical plan, e.g.
	// in the case of union all
	var colNames []string
	for _, colName := range logicalPlan.OutputNames() {
		colNames = append(colNames, colName.ColName.L)
	}
	dag.SetColNames(colNames)
	return dag, internalTables, nil
}

// nolint: gocyclo
func (m *MaterializedView) buildPushDAG(plan planner.PhysicalPlan, aggSequence int, schema *common.Schema, mvName string,
	seqGenerator common.SeqGenerator) (exec.PushExecutor, []*common.InternalTableInfo, error) {
	var internalTables []*common.InternalTableInfo
	var executor exec.PushExecutor
	var err error
	switch op := plan.(type) {
	case *planner.PhysicalProjection:
		var exprs []*common.Expression
		for _, expr := range op.Exprs {
			exprs = append(exprs, common.NewExpression(expr, plan.SCtx()))
		}
		executor = exec.NewPushProjection(exprs)
	case *planner.PhysicalSelection:
		var exprs []*common.Expression
		for _, expr := range op.Conditions {
			exprs = append(exprs, common.NewExpression(expr, plan.SCtx()))
		}
		executor = exec.NewPushSelect(exprs)
	case *planner.PhysicalHashAgg:
		var aggFuncs []*exec.AggregateFunctionInfo

		firstRowFuncs := 0
		for _, aggFunc := range op.AggFuncs {
			argExprs := aggFunc.Args
			if len(argExprs) > 1 {
				return nil, nil, errors.Error("more than one aggregate function arg")
			}
			var argExpr *common.Expression
			if len(argExprs) == 1 {
				argExpr = common.NewExpression(argExprs[0], plan.SCtx())
			}

			var funcType aggfuncs.AggFunctionType
			switch aggFunc.Name {
			case "sum":
				funcType = aggfuncs.SumAggregateFunctionType
			case "count":
				funcType = aggfuncs.CountAggregateFunctionType
			case "firstrow":
				funcType = aggfuncs.FirstRowAggregateFunctionType
				firstRowFuncs++
			default:
				return nil, nil, errors.NewPranaErrorf(errors.InvalidStatement, "Unsupported aggregate function %s", aggFunc.Name)
			}
			colType := common.ConvertTiDBTypeToPranaType(aggFunc.RetTp)
			af := &exec.AggregateFunctionInfo{
				FuncType:   funcType,
				Distinct:   aggFunc.HasDistinct,
				ArgExpr:    argExpr,
				ReturnType: colType,
			}
			aggFuncs = append(aggFuncs, af)
		}

		nonFirstRowFuncs := len(aggFuncs) - firstRowFuncs

		// These are the indexes of the group by cols in the output of the aggregation
		pkCols := make([]int, len(op.GroupByItems))

		// These are the indexes of the group by cols in the input of the aggregation
		groupByCols := make([]int, len(op.GroupByItems))

		for i, expr := range op.GroupByItems {
			col, ok := expr.(*expression.Column)
			if !ok {
				return nil, nil, errors.Error("group by expression not a column")
			}
			groupByCols[i] = col.Index
			pkCols[i] = i + nonFirstRowFuncs
		}

		partialTableID := seqGenerator.GenerateSequence()
		partialTableName := fmt.Sprintf("%s-partial-aggtable-%d", mvName, aggSequence)
		aggSequence++
		partialTableInfo := &common.TableInfo{
			ID:             partialTableID,
			SchemaName:     schema.Name,
			Name:           partialTableName,
			PrimaryKeyCols: pkCols,
			IndexInfos:     nil, // TODO
			Internal:       true,
		}
		fullTableID := seqGenerator.GenerateSequence()
		fullTableName := fmt.Sprintf("%s-full-aggtable-%d", mvName, aggSequence)
		aggSequence++
		fullTableInfo := &common.TableInfo{
			ID:             fullTableID,
			SchemaName:     schema.Name,
			Name:           fullTableName,
			PrimaryKeyCols: pkCols,
			IndexInfos:     nil, // TODO
			Internal:       true,
		}
		partialAggInfo := &common.InternalTableInfo{
			TableInfo:            partialTableInfo,
			MaterializedViewName: mvName,
		}
		internalTables = append(internalTables, partialAggInfo)
		fullAggInfo := &common.InternalTableInfo{
			TableInfo:            fullTableInfo,
			MaterializedViewName: mvName,
		}
		internalTables = append(internalTables, fullAggInfo)
		executor, err = exec.NewAggregator(pkCols, aggFuncs, partialTableInfo, fullTableInfo, groupByCols, m.cluster, m.sharder)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
	case *planner.PhysicalUnionAll:
		executor, err = exec.NewUnionAll()
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
	case *planner.PhysicalTableScan:
		tableName := op.Table.Name
		var scanCols []int
		for _, col := range op.Columns {
			scanCols = append(scanCols, col.Offset)
		}
		executor, err = exec.NewScan(tableName.L, scanCols)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
	case *planner.PhysicalIndexScan:
		// If we create an MV that only selects on index fields the TiDB planner will give us an index reader.
		// As this is a push query we won't use an index but we'll use a push Scan specifying which columns we want
		tableName := op.Table.Name
		table, ok := schema.GetTable(tableName.L)
		if !ok {
			return nil, nil, errors.Errorf("cannot find table %s", tableName.L)
		}
		info := table.GetTableInfo()
		executor, err = exec.NewScan(tableName.L, info.PrimaryKeyCols)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
	default:
		return nil, nil, errors.Errorf("unexpected plan type %T", plan)
	}

	var childExecutors []exec.PushExecutor
	for _, child := range plan.Children() {
		childExecutor, it, err := m.buildPushDAG(child, aggSequence, schema, mvName, seqGenerator)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		internalTables = append(internalTables, it...)
		if childExecutor != nil {
			childExecutors = append(childExecutors, childExecutor)
		}
	}
	exec.ConnectPushExecutors(childExecutors, executor)
	return executor, internalTables, nil
}

// The schema provided by the planner may not be the ones we need. We need to provide information
// on key cols, which the planner does not provide, also we need to propagate keys through
// projections which don't include the key columns. These are needed when subsequently
// identifying a row when it changes
func (m *MaterializedView) updateSchemas(executor exec.PushExecutor, schema *common.Schema) error {
	for _, child := range executor.GetChildren() {
		err := m.updateSchemas(child, schema)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	switch op := executor.(type) {
	case *exec.Scan:
		tableName := op.TableName
		tbl, ok := schema.GetTable(tableName)
		if !ok {
			return errors.Errorf("unknown source or materialized view %s", tableName)
		}
		tableInfo := tbl.GetTableInfo()
		op.SetSchema(tableInfo)
	case *exec.Aggregator:
		// Do nothing
	default:
		return executor.ReCalcSchemaFromChildren()
	}
	return nil
}
