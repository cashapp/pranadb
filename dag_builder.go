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

func BuildPushQueryExecution(schema *Schema, is infoschema.InfoSchema, query string, planner planner2.Planner) (queryDAG exec.PushExecutor, err error) {
	physicalPlan, err := planner.QueryToPlan(query, is)
	if err != nil {
		return nil, err
	}
	dag, err := buildPushDAG(nil, physicalPlan, schema)
	if err != nil {
		return nil, err
	}
	dag.ReCalcSchema()
	return dag, nil
}

func buildPushDAG(parentExecutor exec.PushExecutor, plan core.PhysicalPlan, schema *Schema) (exec.PushExecutor, error) {
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
	case *core.PhysicalHashAgg:
		physAgg := plan.(*core.PhysicalHashAgg)
		var aggFuncs []common.AggFunction
		// TODO agg functions
		var exprs []*common.Expression
		for _, expr := range physAgg.GroupByItems {
			exprs = append(exprs, common.NewExpression(expr))
		}
		executor, err = exec.NewAggPartitioner(colNames, colTypes, aggFuncs, exprs, nil, 0)
	case *core.PhysicalTableReader:
		physTabReader := plan.(*core.PhysicalTableReader)

		if len(physTabReader.TablePlans) != 1 {
			panic("expected one table plan")
		}

		tabPlan := physTabReader.TablePlans[0]
		if physTableScan, ok := tabPlan.(*core.PhysicalTableScan); !ok {
			return nil, errors.New("expected PhysicalTableScan")
		} else {
			tableName := physTableScan.Table.Name

			mv, ok := schema.Mvs[tableName.L]
			if !ok {
				source, ok := schema.Sources[tableName.L]
				if !ok {
					return nil, fmt.Errorf("unknown source or materialized view %s", tableName.L)
				}
				source.AddConsumingExecutor(parentExecutor)
				tableInfo := source.Table.Info()
				parentExecutor.ReCalcSchemaFromSources(tableInfo.ColumnNames, tableInfo.ColumnTypes, tableInfo.PrimaryKeyCols)
				return nil, nil
			}
			mv.AddConsumingExecutor(parentExecutor)
			tableInfo := mv.Table.Info()
			parentExecutor.ReCalcSchemaFromSources(tableInfo.ColumnNames, tableInfo.ColumnTypes, tableInfo.PrimaryKeyCols)
			return nil, nil
		}
	default:
		return nil, fmt.Errorf("unexpected plan type %T", plan)
	}

	var childExecutors []exec.PushExecutor
	for _, child := range plan.Children() {
		childExecutor, err := buildPushDAG(executor, child, schema)
		if err != nil {
			return nil, err
		}
		if childExecutor != nil {
			childExecutors = append(childExecutors, childExecutor)
		}
	}

	exec.ConnectExecutors(childExecutors, executor)

	return executor, nil
}
