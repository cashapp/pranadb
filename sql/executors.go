package sql

import (
	"errors"
	"fmt"
	"github.com/pingcap/tidb/planner/core"
	"github.com/squareup/pranadb/storage"
)

type PushExecutorNode interface {
	HandleRows(rows *PullRows, batch *storage.WriteBatch) error

	SetParent(parent PushExecutorNode)
	AddChild(parent PushExecutorNode)
	RecalcSchema()
	RecalcSchemaFromSources(colNames []string, colTypes []ColumnType, keyCols []int)

	ColNames() []string
	ColTypes() []ColumnType
	KeyCols() []int
}

type pushExecutorBase struct {
	colNames    []string
	colTypes    []ColumnType
	keyCols     []int
	rowsFactory *RowsFactory
	parent      PushExecutorNode
	children    []PushExecutorNode
}

func (p *pushExecutorBase) SetParent(parent PushExecutorNode) {
	p.parent = parent
}

func (p *pushExecutorBase) AddChild(child PushExecutorNode) {
	p.children = append(p.children, child)
}

func (p *pushExecutorBase) KeyCols() []int {
	return p.keyCols
}

func (p *pushExecutorBase) ColNames() []string {
	return p.colNames
}

func (p *pushExecutorBase) ColTypes() []ColumnType {
	return p.colTypes
}

func ConnectNodes(childNodes []PushExecutorNode, parent PushExecutorNode) {
	for _, child := range childNodes {
		child.SetParent(parent)
		parent.AddChild(child)
	}
}

type PushSelect struct {
	pushExecutorBase
	predicates []*Expression
}

func NewPushSelect(colNames []string, colTypes []ColumnType, predicates []*Expression) (*PushSelect, error) {
	rf, err := NewRowsFactory(colTypes)
	if err != nil {
		return nil, err
	}
	base := pushExecutorBase{
		colNames:    colNames,
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	return &PushSelect{
		pushExecutorBase: base,
		predicates:       predicates,
	}, nil
}

func (p *PushSelect) RecalcSchema() {
	if len(p.children) > 1 {
		panic("too many children")
	}
	if len(p.children) == 1 {
		child := p.children[0]
		child.RecalcSchema()
		p.RecalcSchemaFromSources(child.ColNames(), child.ColTypes(), child.KeyCols())
	}
}

func (p *PushSelect) RecalcSchemaFromSources(colNames []string, colTypes []ColumnType, keyCols []int) {
	p.keyCols = keyCols
	p.colNames = colNames
	p.colTypes = colTypes
}

func (p *PushSelect) HandleRows(rows *PullRows, batch *storage.WriteBatch) error {
	result := p.rowsFactory.NewRows(rows.RowCount())
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		ok := true
		for _, predicate := range p.predicates {
			accept, isNull, err := predicate.EvalBoolean(&row)
			if err != nil {
				return err
			}
			if isNull {
				return errors.New("null returned from evaluating select predicate")
			}
			if !accept {
				ok = false
				break
			}
		}
		if ok {
			result.AppendRow(row)
		}
	}
	return p.parent.HandleRows(rows, batch)
}

type PushProjection struct {
	pushExecutorBase
	projColumns         []*Expression
	invisibleKeyColumns []int
}

func (p *PushProjection) RecalcSchema() {
	if len(p.children) > 1 {
		panic("too many children")
	}
	if len(p.children) == 1 {
		child := p.children[0]
		child.RecalcSchema()
		p.RecalcSchemaFromSources(child.ColNames(), child.ColTypes(), child.KeyCols())
	}
}

func (p *PushProjection) RecalcSchemaFromSources(colNames []string, colTypes []ColumnType, keyCols []int) {

	// A projection might not include key columns from the child - but we need to maintain these
	// as invisible columns so we can identify the row and maintain it in storage
	var projColumns = make(map[int]int)
	for projIndex, projExpr := range p.projColumns {
		colIndex, ok := projExpr.getColumnIndex()
		if ok {
			projColumns[colIndex] = projIndex
		}
	}

	invisibleKeyColIndex := len(p.projColumns)
	for _, childKeyCol := range keyCols {
		projIndex, ok := projColumns[childKeyCol]
		if ok {
			// The projection already contains the key column - so we just use that column
			p.keyCols = append(p.keyCols, projIndex)
		} else {
			// The projection doesn't include the key column so we need to include it from
			// the child - we will append this on the end of the row when we handle data
			p.keyCols = append(p.keyCols, invisibleKeyColIndex)
			p.invisibleKeyColumns = append(p.invisibleKeyColumns, childKeyCol)
			invisibleKeyColIndex++
			p.colNames = append(p.colNames, colNames[childKeyCol])
			p.colTypes = append(p.colTypes, colTypes[childKeyCol])
		}
	}
}

func NewPushProjection(colNames []string, colTypes []ColumnType, projColumns []*Expression) (*PushProjection, error) {
	rf, err := NewRowsFactory(colTypes)
	if err != nil {
		return nil, err
	}
	base := pushExecutorBase{
		colNames:    colNames,
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	return &PushProjection{
		pushExecutorBase: base,
		projColumns:      projColumns,
	}, nil
}

func (p *PushProjection) HandleRows(rows *PullRows, batch *storage.WriteBatch) error {
	result := p.rowsFactory.NewRows(rows.RowCount())
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		for j, projColumn := range p.projColumns {
			colType := p.colTypes[j]
			switch colType {
			case TypeTinyInt, TypeInt, TypeBigInt:
				val, null, err := projColumn.EvalInt64(&row)
				if err != nil {
					return err
				}
				if null {
					result.AppendNullToColumn(j)
				} else {
					result.AppendInt64ToColumn(j, val)
				}
			case TypeDecimal:
				val, null, err := projColumn.EvalDecimal(&row)
				if err != nil {
					return err
				}
				if null {
					result.AppendNullToColumn(j)
				} else {
					result.AppendDecimalToColumn(j, val)
				}
			case TypeVarchar:
				val, null, err := projColumn.EvalString(&row)
				if err != nil {
					return err
				}
				if null {
					result.AppendNullToColumn(j)
				} else {
					result.AppendStringToColumn(j, val)
				}
			default:
				return fmt.Errorf("unexpected column type %d", colType)
			}
		}

		// Projections might not include the key columns, but we need to maintain these as they
		// need to be used when persisting the row, and when looking it up to process
		// any changes, so we append any invisible key column values to the end of the row
		appendStart := len(p.projColumns)
		for index, colNumber := range p.invisibleKeyColumns {
			j := appendStart + index
			colType := p.colTypes[j]
			switch colType {
			case TypeTinyInt, TypeInt, TypeBigInt:
				val := row.GetInt64(colNumber)
				result.AppendInt64ToColumn(j, val)
			case TypeDecimal:
				val := row.GetDecimal(colNumber)
				result.AppendDecimalToColumn(j, val)
			case TypeVarchar:
				val := row.GetString(colNumber)
				result.AppendStringToColumn(j, val)
			default:
				return fmt.Errorf("unexpected column type %d", colType)
			}
		}
	}

	return p.parent.HandleRows(rows, batch)
}

// TableExecutor updates the changes into the associated table - used to persist state
// of a materialized view or source
type TableExecutor struct {
	pushExecutorBase
	table          Table
	consumingNodes []PushExecutorNode
	store          storage.Storage
}

func NewTableExecutor(colTypes []ColumnType, table Table, store storage.Storage) (*TableExecutor, error) {
	rf, err := NewRowsFactory(colTypes)
	if err != nil {
		return nil, err
	}
	base := pushExecutorBase{
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	return &TableExecutor{
		pushExecutorBase: base,
		table:            table,
		store:            store,
	}, nil
}

func (t *TableExecutor) RecalcSchema() {
	if len(t.children) > 1 {
		panic("too many children")
	}
	if len(t.children) == 1 {
		child := t.children[0]
		child.RecalcSchema()
		t.RecalcSchemaFromSources(child.ColNames(), child.ColTypes(), child.KeyCols())
	}
}

func (t *TableExecutor) RecalcSchemaFromSources(colNames []string, colTypes []ColumnType, keyCols []int) {
	t.keyCols = keyCols
	t.colNames = colNames
	t.colTypes = colTypes
}

func (m *TableExecutor) AddConsumingNode(node PushExecutorNode) {
	m.consumingNodes = append(m.consumingNodes, node)
}

func (m *TableExecutor) HandleRows(rows *PullRows, batch *storage.WriteBatch) error {
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		m.table.Upsert(&row, batch)
	}
	return m.forwardToConsumingNodes(rows, batch.ShardID)
}

func (m *TableExecutor) forwardToConsumingNodes(rows *PullRows, shardID uint64) error {
	for _, consumingNode := range m.consumingNodes {

		// TODO - execute these on goroutines

		batch := storage.NewWriteBatch(shardID)
		err := consumingNode.HandleRows(rows, batch)
		if err != nil {
			return err
		}
		if batch.HasWrites() {
			err = m.store.WriteBatch(batch, true)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func BuildDAG(parentNode PushExecutorNode, plan core.PhysicalPlan, schema *schema) (PushExecutorNode, error) {
	cols := plan.Schema().Columns
	colTypes := make([]ColumnType, 0, len(cols))
	colNames := make([]string, 0, len(cols))
	for _, col := range cols {
		colType := col.GetType()
		pranaType, err := ConvertTiDBTypeToPranaType(colType)
		if err != nil {
			return nil, err
		}
		colTypes = append(colTypes, pranaType)
		colNames = append(colNames, col.OrigName)
	}
	var node PushExecutorNode
	var err error
	switch plan.(type) {
	case *core.PhysicalProjection:
		physProj := plan.(*core.PhysicalProjection)
		var exprs []*Expression
		for _, expr := range physProj.Exprs {
			exprs = append(exprs, NewExpression(expr))
		}
		node, err = NewPushProjection(colNames, colTypes, exprs)
		if err != nil {
			return nil, err
		}
	case *core.PhysicalSelection:
		physSel := plan.(*core.PhysicalSelection)
		var exprs []*Expression
		for _, expr := range physSel.Conditions {
			exprs = append(exprs, NewExpression(expr))
		}
		node, err = NewPushSelect(colNames, colTypes, exprs)
		if err != nil {
			return nil, err
		}
	case *core.PhysicalHashAgg:
		// physAgg := plan.(*core.PhysicalHashAgg)
		// TODO
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

			mv, ok := schema.mvs[tableName.L]
			if !ok {
				source, ok := schema.sources[tableName.L]
				if !ok {
					return nil, fmt.Errorf("unknown source or materialized view %s", tableName.L)
				}
				source.AddConsumingNode(parentNode)
				tableInfo := source.Table.Info()
				parentNode.RecalcSchemaFromSources(tableInfo.ColumnNames, tableInfo.ColumnTypes, tableInfo.PrimaryKeyCols)
				return nil, nil
			}
			mv.AddConsumingNode(parentNode)
			tableInfo := mv.Table.Info()
			parentNode.RecalcSchemaFromSources(tableInfo.ColumnNames, tableInfo.ColumnTypes, tableInfo.PrimaryKeyCols)
			return nil, nil
		}
	default:
		return nil, fmt.Errorf("unexpected plan type %T", plan)
	}

	var childNodes []PushExecutorNode
	for _, child := range plan.Children() {
		childNode, err := BuildDAG(node, child, schema)
		if err != nil {
			return nil, err
		}
		if childNode != nil {
			childNodes = append(childNodes, childNode)
		}
	}

	ConnectNodes(childNodes, node)

	return node, nil
}
