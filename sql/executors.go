package sql

import (
	"errors"
	"fmt"
	"github.com/squareup/pranadb/storage"
)

type PushExecutorNode interface {
	HandleRows(rows *Rows, batch *storage.WriteBatch) error

	SetParent(parent PushExecutorNode)

	ColNames() []string
	ColTypes() []ColumnType
}

type pushExecutorBase struct {
	colNames    []string
	colTypes    []ColumnType
	rowsFactory *RowsFactory
	parent      PushExecutorNode
}

func (p *pushExecutorBase) SetParent(parent PushExecutorNode) {
	p.parent = parent
}

type PushSelect struct {
	pushExecutorBase
	predicates []*Expression
}

func (p *PushSelect) ColNames() []string {
	return p.colNames
}

func (p *PushSelect) ColTypes() []ColumnType {
	return p.colTypes
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

func (p *PushSelect) HandleRows(rows *Rows, batch *storage.WriteBatch) error {
	result := p.rowsFactory.NewRows(rows.RowCount())
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		ok := true
		for _, predicate := range p.predicates {
			accept, isNull, err := predicate.EvalInt64(&row)
			if err != nil {
				return err
			}
			if isNull {
				return errors.New("null returned from evaluating select predicate")
			}
			if accept == 0 {
				ok = false
				break
			}
		}
		if ok {
			result.AppendRow(row)
		}
	}
	return p.parent.HandleRows(result, batch)
}

type PushProjection struct {
	pushExecutorBase
	projColumns []*Expression
}

func (p *PushProjection) ColNames() []string {
	return p.colNames
}

func (p *PushProjection) ColTypes() []ColumnType {
	return p.colTypes
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

func (p *PushProjection) HandleRows(rows *Rows, batch *storage.WriteBatch) error {
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
	}
	return p.parent.HandleRows(result, batch)
}

type MaterializedViewExecutor struct {
	pushExecutorBase
	table     Table
	sinkNodes []PushExecutorNode
}

func (m *MaterializedViewExecutor) ColNames() []string {
	return m.colNames
}

func (m *MaterializedViewExecutor) ColTypes() []ColumnType {
	return m.colTypes
}

func NewMaterializedViewExecutor(colTypes []ColumnType, table Table, sinkNodes []PushExecutorNode) (*MaterializedViewExecutor, error) {
	rf, err := NewRowsFactory(colTypes)
	if err != nil {
		return nil, err
	}
	base := pushExecutorBase{
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	viewExecutor := &MaterializedViewExecutor{
		pushExecutorBase: base,
		table:            table,
		sinkNodes:        sinkNodes,
	}

	return viewExecutor, nil
}

func (m MaterializedViewExecutor) HandleRows(rows *Rows, batch *storage.WriteBatch) error {
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		m.table.Upsert(&row, batch)
	}
	m.forwardToSinkViews()
	return nil
}

func (m MaterializedViewExecutor) forwardToSinkViews() {
	for _, sinkNode := range m.sinkNodes {
		// TODO write to forward queue for sink views
		println(sinkNode)
	}
	// TODO signal other nodes to process
}
