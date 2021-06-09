package exec

import (
	"errors"
	"fmt"
	"github.com/squareup/pranadb/sql"
)

type RowHandler interface {
	HandleRows(rows *sql.Rows) error
}

type pushExecutorBase struct {
	colTypes []sql.ColumnType
	rowsFactory *sql.RowsFactory
	out RowHandler
}

type PushSelect struct {
	pushExecutorBase
	predicate *sql.Expression
	out       RowHandler
}

func NewPushSelect(colTypes []sql.ColumnType, predicate *sql.Expression, out RowHandler) (*PushSelect, error) {
	rf, err := sql.NewRowsFactory(colTypes)
	if err != nil {
		return nil, err
	}
	base := pushExecutorBase{
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	return &PushSelect{
		pushExecutorBase: base,
		predicate:        predicate,
		out:              out,
	}, nil
}

func (p *PushSelect) HandleRows(rows *sql.Rows) error {
	result := p.rowsFactory.NewRows(rows.RowCount())
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		accept, isNull, err := p.predicate.EvalInt64(&row)
		if err != nil {
			return err
		}
		if isNull {
			return errors.New("null returned from evaluating select predicate")
		}
		if accept == 1 {
			result.AppendRow(row)
		}
	}
	return p.out.HandleRows(result)
}

type PushProjection struct {
	pushExecutorBase
	projColumns []*sql.Expression
}


func NewPushProjection(colTypes []sql.ColumnType, projColumns []*sql.Expression, out RowHandler) (*PushProjection, error) {
	rf, err := sql.NewRowsFactory(colTypes)
	if err != nil {
		return nil, err
	}
	base := pushExecutorBase{
		colTypes:    colTypes,
		rowsFactory: rf,
		out: out,
	}
	return &PushProjection{
		pushExecutorBase: base,
		projColumns:      projColumns,
	}, nil
}

func (p *PushProjection) HandleRows(rows *sql.Rows) error {
	result := p.rowsFactory.NewRows(rows.RowCount())
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		for j, projColumn := range p.projColumns {
			colType := p.colTypes[j]
			switch colType.TypeNumber {
			// No polymorphism in golang!
			case sql.TinyIntColumnType.TypeNumber, sql.IntColumnType.TypeNumber, sql.BigIntColumnType.TypeNumber:
				val, null, err := projColumn.EvalInt64(&row)
				if err != nil {
					return err
				}
				if null {
					result.AppendNullToColumn(j)
				} else {
					result.AppendInt64ToColumn(j, val)
				}
			case sql.DecimalColumnType.TypeNumber:
				val, null, err := projColumn.EvalDecimal(&row)
				if err != nil {
					return err
				}
				if null {
					result.AppendNullToColumn(j)
				} else {
					result.AppendDecimalToColumn(j, val)
				}
			case sql.VarcharColumnType.TypeNumber:
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
	return p.out.HandleRows(result)
}

type PushTwoWayJoin struct {
	pushExecutorBase
	LeftIn RowHandler
	rightIn RowHandler
}

type joinInput struct {
	inputFunc func(*sql.Rows) error
}

func (j joinInput) HandleRows(rows *sql.Rows) error {
	panic("implement me")
}

func newJoinInput(inputFunc func(*sql.Rows) error) joinInput {
	return joinInput{
		inputFunc: inputFunc,
	}
}

func NewPushTwoWayJoin(colTypes []sql.ColumnType, out RowHandler) (*PushTwoWayJoin, error) {
	rf, err := sql.NewRowsFactory(colTypes)
	if err != nil {
		return nil, err
	}
	base := pushExecutorBase{
		colTypes:    colTypes,
		rowsFactory: rf,
		out: out,
	}
	join := &PushTwoWayJoin{
		pushExecutorBase: base,
	}
	leftIn := newJoinInput(join.doLeftIn)
	rightIn := newJoinInput(join.doRightIn)

	join.LeftIn = leftIn
	join.rightIn = rightIn

	return join, nil
}

func (j *PushTwoWayJoin) doLeftIn(rows *sql.Rows) error {
	return nil
}

func (j *PushTwoWayJoin) doRightIn(rows *sql.Rows) error {
    return nil
}


