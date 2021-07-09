package exec

import (
	"fmt"

	"github.com/squareup/pranadb/common"
)

type PullProjection struct {
	pullExecutorBase
	projColumns []*common.Expression
}

func NewPullProjection(colNames []string, colTypes []common.ColumnType, projColumns []*common.Expression) (*PullProjection, error) {
	rf := common.NewRowsFactory(colTypes)
	base := pullExecutorBase{
		colNames:    colNames,
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	return &PullProjection{
		pullExecutorBase: base,
		projColumns:      projColumns,
	}, nil
}

// GetRows returns the projected columns.
func (p *PullProjection) GetRows(limit int) (rows *common.Rows, err error) {

	rows, err = p.GetChildren()[0].GetRows(limit)
	if err != nil {
		return nil, err
	}

	result := p.rowsFactory.NewRows(rows.RowCount())

	// TODO combine with similar logic in PushProjection
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		for j, projColumn := range p.projColumns {
			colType := p.colTypes[j]
			switch colType.Type {
			case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
				val, null, err := projColumn.EvalInt64(&row)
				if err != nil {
					return nil, err
				}
				if null {
					result.AppendNullToColumn(j)
				} else {
					result.AppendInt64ToColumn(j, val)
				}
			case common.TypeDecimal:
				val, null, err := projColumn.EvalDecimal(&row)
				if err != nil {
					return nil, err
				}
				if null {
					result.AppendNullToColumn(j)
				} else {
					result.AppendDecimalToColumn(j, val)
				}
			case common.TypeVarchar:
				val, null, err := projColumn.EvalString(&row)
				if err != nil {
					return nil, err
				}
				if null {
					result.AppendNullToColumn(j)
				} else {
					result.AppendStringToColumn(j, val)
				}
			case common.TypeDouble:
				val, null, err := projColumn.EvalFloat64(&row)
				if err != nil {
					return nil, err
				}
				if null {
					result.AppendNullToColumn(j)
				} else {
					result.AppendFloat64ToColumn(j, val)
				}
			default:
				return nil, fmt.Errorf("unexpected column type %d", colType.Type)
			}
		}

	}

	return rows, nil
}
