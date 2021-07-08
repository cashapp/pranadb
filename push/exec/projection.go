package exec

import (
	"fmt"

	"github.com/squareup/pranadb/common"
)

type PushProjection struct {
	pushExecutorBase
	projColumns         []*common.Expression
	invisibleKeyColumns []int
}

func NewPushProjection(colNames []string, colTypes []common.ColumnType, projColumns []*common.Expression) *PushProjection {
	rf := common.NewRowsFactory(colTypes)
	pushBase := pushExecutorBase{
		colNames:    colNames,
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	return &PushProjection{
		pushExecutorBase: pushBase,
		projColumns:      projColumns,
	}
}

func (p *PushProjection) ReCalcSchemaFromChildren() {
	if len(p.children) > 1 {
		panic("too many children")
	}
	if len(p.children) == 1 {
		child := p.children[0]
		// A projection might not include key columns from the child - but we need to maintain these
		// as invisible columns so we can identify the row and maintain it in storage
		var projColumns = make(map[int]int)
		for projIndex, projExpr := range p.projColumns {
			colIndex, ok := projExpr.GetColumnIndex()
			if ok {
				projColumns[colIndex] = projIndex
			}
		}

		invisibleKeyColIndex := len(p.projColumns)
		for _, childKeyCol := range child.KeyCols() {
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
				p.colNames = append(p.colNames, child.ColNames()[childKeyCol])
				p.colTypes = append(p.colTypes, child.ColTypes()[childKeyCol])
			}
		}
	}
}

func (p *PushProjection) HandleRows(rows *common.Rows, ctx *ExecutionContext) error {
	result := p.rowsFactory.NewRows(rows.RowCount())
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		for j, projColumn := range p.projColumns {
			colType := p.colTypes[j]
			switch colType.Type {
			case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
				val, null, err := projColumn.EvalInt64(&row)
				if err != nil {
					return err
				}
				if null {
					result.AppendNullToColumn(j)
				} else {
					result.AppendInt64ToColumn(j, val)
				}
			case common.TypeDecimal:
				val, null, err := projColumn.EvalDecimal(&row)
				if err != nil {
					return err
				}
				if null {
					result.AppendNullToColumn(j)
				} else {
					result.AppendDecimalToColumn(j, val)
				}
			case common.TypeVarchar:
				val, null, err := projColumn.EvalString(&row)
				if err != nil {
					return err
				}
				if null {
					result.AppendNullToColumn(j)
				} else {
					result.AppendStringToColumn(j, val)
				}
			case common.TypeDouble:
				val, null, err := projColumn.EvalFloat64(&row)
				if err != nil {
					return err
				}
				if null {
					result.AppendNullToColumn(j)
				} else {
					result.AppendFloat64ToColumn(j, val)
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
			switch colType.Type {
			case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
				val := row.GetInt64(colNumber)
				result.AppendInt64ToColumn(j, val)
			case common.TypeDecimal:
				val := row.GetDecimal(colNumber)
				result.AppendDecimalToColumn(j, val)
			case common.TypeVarchar:
				val := row.GetString(colNumber)
				result.AppendStringToColumn(j, val)
			case common.TypeDouble:
				val := row.GetFloat64(colNumber)
				result.AppendFloat64ToColumn(j, val)
			default:
				return fmt.Errorf("unexpected column type %d", colType)
			}
		}
	}

	return p.parent.HandleRows(result, ctx)
}
