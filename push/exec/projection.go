package exec

import (
	"fmt"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
)

type PushProjection struct {
	pushExecutorBase
	projColumns             []*common.Expression
	invisibleKeyColsInChild []int
}

func NewPushProjection(projColumns []*common.Expression) *PushProjection {
	return &PushProjection{
		pushExecutorBase: pushExecutorBase{},
		projColumns:      projColumns,
	}
}

func (p *PushProjection) calculateSchema(childColTypes []common.ColumnType, childKeyCols []int) error {
	p.colTypes = make([]common.ColumnType, len(p.projColumns))
	for i, projColumn := range p.projColumns {
		colType, err := projColumn.ReturnType(childColTypes)
		if err != nil {
			return errors.WithStack(err)
		}
		p.colTypes[i] = colType
		p.colsVisible = append(p.colsVisible, true)
	}

	// A projection might not include key columns from the child - but we need to maintain these
	// as invisible columns so we can identify the row and maintain it in storage
	var projColumns = make(map[int]int)
	for projIndex, projExpr := range p.projColumns {
		colIndex, ok := projExpr.GetColumnIndex()
		if ok {
			projColumns[colIndex] = projIndex
		}
	}

	hiddenIDIndex := 0
	invisibleKeyColIndex := len(p.projColumns)
	for _, childKeyCol := range childKeyCols {
		projIndex, ok := projColumns[childKeyCol]
		if ok {
			// The projection already contains the key column - so we just use that column
			p.keyCols = append(p.keyCols, projIndex)
		} else {
			// The projection doesn't include the key column so we need to include it from
			// the child - we will append this on the end of the row when we handle data
			p.keyCols = append(p.keyCols, invisibleKeyColIndex)
			p.invisibleKeyColsInChild = append(p.invisibleKeyColsInChild, childKeyCol)
			invisibleKeyColIndex++
			p.colNames = append(p.colNames, fmt.Sprintf("__gen_hid_id%d", hiddenIDIndex))
			hiddenIDIndex++
			p.colTypes = append(p.colTypes, childColTypes[childKeyCol])
			p.colsVisible = append(p.colsVisible, false)
		}
	}
	p.rowsFactory = common.NewRowsFactory(p.colTypes)
	return nil
}

func (p *PushProjection) ReCalcSchemaFromChildren() error {
	if len(p.children) > 1 {
		panic("too many children")
	}
	child := p.children[0]
	return p.calculateSchema(child.ColTypes(), child.KeyCols())
}

func (p *PushProjection) HandleRows(rowsBatch RowsBatch, ctx *ExecutionContext) error {
	numEntries := rowsBatch.Len()
	result := p.rowsFactory.NewRows(numEntries)
	rc := 0
	entries := make([]RowsEntry, numEntries)
	for i := 0; i < numEntries; i++ {
		pi := -1
		prevRow := rowsBatch.PreviousRow(i)
		if prevRow != nil {
			if err := p.calcProjection(prevRow, result); err != nil {
				return errors.WithStack(err)
			}
			pi = rc
			rc++
		}
		ci := -1
		currRow := rowsBatch.CurrentRow(i)
		if currRow != nil {
			if err := p.calcProjection(currRow, result); err != nil {
				return errors.WithStack(err)
			}
			ci = rc
			rc++
		}
		entries[i] = RowsEntry{prevIndex: pi, currIndex: ci}
	}
	return p.parent.HandleRows(NewRowsBatch(result, entries), ctx)
}

func (p *PushProjection) calcProjection(row *common.Row, result *common.Rows) error { //nolint:gocyclo
	for j, projColumn := range p.projColumns {
		colType := p.colTypes[j]
		switch colType.Type {
		case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
			val, null, err := projColumn.EvalInt64(row)
			if err != nil {
				return errors.WithStack(err)
			}
			if null {
				result.AppendNullToColumn(j)
			} else {
				result.AppendInt64ToColumn(j, val)
			}
		case common.TypeDecimal:
			val, null, err := projColumn.EvalDecimal(row)
			if err != nil {
				return errors.WithStack(err)
			}
			if null {
				result.AppendNullToColumn(j)
			} else {
				result.AppendDecimalToColumn(j, val)
			}
		case common.TypeVarchar:
			val, null, err := projColumn.EvalString(row)
			if err != nil {
				return errors.WithStack(err)
			}
			if null {
				result.AppendNullToColumn(j)
			} else {
				result.AppendStringToColumn(j, val)
			}
		case common.TypeDouble:
			val, null, err := projColumn.EvalFloat64(row)
			if err != nil {
				return errors.WithStack(err)
			}
			if null {
				result.AppendNullToColumn(j)
			} else {
				result.AppendFloat64ToColumn(j, val)
			}
		case common.TypeTimestamp:
			val, null, err := projColumn.EvalTimestamp(row)
			if err != nil {
				return errors.WithStack(err)
			}
			if null {
				result.AppendNullToColumn(j)
			} else {
				result.AppendTimestampToColumn(j, val)
			}
		default:
			return errors.Errorf("unexpected column type %d", colType)
		}
	}

	// Projections might not include the key columns, but we need to maintain these as they
	// need to be used when persisting the row, and when looking it up to process
	// any changes, so we append any invisible key column values to the end of the row
	appendStart := len(p.projColumns)
	for index, colNumber := range p.invisibleKeyColsInChild {
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
			return errors.Errorf("unexpected column type %d", colType)
		}
	}
	return nil
}
