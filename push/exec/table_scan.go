package exec

import (
	"fmt"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
)

type Scan struct {
	pushExecutorBase
	TableName string
	//pkOnly    bool
	//projExprs []*common.Expression
	cols []int
}

func NewScan(tableName string, cols []int) (*Scan, error) {
	return &Scan{
		pushExecutorBase: pushExecutorBase{},
		TableName:        tableName,
		cols:             cols,
	}, nil
}

func (t *Scan) SetSchema(tableInfo *common.TableInfo) {
	if t.cols == nil {
		t.colNames = tableInfo.ColumnNames
		t.colTypes = tableInfo.ColumnTypes
		t.keyCols = tableInfo.PrimaryKeyCols
		t.colsVisible = tableInfo.ColsVisible
	} else {
		for _, col := range t.cols {
			t.colNames = append(t.colNames, tableInfo.ColumnNames[col])
			t.colTypes = append(t.colTypes, tableInfo.ColumnTypes[col])
			visible := tableInfo.ColsVisible == nil || tableInfo.ColsVisible[col]
			t.colsVisible = append(t.colsVisible, visible)
		}

		// Is the PK covered? We need to check whether all pk cols have been selected otherwise
		// we have to retain pk cols as hidden cols
		pkColsMap := map[int]struct{}{}
		for _, pkCol := range tableInfo.PrimaryKeyCols {
			pkColsMap[pkCol] = struct{}{}
		}
		colsMap := map[int]struct{}{}
		for _, col := range t.cols {
			colsMap[col] = struct{}{}
		}
		for i, col := range t.cols {
			_, ok := pkColsMap[col]
			if ok {
				// The primary key column is covered by the selected columns, we just add it to the keycols
				t.keyCols = append(t.keyCols, i)
			}
		}
		hiddenColSeq := 0
		for _, pkCol := range tableInfo.PrimaryKeyCols {
			_, ok := colsMap[pkCol]
			if !ok {
				// The primary key col is not in the selected cols, we need to add it as a hidden column
				// FIXME - might be able to get duplicates if have chain of executors using same generated column names?
				colName := fmt.Sprintf("__gen_hid_id%d", hiddenColSeq)
				hiddenColSeq++
				colType := tableInfo.ColumnTypes[pkCol]
				colIndex := len(t.colNames)
				t.colNames = append(t.colNames, colName)
				t.colTypes = append(t.colTypes, colType)
				t.keyCols = append(t.keyCols, colIndex)
				t.colsVisible = append(t.colsVisible, false)
				t.cols = append(t.cols, pkCol)
			}
		}
		t.rowsFactory = common.NewRowsFactory(t.colTypes)
	}
}

func (t *Scan) ReCalcSchemaFromChildren() error {
	// NOOP
	return nil
}

func (t *Scan) HandleRows(rowsBatch RowsBatch, ctx *ExecutionContext) error {
	numRows := rowsBatch.Len()
	if t.cols != nil {
		newRows := t.rowsFactory.NewRows(numRows)
		entries := make([]RowsEntry, numRows)
		// Convert the previous and current rows into the required columns
		rc := 0
		for i := 0; i < numRows; i++ {
			pi := -1
			prevRow := rowsBatch.PreviousRow(i)
			if prevRow != nil {
				if err := t.appendResultRow(prevRow, newRows); err != nil {
					return err
				}
				pi = rc
				rc++
			}
			ci := -1
			currRow := rowsBatch.CurrentRow(i)
			if currRow != nil {
				if err := t.appendResultRow(currRow, newRows); err != nil {
					return err
				}
				ci = rc
				rc++
			}
			entries[i] = NewRowsEntry(pi, ci)
		}
		return t.parent.HandleRows(NewRowsBatch(newRows, entries), ctx)
	}
	return t.parent.HandleRows(rowsBatch, ctx)
}

func (t *Scan) appendResultRow(row *common.Row, outRows *common.Rows) error {
	for i, incomingColIndex := range t.cols {
		if row.IsNull(incomingColIndex) {
			outRows.AppendNullToColumn(i)
		} else {
			colType := t.colTypes[i]
			switch colType.Type {
			case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
				val := row.GetInt64(incomingColIndex)
				outRows.AppendInt64ToColumn(i, val)
			case common.TypeDouble:
				val := row.GetFloat64(incomingColIndex)
				outRows.AppendFloat64ToColumn(i, val)
			case common.TypeVarchar:
				val := row.GetString(incomingColIndex)
				outRows.AppendStringToColumn(i, val)
			case common.TypeDecimal:
				val := row.GetDecimal(incomingColIndex)
				outRows.AppendDecimalToColumn(i, val)
			case common.TypeTimestamp:
				val := row.GetTimestamp(incomingColIndex)
				outRows.AppendTimestampToColumn(i, val)
			default:
				return errors.Errorf("unexpected column type %v", colType)
			}
		}
	}
	return nil
}
