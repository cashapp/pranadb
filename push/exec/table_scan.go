package exec

import (
	"fmt"
	"github.com/squareup/pranadb/common"
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

func (t *Scan) HandleRows(rows *common.Rows, ctx *ExecutionContext) error {
	if t.cols != nil {
		newRows := t.rowsFactory.NewRows(rows.RowCount())
		for i := 0; i < rows.RowCount(); i++ {
			incomingRow := rows.GetRow(i)
			for i, incomingColIndex := range t.cols {
				if incomingRow.IsNull(incomingColIndex) {
					newRows.AppendNullToColumn(i)
				} else {
					colType := t.colTypes[i]
					switch colType.Type {
					case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
						val := incomingRow.GetInt64(incomingColIndex)
						newRows.AppendInt64ToColumn(i, val)
					case common.TypeDouble:
						val := incomingRow.GetFloat64(incomingColIndex)
						newRows.AppendFloat64ToColumn(i, val)
					case common.TypeVarchar:
						val := incomingRow.GetString(incomingColIndex)
						newRows.AppendStringToColumn(i, val)
					case common.TypeDecimal:
						val := incomingRow.GetDecimal(incomingColIndex)
						newRows.AppendDecimalToColumn(i, val)
					case common.TypeTimestamp:
						val := incomingRow.GetTimestamp(incomingColIndex)
						newRows.AppendTimestampToColumn(i, val)
					default:
						return fmt.Errorf("unexpected column type %v", colType)
					}
				}
			}
		}
		return t.parent.HandleRows(newRows, ctx)
	}
	return t.parent.HandleRows(rows, ctx)
}
