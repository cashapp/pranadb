package exec

import (
	"fmt"
	"sort"
	"strings"

	"github.com/squareup/pranadb/common"
)

// PullSort - a simple in memory sort executor
// TODO this won't work for large resultsets - we need to store in chunks on disk and use a multi-way merge sort in
// that case
type PullSort struct {
	pullExecutorBase
	sortByExpressions []*common.Expression
	rows              *common.Rows
	descending        []bool
}

func NewPullSort(colNames []string, colTypes []common.ColumnType, desc []bool, sortByExpressions []*common.Expression) *PullSort {
	rf := common.NewRowsFactory(colTypes)
	base := pullExecutorBase{
		colNames:       colNames,
		colTypes:       colTypes,
		simpleColNames: common.ToSimpleColNames(colNames),
		rowsFactory:    rf,
	}
	return &PullSort{
		pullExecutorBase:  base,
		sortByExpressions: sortByExpressions,
		rows:              rf.NewRows(1000),
		descending:        desc,
	}
}

func (p PullSort) GetRows(limit int) (*common.Rows, error) { //nolint: gocyclo
	if limit < 1 {
		return nil, fmt.Errorf("invalid limit %d", limit)
	}

	for {
		// We call getRows on the child until there are no more rows to get
		batch, err := p.GetChildren()[0].GetRows(limit)
		if err != nil {
			return nil, err
		}
		rc := batch.RowCount()
		if rc == 0 {
			break
		}
		if p.rows.RowCount()+rc > OrderByMaxRows {
			return nil, fmt.Errorf("query with order by cannot return more than %d rows", OrderByMaxRows)
		}
		p.rows.AppendAll(batch)
		if rc < limit {
			break
		}
	}

	numRows := p.rows.RowCount()
	if numRows == 0 {
		return p.rows, nil
	}

	indexes := make([]int, numRows)
	for i := range indexes {
		indexes[i] = i
	}

	var err error

	sort.SliceStable(indexes, func(i, j int) bool {
		if err != nil {
			return false
		}
		row1 := p.rows.GetRow(indexes[i])
		row2 := p.rows.GetRow(indexes[j])
		for sortColIndex, sortbyExpr := range p.sortByExpressions {
			descending := p.descending[sortColIndex]
			colType, errType := sortbyExpr.ReturnType(p.colTypes)
			if errType != nil {
				err = errType
				return false
			}
			switch colType.Type {
			case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
				val1, null1, err1 := sortbyExpr.EvalInt64(&row1)
				if err1 != nil {
					err = err1
					return false
				}
				val2, null2, err2 := sortbyExpr.EvalInt64(&row2)
				if err2 != nil {
					err = err2
					return false
				}
				if null1 && !null2 {
					return !descending
				}
				if null2 && !null1 {
					return descending
				}
				if !null1 && !null2 {
					if val1 < val2 {
						return !descending
					}
					if val2 < val1 {
						return descending
					}
				}
			case common.TypeDouble:
				val1, null1, err1 := sortbyExpr.EvalFloat64(&row1)
				if err1 != nil {
					err = err1
					return false
				}
				val2, null2, err2 := sortbyExpr.EvalFloat64(&row2)
				if err2 != nil {
					err = err2
					return false
				}
				if null1 && !null2 {
					return !descending
				}
				if null2 && !null1 {
					return descending
				}
				if !null1 && !null2 {
					if val1 < val2 {
						return !descending
					}
					if val2 < val1 {
						return descending
					}
				}
			case common.TypeDecimal:
				val1, null1, err1 := sortbyExpr.EvalDecimal(&row1)
				if err1 != nil {
					err = err1
					return false
				}
				val2, null2, err2 := sortbyExpr.EvalDecimal(&row2)
				if err2 != nil {
					err = err2
					return false
				}
				if null1 && !null2 {
					return !descending
				}
				if null2 && !null1 {
					return descending
				}
				if !null1 && !null2 {
					diff := val1.CompareTo(&val2)
					if diff < 0 {
						return !descending
					}
					if diff > 0 {
						return descending
					}
				}
			case common.TypeVarchar:
				val1, null1, err1 := sortbyExpr.EvalString(&row1)
				if err1 != nil {
					err = err1
					return false
				}
				val2, null2, err2 := sortbyExpr.EvalString(&row2)
				if err2 != nil {
					err = err2
					return false
				}
				if null1 && !null2 {
					return !descending
				}
				if null2 && !null1 {
					return descending
				}
				if !null1 && !null2 {
					diff := strings.Compare(val1, val2)
					if diff < 0 {
						return !descending
					}
					if diff > 0 {
						return descending
					}
				}
			case common.TypeTimestamp:
				val1, null1, err1 := sortbyExpr.EvalTimestamp(&row1)
				if err1 != nil {
					err = err1
					return false
				}
				val2, null2, err2 := sortbyExpr.EvalTimestamp(&row2)
				if err2 != nil {
					err = err2
					return false
				}
				if null1 && !null2 {
					return !descending
				}
				if null2 && !null1 {
					return descending
				}
				if !null1 && !null2 {
					diff := val1.Compare(val2)
					if diff < 0 {
						return !descending
					}
					if diff > 0 {
						return descending
					}
				}
			default:
				panic(fmt.Sprintf("unexpected type %d", colType.Type))
			}
			if err != nil {
				break
			}
		}
		return false
	})

	if err != nil {
		return nil, err
	}

	res := p.rowsFactory.NewRows(numRows)
	for _, index := range indexes {
		res.AppendRow(p.rows.GetRow(index))
	}

	return res, nil
}
