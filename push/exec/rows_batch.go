package exec

import (
	"github.com/squareup/pranadb/common"
)

type RowsBatch struct {
	rows    *common.Rows
	entries []RowsEntry
}

type RowsEntry struct {
	prevIndex int
	currIndex int
}

func NewCurrentRowsBatch(currentRows *common.Rows) RowsBatch {
	return RowsBatch{rows: currentRows}
}

func NewRowsBatch(rows *common.Rows, entries []RowsEntry) RowsBatch {
	return RowsBatch{rows: rows, entries: entries}
}

func NewRowsEntry(prevIndex int, currIndex int) RowsEntry {
	return RowsEntry{prevIndex: prevIndex, currIndex: currIndex}
}

func (r *RowsBatch) AppendEntry(rowPrev *common.Row, rowCurr *common.Row) {
	pi := -1
	ci := -1
	if rowPrev != nil {
		r.rows.AppendRow(*rowPrev)
		pi = r.rows.RowCount() - 1
	}
	if rowCurr != nil {
		r.rows.AppendRow(*rowCurr)
		ci = r.rows.RowCount() - 1
	}
	r.entries = append(r.entries, RowsEntry{prevIndex: pi, currIndex: ci})
}

func (r *RowsBatch) Len() int {
	if r.entries == nil {
		return r.rows.RowCount()
	}
	return len(r.entries)
}

func (r *RowsBatch) PreviousRow(index int) *common.Row {
	if r.entries == nil {
		return nil
	}
	entry := r.entries[index]
	if entry.prevIndex == -1 {
		return nil
	}
	row := r.rows.GetRow(entry.prevIndex)
	return &row
}

func (r *RowsBatch) CurrentRow(index int) *common.Row {
	if r.entries == nil {
		row := r.rows.GetRow(index)
		return &row
	}
	entry := r.entries[index]
	if entry.currIndex == -1 {
		return nil
	}
	row := r.rows.GetRow(entry.currIndex)
	return &row
}

func (r *RowsBatch) Rows() *common.Rows {
	return r.rows
}
