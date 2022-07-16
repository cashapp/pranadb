package exec

import (
	"github.com/squareup/pranadb/common"
	"strings"
)

type RowsBatch struct {
	rows    *common.Rows
	entries []RowsEntry
}

type RowsEntry struct {
	prevIndex     int
	currIndex     int
	receiverIndex int64
}

func NewCurrentRowsBatch(currentRows *common.Rows) RowsBatch {
	return RowsBatch{rows: currentRows}
}

func NewRowsBatch(rows *common.Rows, entries []RowsEntry) RowsBatch {
	return RowsBatch{rows: rows, entries: entries}
}

func NewRowsEntry(prevIndex int, currIndex int, receiverIndex int64) RowsEntry {
	return RowsEntry{prevIndex: prevIndex, currIndex: currIndex, receiverIndex: receiverIndex}
}

func (r *RowsBatch) AppendEntry(rowPrev *common.Row, rowCurr *common.Row, receiverIndex int64) {
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
	r.entries = append(r.entries, RowsEntry{prevIndex: pi, currIndex: ci, receiverIndex: receiverIndex})
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

func (r *RowsBatch) ReceiverIndex(index int) int64 {
	if r.entries == nil {
		// This only happens when there's no duplicate detection so it's ok
		return 0
	}
	return r.entries[index].receiverIndex
}

func (r *RowsBatch) Rows() *common.Rows {
	return r.rows
}

func (r *RowsBatch) String() string {
	sb := strings.Builder{}
	l := r.Len()
	for i := 0; i < l; i++ {
		prev := r.PreviousRow(i)
		sb.WriteString("prev:")
		if prev == nil {
			sb.WriteString("nil")
		} else {
			sb.WriteString(prev.String())
		}
		curr := r.CurrentRow(i)
		sb.WriteString(" curr:")
		if curr == nil {
			sb.WriteString("nil")
		} else {
			sb.WriteString(curr.String())
		}
		if i != l-1 {
			sb.WriteString("\n")
		}
	}
	return sb.String()
}
