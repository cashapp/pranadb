package sql

import "unsafe"

// Rows is a list of Rows, represented in column format
// Column format is more efficient for processing in executors as we can benefit from
// vectorisation
// We use for both push and pull queries - for push queries we can often batch the processing
// of rows so we're not always processing just one
type Rows struct {
	ColCount int
	Columns []Column
}

type Column struct {
	count  int
	stride int
	values []byte
}

func (c *Column) GetUint64(index int) uint64 {
	return *(*uint64)(unsafe.Pointer(&c.values[index * 8]))
}

// etc




