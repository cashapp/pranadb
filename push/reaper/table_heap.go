package reaper

import (
	"container/heap"
	"github.com/squareup/pranadb/common"
)

type itemHolder struct {
	CheckTime int64
	Table     *common.TableInfo
}

type inner struct {
	elems []itemHolder
}

type tableHeap struct {
	in inner
}

func (h *inner) Len() int {
	return len(h.elems)
}
func (h *inner) Less(i, j int) bool {
	return h.elems[i].CheckTime < h.elems[j].CheckTime
}

func (h inner) Swap(i, j int) {
	h.elems[i], h.elems[j] = h.elems[j], h.elems[i]
}

func (h *inner) Push(x interface{}) {
	h.elems = append(h.elems, x.(itemHolder))
}

func (h *inner) Pop() interface{} {
	n := len(h.elems)
	ret := h.elems[n-1]
	h.elems = h.elems[:n-1]
	return ret
}

func (h *tableHeap) peek() *itemHolder {
	if len(h.in.elems) == 0 {
		return nil
	}
	copied := h.in.elems[0]
	return &copied
}

func (h *tableHeap) push(holder itemHolder) {
	heap.Push(&h.in, holder)
}

func (h *tableHeap) pop() itemHolder {
	return heap.Pop(&h.in).(itemHolder)
}

func (h *tableHeap) elems() []itemHolder {
	return h.in.elems
}

func (h *tableHeap) len() int {
	return h.in.Len()
}
