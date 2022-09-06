package reaper

import (
	"github.com/stretchr/testify/require"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestTableHeapOrder(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	th := &tableHeap{}
	numItems := 1000
	var allItems []itemHolder
	for i := 0; i < numItems; i++ {
		ct := rand.Int63n(math.MaxInt64)
		item := itemHolder{
			CheckTime: ct,
			Table:     nil,
		}
		allItems = append(allItems, item)
		th.push(item)
	}
	sort.Slice(allItems, func(i, j int) bool {
		return allItems[i].CheckTime < allItems[j].CheckTime
	})
	for i := 0; i < numItems; i++ {
		popped := th.pop()
		require.Equal(t, allItems[i].CheckTime, popped.CheckTime)
	}
}

func TestTableHeapPeek(t *testing.T) {
	th := &tableHeap{}
	require.Nil(t, th.peek())
	th.push(itemHolder{CheckTime: 5})
	th.push(itemHolder{CheckTime: 3})
	th.push(itemHolder{CheckTime: 7})
	require.Equal(t, int64(3), th.peek().CheckTime)
	require.Equal(t, int64(3), th.peek().CheckTime)
}

func TestTableHeapLen(t *testing.T) {
	th := &tableHeap{}
	require.Equal(t, 0, th.len())
	th.push(itemHolder{CheckTime: 5})
	require.Equal(t, 1, th.len())
	th.push(itemHolder{CheckTime: 3})
	require.Equal(t, 2, th.len())
	th.push(itemHolder{CheckTime: 7})
	require.Equal(t, 3, th.len())
	require.Equal(t, int64(3), th.peek().CheckTime)
	require.Equal(t, 3, th.len())
	th.pop()
	require.Equal(t, 2, th.len())
	th.pop()
	require.Equal(t, 1, th.len())
	th.pop()
	require.Equal(t, 0, th.len())
}
