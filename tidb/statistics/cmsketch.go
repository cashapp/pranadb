//
// This source code is a modified form of original source from the TiDB project, which has the following copyright header(s):
//

// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"

	"github.com/squareup/pranadb/tidb/sessionctx"

	"github.com/cznic/mathutil"
	"github.com/cznic/sortutil"
	"github.com/pingcap/failpoint"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/tidb/types"
	"github.com/twmb/murmur3"
)

// CMSketch is used to estimate point queries.
// Refer: https://en.wikipedia.org/wiki/Count-min_sketch
type CMSketch struct {
	depth        int32
	width        int32
	count        uint64 // TopN is not counted in count
	defaultValue uint64 // In sampled data, if cmsketch returns a small value (less than avg value / 2), then this will returned.
	table        [][]uint32
}

// MemoryUsage returns the total memory usage of a CMSketch.
// only calc the hashtable size(CMSketch.table) and the CMSketch.topN
// data are not tracked because size of CMSketch.topN take little influence
// We ignore the size of other metadata in CMSketch.
func (c *CMSketch) MemoryUsage() (sum int64) {
	sum = int64(c.depth * c.width * 4)
	return
}

// queryAddTopN TopN adds count to CMSketch.topN if exists, and returns the count of such elements after insert.
// If such elements does not in topn elements, nothing will happen and false will be returned.
func (c *TopN) updateTopNWithDelta(d []byte, delta uint64, increase bool) bool {
	if c == nil || c.TopN == nil {
		return false
	}
	idx := c.findTopN(d)
	if idx >= 0 {
		if increase {
			c.TopN[idx].Count += delta
		} else {
			c.TopN[idx].Count -= delta
		}
		return true
	}
	return false
}

// InsertBytes inserts the bytes value into the CM Sketch.
func (c *CMSketch) InsertBytes(bytes []byte) {
	c.InsertBytesByCount(bytes, 1)
}

// InsertBytesByCount adds the bytes value into the TopN (if value already in TopN) or CM Sketch by delta, this does not updates c.defaultValue.
func (c *CMSketch) InsertBytesByCount(bytes []byte, count uint64) {
	h1, h2 := murmur3.Sum128(bytes)
	c.count += count
	for i := range c.table {
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		c.table[i][j] += uint32(count)
	}
}

func (c *CMSketch) considerDefVal(cnt uint64) bool {
	return (cnt == 0 || (cnt > c.defaultValue && cnt < 2*(c.count/uint64(c.width)))) && c.defaultValue > 0
}

// setValue sets the count for value that hashed into (h1, h2), and update defaultValue if necessary.
func (c *CMSketch) setValue(h1, h2 uint64, count uint64) {
	oriCount := c.queryHashValue(h1, h2)
	if c.considerDefVal(oriCount) {
		// We should update c.defaultValue if we used c.defaultValue when getting the estimate count.
		// This should make estimation better, remove this line if it does not work as expected.
		c.defaultValue = uint64(float64(c.defaultValue)*0.95 + float64(c.defaultValue)*0.05)
		if c.defaultValue == 0 {
			// c.defaultValue never guess 0 since we are using a sampled data.
			c.defaultValue = 1
		}
	}

	c.count += count - oriCount
	// let it overflow naturally
	deltaCount := uint32(count) - uint32(oriCount)
	for i := range c.table {
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		c.table[i][j] = c.table[i][j] + deltaCount
	}
}

// SubValue remove a value from the CMSketch.
func (c *CMSketch) SubValue(h1, h2 uint64, count uint64) {
	c.count -= count
	for i := range c.table {
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		c.table[i][j] = c.table[i][j] - uint32(count)
	}
}

// QueryBytes is used to query the count of specified bytes.
func (c *CMSketch) QueryBytes(d []byte) uint64 {
	failpoint.Inject("mockQueryBytesMaxUint64", func(val failpoint.Value) {
		failpoint.Return(uint64(val.(int)))
	})
	h1, h2 := murmur3.Sum128(d)
	return c.queryHashValue(h1, h2)
}

func (c *CMSketch) queryHashValue(h1, h2 uint64) uint64 {
	vals := make([]uint32, c.depth)
	min := uint32(math.MaxUint32)
	// We want that when res is 0 before the noise is eliminated, the default value is not used.
	// So we need a temp value to distinguish before and after eliminating noise.
	temp := uint32(1)
	for i := range c.table {
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		if min > c.table[i][j] {
			min = c.table[i][j]
		}
		noise := (c.count - uint64(c.table[i][j])) / (uint64(c.width) - 1)
		if uint64(c.table[i][j]) == 0 {
			vals[i] = 0
		} else if uint64(c.table[i][j]) < noise {
			vals[i] = temp
		} else {
			vals[i] = c.table[i][j] - uint32(noise) + temp
		}
	}
	sort.Sort(sortutil.Uint32Slice(vals))
	res := vals[(c.depth-1)/2] + (vals[c.depth/2]-vals[(c.depth-1)/2])/2
	if res > min+temp {
		res = min + temp
	}
	if res == 0 {
		return uint64(0)
	}
	res = res - temp
	if c.considerDefVal(uint64(res)) {
		return c.defaultValue
	}
	return uint64(res)
}

// MergeCMSketch merges two CM Sketch.
func (c *CMSketch) MergeCMSketch(rc *CMSketch) error {
	if c == nil || rc == nil {
		return nil
	}
	if c.depth != rc.depth || c.width != rc.width {
		return errors.Error("Dimensions of Count-Min Sketch should be the same")
	}
	c.count += rc.count
	for i := range c.table {
		for j := range c.table[i] {
			c.table[i][j] += rc.table[i][j]
		}
	}
	return nil
}

// MergeCMSketch4IncrementalAnalyze merges two CM Sketch for incremental analyze. Since there is no value
// that appears partially in `c` and `rc` for incremental analyze, it uses `max` to merge them.
// Here is a simple proof: when we query from the CM sketch, we use the `min` to get the answer:
//   (1): For values that only appears in `c, using `max` to merge them affects the `min` query result less than using `sum`;
//   (2): For values that only appears in `rc`, it is the same as condition (1);
//   (3): For values that appears both in `c` and `rc`, if they do not appear partially in `c` and `rc`, for example,
//        if `v` appears 5 times in the table, it can appears 5 times in `c` and 3 times in `rc`, then `max` also gives the correct answer.
// So in fact, if we can know the number of appearances of each value in the first place, it is better to use `max` to construct the CM sketch rather than `sum`.
func (c *CMSketch) MergeCMSketch4IncrementalAnalyze(rc *CMSketch, numTopN uint32) error {
	if c.depth != rc.depth || c.width != rc.width {
		return errors.Error("Dimensions of Count-Min Sketch should be the same")
	}
	for i := range c.table {
		c.count = 0
		for j := range c.table[i] {
			c.table[i][j] = mathutil.MaxUint32(c.table[i][j], rc.table[i][j])
			c.count += uint64(c.table[i][j])
		}
	}
	return nil
}

// TotalCount returns the total count in the sketch, it is only used for test.
func (c *CMSketch) TotalCount() uint64 {
	return c.count
}

// Equal tests if two CM Sketch equal, it is only used for test.
func (c *CMSketch) Equal(rc *CMSketch) bool {
	return reflect.DeepEqual(c, rc)
}

// Copy makes a copy for current CMSketch.
func (c *CMSketch) Copy() *CMSketch {
	if c == nil {
		return nil
	}
	tbl := make([][]uint32, c.depth)
	for i := range tbl {
		tbl[i] = make([]uint32, c.width)
		copy(tbl[i], c.table[i])
	}
	return &CMSketch{count: c.count, width: c.width, depth: c.depth, table: tbl, defaultValue: c.defaultValue}
}

// AppendTopN appends a topn into the TopN struct.
func (c *TopN) AppendTopN(data []byte, count uint64) {
	c.TopN = append(c.TopN, TopNMeta{data, count})
}

// GetWidthAndDepth returns the width and depth of CM Sketch.
func (c *CMSketch) GetWidthAndDepth() (int32, int32) {
	return c.width, c.depth
}

// CalcDefaultValForAnalyze calculate the default value for Analyze.
// The value of it is count / NDV in CMSketch. This means count and NDV are not include topN.
func (c *CMSketch) CalcDefaultValForAnalyze(NDV uint64) {
	c.defaultValue = c.count / mathutil.MaxUint64(1, NDV)
}

// TopN stores most-common values, which is used to estimate point queries.
type TopN struct {
	TopN []TopNMeta
}

func (c *TopN) String() string {
	if c == nil {
		return "EmptyTopN"
	}
	builder := &strings.Builder{}
	fmt.Fprintf(builder, "TopN{length: %v, ", len(c.TopN))
	fmt.Fprint(builder, "[")
	for i := 0; i < len(c.TopN); i++ {
		fmt.Fprintf(builder, "(%v, %v)", c.TopN[i].Encoded, c.TopN[i].Count)
		if i+1 != len(c.TopN) {
			fmt.Fprint(builder, ", ")
		}
	}
	fmt.Fprint(builder, "]")
	fmt.Fprint(builder, "}")
	return builder.String()
}

// Num returns the ndv of the TopN.
//   TopN is declared directly in Histogram. So the Len is occupied by the Histogram. We use Num instead.
func (c *TopN) Num() int {
	if c == nil {
		return 0
	}
	return len(c.TopN)
}

// outOfRange checks whether the the given value falls back in [TopN.LowestOne, TopN.HighestOne].
func (c *TopN) outOfRange(val []byte) bool {
	if c == nil || len(c.TopN) == 0 {
		return true
	}
	return bytes.Compare(c.TopN[0].Encoded, val) > 0 || bytes.Compare(val, c.TopN[c.Num()-1].Encoded) > 0
}

// DecodedString returns the value with decoded result.
func (c *TopN) DecodedString(ctx sessionctx.Context, colTypes []byte) (string, error) {
	builder := &strings.Builder{}
	fmt.Fprintf(builder, "TopN{length: %v, ", len(c.TopN))
	fmt.Fprint(builder, "[")
	var tmpDatum types.Datum
	for i := 0; i < len(c.TopN); i++ {
		tmpDatum.SetBytes(c.TopN[i].Encoded)
		valStr, err := ValueToString(ctx.GetSessionVars(), &tmpDatum, len(colTypes), colTypes)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(builder, "(%v, %v)", valStr, c.TopN[i].Count)
		if i+1 != len(c.TopN) {
			fmt.Fprint(builder, ", ")
		}
	}
	fmt.Fprint(builder, "]")
	fmt.Fprint(builder, "}")
	return builder.String(), nil
}

// Copy makes a copy for current TopN.
func (c *TopN) Copy() *TopN {
	if c == nil {
		return nil
	}
	topN := make([]TopNMeta, len(c.TopN))
	for i, t := range c.TopN {
		topN[i].Encoded = make([]byte, len(t.Encoded))
		copy(topN[i].Encoded, t.Encoded)
		topN[i].Count = t.Count
	}
	return &TopN{
		TopN: topN,
	}
}

// TopNMeta stores the unit of the TopN.
type TopNMeta struct {
	Encoded []byte
	Count   uint64
}

// QueryTopN returns the results for (h1, h2) in murmur3.Sum128(), if not exists, return (0, false).
func (c *TopN) QueryTopN(d []byte) (uint64, bool) {
	if c == nil {
		return 0, false
	}
	idx := c.findTopN(d)
	if idx < 0 {
		return 0, false
	}
	return c.TopN[idx].Count, true
}

func (c *TopN) findTopN(d []byte) int {
	if c == nil {
		return -1
	}
	match := false
	idx := sort.Search(len(c.TopN), func(i int) bool {
		cmp := bytes.Compare(c.TopN[i].Encoded, d)
		if cmp == 0 {
			match = true
		}
		return cmp >= 0
	})
	if !match {
		return -1
	}
	return idx
}

// LowerBound searches on the sorted top-n items,
// returns the smallest index i such that the value at element i is not less than `d`.
func (c *TopN) LowerBound(d []byte) (idx int, match bool) {
	if c == nil {
		return 0, false
	}
	idx = sort.Search(len(c.TopN), func(i int) bool {
		cmp := bytes.Compare(c.TopN[i].Encoded, d)
		if cmp == 0 {
			match = true
		}
		return cmp >= 0
	})
	return idx, match
}

// BetweenCount estimates the row count for interval [l, r).
func (c *TopN) BetweenCount(l, r []byte) uint64 {
	if c == nil {
		return 0
	}
	lIdx, _ := c.LowerBound(l)
	rIdx, _ := c.LowerBound(r)
	ret := uint64(0)
	for i := lIdx; i < rIdx; i++ {
		ret += c.TopN[i].Count
	}
	return ret
}

// Sort sorts the topn items.
func (c *TopN) Sort() {
	if c == nil {
		return
	}
	sort.Slice(c.TopN, func(i, j int) bool {
		return bytes.Compare(c.TopN[i].Encoded, c.TopN[j].Encoded) < 0
	})
}

// TotalCount returns how many data is stored in TopN.
func (c *TopN) TotalCount() uint64 {
	if c == nil {
		return 0
	}
	total := uint64(0)
	for _, t := range c.TopN {
		total += t.Count
	}
	return total
}

// Equal checks whether the two TopN are equal.
func (c *TopN) Equal(cc *TopN) bool {
	if c.TotalCount() == 0 && cc.TotalCount() == 0 {
		return true
	} else if c.TotalCount() != cc.TotalCount() {
		return false
	}
	if len(c.TopN) != len(cc.TopN) {
		return false
	}
	for i := range c.TopN {
		if !bytes.Equal(c.TopN[i].Encoded, cc.TopN[i].Encoded) {
			return false
		}
		if c.TopN[i].Count != cc.TopN[i].Count {
			return false
		}
	}
	return true
}

// RemoveVal remove the val from TopN if it exists.
func (c *TopN) RemoveVal(val []byte) {
	if c == nil {
		return
	}
	pos := c.findTopN(val)
	if pos == -1 {
		return
	}
	c.TopN = append(c.TopN[:pos], c.TopN[pos+1:]...)
}
