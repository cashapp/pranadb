//
// This source code is a modified form of original source from the TiDB project, which has the following copyright header(s):
//

// Copyright 2018 PingCAP, Inc.
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

package property

import (
	"fmt"

	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/util/codec"
)

// SortItem wraps the column and its order.
type SortItem struct {
	Col  *expression.Column
	Desc bool
}

// PhysicalProperty stands for the required physical property by parents.
// It contains the orders and the task types.
type PhysicalProperty struct {
	// SortItems contains the required sort attributes.
	SortItems []SortItem

	// ExpectedCnt means this operator may be closed after fetching ExpectedCnt
	// records.
	ExpectedCnt float64

	// hashcode stores the hash code of a PhysicalProperty, will be lazily
	// calculated when function "HashCode()" being called.
	hashcode []byte
}

// SortItemsFromCols builds property items from columns.
func SortItemsFromCols(cols []*expression.Column, desc bool) []SortItem {
	items := make([]SortItem, 0, len(cols))
	for _, col := range cols {
		items = append(items, SortItem{Col: col, Desc: desc})
	}
	return items
}

// IsEmpty checks whether the order property is empty.
func (p *PhysicalProperty) IsEmpty() bool {
	return len(p.SortItems) == 0
}

// HashCode calculates hash code for a PhysicalProperty object.
func (p *PhysicalProperty) HashCode() []byte {
	if p.hashcode != nil {
		return p.hashcode
	}
	hashcodeSize := 8 + 8 + 8 + (16+8)*len(p.SortItems) + 8
	p.hashcode = make([]byte, 0, hashcodeSize)
	p.hashcode = codec.EncodeFloat(p.hashcode, p.ExpectedCnt)
	for _, item := range p.SortItems {
		p.hashcode = append(p.hashcode, item.Col.HashCode(nil)...)
		if item.Desc {
			p.hashcode = codec.EncodeInt(p.hashcode, 1)
		} else {
			p.hashcode = codec.EncodeInt(p.hashcode, 0)
		}
	}
	return p.hashcode
}

// String implements fmt.Stringer interface. Just for test.
func (p *PhysicalProperty) String() string {
	return fmt.Sprintf("Prop{cols: %v, expectedCount: %v}", p.SortItems, p.ExpectedCnt)
}

// CloneEssentialFields returns a copy of PhysicalProperty. We only copy the essential fields that really indicate the
// property, specifically, `CanAddEnforcer` should not be included.
func (p *PhysicalProperty) CloneEssentialFields() *PhysicalProperty {
	prop := &PhysicalProperty{
		SortItems:   p.SortItems,
		ExpectedCnt: p.ExpectedCnt,
	}
	return prop
}

// AllSameOrder checks if all the items have same order.
func (p *PhysicalProperty) AllSameOrder() (bool, bool) {
	if len(p.SortItems) == 0 {
		return true, false
	}
	for i := 1; i < len(p.SortItems); i++ {
		if p.SortItems[i].Desc != p.SortItems[i-1].Desc {
			return false, false
		}
	}
	return true, p.SortItems[0].Desc
}
