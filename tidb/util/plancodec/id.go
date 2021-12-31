//
// This source code is a modified form of original source from the TiDB project, which has the following copyright header(s):
//

// Copyright 2019 PingCAP, Inc.
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

package plancodec

const (
	// TypeSel is the type of Selection.
	TypeSel = "Selection"
	// TypeSet is the type of Set.
	TypeSet = "Set"
	// TypeProj is the type of Projection.
	TypeProj = "Projection"
	// TypeAgg is the type of Aggregation.
	TypeAgg = "Aggregation"
	// TypeHashAgg is the type of HashAgg.
	TypeHashAgg = "HashAgg"
	// TypeJoin is the type of Join.
	TypeJoin = "Join"
	// TypeUnion is the type of Union.
	TypeUnion = "Union"
	// TypeTableScan is the type of TableScan.
	TypeTableScan = "TableScan"
	// TypeUnionScan is the type of UnionScan.
	TypeUnionScan = "UnionScan"
	// TypeIdxScan is the type of IndexScan.
	TypeIdxScan = "IndexScan"
	// TypeSort is the type of Sort.
	TypeSort = "Sort"
	// TypeTopN is the type of TopN.
	TypeTopN = "TopN"
	// TypeLimit is the type of Limit.
	TypeLimit = "Limit"
	// TypeHashJoin is the type of hash join.
	TypeHashJoin = "HashJoin"
	// TypeMergeJoin is the type of merge join.
	TypeMergeJoin = "MergeJoin"
	// TypeIndexJoin is the type of index look up join.
	TypeIndexJoin = "IndexJoin"
	// TypeIndexMergeJoin is the type of index look up merge join.
	TypeIndexMergeJoin = "IndexMergeJoin"
	// TypeIndexHashJoin is the type of index nested loop hash join.
	TypeIndexHashJoin = "IndexHashJoin"
	// TypeMaxOneRow is the type of MaxOneRow.
	TypeMaxOneRow = "MaxOneRow"
	// TypeExists is the type of Exists.
	TypeExists = "Exists"
	// TypeDual is the type of TableDual.
	TypeDual = "TableDual"
	// TypeTableReader is the type of TableReader.
	TypeTableReader = "TableReader"
	// TypeIndexReader is the type of IndexReader.
	TypeIndexReader = "IndexReader"
	// TypeIndexMerge is the type of IndexMergeReader
	TypeIndexMerge = "IndexMerge"
	// TypePointGet is the type of PointGetPlan.
	TypePointGet = "Point_Get"
	// TypeDataSource is the type of DataSource.
	TypeDataSource = "DataSource"
	// TypeTableFullScan is the type of TableFullScan.
	TypeTableFullScan = "TableFullScan"
	// TypeTableRangeScan is the type of TableRangeScan.
	TypeTableRangeScan = "TableRangeScan"
	// TypeTableRowIDScan is the type of TableRowIDScan.
	TypeTableRowIDScan = "TableRowIDScan"
	// TypeIndexFullScan is the type of IndexFullScan.
	TypeIndexFullScan = "IndexFullScan"
	// TypeIndexRangeScan is the type of IndexRangeScan.
	TypeIndexRangeScan = "IndexRangeScan"
)

// plan id.
// Attention: for compatibility of encode/decode plan, The plan id shouldn't be changed.
const (
	typeSelID            int = 1
	typeSetID            int = 2
	typeProjID           int = 3
	typeAggID            int = 4
	typeHashAggID        int = 6
	typeJoinID           int = 8
	typeUnionID          int = 9
	typeTableScanID      int = 10
	typeUnionScanID      int = 12
	typeIdxScanID        int = 13
	typeSortID           int = 14
	typeTopNID           int = 15
	typeLimitID          int = 16
	typeHashJoinID       int = 17
	typeMergeJoinID      int = 18
	typeIndexJoinID      int = 19
	typeIndexMergeJoinID int = 20
	typeIndexHashJoinID  int = 21
	typeMaxOneRowID      int = 23
	typeExistsID         int = 24
	typeDualID           int = 25
	typeTableReaderID    int = 31
	typeIndexReaderID    int = 32
	typeIndexMergeID     int = 35
	typePointGet         int = 36
	typeTableFullScan    int = 43
	typeTableRangeScan   int = 44
	typeTableRowIDScan   int = 45
	typeIndexFullScan    int = 46
	typeIndexRangeScan   int = 47
)

// TypeStringToPhysicalID converts the plan type string to plan id.
func TypeStringToPhysicalID(tp string) int {
	switch tp {
	case TypeSel:
		return typeSelID
	case TypeSet:
		return typeSetID
	case TypeProj:
		return typeProjID
	case TypeAgg:
		return typeAggID
	case TypeHashAgg:
		return typeHashAggID
	case TypeJoin:
		return typeJoinID
	case TypeUnion:
		return typeUnionID
	case TypeTableScan:
		return typeTableScanID
	case TypeUnionScan:
		return typeUnionScanID
	case TypeIdxScan:
		return typeIdxScanID
	case TypeSort:
		return typeSortID
	case TypeTopN:
		return typeTopNID
	case TypeLimit:
		return typeLimitID
	case TypeHashJoin:
		return typeHashJoinID
	case TypeMergeJoin:
		return typeMergeJoinID
	case TypeIndexJoin:
		return typeIndexJoinID
	case TypeIndexMergeJoin:
		return typeIndexMergeJoinID
	case TypeIndexHashJoin:
		return typeIndexHashJoinID
	case TypeMaxOneRow:
		return typeMaxOneRowID
	case TypeExists:
		return typeExistsID
	case TypeDual:
		return typeDualID
	case TypeTableReader:
		return typeTableReaderID
	case TypeIndexReader:
		return typeIndexReaderID
	case TypeIndexMerge:
		return typeIndexMergeID
	case TypePointGet:
		return typePointGet
	case TypeTableFullScan:
		return typeTableFullScan
	case TypeTableRangeScan:
		return typeTableRangeScan
	case TypeTableRowIDScan:
		return typeTableRowIDScan
	case TypeIndexFullScan:
		return typeIndexFullScan
	case TypeIndexRangeScan:
		return typeIndexRangeScan
	}
	// Should never reach here.
	return 0
}
