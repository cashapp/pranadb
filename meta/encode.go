package meta

import (
	"encoding/json"

	"github.com/squareup/pranadb/common"
)

var tableInfoRowsFactory = common.NewRowsFactory(TableDefTableInfo.ColumnTypes)
var indexInfoRowsFactory = common.NewRowsFactory(IndexDefTableInfo.ColumnTypes)

const (
	TableKindSource           = "source"
	TableKindMaterializedView = "materialized_view"
	TableKindInternal         = "internal"
)

// EncodeIndexInfoToRow encodes a common.IndexInfo into a database row.
func EncodeIndexInfoToRow(info *common.IndexInfo) *common.Row {
	rows := indexInfoRowsFactory.NewRows(1)
	rows.AppendInt64ToColumn(0, int64(info.ID))
	rows.AppendStringToColumn(1, info.SchemaName)
	rows.AppendStringToColumn(2, info.Name)
	rows.AppendStringToColumn(3, jsonEncode(info))
	rows.AppendStringToColumn(4, info.TableName)
	row := rows.GetRow(0)
	return &row
}

// DecodeIndexInfoRow decodes a database row into a common.IndexInfo.
func DecodeIndexInfoRow(row *common.Row) *common.IndexInfo {
	info := common.IndexInfo{}
	jsonDecode(row.GetString(3), &info)
	return &info
}

// EncodeSourceInfoToRow encodes a common.SourceInfo into a database row.
func EncodeSourceInfoToRow(info *common.SourceInfo) *common.Row {
	rows := tableInfoRowsFactory.NewRows(1)
	rows.AppendInt64ToColumn(0, int64(info.TableInfo.ID))
	rows.AppendStringToColumn(1, TableKindSource)
	rows.AppendStringToColumn(2, info.SchemaName)
	rows.AppendStringToColumn(3, info.Name)
	rows.AppendStringToColumn(4, jsonEncode(info.TableInfo))
	rows.AppendStringToColumn(5, jsonEncode(info.OriginInfo))
	rows.AppendNullToColumn(6)
	rows.AppendNullToColumn(7)
	row := rows.GetRow(0)
	return &row
}

// DecodeSourceInfoRow decodes a database row into a common.SourceInfo.
func DecodeSourceInfoRow(row *common.Row) *common.SourceInfo {
	info := common.SourceInfo{}
	jsonDecode(row.GetString(4), &info.TableInfo)
	jsonDecode(row.GetString(5), &info.OriginInfo)
	return &info
}

// EncodeMaterializedViewInfoToRow encodes a common.MaterializedViewInfo into a database row.
func EncodeMaterializedViewInfoToRow(info *common.MaterializedViewInfo) *common.Row {
	rows := tableInfoRowsFactory.NewRows(1)
	rows.AppendInt64ToColumn(0, int64(info.TableInfo.ID))
	rows.AppendStringToColumn(1, TableKindMaterializedView)
	rows.AppendStringToColumn(2, info.SchemaName)
	rows.AppendStringToColumn(3, info.Name)
	rows.AppendStringToColumn(4, jsonEncode(info.TableInfo))
	rows.AppendNullToColumn(5)
	rows.AppendStringToColumn(6, info.Query)
	rows.AppendNullToColumn(7)
	row := rows.GetRow(0)
	return &row
}

// DecodeMaterializedViewInfoRow decodes a database row into a common.MaterializedViewInfo.
func DecodeMaterializedViewInfoRow(row *common.Row) *common.MaterializedViewInfo {
	info := common.MaterializedViewInfo{
		Query:      row.GetString(6),
		OriginInfo: &common.MaterializedViewOriginInfo{},
	}
	jsonDecode(row.GetString(4), &info.TableInfo)
	return &info
}

// EncodeInternalTableInfoToRow encodes a common.InternalTableInfo into a database row.
func EncodeInternalTableInfoToRow(info *common.InternalTableInfo) *common.Row {
	rows := tableInfoRowsFactory.NewRows(1)
	rows.AppendInt64ToColumn(0, int64(info.TableInfo.ID))
	rows.AppendStringToColumn(1, TableKindInternal)
	rows.AppendStringToColumn(2, info.SchemaName)
	rows.AppendStringToColumn(3, info.Name)
	rows.AppendStringToColumn(4, jsonEncode(info.TableInfo))
	rows.AppendNullToColumn(5)
	rows.AppendNullToColumn(6)
	rows.AppendStringToColumn(7, info.MaterializedViewName)
	row := rows.GetRow(0)
	return &row
}

// DecodeInternalTableInfoRow decodes a database row into a common.InternalTableInfo.
func DecodeInternalTableInfoRow(row *common.Row) *common.InternalTableInfo {
	info := common.InternalTableInfo{
		MaterializedViewName: row.GetString(7),
	}
	jsonDecode(row.GetString(4), &info.TableInfo)
	return &info
}

func jsonEncode(v interface{}) string {
	s, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(s)
}

func jsonDecode(data string, v interface{}) {
	if err := json.Unmarshal([]byte(data), v); err != nil {
		panic(err)
	}
}
