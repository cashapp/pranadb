package meta

import (
	"encoding/json"

	"github.com/squareup/pranadb/common"
)

var tableInfoRowsFactory = common.NewRowsFactory(SchemaTableInfo.ColumnTypes)

const (
	TableKindSource           = "source"
	TableKindMaterializedView = "materialized_view"
)

// EncodeSourceInfoToRow encodes a common.SourceInfo into a database row.
func EncodeSourceInfoToRow(info *common.SourceInfo) *common.Row {
	rows := tableInfoRowsFactory.NewRows(1)
	rows.AppendInt64ToColumn(0, int64(info.TableInfo.ID))
	rows.AppendStringToColumn(1, "source")
	rows.AppendStringToColumn(2, info.SchemaName)
	rows.AppendStringToColumn(3, info.Name)
	rows.AppendStringToColumn(4, jsonEncode(info.TableInfo))
	rows.AppendStringToColumn(5, jsonEncode(info.TopicInfo))
	rows.AppendNullToColumn(6)
	row := rows.GetRow(0)
	return &row
}

// DecodeSourceInfoRow decodes a database row into a common.SourceInfo.
func DecodeSourceInfoRow(row *common.Row) *common.SourceInfo {
	info := common.SourceInfo{
		TableInfo: &common.TableInfo{
			SchemaName: row.GetString(2),
			Name:       row.GetString(3),
		},
	}
	jsonDecode(row.GetString(4), &info.TableInfo)
	jsonDecode(row.GetString(5), &info.TopicInfo)
	return &info
}

// EncodeMaterializedViewInfoToRow encodes a common.MaterializedViewInfo into a database row.
func EncodeMaterializedViewInfoToRow(info *common.MaterializedViewInfo) *common.Row {
	rows := tableInfoRowsFactory.NewRows(1)
	rows.AppendInt64ToColumn(0, int64(info.TableInfo.ID))
	rows.AppendStringToColumn(1, "materialized_view")
	rows.AppendStringToColumn(2, info.SchemaName)
	rows.AppendStringToColumn(3, info.Name)
	rows.AppendStringToColumn(4, jsonEncode(info.TableInfo))
	rows.AppendNullToColumn(5)
	rows.AppendStringToColumn(6, info.Query)
	row := rows.GetRow(0)
	return &row
}

// DecodeMaterializedViewInfoRow decodes a database row into a common.MaterializedViewInfo.
func DecodeMaterializedViewInfoRow(row *common.Row) *common.MaterializedViewInfo {
	info := common.MaterializedViewInfo{
		TableInfo: &common.TableInfo{
			SchemaName: row.GetString(2),
			Name:       row.GetString(3),
		},
		Query: row.GetString(6),
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
