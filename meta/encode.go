package meta

import (
	"encoding/json"

	"github.com/squareup/pranadb/common"
)

var sourceInfoRowFactory = common.NewRowsFactory(SchemaTableInfo.ColumnTypes)

// EncodeSourceInfoToRow encodes a common.SourceInfo into a database row.
func EncodeSourceInfoToRow(info *common.SourceInfo) *common.Row {
	rows := sourceInfoRowFactory.NewRows(1)
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
		SchemaName: row.GetString(2),
		Name:       row.GetString(3),
	}
	jsonDecode(row.GetString(4), info.TableInfo)
	jsonDecode(row.GetString(5), info.TopicInfo)
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
