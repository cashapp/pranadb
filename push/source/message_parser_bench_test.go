package source

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/kafka"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var messageParserJSON = setupMessageParserJSON()

func setupMessageParserJSON() *MessageParser {
	tableInfo := &common.TableInfo{
		ID:             0,
		SchemaName:     "test",
		Name:           "test_table",
		PrimaryKeyCols: []int{0},
		ColumnNames:    []string{"col0", "col1", "col2"},
		ColumnTypes:    []common.ColumnType{common.BigIntColumnType, common.DoubleColumnType, common.VarcharColumnType},
		IndexInfos:     nil,
	}
	colSelectors := []string{"k.F1", "v.F1", "v.F2"}
	topicInfo := &common.TopicInfo{
		BrokerName:    "test_broker",
		TopicName:     "test_topic",
		KeyEncoding:   common.EncodingProtobuf,
		ValueEncoding: common.EncodingProtobuf,
		ColSelectors:  colSelectors,
		Properties:    nil,
	}

	sourceInfo := &common.SourceInfo{
		TableInfo: tableInfo,
		TopicInfo: topicInfo,
	}
	mp, err := NewMessageParser(sourceInfo)
	if err != nil {
		panic(err)
	}
	return mp
}

func BenchmarkParseMessagesJSON(b *testing.B) {
	for n := 0; n < b.N; n++ {
		doParseJSON(b)
	}
}

func doParseJSON(b *testing.B) {
	b.Helper()
	keyJSON := "key"
	valJSON := "value"
	msg := &kafka.Message{
		PartInfo:  kafka.PartInfo{},
		TimeStamp: time.Time{},
		Key:       []byte(keyJSON),
		Value:     []byte(valJSON),
		Headers:   nil,
	}
	rows, err := messageParserJSON.ParseMessages([]*kafka.Message{msg})
	require.NoError(b, err)
	require.NotNil(b, rows)
	require.Equal(b, 1, rows.RowCount())
	row := rows.GetRow(0)
	require.Equal(b, int64(1234), row.GetInt64(0))
	require.Equal(b, 12.21, row.GetFloat64(1))
	require.Equal(b, "foo", row.GetString(2))
}
