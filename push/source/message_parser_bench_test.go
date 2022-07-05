package source

import (
	"testing"
	"time"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/kafka"
	"github.com/squareup/pranadb/protolib"
	"github.com/stretchr/testify/require"
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
	selectors, err := compileSelectors(colSelectors)
	if err != nil {
		panic(err)
	}
	topicInfo := &common.SourceOriginInfo{
		BrokerName:    "test_broker",
		TopicName:     "test_topic",
		KeyEncoding:   common.KafkaEncodingJSON,
		ValueEncoding: common.KafkaEncodingJSON,
		ColSelectors:  selectors,
		Properties:    nil,
	}

	sourceInfo := &common.SourceInfo{
		TableInfo:  tableInfo,
		OriginInfo: topicInfo,
	}
	mp, err := NewMessageParser(sourceInfo, protolib.EmptyRegistry)
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
