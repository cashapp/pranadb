package source

import (
	"fmt"
	"testing"
	"time"

	"github.com/squareup/pranadb/command/parser/selector"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/kafka"
	"github.com/squareup/pranadb/protolib"
	"github.com/stretchr/testify/require"
)

var colNames = []string{"col0", "col1", "col2", "col3", "col4"}
var dt = common.NewDecimalColumnType(10, 2)
var colTypes = []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType, common.DoubleColumnType, common.VarcharColumnType, dt}

type verifyExpectedValuesFunc = func(t *testing.T, row *common.Row)

func TestParseMessageFloat32BEKey(t *testing.T) {
	f := float32(123.25)
	keyBytes := common.AppendFloat32ToBufferBE(nil, f)

	vf := func(t *testing.T, row *common.Row) { //nolint:thelper
		require.Equal(t, float64(f), row.GetFloat64(0))
	}
	testParseMessageBinaryKey(t, common.DoubleColumnType, common.KafkaEncodingFloat32BE, keyBytes, vf)
}

func TestParseMessageFloat64BEKey(t *testing.T) {
	f := 432.25
	keyBytes := common.AppendFloat64ToBufferBE(nil, f)

	vf := func(t *testing.T, row *common.Row) { //nolint:thelper
		require.Equal(t, f, row.GetFloat64(0))
	}
	testParseMessageBinaryKey(t, common.DoubleColumnType, common.KafkaEncodingFloat64BE, keyBytes, vf)
}

func TestParseMessageInt16BEKey(t *testing.T) {
	s := 12354
	var keyBytes []byte
	keyBytes = common.AppendUint16ToBufferBE(keyBytes, uint16(s))

	vf := func(t *testing.T, row *common.Row) { //nolint:thelper
		require.Equal(t, int64(s), row.GetInt64(0))
	}
	testParseMessageBinaryKey(t, common.BigIntColumnType, common.KafkaEncodingInt16BE, keyBytes, vf)
}

func TestParseMessageInt32BEKey(t *testing.T) {
	i := 7654321
	var keyBytes []byte
	keyBytes = common.AppendUint32ToBufferBE(keyBytes, uint32(i))

	vf := func(t *testing.T, row *common.Row) { //nolint:thelper
		require.Equal(t, int64(i), row.GetInt64(0))
	}
	testParseMessageBinaryKey(t, common.BigIntColumnType, common.KafkaEncodingInt32BE, keyBytes, vf)
}

func TestParseMessageInt64BEKey(t *testing.T) {
	l := 987654321
	var keyBytes []byte
	keyBytes = common.AppendUint64ToBufferBE(keyBytes, uint64(l))

	vf := func(t *testing.T, row *common.Row) { //nolint:thelper
		require.Equal(t, int64(l), row.GetInt64(0))
	}
	testParseMessageBinaryKey(t, common.BigIntColumnType, common.KafkaEncodingInt64BE, keyBytes, vf)
}

func TestParseMessageStringBytesKey(t *testing.T) {
	s := "armadillos"

	vf := func(t *testing.T, row *common.Row) { //nolint:thelper
		require.Equal(t, s, row.GetString(0))
	}
	testParseMessageBinaryKey(t, common.VarcharColumnType, common.KafkaEncodingStringBytes, []byte(s), vf)
}

func testParseMessageBinaryKey(t *testing.T, keyType common.ColumnType, keyEncoding common.KafkaEncoding, keyBytes []byte,
	vf verifyExpectedValuesFunc) {
	t.Helper()
	theColTypes := []common.ColumnType{keyType, common.BigIntColumnType, common.DoubleColumnType, common.VarcharColumnType, dt}

	vf2 := func(t *testing.T, row *common.Row) { //nolint:thelper
		vf(t, row)
		require.Equal(t, int64(4321), row.GetInt64(1))
		require.Equal(t, 23.12, row.GetFloat64(2))
		require.Equal(t, "foo", row.GetString(3))
		dec := row.GetDecimal(4)
		require.Equal(t, "12345678.99", dec.String())
	}

	testParseMessage(t, colNames, theColTypes,
		common.KafkaEncodingJSON,
		keyEncoding, common.KafkaEncodingJSON,
		nil, keyBytes, []byte(`{"vf1":4321,"vf2":23.12,"vf3":"foo","vf4":"12345678.99"}`),
		[]string{"k", "v.vf1", "v.vf2", "v.vf3", "v.vf4"}, time.Now(), vf2)
}

func TestParseMessageNilInt64BEKeyAndVals(t *testing.T) {
	testParseMessageNilKeyAndNilJSONVals(t, common.BigIntColumnType, common.KafkaEncodingInt64BE)
}

func TestParseMessageNilInt32BEKeyAndVals(t *testing.T) {
	testParseMessageNilKeyAndNilJSONVals(t, common.BigIntColumnType, common.KafkaEncodingInt32BE)
}

func TestParseMessageNilInt16BEKeyAndVals(t *testing.T) {
	testParseMessageNilKeyAndNilJSONVals(t, common.BigIntColumnType, common.KafkaEncodingInt16BE)
}

func TestParseMessageNilFloat32BEKeyAndVals(t *testing.T) {
	testParseMessageNilKeyAndNilJSONVals(t, common.DoubleColumnType, common.KafkaEncodingFloat32BE)
}

func TestParseMessageNilFloat64BEKeyAndVals(t *testing.T) {
	testParseMessageNilKeyAndNilJSONVals(t, common.DoubleColumnType, common.KafkaEncodingFloat64BE)
}

func TestParseMessageNilStringBytesKeyAndVals(t *testing.T) {
	testParseMessageNilKeyAndNilJSONVals(t, common.VarcharColumnType, common.KafkaEncodingStringBytes)
}

func testParseMessageNilKeyAndNilJSONVals(t *testing.T, colType common.ColumnType, keyEncoding common.KafkaEncoding) {
	t.Helper()
	theColTypes := []common.ColumnType{colType, common.BigIntColumnType, common.DoubleColumnType, common.VarcharColumnType, dt}

	vf := func(t *testing.T, row *common.Row) {
		t.Helper()
		require.True(t, row.IsNull(0))
		require.True(t, row.IsNull(1))
		require.True(t, row.IsNull(2))
		require.True(t, row.IsNull(3))
		require.True(t, row.IsNull(4))
	}

	testParseMessage(t, colNames, theColTypes,
		common.KafkaEncodingJSON,
		keyEncoding, common.KafkaEncodingJSON,
		nil,
		[]byte{}, []byte(`{"vf1":null,"vf2":null,"vf3":null,"vf4":null}`),
		[]string{"k", "v.vf1", "v.vf2", "v.vf3", "v.vf4"}, time.Now(), vf)
}

func TestParseMessageNilJsonKeyAndNilJsonVals(t *testing.T) {
	theColTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType, common.DoubleColumnType, common.VarcharColumnType, dt}
	t.Helper()
	vf := func(t *testing.T, row *common.Row) {
		t.Helper()
		require.True(t, row.IsNull(0))
		require.True(t, row.IsNull(1))
		require.True(t, row.IsNull(2))
		require.True(t, row.IsNull(3))
		require.True(t, row.IsNull(4))
	}

	testParseMessage(t, colNames, theColTypes,
		common.KafkaEncodingJSON, common.KafkaEncodingJSON, common.KafkaEncodingJSON,
		nil, []byte(`{"kf1":null}`), []byte(`{"vf1":null,"vf2":null,"vf3":null,"vf4":null}`),
		[]string{"k.kf1", "v.vf1", "v.vf2", "v.vf3", "v.vf4"}, time.Now(), vf)
}

func verifyJSONExpectedValues(t *testing.T, row *common.Row) {
	t.Helper()
	require.Equal(t, int64(1234), row.GetInt64(0))
	require.Equal(t, int64(4321), row.GetInt64(1))
	require.Equal(t, 23.12, row.GetFloat64(2))
	require.Equal(t, "foo", row.GetString(3))
	dec := row.GetDecimal(4)
	require.Equal(t, "12345678.99", dec.String())
}

func TestParseMessageJSONSimple(t *testing.T) {
	testParseMessage(t, colNames, colTypes,
		common.KafkaEncodingJSON, common.KafkaEncodingJSON, common.KafkaEncodingJSON,
		nil, []byte(`{"kf1":1234}`), []byte(`{"vf1":4321,"vf2":23.12,"vf3":"foo","vf4":"12345678.99"}`),
		[]string{"k.kf1", "v.vf1", "v.vf2", "v.vf3", "v.vf4"}, time.Now(),
		verifyJSONExpectedValues)
}

func TestParseMessageJSONArray(t *testing.T) {
	testParseMessage(t, colNames, colTypes,
		common.KafkaEncodingJSON, common.KafkaEncodingJSON, common.KafkaEncodingJSON,
		nil, []byte(`{"kf1":[4321,1234]}`), []byte(`{"vf1":[4321,6789],"vf2":[0.1,9.99,23.12],"vf3":["a","foo","bar"],"vf4":["12345678.99"]}`),
		[]string{"k.kf1[1]", "v.vf1[0]", "v.vf2[2]", "v.vf3[1]", "v.vf4[0]"}, time.Now(),
		verifyJSONExpectedValues)
}

func TestParseMessageJSONNested(t *testing.T) {
	testParseMessage(t, colNames, colTypes,
		common.KafkaEncodingJSON, common.KafkaEncodingJSON, common.KafkaEncodingJSON,
		nil, []byte(`{"kf1":{"kf2":123,"kf3":1234}}`), []byte(`{"vf1":{"vf2":4321,"vf3": {"vf4": 23.12, "vf5": {"vf6": "foo", "vf7": "12345678.99"}}}}`),
		[]string{"k.kf1.kf3", "v.vf1.vf2", "v.vf1.vf3.vf4", "v.vf1.vf3.vf5.vf6", "v.vf1.vf3.vf5.vf7"}, time.Now(),
		verifyJSONExpectedValues)
}

func TestParseMessageTimestamp(t *testing.T) {
	theColNames := []string{"col0", "col1", "col2"}
	theColTypes := []common.ColumnType{common.TimestampColumnType, common.TimestampColumnType, common.TimestampColumnType}

	now := time.Now()
	// round it to the nearest millisecond
	unixMillisPastEpoch := now.UnixNano() / 1000000
	unixSeconds := unixMillisPastEpoch / 1000
	ts := time.Unix(unixSeconds, (unixMillisPastEpoch%1000)*1000000)
	tsMysql := common.NewTimestampFromGoTime(ts)
	sTS := tsMysql.String()

	// We get col0 from the timestamp of the Kafka message itself
	// We get col1 from a string field in the message
	// We get col2 from a numeric field in the message assumed to be unix milliseconds (like new Date().getTime())
	vf := func(t *testing.T, row *common.Row) { //nolint:thelper
		require.Equal(t, tsMysql, row.GetTimestamp(0))
		require.Equal(t, tsMysql, row.GetTimestamp(1))
		require.Equal(t, tsMysql, row.GetTimestamp(2))
	}

	testParseMessage(t, theColNames, theColTypes, common.KafkaEncodingJSON, common.KafkaEncodingJSON, common.KafkaEncodingJSON,
		nil,
		[]byte(fmt.Sprintf(`{"kf1":"%s"}`, sTS)), // Tests decoding mysql timestamp from string field in message
		[]byte(fmt.Sprintf(`{"vf1":%d}`, unixMillisPastEpoch)), // Tests decoding mysql timestamp from numeric field - assumed to be milliseconds past Unix epoch
		[]string{"t", "k.kf1", "v.vf1"}, ts,
		vf)
}

func TestParseMessagesJSONHeaders(t *testing.T) {
	theColNames := []string{"col0", "col1", "col2", "col3"}
	theColTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType, common.VarcharColumnType, common.DoubleColumnType}
	vf := func(t *testing.T, row *common.Row) {
		t.Helper()
		require.Equal(t, int64(1234), row.GetInt64(0))
		require.Equal(t, int64(4321), row.GetInt64(1))
		require.Equal(t, "blah", row.GetString(2))
		require.Equal(t, 12.12, row.GetFloat64(3))
	}
	testParseMessage(t, theColNames, theColTypes,
		common.KafkaEncodingJSON,
		common.KafkaEncodingJSON, common.KafkaEncodingJSON,
		[]kafka.MessageHeader{
			{Key: "hdr1", Value: []byte(`{"hf1":4321}`)},
			{Key: "hdr2", Value: []byte(`{"hf2":"blah"}`)},
			{Key: "hdr3", Value: []byte(`{"hf3":12.12}`)},
		},
		[]byte(`{"kf1":1234}`), []byte(`{}`),
		[]string{"k.kf1", "h.hdr1.hf1", "h.hdr2.hf2", "h.hdr3.hf3"}, time.Now(),
		vf)
}

func TestParseMessagesStringBytesHeaders(t *testing.T) {
	theColNames := []string{"col0", "col1", "col2", "col3"}
	theColTypes := []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType, common.VarcharColumnType, common.VarcharColumnType}
	vf := func(t *testing.T, row *common.Row) {
		t.Helper()
		require.Equal(t, int64(1234), row.GetInt64(0))
		require.Equal(t, "val1", row.GetString(1))
		require.Equal(t, "val2", row.GetString(2))
		require.Equal(t, "val3", row.GetString(3))
	}
	testParseMessage(t, theColNames, theColTypes,
		common.KafkaEncodingStringBytes,
		common.KafkaEncodingJSON, common.KafkaEncodingJSON,
		[]kafka.MessageHeader{
			{Key: "hdr1", Value: []byte("val1")},
			{Key: "hdr2", Value: []byte("val2")},
			{Key: "hdr3", Value: []byte("val3")},
		},
		[]byte(`{"kf1":1234}`), []byte(`{}`),
		[]string{"k.kf1", "h.hdr1", "h.hdr2", "h.hdr3"}, time.Now(),
		vf)
}

func TestParseMessagesInt64BEHeaders(t *testing.T) {
	theColNames := []string{"col0", "col1", "col2", "col3"}
	theColTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType, common.BigIntColumnType, common.BigIntColumnType}
	vf := func(t *testing.T, row *common.Row) {
		t.Helper()
		require.Equal(t, int64(1234), row.GetInt64(0))
		require.Equal(t, int64(12345678), row.GetInt64(1))
		require.Equal(t, int64(87654321), row.GetInt64(2))
		require.Equal(t, int64(54321234), row.GetInt64(3))
	}
	testParseMessage(t, theColNames, theColTypes,
		common.KafkaEncodingInt64BE,
		common.KafkaEncodingJSON, common.KafkaEncodingJSON,
		[]kafka.MessageHeader{
			{Key: "hdr1", Value: common.AppendUint64ToBufferBE(nil, 12345678)},
			{Key: "hdr2", Value: common.AppendUint64ToBufferBE(nil, 87654321)},
			{Key: "hdr3", Value: common.AppendUint64ToBufferBE(nil, 54321234)},
		},
		[]byte(`{"kf1":1234}`), []byte(`{}`),
		[]string{"k.kf1", "h.hdr1", "h.hdr2", "h.hdr3"}, time.Now(),
		vf)
}

func TestParseMessagesInt32BEHeaders(t *testing.T) {
	theColNames := []string{"col0", "col1", "col2", "col3"}
	theColTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType, common.BigIntColumnType, common.BigIntColumnType}
	vf := func(t *testing.T, row *common.Row) {
		t.Helper()
		require.Equal(t, int64(1234), row.GetInt64(0))
		require.Equal(t, int64(12345678), row.GetInt64(1))
		require.Equal(t, int64(87654321), row.GetInt64(2))
		require.Equal(t, int64(54321234), row.GetInt64(3))
	}
	testParseMessage(t, theColNames, theColTypes,
		common.KafkaEncodingInt32BE,
		common.KafkaEncodingJSON, common.KafkaEncodingJSON,
		[]kafka.MessageHeader{
			{Key: "hdr1", Value: common.AppendUint32ToBufferBE(nil, 12345678)},
			{Key: "hdr2", Value: common.AppendUint32ToBufferBE(nil, 87654321)},
			{Key: "hdr3", Value: common.AppendUint32ToBufferBE(nil, 54321234)},
		},
		[]byte(`{"kf1":1234}`), []byte(`{}`),
		[]string{"k.kf1", "h.hdr1", "h.hdr2", "h.hdr3"}, time.Now(),
		vf)
}

func TestParseMessagesInt16BEHeaders(t *testing.T) {
	theColNames := []string{"col0", "col1", "col2", "col3"}
	theColTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType, common.BigIntColumnType, common.BigIntColumnType}
	vf := func(t *testing.T, row *common.Row) {
		t.Helper()
		require.Equal(t, int64(1234), row.GetInt64(0))
		require.Equal(t, int64(2134), row.GetInt64(1))
		require.Equal(t, int64(4321), row.GetInt64(2))
		require.Equal(t, int64(5423), row.GetInt64(3))
	}
	testParseMessage(t, theColNames, theColTypes,
		common.KafkaEncodingInt16BE,
		common.KafkaEncodingJSON, common.KafkaEncodingJSON,
		[]kafka.MessageHeader{
			{Key: "hdr1", Value: common.AppendUint16ToBufferBE(nil, 2134)},
			{Key: "hdr2", Value: common.AppendUint16ToBufferBE(nil, 4321)},
			{Key: "hdr3", Value: common.AppendUint16ToBufferBE(nil, 5423)},
		},
		[]byte(`{"kf1":1234}`), []byte(`{}`),
		[]string{"k.kf1", "h.hdr1", "h.hdr2", "h.hdr3"}, time.Now(),
		vf)
}

func TestParseMessagesFloat32BEHeaders(t *testing.T) {
	theColNames := []string{"col0", "col1", "col2", "col3"}
	theColTypes := []common.ColumnType{common.BigIntColumnType, common.DoubleColumnType, common.DoubleColumnType, common.DoubleColumnType}
	vf := func(t *testing.T, row *common.Row) {
		t.Helper()
		require.Equal(t, int64(1234), row.GetInt64(0))
		require.Equal(t, 2134.25, row.GetFloat64(1))
		require.Equal(t, 4321.25, row.GetFloat64(2))
		require.Equal(t, 5423.25, row.GetFloat64(3))
	}
	testParseMessage(t, theColNames, theColTypes,
		common.KafkaEncodingFloat32BE,
		common.KafkaEncodingJSON, common.KafkaEncodingJSON,
		[]kafka.MessageHeader{
			{Key: "hdr1", Value: common.AppendFloat32ToBufferBE(nil, 2134.25)},
			{Key: "hdr2", Value: common.AppendFloat32ToBufferBE(nil, 4321.25)},
			{Key: "hdr3", Value: common.AppendFloat32ToBufferBE(nil, 5423.25)},
		},
		[]byte(`{"kf1":1234}`), []byte(`{}`),
		[]string{"k.kf1", "h.hdr1", "h.hdr2", "h.hdr3"}, time.Now(),
		vf)
}

func TestParseMessagesFloat64BEHeaders(t *testing.T) {
	theColNames := []string{"col0", "col1", "col2", "col3"}
	theColTypes := []common.ColumnType{common.BigIntColumnType, common.DoubleColumnType, common.DoubleColumnType, common.DoubleColumnType}
	vf := func(t *testing.T, row *common.Row) {
		t.Helper()
		require.Equal(t, int64(1234), row.GetInt64(0))
		require.Equal(t, 2134.25, row.GetFloat64(1))
		require.Equal(t, 4321.25, row.GetFloat64(2))
		require.Equal(t, 5423.25, row.GetFloat64(3))
	}
	testParseMessage(t, theColNames, theColTypes,
		common.KafkaEncodingFloat64BE,
		common.KafkaEncodingJSON, common.KafkaEncodingJSON,
		[]kafka.MessageHeader{
			{Key: "hdr1", Value: common.AppendFloat64ToBufferBE(nil, 2134.25)},
			{Key: "hdr2", Value: common.AppendFloat64ToBufferBE(nil, 4321.25)},
			{Key: "hdr3", Value: common.AppendFloat64ToBufferBE(nil, 5423.25)},
		},
		[]byte(`{"kf1":1234}`), []byte(`{}`),
		[]string{"k.kf1", "h.hdr1", "h.hdr2", "h.hdr3"}, time.Now(),
		vf)
}

func compileSelectors(raw []string) ([]selector.Selector, error) {
	cs := make([]selector.Selector, len(raw))
	for i := range raw {
		col, err := selector.ParseSelector(raw[i])
		if err != nil {
			return nil, err
		}
		cs[i] = col
	}
	return cs, nil
}

//nolint:unparam
func testParseMessage(t *testing.T, colNames []string, colTypes []common.ColumnType, headerEncoding common.KafkaEncoding, keyEncoding common.KafkaEncoding,
	valueEncoding common.KafkaEncoding, headers []kafka.MessageHeader, keyBytes []byte, valueBytes []byte, colSelectors []string, timestamp time.Time,
	vf verifyExpectedValuesFunc) {
	t.Helper()
	tableInfo := &common.TableInfo{
		ID:             0,
		SchemaName:     "test",
		Name:           "test_table",
		PrimaryKeyCols: []int{0},
		ColumnNames:    colNames,
		ColumnTypes:    colTypes,
		IndexInfos:     nil,
	}
	selectors, err := compileSelectors(colSelectors)
	require.NoError(t, err)
	topicInfo := &common.TopicInfo{
		BrokerName:     "test_broker",
		TopicName:      "test_topic",
		HeaderEncoding: headerEncoding,
		KeyEncoding:    keyEncoding,
		ValueEncoding:  valueEncoding,
		ColSelectors:   selectors,
		Properties:     nil,
	}
	sourceInfo := &common.SourceInfo{
		TableInfo: tableInfo,
		TopicInfo: topicInfo,
	}
	mp, err := NewMessageParser(sourceInfo, protolib.EmptyRegistry)
	require.NoError(t, err)

	msg := &kafka.Message{
		PartInfo:  kafka.PartInfo{},
		TimeStamp: timestamp,
		Key:       keyBytes,
		Value:     valueBytes,
		Headers:   headers,
	}
	rows, err := mp.ParseMessages([]*kafka.Message{msg})
	require.NoError(t, err)
	require.NotNil(t, rows)
	require.Equal(t, 1, rows.RowCount())
	row := rows.GetRow(0)
	vf(t, &row)
}
