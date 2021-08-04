package source

import (
	"encoding/binary"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/kafka"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
	"time"
)

var colNames = []string{"col0", "col1", "col2", "col3", "col4"}
var dt = common.NewDecimalColumnType(10, 2)
var colTypes = []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType, common.DoubleColumnType, common.VarcharColumnType, dt}

type verifyExpectedValuesFunc = func(t *testing.T, row *common.Row)

func TestParseMessageKafkaFloatKey(t *testing.T) {
	f := float32(123.25)
	u := math.Float32bits(f)
	keyBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyBytes, u)

	vf := func(t *testing.T, row *common.Row) { //nolint:thelper
		require.Equal(t, float64(f), row.GetFloat64(0))
	}
	testParseMessageKafkaKey(t, common.DoubleColumnType, common.EncodingKafkaFloat, keyBytes, vf)
}

func TestParseMessageKafkaDoubleKey(t *testing.T) {
	f := 432.25
	u := math.Float64bits(f)
	keyBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(keyBytes, u)

	vf := func(t *testing.T, row *common.Row) { //nolint:thelper
		require.Equal(t, f, row.GetFloat64(0))
	}
	testParseMessageKafkaKey(t, common.DoubleColumnType, common.EncodingKafkaDouble, keyBytes, vf)
}

func TestParseMessageKafkaShortKey(t *testing.T) {
	s := 12354
	var keyBytes []byte
	keyBytes = common.AppendUint16ToBufferBE(keyBytes, uint16(s))

	vf := func(t *testing.T, row *common.Row) { //nolint:thelper
		require.Equal(t, int64(s), row.GetInt64(0))
	}
	testParseMessageKafkaKey(t, common.BigIntColumnType, common.EncodingKafkaShort, keyBytes, vf)
}

func TestParseMessageKafkaIntegerKey(t *testing.T) {
	i := 7654321
	var keyBytes []byte
	keyBytes = common.AppendUint32ToBufferBE(keyBytes, uint32(i))

	vf := func(t *testing.T, row *common.Row) { //nolint:thelper
		require.Equal(t, int64(i), row.GetInt64(0))
	}
	testParseMessageKafkaKey(t, common.BigIntColumnType, common.EncodingKafkaInteger, keyBytes, vf)
}

func TestParseMessageKafkaLongKey(t *testing.T) {
	l := 987654321
	var keyBytes []byte
	keyBytes = common.AppendUint64ToBufferBE(keyBytes, uint64(l))

	vf := func(t *testing.T, row *common.Row) { //nolint:thelper
		require.Equal(t, int64(l), row.GetInt64(0))
	}
	testParseMessageKafkaKey(t, common.BigIntColumnType, common.EncodingKafkaLong, keyBytes, vf)
}

func TestParseMessageKafkaStringKey(t *testing.T) {
	s := "armadillos"

	vf := func(t *testing.T, row *common.Row) { //nolint:thelper
		require.Equal(t, s, row.GetString(0))
	}
	testParseMessageKafkaKey(t, common.VarcharColumnType, common.EncodingKafkaString, []byte(s), vf)
}

func testParseMessageKafkaKey(t *testing.T, keyType common.ColumnType, keyEncoding common.KafkaEncoding, keyBytes []byte,
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
		keyEncoding, common.EncodingJSON,
		keyBytes, []byte(`{"vf1":4321,"vf2":23.12,"vf3":"foo","vf4":"12345678.99"}`),
		[]string{"k", "v.vf1", "v.vf2", "v.vf3", "v.vf4"}, vf2)
}

func TestParseMessageNilKafkaLongKeyAndVals(t *testing.T) {
	testParseMessageNilKeyAndNilJSONVals(t, common.BigIntColumnType, common.EncodingKafkaLong)
}

func TestParseMessageNilKafkaIntegerKeyAndVals(t *testing.T) {
	testParseMessageNilKeyAndNilJSONVals(t, common.BigIntColumnType, common.EncodingKafkaInteger)
}

func TestParseMessageNilKafkaShortKeyAndVals(t *testing.T) {
	testParseMessageNilKeyAndNilJSONVals(t, common.BigIntColumnType, common.EncodingKafkaShort)
}

func TestParseMessageNilKafkaFloatKeyAndVals(t *testing.T) {
	testParseMessageNilKeyAndNilJSONVals(t, common.DoubleColumnType, common.EncodingKafkaFloat)
}

func TestParseMessageNilKafkaDoubleKeyAndVals(t *testing.T) {
	testParseMessageNilKeyAndNilJSONVals(t, common.DoubleColumnType, common.EncodingKafkaDouble)
}

func TestParseMessageNilKafkaStringKeyAndVals(t *testing.T) {
	testParseMessageNilKeyAndNilJSONVals(t, common.VarcharColumnType, common.EncodingKafkaString)
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
		keyEncoding, common.EncodingJSON,
		[]byte{}, []byte(`{"vf1":null,"vf2":null,"vf3":null,"vf4":null}`),
		[]string{"k", "v.vf1", "v.vf2", "v.vf3", "v.vf4"}, vf)
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
		common.EncodingJSON, common.EncodingJSON,
		[]byte(`{"kf1":null}`), []byte(`{"vf1":null,"vf2":null,"vf3":null,"vf4":null}`),
		[]string{"k.kf1", "v.vf1", "v.vf2", "v.vf3", "v.vf4"}, vf)
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
		common.EncodingJSON, common.EncodingJSON,
		[]byte(`{"kf1":1234}`), []byte(`{"vf1":4321,"vf2":23.12,"vf3":"foo","vf4":"12345678.99"}`),
		[]string{"k.kf1", "v.vf1", "v.vf2", "v.vf3", "v.vf4"},
		verifyJSONExpectedValues)
}

func TestParseMessageJSONArray(t *testing.T) {
	testParseMessage(t, colNames, colTypes,
		common.EncodingJSON, common.EncodingJSON,
		[]byte(`{"kf1":[4321,1234]}`), []byte(`{"vf1":[4321,6789],"vf2":[0.1,9.99,23.12],"vf3":["a","foo","bar"],"vf4":["12345678.99"]}`),
		[]string{"k.kf1[1]", "v.vf1[0]", "v.vf2[2]", "v.vf3[1]", "v.vf4[0]"},
		verifyJSONExpectedValues)
}

func TestParseMessageJSONNested(t *testing.T) {
	testParseMessage(t, colNames, colTypes,
		common.EncodingJSON, common.EncodingJSON,
		[]byte(`{"kf1":{"kf2":123,"kf3":1234}}`), []byte(`{"vf1":{"vf2":4321,"vf3": {"vf4": 23.12, "vf5": {"vf6": "foo", "vf7": "12345678.99"}}}}`),
		[]string{"k.kf1.kf3", "v.vf1.vf2", "v.vf1.vf3.vf4", "v.vf1.vf3.vf5.vf6", "v.vf1.vf3.vf5.vf7"},
		verifyJSONExpectedValues)
}

//nolint:unparam
func testParseMessage(t *testing.T, colNames []string, colTypes []common.ColumnType, keyEncoding common.KafkaEncoding,
	valueEncoding common.KafkaEncoding, keyBytes []byte, valueBytes []byte, colSelectors []string,
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
	topicInfo := &common.TopicInfo{
		BrokerName:    "test_broker",
		TopicName:     "test_topic",
		KeyEncoding:   keyEncoding,
		ValueEncoding: valueEncoding,
		ColSelectors:  colSelectors,
		Properties:    nil,
	}
	sourceInfo := &common.SourceInfo{
		TableInfo: tableInfo,
		TopicInfo: topicInfo,
	}
	mp, err := NewMessageParser(sourceInfo)
	require.NoError(t, err)

	msg := &kafka.Message{
		PartInfo:  kafka.PartInfo{},
		TimeStamp: time.Time{},
		Key:       keyBytes,
		Value:     valueBytes,
		Headers:   nil,
	}
	rows, err := mp.ParseMessages([]*kafka.Message{msg})
	require.NoError(t, err)
	require.NotNil(t, rows)
	require.Equal(t, 1, rows.RowCount())
	row := rows.GetRow(0)
	vf(t, &row)
}
