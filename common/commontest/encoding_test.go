package commontest

import (
	"math"
	"testing"
	"time"

	"github.com/squareup/pranadb/common"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

var singleVarcharColumn = []common.ColumnType{common.VarcharColumnType}
var singleIntColumn = []common.ColumnType{common.IntColumnType}
var singleFloatColumn = []common.ColumnType{common.DoubleColumnType}

func TestEncodeDecodeInt(t *testing.T) {
	rf := common.NewRowsFactory(singleIntColumn)
	encodeDecodeInt(t, rf, 0)
	encodeDecodeInt(t, rf, math.MinInt64)
	encodeDecodeInt(t, rf, math.MaxInt64)
	encodeDecodeInt(t, rf, -1)
	encodeDecodeInt(t, rf, 1)
	encodeDecodeInt(t, rf, -10)
	encodeDecodeInt(t, rf, 10)
}

func TestEncodeDecodeString(t *testing.T) {
	rf := common.NewRowsFactory(singleVarcharColumn)
	encodeDecodeString(t, rf, "")
	encodeDecodeString(t, rf, "zxy123")
	encodeDecodeString(t, rf, "\u2318")
}

func TestEncodeDecodeFloat(t *testing.T) {
	rf := common.NewRowsFactory(singleFloatColumn)
	encodeDecodeFloat(t, rf, 0)
	encodeDecodeFloat(t, rf, -1234.5678)
	encodeDecodeFloat(t, rf, 1234.5678)
	encodeDecodeFloat(t, rf, math.MaxFloat64)
}

func TestEncodeDecodeRow(t *testing.T) {
	decType1 := common.NewDecimalColumnType(10, 2)
	colTypes := []common.ColumnType{common.TinyIntColumnType, common.IntColumnType, common.BigIntColumnType, common.DoubleColumnType, common.VarcharColumnType, decType1}
	rf := common.NewRowsFactory(colTypes)
	rows := rf.NewRows(10)
	rows.AppendInt64ToColumn(0, 255)
	rows.AppendInt64ToColumn(1, math.MaxInt32)
	rows.AppendInt64ToColumn(2, math.MaxInt64)
	rows.AppendFloat64ToColumn(3, math.MaxFloat64)
	rows.AppendStringToColumn(4, "somestringxyz")
	dec, err := common.NewDecFromString("12345678.32")
	require.NoError(t, err)
	rows.AppendDecimalToColumn(5, *dec)
	testEncodeDecodeRow(t, rows, colTypes)
}

func TestEncodeDecodeRowWithNulls(t *testing.T) {
	decType1 := common.NewDecimalColumnType(10, 2)
	colTypes := []common.ColumnType{common.TinyIntColumnType, common.TinyIntColumnType, common.IntColumnType, common.IntColumnType, common.BigIntColumnType, common.BigIntColumnType, common.DoubleColumnType, common.DoubleColumnType, common.VarcharColumnType, common.VarcharColumnType, decType1, decType1}
	rf := common.NewRowsFactory(colTypes)
	rows := rf.NewRows(10)
	rows.AppendInt64ToColumn(0, 255)
	rows.AppendNullToColumn(1)
	rows.AppendInt64ToColumn(2, math.MaxInt32)
	rows.AppendNullToColumn(3)
	rows.AppendInt64ToColumn(4, math.MaxInt64)
	rows.AppendNullToColumn(5)
	rows.AppendFloat64ToColumn(6, math.MaxFloat64)
	rows.AppendNullToColumn(7)
	rows.AppendStringToColumn(8, "somestringxyz")
	rows.AppendNullToColumn(9)
	dec, err := common.NewDecFromString("12345678.32")
	require.NoError(t, err)
	rows.AppendDecimalToColumn(10, *dec)
	rows.AppendNullToColumn(11)
	testEncodeDecodeRow(t, rows, colTypes)
}

func testEncodeDecodeRow(t *testing.T, rows *common.Rows, colTypes []common.ColumnType) {
	t.Helper()
	row := rows.GetRow(0)
	var buffer []byte
	buff, err := common.EncodeRow(&row, colTypes, buffer)
	require.NoError(t, err)
	err = common.DecodeRow(buff, colTypes, rows)
	require.NoError(t, err)
	actualRow := rows.GetRow(1)
	RowsEqual(t, row, actualRow, colTypes)
}

func TestEncodeDecodeDecimal(t *testing.T) {
	colTypes := []common.ColumnType{common.NewDecimalColumnType(10, 2)}
	rf := common.NewRowsFactory(colTypes)
	dec, err := common.NewDecFromString("0.00")
	require.NoError(t, err)
	encodeDecodeDecimal(t, rf, *dec, colTypes)
	dec, err = common.NewDecFromString("-12345678.12")
	require.NoError(t, err)
	encodeDecodeDecimal(t, rf, *dec, colTypes)
	dec, err = common.NewDecFromString("12345678.12")
	require.NoError(t, err)
	encodeDecodeDecimal(t, rf, *dec, colTypes)
}

func encodeDecodeInt(t *testing.T, rf *common.RowsFactory, val int64) {
	t.Helper()
	rows := rf.NewRows(1)
	rows.AppendInt64ToColumn(0, val)
	encodeDecode(t, rows, singleIntColumn)
}

func encodeDecodeString(t *testing.T, rf *common.RowsFactory, val string) {
	t.Helper()
	rows := rf.NewRows(1)
	rows.AppendStringToColumn(0, val)
	encodeDecode(t, rows, singleVarcharColumn)
}

func encodeDecodeFloat(t *testing.T, rf *common.RowsFactory, val float64) {
	t.Helper()
	rows := rf.NewRows(1)
	rows.AppendFloat64ToColumn(0, val)
	encodeDecode(t, rows, singleFloatColumn)
}

func encodeDecodeDecimal(t *testing.T, rf *common.RowsFactory, val common.Decimal, colTypes []common.ColumnType) {
	t.Helper()
	rows := rf.NewRows(1)
	rows.AppendDecimalToColumn(0, val)
	encodeDecode(t, rows, colTypes)
}

func encodeDecode(t *testing.T, rows *common.Rows, columnTypes []common.ColumnType) {
	t.Helper()
	row := rows.GetRow(0)
	var buffer []byte
	buffer, err := common.EncodeRow(&row, columnTypes, buffer)
	require.NoError(t, err)
	err = common.DecodeRow(buffer, columnTypes, rows)
	require.NoError(t, err)

	row1 := rows.GetRow(0)
	row2 := rows.GetRow(1)

	RowsEqual(t, row1, row2, columnTypes)
}

func TestIsLittleEndian(t *testing.T) {
	require.True(t, common.IsLittleEndian)
}

func TestEncodeDecodeUint64sLittleEndianArch(t *testing.T) {
	setEndianness(t, true)
	testEncodeDecodeUint64s(t, 0, 1, math.MaxUint64, 12345678)
}

func TestEncodeDecodeUint64sBigEndianArch(t *testing.T) {
	setEndianness(t, false)
	testEncodeDecodeUint64s(t, 0, 1, math.MaxUint64, 12345678)
}

func testEncodeDecodeUint64s(t *testing.T, vals ...uint64) {
	t.Helper()
	for _, val := range vals {
		testEncodeDecodeUint64(t, val)
	}
}

func testEncodeDecodeUint64(t *testing.T, val uint64) {
	t.Helper()
	buff := make([]byte, 0, 8)
	buff = common.AppendUint64ToBufferLE(buff, val)
	valRead, _ := common.ReadUint64FromBufferLE(buff, 0)
	require.Equal(t, val, valRead)
}

func TestEncodeDecodeUint32sLittleEndianArch(t *testing.T) {
	setEndianness(t, true)
	testEncodeDecodeUint32s(t, 0, 1, math.MaxUint32, 12345678)
}

func TestEncodeDecodeUint32sBigEndianArch(t *testing.T) {
	setEndianness(t, false)
	testEncodeDecodeUint32s(t, 0, 1, math.MaxUint32, 12345678)
}

func testEncodeDecodeUint32s(t *testing.T, vals ...uint32) {
	t.Helper()
	for _, val := range vals {
		testEncodeDecodeUint32(t, val)
	}
}

func testEncodeDecodeUint32(t *testing.T, val uint32) {
	t.Helper()
	buff := make([]byte, 0, 4)
	buff = common.AppendUint32ToBufferLE(buff, val)
	valRead, _ := common.ReadUint32FromBufferLE(buff, 0)
	require.Equal(t, val, valRead)
}

func TestEncodeDecodeTimestampFromString(t *testing.T) {
	ts := common.NewTimestampFromString("2021-08-01 12:34:56.789")
	buf, err := common.AppendTimestampToBuffer(nil, ts)
	require.NoError(t, err)
	valRead, _, err := common.ReadTimestampFromBuffer(buf, 0, ts.Fsp())
	require.NoError(t, err)
	assert.Equal(t, 0, ts.Compare(valRead))
	assert.Equal(t, "2021-08-01 12:34:56.789", valRead.String())

	// And check the other way
	s := ts.String()
	ts2 := common.NewTimestampFromString(s)
	assert.Equal(t, ts, ts2)
}

func TestEncodeDecodeTimestampToString(t *testing.T) {
	now := time.Now()
	ts1 := common.NewTimestampFromGoTime(now)
	s := ts1.String()
	ts2 := common.NewTimestampFromString(s)
	require.Equal(t, ts1, ts2)
}

func TestEncodeDecodeTimestampFromGoTime(t *testing.T) {
	now := time.Now()
	ts := common.NewTimestampFromGoTime(now)
	buf, err := common.AppendTimestampToBuffer(nil, ts)
	require.NoError(t, err)
	valRead, _, err := common.ReadTimestampFromBuffer(buf, 0, 6)
	require.NoError(t, err)
	assert.Equal(t, 0, ts.Compare(valRead))
}

func TestEncodeDecodeTimestampFromUnixEpochMillis(t *testing.T) {
	// Get a time that is rounded to nearest ms
	now := time.Now()
	unixMillisPastEpoch := now.UnixNano() / 1000000
	unixSeconds := unixMillisPastEpoch / 1000
	trounded := time.Unix(unixSeconds, (unixMillisPastEpoch%1000)*1000000)
	ts1 := common.NewTimestampFromGoTime(trounded)
	ts2 := common.NewTimestampFromUnixEpochMillis(unixMillisPastEpoch)
	require.Equal(t, ts1, ts2)
}

func setEndianness(t *testing.T, endianness bool) {
	t.Helper()

	prev := common.IsLittleEndian
	t.Cleanup(func() {
		common.IsLittleEndian = prev
	})
	common.IsLittleEndian = endianness
}
