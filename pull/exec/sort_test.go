package exec

import (
	"testing"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/common/commontest"
	"github.com/stretchr/testify/require"
)

var sortColTypes = []common.ColumnType{common.BigIntColumnType, common.TinyIntColumnType, common.IntColumnType, common.BigIntColumnType, common.DoubleColumnType, common.VarcharColumnType, common.NewDecimalColumnType(10, 2)}
var sortColNames = []string{"pk_bigint", "c_tinyint", "c_int", "c_bigint", "c_double", "c_varchar", "c_decimal"}

func TestSortTinyIntAsc(t *testing.T) {
	inpRows := [][]interface{}{
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{1, nil, nil, nil, nil, nil, nil},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
	}
	expRows := [][]interface{}{
		{0, nil, nil, nil, nil, nil, nil},
		{1, nil, nil, nil, nil, nil, nil},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
	}
	descending := []bool{
		false,
	}
	testSort(t, inpRows, expRows, sortColNames, sortColTypes, descending, common.NewColumnExpression(1, sortColTypes[1]))
}

func TestSortTinyIntDesc(t *testing.T) {
	inpRows := [][]interface{}{
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{1, nil, nil, nil, nil, nil, nil},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
	}
	expRows := [][]interface{}{
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{1, nil, nil, nil, nil, nil, nil},
	}
	descending := []bool{
		true,
	}
	testSort(t, inpRows, expRows, sortColNames, sortColTypes, descending, common.NewColumnExpression(1, sortColTypes[1]))
}

func TestSortIntAsc(t *testing.T) {
	inpRows := [][]interface{}{
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{1, nil, nil, nil, nil, nil, nil},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
	}
	expRows := [][]interface{}{
		{0, nil, nil, nil, nil, nil, nil},
		{1, nil, nil, nil, nil, nil, nil},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
	}
	descending := []bool{
		false,
	}
	testSort(t, inpRows, expRows, sortColNames, sortColTypes, descending, common.NewColumnExpression(2, sortColTypes[2]))
}

func TestSortIntDesc(t *testing.T) {
	inpRows := [][]interface{}{
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{1, nil, nil, nil, nil, nil, nil},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
	}
	expRows := [][]interface{}{
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{1, nil, nil, nil, nil, nil, nil},
	}
	descending := []bool{
		true,
	}
	testSort(t, inpRows, expRows, sortColNames, sortColTypes, descending, common.NewColumnExpression(2, sortColTypes[2]))
}

func TestSortBigIntAsc(t *testing.T) {
	inpRows := [][]interface{}{
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{1, nil, nil, nil, nil, nil, nil},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
	}
	expRows := [][]interface{}{
		{0, nil, nil, nil, nil, nil, nil},
		{1, nil, nil, nil, nil, nil, nil},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
	}
	descending := []bool{
		false,
	}
	testSort(t, inpRows, expRows, sortColNames, sortColTypes, descending, common.NewColumnExpression(3, sortColTypes[3]))
}

func TestSortBigIntDesc(t *testing.T) {
	inpRows := [][]interface{}{
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{1, nil, nil, nil, nil, nil, nil},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
	}
	expRows := [][]interface{}{
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{1, nil, nil, nil, nil, nil, nil},
	}
	descending := []bool{
		true,
	}
	testSort(t, inpRows, expRows, sortColNames, sortColTypes, descending, common.NewColumnExpression(2, sortColTypes[2]))
}

func TestSortDoubleAsc(t *testing.T) {
	inpRows := [][]interface{}{
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{1, nil, nil, nil, nil, nil, nil},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
	}
	expRows := [][]interface{}{
		{0, nil, nil, nil, nil, nil, nil},
		{1, nil, nil, nil, nil, nil, nil},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
	}
	descending := []bool{
		false,
	}
	testSort(t, inpRows, expRows, sortColNames, sortColTypes, descending, common.NewColumnExpression(4, sortColTypes[4]))
}

func TestSortDoubleDesc(t *testing.T) {
	inpRows := [][]interface{}{
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{1, nil, nil, nil, nil, nil, nil},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
	}
	expRows := [][]interface{}{
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{1, nil, nil, nil, nil, nil, nil},
	}
	descending := []bool{
		true,
	}
	testSort(t, inpRows, expRows, sortColNames, sortColTypes, descending, common.NewColumnExpression(4, sortColTypes[4]))
}

func TestSortVarcharAsc(t *testing.T) {
	inpRows := [][]interface{}{
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{1, nil, nil, nil, nil, nil, nil},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
	}
	expRows := [][]interface{}{
		{0, nil, nil, nil, nil, nil, nil},
		{1, nil, nil, nil, nil, nil, nil},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
	}
	descending := []bool{
		false,
	}
	testSort(t, inpRows, expRows, sortColNames, sortColTypes, descending, common.NewColumnExpression(5, sortColTypes[5]))
}

func TestSortVarcharDesc(t *testing.T) {
	inpRows := [][]interface{}{
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{1, nil, nil, nil, nil, nil, nil},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
	}
	expRows := [][]interface{}{
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{1, nil, nil, nil, nil, nil, nil},
	}
	descending := []bool{
		true,
	}
	testSort(t, inpRows, expRows, sortColNames, sortColTypes, descending, common.NewColumnExpression(5, sortColTypes[5]))
}

func TestSortDecimalAsc(t *testing.T) {
	inpRows := [][]interface{}{
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{1, nil, nil, nil, nil, nil, nil},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
	}
	expRows := [][]interface{}{
		{0, nil, nil, nil, nil, nil, nil},
		{1, nil, nil, nil, nil, nil, nil},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
	}
	descending := []bool{
		false,
	}
	testSort(t, inpRows, expRows, sortColNames, sortColTypes, descending, common.NewColumnExpression(6, sortColTypes[6]))
}

func TestSortDecimalDesc(t *testing.T) {
	inpRows := [][]interface{}{
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{1, nil, nil, nil, nil, nil, nil},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
	}
	expRows := [][]interface{}{
		{9, 4, 40, 400, 40.01, "str7", "4000.01"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01"},
		{7, 2, 20, 200, 20.01, "str6", "2000.01"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01"},
		{5, 0, 0, 0, 0.0, "str4", "0.00"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01"},
		{0, nil, nil, nil, nil, nil, nil},
		{1, nil, nil, nil, nil, nil, nil},
	}
	descending := []bool{
		true,
	}
	testSort(t, inpRows, expRows, sortColNames, sortColTypes, descending, common.NewColumnExpression(6, sortColTypes[6]))
}

func TestSortTimestampAsc(t *testing.T) {
	tsSortColTypes := []common.ColumnType{common.BigIntColumnType, common.TinyIntColumnType, common.IntColumnType, common.BigIntColumnType, common.DoubleColumnType, common.VarcharColumnType, common.NewDecimalColumnType(10, 2), common.TimestampColumnType}
	tsSortColNames := []string{"pk_bigint", "c_tinyint", "c_int", "c_bigint", "c_double", "c_varchar", "c_decimal", "c_timestamp"}
	inpRows := [][]interface{}{
		{7, 2, 20, 200, 20.01, "str6", "2000.01", "2021-01-02"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01", "2021-01-03 01:02:03"},
		{0, nil, nil, nil, nil, nil, nil, nil},
		{5, 0, 0, 0, 0.0, "str4", "0.00", "2021-01-03 01:02:03.1234"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01", "1998-07-24"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01", "2021-01-02 12:34:56.123456"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01", "2021-01-01 12:34:56"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01", "2020-01-03"},
		{1, nil, nil, nil, nil, nil, nil, nil},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01", "2021-01-02 12:34:56"},
	}
	expRows := [][]interface{}{
		{0, nil, nil, nil, nil, nil, nil, nil},
		{1, nil, nil, nil, nil, nil, nil, nil},
		{9, 4, 40, 400, 40.01, "str7", "4000.01", "1998-07-24"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01", "2020-01-03"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01", "2021-01-01 12:34:56"},
		{7, 2, 20, 200, 20.01, "str6", "2000.01", "2021-01-02"},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01", "2021-01-02 12:34:56"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01", "2021-01-02 12:34:56.123456"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01", "2021-01-03 01:02:03"},
		{5, 0, 0, 0, 0.0, "str4", "0.00", "2021-01-03 01:02:03.1234"},
	}
	descending := []bool{
		false,
	}
	testSort(t, inpRows, expRows, tsSortColNames, tsSortColTypes, descending, common.NewColumnExpression(7, tsSortColTypes[7]))
}

func TestSortTimestampDesc(t *testing.T) {
	tsSortColTypes := []common.ColumnType{common.BigIntColumnType, common.TinyIntColumnType, common.IntColumnType, common.BigIntColumnType, common.DoubleColumnType, common.VarcharColumnType, common.NewDecimalColumnType(10, 2), common.TimestampColumnType}
	tsSortColNames := []string{"pk_bigint", "c_tinyint", "c_int", "c_bigint", "c_double", "c_varchar", "c_decimal", "c_timestamp"}
	inpRows := [][]interface{}{
		{7, 2, 20, 200, 20.01, "str6", "2000.01", "2021-01-02"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01", "2021-01-03 01:02:03"},
		{0, nil, nil, nil, nil, nil, nil, nil},
		{5, 0, 0, 0, 0.0, "str4", "0.00", "2021-01-03 01:02:03.1234"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01", "1998-07-24"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01", "2021-01-02 12:34:56.123456"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01", "2021-01-01 12:34:56"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01", "2020-01-03"},
		{1, nil, nil, nil, nil, nil, nil, nil},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01", "2021-01-02 12:34:56"},
	}
	expRows := [][]interface{}{
		{5, 0, 0, 0, 0.0, "str4", "0.00", "2021-01-03 01:02:03.1234"},
		{2, -3, -30, -300, -30.01, "str1", "-3000.01", "2021-01-03 01:02:03"},
		{3, -2, -20, -200, -20.01, "str2", "-2000.01", "2021-01-02 12:34:56.123456"},
		{4, -2, -20, -200, -20.01, "str2", "-2000.01", "2021-01-02 12:34:56"},
		{7, 2, 20, 200, 20.01, "str6", "2000.01", "2021-01-02"},
		{8, 4, 40, 400, 40.01, "str7", "4000.01", "2021-01-01 12:34:56"},
		{6, 1, 10, 100, 10.01, "str5", "1000.01", "2020-01-03"},
		{9, 4, 40, 400, 40.01, "str7", "4000.01", "1998-07-24"},
		{0, nil, nil, nil, nil, nil, nil, nil},
		{1, nil, nil, nil, nil, nil, nil, nil},
	}
	descending := []bool{
		true,
	}
	testSort(t, inpRows, expRows, tsSortColNames, tsSortColTypes, descending, common.NewColumnExpression(7, tsSortColTypes[7]))
}

func TestSortMultipleColumns(t *testing.T) {
	inpRows := [][]interface{}{
		{5, 0, -10, 0, -10.01, "str4", "-1000.01"},
		{7, 0, -20, 200, -30.01, "str6", "-3000.01"},
		{2, -3, 20, -300, 20.01, "str1", "2000.01"},
		{0, nil, 40, nil, 40.01, nil, "4000.01"},
		{4, -3, 10, -200, 0.00, "str2", "0.00"},
		{6, 0, -10, 100, -20.01, "str5", "-2000.01"},
		{8, 1, nil, 400, nil, "str7", nil},
		{1, nil, 30, nil, 40.01, nil, "4000.01"},
		{3, -3, 20, -200, 10.01, "str2", "1000.01"},
		{9, 1, -30, 400, nil, "str7", nil},
	}
	expRows := [][]interface{}{
		{0, nil, 40, nil, 40.01, nil, "4000.01"},
		{1, nil, 30, nil, 40.01, nil, "4000.01"},
		{2, -3, 20, -300, 20.01, "str1", "2000.01"},
		{3, -3, 20, -200, 10.01, "str2", "1000.01"},
		{4, -3, 10, -200, 0.00, "str2", "0.00"},
		{5, 0, -10, 0, -10.01, "str4", "-1000.01"},
		{6, 0, -10, 100, -20.01, "str5", "-2000.01"},
		{7, 0, -20, 200, -30.01, "str6", "-3000.01"},
		{9, 1, -30, 400, nil, "str7", nil},
		{8, 1, nil, 400, nil, "str7", nil},
	}
	descending := []bool{
		false, true, false, true, false, true,
	}
	colExprs := make([]*common.Expression, 6)
	for i := 0; i < 6; i++ {
		colExprs[i] = common.NewColumnExpression(i+1, sortColTypes[i+1])
	}
	testSort(t, inpRows, expRows, sortColNames, sortColTypes, descending, colExprs...)
}

func TestSortNonColExpression(t *testing.T) {
	inpRows := [][]interface{}{
		{3, 1, 9},
		{0, 2, 3},
		{1, 6, 4},
		{6, 8, 2},
		{2, -1, 6},
		{4, 5, 5},
		{7, 3, 7},
	}
	expRows := [][]interface{}{
		{3, 1, 9},
		{0, 2, 3},
		{2, -1, 6},
		{7, 3, 7},
		{1, 6, 4},
		{6, 8, 2},
		{4, 5, 5},
	}
	descending := []bool{
		false,
	}
	f, err := common.NewScalarFunctionExpression(common.IntColumnType, "ge", common.NewColumnExpression(1, common.IntColumnType), common.NewColumnExpression(2, common.IntColumnType))
	require.NoError(t, err)
	testSort(t, inpRows, expRows, []string{"a", "b", "c"}, []common.ColumnType{common.IntColumnType, common.IntColumnType, common.IntColumnType}, descending, f)
}

func testSort(t *testing.T, inpRows [][]interface{}, expRows [][]interface{}, sortColNames []string, sortColTypes []common.ColumnType, descending []bool, sortByExprs ...*common.Expression) {
	t.Helper()
	sort := setupSort(t, inpRows, sortColNames, sortColTypes, descending, sortByExprs...)
	rows, err := sort.GetRows(1000)
	require.NoError(t, err)
	require.Equal(t, len(expRows), rows.RowCount())
	expected := toRows(t, expRows, sortColTypes)
	commontest.AllRowsEqual(t, expected, rows, sortColTypes)
}

func setupSort(t *testing.T, inputRows [][]interface{}, colNames []string, colTypes []common.ColumnType, descending []bool, sortByExprs ...*common.Expression) PullExecutor {
	t.Helper()

	sort := NewPullSort(colNames, colTypes, descending, sortByExprs)
	inpRows := toRows(t, inputRows, colTypes)
	rf := common.NewRowsFactory(colTypes)
	rowsProvider := rowProvider{
		rowsFactory: rf,
		rows:        inpRows,
	}
	sort.AddChild(&rowsProvider)

	return sort
}
