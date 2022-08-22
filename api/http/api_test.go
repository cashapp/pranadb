package http

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/cluster/fake"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/execctx"
	"github.com/squareup/pranadb/internal/testcerts"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/pull/exec"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const testSchemaName = "test_schema"

var (
	serverKeyPath  string
	serverCertPath string
)

func TestMain(m *testing.M) {
	// Set up all the certs we need here. Creating certs is slow, we don't want to do it on each test

	log.Info("creating certificates required for tests")

	tmpDir, err := ioutil.TempDir("", "cli_test")
	if err != nil {
		log.Fatalf("failed to create tmp dir %v", err)
	}

	serverCertPath, serverKeyPath, err = testcerts.CreateCertKeyPairToTmpFile(tmpDir, nil, "acme badgers ltd.")
	if err != nil {
		log.Fatalf("failed to cert key pair %v", err)
	}

	defer func() {
		defer func() {
			err := os.RemoveAll(tmpDir)
			if err != nil {
				log.Fatalf("failed to remove test tmpdir %v", err)
			}
		}()
	}()
	m.Run()
}

func TestExecuteQuery(t *testing.T) {
	ex, err := createTestSQLExec()
	require.NoError(t, err)
	server := startServer(t, ex)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, true)

	stmt := "select * from my_table"

	resp, err := client.Get(fmt.Sprintf("https://localhost:6888/pranadb?stmt=%s&schema=%s", url.QueryEscape(stmt), testSchemaName))
	require.NoError(t, err)
	defer closeRespBody(t, resp)

	sql, args, argTypes, schema, rows := ex.GetState()
	require.Equal(t, stmt, sql)
	require.Nil(t, args)
	require.Nil(t, argTypes)
	require.Equal(t, testSchemaName, schema)

	verifyExpectedRows(t, rows, resp)
}

func closeRespBody(t *testing.T, resp *http.Response) {
	t.Helper()
	if resp != nil {
		err := resp.Body.Close()
		require.NoError(t, err)
	}
}

func TestExecuteQueryWithColumnHeaders(t *testing.T) {
	ex, err := createTestSQLExec()
	require.NoError(t, err)
	server := startServer(t, ex)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, true)

	stmt := "select * from my_table"
	schemaName := testSchemaName

	//nolint:bodyclose
	resp, err := client.Get(fmt.Sprintf("https://localhost:6888/pranadb?stmt=%s&schema=%s&colheaders=true",
		url.QueryEscape(stmt), schemaName))
	require.NoError(t, err)
	defer closeRespBody(t, resp)

	sql, args, argTypes, schema, rows := ex.GetState()
	require.Equal(t, stmt, sql)
	require.Nil(t, args)
	require.Nil(t, argTypes)
	require.Equal(t, schemaName, schema)

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyString := string(bodyBytes)
	parts := strings.Split(bodyString, "\n")
	require.Greater(t, len(parts), 2)
	colNames := parts[0]
	colTypes := parts[1]
	expectedColNames := "[\"big_int_col\",\"int_col\",\"tiny_int_col\",\"double_col\",\"varchar_col\",\"decimal_col\",\"timestamp_col\"]"
	require.Equal(t, expectedColNames, colNames)
	expectedColTypes := "[\"bigint\",\"int\",\"tinyint\",\"double\",\"varchar\",\"decimal(10,2)\",\"timestamp(6)\"]"
	require.Equal(t, expectedColTypes, colTypes)

	i1 := strings.IndexRune(bodyString, '\n')
	i2 := strings.IndexRune(bodyString[i1+1:], '\n')
	restOfBody := bodyString[i1+i2+2:]
	verifyExpectedRowsFromBodyString(t, rows, restOfBody)
}

func TestExecutePreparedStatement(t *testing.T) {
	sargTypes := "tinyint,int,bigint,double,varchar,\"decimal(25,4)\",timestamp(4)"
	sargs := "123,123456,123456789876543,123.23,badgers,1000.1234,2022-04-03 12:34:12.123456"
	expectedArgTypes := []common.ColumnType{common.TinyIntColumnType, common.IntColumnType, common.BigIntColumnType,
		common.DoubleColumnType, common.VarcharColumnType, common.NewDecimalColumnType(25, 4), common.NewTimestampColumnType(4)}
	expectedArgs := []interface{}{int64(123), int64(123456), int64(123456789876543), float64(123.23), "badgers", "1000.1234", "2022-04-03 12:34:12.123456"}
	testExecutePreparedStatement(t, sargTypes, sargs, expectedArgTypes, expectedArgs)
}

func TestExecutePreparedStatementArgWithQuote(t *testing.T) {
	sarg := "here is a \"quote\""
	testExecutePreparedStatementArgThatNeedsEscaping(t, sarg)
}

func TestExecutePreparedStatementArgWithCR(t *testing.T) {
	sarg := "this has a new line\nin it"
	testExecutePreparedStatementArgThatNeedsEscaping(t, sarg)
}

func testExecutePreparedStatementArgThatNeedsEscaping(t *testing.T, sarg string) {
	t.Helper()
	sargTypes := "varchar"
	quoted := common.CSVQuote(sarg)
	quoted = quoted[:len(quoted)-1] // remove trailing CR
	expectedArgTypes := []common.ColumnType{common.VarcharColumnType}
	expectedArgs := []interface{}{sarg}
	testExecutePreparedStatement(t, sargTypes, quoted, expectedArgTypes, expectedArgs)
}

func testExecutePreparedStatement(t *testing.T, sargTypes string, sargs string, expectedArgTypes []common.ColumnType,
	expectedArgs []interface{}) {
	t.Helper()
	ex, err := createTestSQLExec()
	require.NoError(t, err)
	server := startServer(t, ex)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, true)

	stmt := "select * from my_table where col1=? and col2=? and col3=? and col4=?"
	schemaName := testSchemaName

	sargTypes = url.QueryEscape(sargTypes)
	sargs = url.QueryEscape(sargs)
	resp, err := client.Get(fmt.Sprintf("https://localhost:6888/pranadb?stmt=%s&schema=%s&argtypes=%s&args=%s",
		url.QueryEscape(stmt), schemaName, sargTypes, sargs))
	require.NoError(t, err)
	defer closeRespBody(t, resp)

	require.Equal(t, http.StatusOK, resp.StatusCode)

	sql, argTypes, args, schema, rows := ex.GetState()
	require.Equal(t, expectedArgs, args)
	require.Equal(t, expectedArgTypes, argTypes)
	require.Equal(t, stmt, sql)
	require.Equal(t, schemaName, schema)

	verifyExpectedRows(t, rows, resp)
}

func TestMissingSQL(t *testing.T) {
	testErrorResponse(t, "https://localhost:6888/pranadb?schema=test_schema",
		"missing 'stmt' query parameter\n", http.StatusBadRequest, true, false)
}

func TestMissingSchema(t *testing.T) {
	testErrorResponse(t, "https://localhost:6888/pranadb?stmt=foo",
		"missing 'schema' query parameter\n", http.StatusBadRequest, true, false)
}

func TestHttp2Only(t *testing.T) {
	testErrorResponse(t, "https://localhost:6888/pranadb?schema=test_schema",
		"the pranadb HTTP API supports HTTP2 only\n", http.StatusHTTPVersionNotSupported, false, false)
}

func TestWrongMethod(t *testing.T) {
	testErrorResponse(t, "https://localhost:6888/pranadb?schema=test_schema",
		"", http.StatusMethodNotAllowed, true, true)
}

func TestWrongPath(t *testing.T) {
	testErrorResponse(t, "https://localhost:6888/some/other/path?stmt=foo&schema=test_schema",
		"404 page not found\n", http.StatusNotFound, true, false)
}

func TestDifferentNumsPSArgsAndArgTypes(t *testing.T) {
	testErrorResponse(t, "https://localhost:6888/pranadb?schema=test_schema&stmt=foo&argtypes=bigint,int,varchar&args=1,4,5,6,7,8,9",
		"number of argtypes does not match number of args\n", http.StatusBadRequest, true, false)
}

func TestInvalidPSArgTypes(t *testing.T) {
	argTypes := "qwudhqwuidh\""
	testErrorResponse(t, fmt.Sprintf("https://localhost:6888/pranadb?schema=test_schema&stmt=foo&argtypes=%s&args=1,2,3", argTypes),
		"invalid arg types qwudhqwuidh\"\n", http.StatusBadRequest, true, false)
}

func TestUnknownPSArgType(t *testing.T) {
	argTypes := "bigint,mediumint,varchar"
	testErrorResponse(t, fmt.Sprintf("https://localhost:6888/pranadb?schema=test_schema&stmt=foo&argtypes=%s&args=1,2,foo", argTypes),
		"invalid argtype mediumint\n", http.StatusBadRequest, true, false)
}

func TestInvalidPSArgs(t *testing.T) {
	args := "qwudhqwuidh\""
	testErrorResponse(t, fmt.Sprintf("https://localhost:6888/pranadb?schema=test_schema&stmt=foo&argtypes=bigint,int,varchar&args=%s", args),
		"invalid args qwudhqwuidh\"\n", http.StatusBadRequest, true, false)
}

func TestInvalidPSDecimalArgType(t *testing.T) {
	testInvalidPSArgTypeDecimal(t, "decimal(", "invalid decimal argument type: decimal(\n")
	testInvalidPSArgTypeDecimal(t, "decimal()", "invalid decimal argument type: decimal()\n")
	testInvalidPSArgTypeDecimal(t, "decimal(foo)", "invalid decimal argument type: decimal(foo)\n")
	testInvalidPSArgTypeDecimal(t, "decimal(foo,bar)", "invalid decimal precision, not a valid integer foo\n")
}

func testInvalidPSArgTypeDecimal(t *testing.T, argType string, errMsg string) {
	t.Helper()
	escArgType := url.QueryEscape(common.CSVQuote(argType))
	testErrorResponse(t, fmt.Sprintf("https://localhost:6888/pranadb?schema=test_schema&stmt=foo&argtypes=%s&args=123.123", escArgType),
		errMsg, http.StatusBadRequest, true, false)
}

func TestInvalidPSDecimalArgPrecision(t *testing.T) {
	testInvalidPSDecimalArgPrecision(t, "decimal(1000,1)")
	testInvalidPSDecimalArgPrecision(t, "decimal(0,1)")
	testInvalidPSDecimalArgPrecision(t, "decimal(-1,1)")
	testInvalidPSDecimalArgPrecision(t, "decimal(66,1)")
}

func TestInvalidPSDecimalArgScale(t *testing.T) {
	testInvalidPSDecimalArgScale(t, "decimal(23,-1)")
	testInvalidPSDecimalArgScale(t, "decimal(23,31)")
}

func testInvalidPSDecimalArgPrecision(t *testing.T, argType string) {
	t.Helper()
	escArgType := url.QueryEscape(common.CSVQuote(argType))
	testErrorResponse(t, fmt.Sprintf("https://localhost:6888/pranadb?schema=test_schema&stmt=foo&argtypes=%s&args=123.123", escArgType),
		fmt.Sprintf("invalid decimal precision, must be > 1 and <= 65 %s\n", argType), http.StatusBadRequest, true, false)
}

func testInvalidPSDecimalArgScale(t *testing.T, argType string) {
	t.Helper()
	escArgType := url.QueryEscape(common.CSVQuote(argType))
	testErrorResponse(t, fmt.Sprintf("https://localhost:6888/pranadb?schema=test_schema&stmt=foo&argtypes=%s&args=123.123", escArgType),
		fmt.Sprintf("invalid decimal scale, must be > 0 and <= 30 %s\n", argType), http.StatusBadRequest, true, false)
}

func TestInvalidPSTimestampArgType(t *testing.T) {
	testInvalidPSArgTypeTimestamp(t, "timestamp", "invalid timestamp argument type: timestamp\n")
	testInvalidPSArgTypeTimestamp(t, "timestamp()", "invalid timestamp argument type: timestamp()\n")
	testInvalidPSArgTypeTimestamp(t, "timestamp(-1)", "invalid timestamp fsp, must be >= 0 and <= 6 :timestamp(-1)\n")
	testInvalidPSArgTypeTimestamp(t, "timestamp(10)", "invalid timestamp fsp, must be >= 0 and <= 6 :timestamp(10)\n")
	testInvalidPSArgTypeTimestamp(t, "timestamp(4,4)", "invalid timestamp fsp, not a valid integer 4,4\n")
}

func testInvalidPSArgTypeTimestamp(t *testing.T, argType string, errMsg string) {
	t.Helper()
	escArgType := url.QueryEscape(common.CSVQuote(argType))
	arg := url.QueryEscape("2022-06-04 13:54:23.364935")
	testErrorResponse(t, fmt.Sprintf("https://localhost:6888/pranadb?schema=test_schema&stmt=foo&argtypes=%s&args=%s", escArgType, arg),
		errMsg, http.StatusBadRequest, true, false)
}

func TestInvalidArgs(t *testing.T) {
	testInvalidArg(t, "tinyint", "xyz", "invalid integer argument xyz\n")
	testInvalidArg(t, "tinyint", "1.23", "invalid integer argument 1.23\n")
	testInvalidArg(t, "tinyint", "-129", "tinyint value out of range -129\n")
	testInvalidArg(t, "tinyint", "128", "tinyint value out of range 128\n")
	testInvalidArg(t, "tinyint", "71281278367123671237123712378612736178236781263871623871627836", "invalid integer argument 71281278367123671237123712378612736178236781263871623871627836\n")

	testInvalidArg(t, "int", "xyz", "invalid integer argument xyz\n")
	testInvalidArg(t, "int", "1.23", "invalid integer argument 1.23\n")
	testInvalidArg(t, "int", "-2147483649", "int value out of range -2147483649\n")
	testInvalidArg(t, "int", "2147483648", "int value out of range 2147483648\n")
	testInvalidArg(t, "int", "71281278367123671237123712378612736178236781263871623871627836", "invalid integer argument 71281278367123671237123712378612736178236781263871623871627836\n")

	testInvalidArg(t, "bigint", "xyz", "invalid integer argument xyz\n")
	testInvalidArg(t, "bigint", "1.23", "invalid integer argument 1.23\n")
	testInvalidArg(t, "bigint", "71281278367123671237123712378612736178236781263871623871627836", "invalid integer argument 71281278367123671237123712378612736178236781263871623871627836\n")

	testInvalidArg(t, "double", "xyz", "invalid double argument xyz\n")
}

func testInvalidArg(t *testing.T, argType string, arg string, errMsg string) {
	t.Helper()
	arg = url.QueryEscape(arg)
	argType = url.QueryEscape(argType)
	testErrorResponse(t, fmt.Sprintf("https://localhost:6888/pranadb?schema=test_schema&stmt=foo&argtypes=%s&args=%s", argType, arg),
		errMsg, http.StatusBadRequest, true, false)
}

func testErrorResponse(t *testing.T, query string, errorMsg string, statusCode int, http2 bool, head bool) {
	t.Helper()
	ex, err := createTestSQLExec()
	require.NoError(t, err)
	server := startServer(t, ex)
	defer func() {
		err := server.Stop()
		require.NoError(t, err)
	}()
	client := createClient(t, http2)

	var resp *http.Response
	if head {
		resp, err = client.Head(query) //nolint:bodyclose
	} else {
		resp, err = client.Get(query) //nolint:bodyclose
	}
	require.NoError(t, err)
	defer closeRespBody(t, resp)
	require.Equal(t, statusCode, resp.StatusCode)
	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyString := string(bodyBytes)
	require.Equal(t, errorMsg, bodyString)
	require.NoError(t, err)
}

func startServer(t *testing.T, ex *testSQLExecutor) *HTTPAPIServer {
	t.Helper()
	cluster := fake.NewFakeCluster(0, 10)
	metaController := meta.NewController(cluster)
	tlsConf := conf.TLSConfig{
		EnableTLS: true,
		KeyPath:   serverKeyPath,
		CertPath:  serverCertPath,
	}
	server := NewHTTPAPIServer("localhost:6888", "/pranadb", ex, metaController, nil, tlsConf)
	err := server.Start()
	require.NoError(t, err)
	return server
}

func createClient(t *testing.T, enableHTTP2 bool) *http.Client {
	t.Helper()
	// Create config for the test client
	caCert, err := ioutil.ReadFile(serverCertPath)
	require.NoError(t, err)
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		InsecureSkipVerify: true, //nolint:gosec
	}
	client := &http.Client{}
	if enableHTTP2 {
		client.Transport = &http2.Transport{
			TLSClientConfig: tlsConfig,
		}
	} else {
		client.Transport = &http.Transport{
			TLSNextProto:    make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
			TLSClientConfig: tlsConfig,
		}
	}
	return client
}

func verifyExpectedRows(t *testing.T, expectedRows *common.Rows, resp *http.Response) {
	t.Helper()
	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyString := string(bodyBytes)
	verifyExpectedRowsFromBodyString(t, expectedRows, bodyString)
}

func verifyExpectedRowsFromBodyString(t *testing.T, expectedRows *common.Rows, bodyString string) {
	t.Helper()
	lines := strings.Split(bodyString, "\n")
	// Newline at end so we omit the last one which will be empty
	lines = lines[:len(lines)-1]
	require.Equal(t, expectedRows.RowCount(), len(lines))
	for i, line := range lines {
		arr := make([]interface{}, len(expectedRows.ColumnTypes()))
		err := json.Unmarshal([]byte(line), &arr)
		require.NoError(t, err)
		expRow := expectedRows.GetRow(i)
		for j, colType := range expectedRows.ColumnTypes() {
			if j == 1 && i%2 == 1 {
				require.Nil(t, arr[j])
				continue
			}
			switch colType.Type {
			case common.TypeBigInt, common.TypeInt, common.TypeTinyInt:
				expVal := expRow.GetInt64(j)
				require.Equal(t, float64(expVal), arr[j])
			case common.TypeDouble:
				expVal := expRow.GetFloat64(j)
				require.Equal(t, expVal, arr[j])
			case common.TypeDecimal:
				expVal := expRow.GetDecimal(j)
				require.Equal(t, expVal.String(), arr[j])
			case common.TypeTimestamp:
				expVal := expRow.GetTimestamp(j)
				gt, err := expVal.GoTime(time.UTC)
				require.NoError(t, err)
				val := gt.UnixNano() / 1000
				require.Equal(t, float64(val), arr[j])
			case common.TypeVarchar:
				expVal := expRow.GetString(j)
				require.Equal(t, expVal, arr[j])
			default:
				panic("unexpected col type")
			}
		}
	}
}

func createTestSQLExec() (*testSQLExecutor, error) {
	numRows := 10
	rows, err := createRows(numRows)
	if err != nil {
		return nil, err
	}
	colNames := []string{"big_int_col", "int_col", "tiny_int_col", "double_col", "varchar_col", "decimal_col", "timestamp_col"}
	sr, err := exec.NewStaticRows(colNames, rows)
	if err != nil {
		return nil, err
	}
	return &testSQLExecutor{pe: sr, rows: rows}, nil
}

func createRows(numRows int) (*common.Rows, error) {
	colTypes := []common.ColumnType{common.BigIntColumnType, common.IntColumnType, common.TinyIntColumnType, common.DoubleColumnType,
		common.VarcharColumnType, common.NewDecimalColumnType(10, 2), common.NewTimestampColumnType(6)}
	rf := common.NewRowsFactory(colTypes)
	rows := rf.NewRows(numRows)
	for i := 0; i < numRows; i++ {
		rows.AppendInt64ToColumn(0, int64(1000000+i))
		if i%2 == 0 {
			rows.AppendInt64ToColumn(1, int64(1000+i))
		} else {
			rows.AppendNullToColumn(1)
		}
		rows.AppendInt64ToColumn(2, int64(i%127))
		rows.AppendFloat64ToColumn(3, float64(i)+0.12345)
		rows.AppendStringToColumn(4, fmt.Sprintf("foobar-%d", i))
		dec, err := common.NewDecFromString(fmt.Sprintf("%d123456789.987654321", i))
		if err != nil {
			return nil, err
		}
		rows.AppendDecimalToColumn(5, *dec)
		ts, err := common.NewTimestampFromString("2012-03-21 13:23:05.123456")
		if err != nil {
			return nil, err
		}
		rows.AppendTimestampToColumn(6, ts)
	}
	return rows, nil
}

type testSQLExecutor struct {
	lock     sync.Mutex
	sql      string
	argTypes []common.ColumnType
	args     []interface{}
	schema   string
	pe       exec.PullExecutor
	rows     *common.Rows
}

func (t *testSQLExecutor) ExecuteSQLStatement(execCtx *execctx.ExecutionContext, sql string, argTypes []common.ColumnType, args []interface{}) (exec.PullExecutor, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.sql = sql
	t.argTypes = argTypes
	t.args = args
	t.schema = execCtx.Schema.Name
	return t.pe, nil
}

func (t *testSQLExecutor) CreateExecutionContext(schema *common.Schema) *execctx.ExecutionContext {
	return execctx.NewExecutionContext("testcontext", schema)
}

func (t *testSQLExecutor) GetState() (sql string, argTypes []common.ColumnType, args []interface{}, schema string, rows *common.Rows) {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.sql, t.argTypes, t.args, t.schema, t.rows
}

func TestFoo(t *testing.T) {
	s := "[123,3.45,\"foo\"]"

	arr := make([]interface{}, 3)
	arr[0] = &intUnmarshaller{}

	err := json.Unmarshal([]byte(s), &arr)
	require.NoError(t, err)

	log.Printf("arr is %v", arr)
}

type intUnmarshaller struct {
	iVal int64
}

func (r *intUnmarshaller) UnmarshalJSON(i []byte) error {
	s := string(i)
	in, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return err
	}
	r.iVal = in
	return nil
}
