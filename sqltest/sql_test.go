package sqltest

import (
	"bufio"
	"fmt"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/server"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type sqlTestsuite struct {
	pranaCluster []*server.Server
	suite        suite.Suite
	tests        map[string]*sqlTest
	t            *testing.T
	dataDir      string
}

func (w *sqlTestsuite) T() *testing.T {
	return w.suite.T()
}

func (w *sqlTestsuite) SetT(t *testing.T) {
	t.Helper()
	w.suite.SetT(t)
}

func TestSqlFakeCluster(t *testing.T) {
	testSQL(t, true, 1)
}

func TestSqlClustered(t *testing.T) {
	testSQL(t, false, 3)
}

func testSQL(t *testing.T, fakeCluster bool, numNodes int) {
	t.Helper()
	ts := &sqlTestsuite{tests: make(map[string]*sqlTest), t: t}
	ts.setup(fakeCluster, numNodes)
	defer ts.teardown()
	suite.Run(t, ts)
}

func (w *sqlTestsuite) TestSql() {
	for testName, sTest := range w.tests {
		w.suite.Run(testName, sTest.run)
	}
}

func (w *sqlTestsuite) setupPranaCluster(fakeCluster bool, numNodes int) {

	if fakeCluster && numNodes != 1 {
		log.Fatal("fake cluster only supports one node")
	}
	w.pranaCluster = make([]*server.Server, numNodes)
	if fakeCluster {
		s, err := server.NewServer(server.Config{
			NodeID:     0,
			NumShards:  10,
			TestServer: true,
		})
		if err != nil {
			log.Fatal(err)
		}
		w.pranaCluster[0] = s
	} else {
		nodeAddresses := []string{
			"localhost:63201",
			"localhost:63202",
			"localhost:63203",
		}
		dataDir, err := ioutil.TempDir("", "sql-test")
		if err != nil {
			log.Fatal(err)
		}
		w.dataDir = dataDir
		for i := 0; i < numNodes; i++ {
			s, err := server.NewServer(server.Config{
				NodeID:            i,
				ClusterID:         12345678,
				NodeAddresses:     nodeAddresses,
				NumShards:         30,
				ReplicationFactor: 3,
				DataDir:           dataDir,
				TestServer:        false,
			})
			if err != nil {
				log.Fatal(err)
			}
			w.pranaCluster[i] = s
		}
	}

	for _, prana := range w.pranaCluster {
		err := prana.Start()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (w *sqlTestsuite) setup(fakeCluster bool, numNodes int) {
	w.setupPranaCluster(fakeCluster, numNodes)

	files, err := ioutil.ReadDir("./testdata")
	if err != nil {
		log.Fatal(err)
	}
	sort.SliceStable(files, func(i, j int) bool {
		return strings.Compare(files[i].Name(), files[j].Name()) < 0
	})
	currTestName := ""
	currSQLTest := &sqlTest{}
	for _, file := range files {
		fileName := file.Name()
		if !strings.HasSuffix(fileName, "_test_data.txt") && !strings.HasSuffix(fileName, "_test_out.txt") && !strings.HasSuffix(fileName, "_test_script.txt") {
			log.Fatalf("test file %s has invalid name. test files should be of the form <test_name>_test_data.txt, <test_name>_test_script.txt or <test_name>_test_out.txt,", fileName)
		}
		if (currTestName == "") || !strings.HasPrefix(fileName, currTestName) {
			index := strings.Index(fileName, "_test_")
			if index == -1 {
				log.Fatalf("invalid test file %s", fileName)
			}
			currTestName = fileName[:index]
			currSQLTest = &sqlTest{testName: currTestName, testSuite: w, rnd: rand.New(rand.NewSource(time.Now().UTC().UnixNano()))}
			w.tests[currTestName] = currSQLTest
		}
		if strings.HasSuffix(fileName, "_test_data.txt") {
			currSQLTest.testDataFile = fileName
		} else if strings.HasSuffix(fileName, "_test_out.txt") {
			currSQLTest.outFile = fileName
		} else if strings.HasSuffix(fileName, "_test_script.txt") {
			currSQLTest.scriptFile = fileName
		}
	}
}

func (w *sqlTestsuite) teardown() {
	for _, prana := range w.pranaCluster {
		err := prana.Stop()
		if err != nil {
			log.Fatal(err)
		}
	}
	if w.dataDir != "" {
		err := os.RemoveAll(w.dataDir)
		if err != nil {
			log.Fatal(err)
		}
	}
}

type sqlTest struct {
	testSuite    *sqlTestsuite
	testName     string
	scriptFile   string
	testDataFile string
	outFile      string
	output       *strings.Builder
	rnd          *rand.Rand
}

func (st *sqlTest) run() {
	require := st.testSuite.suite.Require()
	start := time.Now()

	require.NotEmpty(st.scriptFile, fmt.Sprintf("sql test %s is missing script file %s", st.testName, fmt.Sprintf("%s_test_script.txt", st.testName)))
	require.NotEmpty(st.testDataFile, fmt.Sprintf("sql test %s is missing test data file %s", st.testName, fmt.Sprintf("%s_test_data.txt", st.testName)))
	require.NotEmpty(st.outFile, fmt.Sprintf("sql test %s is missing out file %s", st.testName, fmt.Sprintf("%s_test_out.txt", st.testName)))

	log.Printf("**** in run for test %s %s %s %s", st.testName, st.scriptFile, st.testDataFile, st.outFile)

	scriptFile, closeFunc := openFile("./testdata/" + st.scriptFile)
	defer closeFunc()

	st.output = &strings.Builder{}

	b, err := ioutil.ReadAll(scriptFile)
	require.NoError(err)

	scriptContents := string(b)

	// All executable script commands whether they are special comments or sql statements must end with semi-colon followed by newline
	commands := strings.Split(scriptContents, ";\n")
	for _, command := range commands {
		command = trimBothEnds(command)
		if command == "" {
			continue
		}
		if strings.HasPrefix(command, "--load data") {
			st.executeLoadData(require, command)
			log.Println("loaded dataset ok")
		} else if strings.HasPrefix(command, "--") {
			// Just a normal comment - ignore
		} else {
			// sql statement
			st.executeSQLStatement(require, command)
		}
	}

	log.Printf("Test output is:\n%s", st.output.String())

	outfile, closeFunc := openFile("./testdata/" + st.outFile)
	defer closeFunc()
	b, err = ioutil.ReadAll(outfile)
	require.NoError(err)
	expectedOutput := string(b)
	actualOutput := st.output.String()
	end := time.Now()
	dur := end.Sub(start)
	log.Printf("Test took %d ms to run", dur.Milliseconds())
	st.testSuite.suite.Require().Equal(trimBothEnds(expectedOutput), trimBothEnds(actualOutput))
}

type dataset struct {
	name       string
	sourceInfo *common.SourceInfo
	colTypes   []common.ColumnType
	rows       *common.Rows
}

func (st *sqlTest) loadDataset(require *require.Assertions, fileName string, dsName string) *dataset {
	dataFile, closeFunc := openFile("./testdata/" + st.testDataFile)
	defer closeFunc()
	scanner := bufio.NewScanner(dataFile)
	var currDataSet *dataset
	lineNum := 1
	for scanner.Scan() {
		line := scanner.Text()
		line = trimBothEnds(line)
		if strings.HasPrefix(line, "dataset:") {
			line = line[8:]
			parts := strings.Split(line, " ")
			require.Equal(2, len(parts), fmt.Sprintf("invalid dataset line in file %s: %s", fileName, line))
			dataSetName := parts[0]
			if dsName != dataSetName {
				if currDataSet != nil {
					return currDataSet
				}
				continue
			}
			sourceName := parts[1]
			sourceInfo, ok := st.choosePrana().GetMetaController().GetSource("test", sourceName)
			require.True(ok, fmt.Sprintf("unknown source %s", sourceName))
			rf := common.NewRowsFactory(sourceInfo.TableInfo.ColumnTypes)
			rows := rf.NewRows(100)
			currDataSet = &dataset{name: dataSetName, sourceInfo: sourceInfo, rows: rows, colTypes: sourceInfo.TableInfo.ColumnTypes}
		} else {
			if currDataSet == nil {
				continue
			}
			parts := strings.Split(line, ",")
			require.Equal(len(currDataSet.colTypes), len(parts), fmt.Sprintf("source %s has %d columns but data has %d columns at line %d in file %s", currDataSet.sourceInfo.Name, len(currDataSet.colTypes), len(parts), lineNum, fileName))
			for i, colType := range currDataSet.colTypes {
				part := parts[i]
				if part == "" {
					currDataSet.rows.AppendNullToColumn(i)
				} else {
					switch colType.Type {
					case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
						val, err := strconv.ParseInt(part, 10, 64)
						require.NoError(err)
						currDataSet.rows.AppendInt64ToColumn(i, val)
					case common.TypeDouble:
						val, err := strconv.ParseFloat(part, 64)
						require.NoError(err)
						currDataSet.rows.AppendFloat64ToColumn(i, val)
					case common.TypeVarchar:
						currDataSet.rows.AppendStringToColumn(i, part)
					case common.TypeDecimal:
						val, err := common.NewDecFromString(part)
						require.NoError(err)
						currDataSet.rows.AppendDecimalToColumn(i, *val)
					default:
						require.Fail(fmt.Sprintf("unexpected data type %d", colType.Type))
					}
				}
			}
		}
		lineNum++
	}
	require.NotNil(currDataSet, fmt.Sprintf("Cannot find dataset %s in test data file %s", dsName, fileName))
	return currDataSet
}

func (st *sqlTest) executeLoadData(require *require.Assertions, command string) {
	log.Printf("Executing load data %s", command)
	start := time.Now()
	datasetName := command[12:]
	dataset := st.loadDataset(require, st.testDataFile, datasetName)
	engine := st.choosePrana().GetPushEngine()
	err := engine.IngestRows(dataset.rows, dataset.sourceInfo.TableInfo.ID)
	require.NoError(err)
	st.waitForProcessingToComplete(require)
	end := time.Now()
	dur := end.Sub(start)
	log.Printf("Load data %s execute time ms %d", command, dur.Milliseconds())
}

func (st *sqlTest) waitForProcessingToComplete(require *require.Assertions) {
	for _, prana := range st.testSuite.pranaCluster {
		err := prana.GetPushEngine().WaitForProcessingToComplete()
		require.NoError(err)
	}
}

func (st *sqlTest) executeSQLStatement(require *require.Assertions, statement string) {
	log.Printf("sqltest execute statement %s", statement)
	start := time.Now()
	exec, err := st.choosePrana().CreateSession("test").ExecuteSQLStatement(statement)
	require.NoError(err)
	rows, err := exec.GetRows(100000)
	require.NoError(err)
	isQuery := strings.HasPrefix(strings.ToLower(statement), "select ")
	if !isQuery {
		// DDL
		st.output.WriteString(statement + ";\n")
		st.output.WriteString("Ok\n")
	} else {
		// Query
		st.output.WriteString(statement + ";\n")
		for i := 0; i < rows.RowCount(); i++ {
			row := rows.GetRow(i)
			st.output.WriteString(row.String() + "\n")
		}
		st.output.WriteString(fmt.Sprintf("%d rows returned\n", rows.RowCount()))
	}
	end := time.Now()
	dur := end.Sub(start)
	log.Printf("Statement %s execute time ms %d", statement, dur.Milliseconds())
}

func (st *sqlTest) choosePrana() *server.Server {
	pranas := st.testSuite.pranaCluster
	lp := len(pranas)
	if lp == 1 {
		return pranas[0]
	}
	// We choose a Prana server randomly for executing statements - statements should work consistently irrespective
	// of what server they are run on
	index := st.rnd.Int31n(int32(lp))
	return pranas[index]
}

func trimBothEnds(str string) string {
	str = strings.TrimLeft(str, " \t\n")
	str = strings.TrimRight(str, " \t\n")
	return str
}

func openFile(fileName string) (*os.File, func()) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	closeFunc := func() {
		if err = file.Close(); err != nil {
			log.Fatal(err)
		}
	}
	return file, closeFunc
}
