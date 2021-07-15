package sqltest

import (
	"bufio"
	"fmt"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/server"
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type sqlTestsuite struct {
	prana *server.Server
	suite suite.Suite
	testdataDir string
	tests map[string]*sqlTest
	t *testing.T
}

func (e *sqlTestsuite) T() *testing.T {
	return e.suite.T()
}

func (e *sqlTestsuite) SetT(t *testing.T) {
	e.suite.SetT(t)
}

func TestSql(t *testing.T) {
	ts := &sqlTestsuite{tests: make(map[string]*sqlTest), t:t}
	ts.setup()
	defer ts.teardown()
	suite.Run(t, ts)
}

func (e *sqlTestsuite) TestSql() {
	for testName, sTest := range e.tests {
		e.suite.Run(testName, sTest.run)
	}
}

func (w *sqlTestsuite) setupPrana() *server.Server {
	server, err := server.NewServer(server.Config{
		NodeID:            0,
		NumShards:         10,
		TestServer:        true,
	})
	if err != nil {
		log.Fatal(err)
	}
	err = server.Start()
	if err != nil {
		log.Fatal(err)
	}
	return server
}

func (w *sqlTestsuite) setup() {

	w.prana = w.setupPrana()

	files, err := ioutil.ReadDir("./testdata")
	if err != nil {
		log.Fatal(err)
	}
	sort.SliceStable(files, func(i, j int) bool {
		return strings.Compare(files[i].Name(), files[j].Name()) < 0
	})
	currTestName := ""
	currSqlTest := &sqlTest{}
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
			currSqlTest = &sqlTest{testName: currTestName, testSuite: w}
			w.tests[currTestName] = currSqlTest
		}
		if strings.HasSuffix(fileName, "_test_data.txt") {
			currSqlTest.testDataFile = fileName
		} else if strings.HasSuffix(fileName, "_test_out.txt") {
			currSqlTest.outFile = fileName
		} else if strings.HasSuffix(fileName, "_test_script.txt") {
			currSqlTest.scriptFile = fileName
		}
	}
}

func (w *sqlTestsuite) teardown() {
	//err := w.prana.Stop()
	//if err != nil {
	//	log.Fatal(err)
	//}
}

type sqlTest struct {
	testSuite *sqlTestsuite
	testName string
	scriptFile string
	testDataFile string
	outFile string
	output *strings.Builder
}

func trimBothEnds(str string) string {
	str = strings.TrimLeft(str, " \t\n")
	str = strings.TrimRight(str, " \t\n")
	return str
}

func (st *sqlTest) run() {
	start := time.Now()
	if st.scriptFile == "" {
		log.Fatalf("sql test %s is missing script file %s", st.testName, fmt.Sprintf("%s_test_script.txt", st.testName))
	}
	if st.testDataFile == "" {
		log.Fatalf("sql test %s is missing test data file %s", st.testName, fmt.Sprintf("%s_test_data.txt", st.testName))
	}
	if st.outFile == "" {
		log.Fatalf("sql test %s is missing out file %s", st.testName, fmt.Sprintf("%s_test_out.txt", st.testName))
	}
	log.Printf("**** in run for test %s %s %s %s", st.testName, st.scriptFile, st.testDataFile, st.outFile)

	scriptFile, err := os.Open("./testdata/" + st.scriptFile)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = scriptFile.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	st.output = &strings.Builder{}

	b, err := ioutil.ReadAll(scriptFile)
	if err != nil {
		log.Fatal(err)
	}
	scriptContents := string(b)

	// All executable script commands whether they are special comments or sql statements must end with semi-colon followed by newline
	commands := strings.Split(scriptContents, ";\n")
	for _, command := range commands {
		command = trimBothEnds(command)
		if command == "" {
			continue
		}
		if strings.HasPrefix(command, "--load data") {
			err := st.executeLoadData(command)
			if err != nil {
				log.Fatalf("failed to load data with command %s %v", command, err)
			}
			log.Println("loaded dataset ok")
		} else if strings.HasPrefix(command, "--") {
			// Just a normal comment - ignore
		} else {
			// sql statement
			err := st.executeSqlStatement(command)
			if err != nil {
				log.Fatalf("failed to execute sql statement %s %v", command, err)
			}
		}
	}

	log.Printf("Test output is:\n%s", st.output.String())

	outfile, err := os.Open("./testdata/" + st.outFile)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = outfile.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	b, err = ioutil.ReadAll(outfile)
	if err != nil {
		log.Fatal(err)
	}
	expectedOutput := string(b)
	actualOutput := st.output.String()
	end := time.Now()
	dur := end.Sub(start)
	log.Printf("Test took %d ms to run", dur.Milliseconds())
	st.testSuite.suite.Require().Equal(expectedOutput, actualOutput)
}

type dataset struct {
	name string
	sourceInfo *common.SourceInfo
	colTypes []common.ColumnType
	rows *common.Rows
}

func (st *sqlTest) loadDataset(fileName string, dsName string) (*dataset, error) {

	dataFile, err := os.Open("./testdata/" + st.testDataFile)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = dataFile.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	scanner := bufio.NewScanner(dataFile)
	var currDataSet *dataset
	lineNum := 1
	for scanner.Scan() {
		line := scanner.Text()
		line = trimBothEnds(line)
		if strings.HasPrefix(line, "dataset:") {
			line = line[8:]
			parts := strings.Split(line, " ")
			if len(parts) != 2 {
				log.Fatalf("invalid dataset line in file %s: %s", fileName, line)
			}
			dataSetName := parts[0]
			if dsName != dataSetName {
				if currDataSet != nil {
					return currDataSet, nil
				}
				continue
			}
			sourceName := parts[1]
			sourceInfo, ok := st.testSuite.prana.GetMetaController().GetSource("test", sourceName)
			if !ok {
				log.Fatalf("unknown source %s", sourceName)
			}
			rf := common.NewRowsFactory(sourceInfo.TableInfo.ColumnTypes)
			rows := rf.NewRows(100)
			currDataSet = &dataset{name:dataSetName, sourceInfo: sourceInfo, rows: rows, colTypes: sourceInfo.TableInfo.ColumnTypes}
		} else {
			if currDataSet == nil {
				continue
			}
			parts := strings.Split(line, ",")
			if len(parts) != len(currDataSet.colTypes) {
				log.Fatalf("source %s has %d columns but data has %d columns at line %d in file %s", currDataSet.sourceInfo.Name, len(currDataSet.colTypes), len(parts), lineNum, fileName)
			}
			for i, colType := range currDataSet.colTypes {
				part := parts[i]
				if part == "" {
					currDataSet.rows.AppendNullToColumn(i)
				} else {
					switch colType.Type {
					case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
						val, err := strconv.ParseInt(part, 10, 64)
						if err != nil {
							log.Fatal(err)
						}
						currDataSet.rows.AppendInt64ToColumn(i, val)
					case common.TypeDouble:
						val, err := strconv.ParseFloat(part, 64)
						if err != nil {
							log.Fatal(err)
						}
						currDataSet.rows.AppendFloat64ToColumn(i, val)
					case common.TypeVarchar:
						currDataSet.rows.AppendStringToColumn(i, part)
					case common.TypeDecimal:
						val, err := common.NewDecFromString(part)
						if err != nil {
							log.Fatal(err)
						}
						currDataSet.rows.AppendDecimalToColumn(i, *val)
					default:
						log.Fatalf("unexpected data type %d", colType.Type)
					}
				}
			}
		}
		lineNum++
	}
	if currDataSet == nil {
		log.Fatalf("Cannot find dataset %s in test data file %s", dsName, fileName)
	}
	return currDataSet, nil
}

func (st *sqlTest) executeLoadData(command string) error {
	log.Printf("Executing load: %s", command)
	datasetName := command[12:]
	log.Printf("attempting to load dataset %s", datasetName)

	dataset, err := st.loadDataset(st.testDataFile, datasetName)
	if err != nil {
		log.Fatal(err)
	}
	engine := st.testSuite.prana.GetPushEngine()
	err = engine.IngestRows(dataset.rows, dataset.sourceInfo.TableInfo.ID)
	if err != nil {
		return err
	}
	return engine.WaitForProcessingToComplete()
}

func (st *sqlTest) executeSqlStatement(statement string) error {
	log.Printf("Executing statement: %s", statement)
	exec, err := st.testSuite.prana.GetCommandExecutor().ExecuteSQLStatement("test", statement)
	if err != nil {
		log.Fatal(err)
	}
	rows, err := exec.GetRows(100000)
	if err != nil {
		log.Fatal(err)
	}

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

	return nil
}