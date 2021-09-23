package sqltest

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/squareup/pranadb/client"
	"github.com/squareup/pranadb/protolib"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/service"
	"google.golang.org/protobuf/types/descriptorpb"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/common/commontest"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/kafka"
	"github.com/squareup/pranadb/push/source"
	"github.com/squareup/pranadb/server"
	"github.com/squareup/pranadb/table"
)

// Set this to the name of a test if you want to only run that test, e.g. during development
const (
	TestPrefix         = ""
	ExcludedTestPrefix = ""
	TestClusterID      = 12345678
	ProtoDescriptorDir = "../protos"
)

var (
	lock           sync.Mutex
	defaultEncoder = &kafka.JSONKeyJSONValueEncoder{}
)

const apiServerListenAddressBase = 63401

func TestSQLFakeCluster(t *testing.T) {
	testSQL(t, true, 1)
}

func TestSQLClustered(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: skipped")
	}
	testSQL(t, false, 3)
}

type sqlTestsuite struct {
	fakeCluster  bool
	numNodes     int
	fakeKafka    *kafka.FakeKafka
	pranaCluster []*server.Server
	suite        suite.Suite
	tests        map[string]*sqlTest
	t            *testing.T
	dataDir      string
	lock         sync.Mutex
	encoders     map[string]encoderFactory
}

type encoderFactory func(options string) (kafka.MessageEncoder, error)

func staticEncoderFactory(encoder kafka.MessageEncoder) encoderFactory {
	return func(options string) (kafka.MessageEncoder, error) {
		return encoder, nil
	}
}

func (w *sqlTestsuite) T() *testing.T {
	return w.suite.T()
}

func (w *sqlTestsuite) SetT(t *testing.T) {
	t.Helper()
	w.suite.SetT(t)
}

func testSQL(t *testing.T, fakeCluster bool, numNodes int) {
	t.Helper()

	log.SetFormatter(&log.TextFormatter{
		ForceColors:            true,
		DisableQuote:           true,
		FullTimestamp:          true,
		DisableLevelTruncation: true,
	})

	// Make sure we don't run tests in parallel
	lock.Lock()
	defer lock.Unlock()

	ts := &sqlTestsuite{tests: make(map[string]*sqlTest), t: t, encoders: make(map[string]encoderFactory)}
	ts.setup(fakeCluster, numNodes)
	defer ts.teardown()

	suite.Run(t, ts)
}

func (w *sqlTestsuite) TestSQL() {
	for testName, sTest := range w.tests {
		w.suite.Run(testName, sTest.run)
	}
}

func (w *sqlTestsuite) restartCluster() {
	log.Infof("Restarting cluster")
	w.stopCluster()
	log.Infof("Stopped cluster")
	w.startCluster()
	log.Infof("Restarted it")
}

func (w *sqlTestsuite) setupPranaCluster() {

	if w.fakeCluster && w.numNodes != 1 {
		log.Fatal("fake cluster only supports one node")
	}

	brokerConfigs := map[string]conf.BrokerConfig{
		"testbroker": {
			ClientType: conf.BrokerClientFake,
			Properties: map[string]string{
				"fakeKafkaID": fmt.Sprintf("%d", w.fakeKafka.ID),
			},
		},
	}
	w.pranaCluster = make([]*server.Server, w.numNodes)
	if w.fakeCluster {
		cnf := conf.NewDefaultConfig()
		cnf.NodeID = 0
		cnf.ClusterID = TestClusterID
		cnf.NumShards = 10
		cnf.TestServer = true
		cnf.KafkaBrokers = brokerConfigs
		cnf.EnableAPIServer = true
		cnf.Debug = true
		cnf.EnableSourceStats = true
		cnf.APIServerListenAddresses = []string{
			"localhost:63401",
		}
		cnf.ProtobufDescriptorDir = ProtoDescriptorDir
		s, err := server.NewServer(*cnf)
		if err != nil {
			log.Fatal(err)
		}
		w.pranaCluster[0] = s
	} else {
		raftAddresses := []string{
			"localhost:63201",
			"localhost:63202",
			"localhost:63203",
		}
		notifAddresses := []string{
			"localhost:63301",
			"localhost:63302",
			"localhost:63303",
		}
		apiServerListenAddresses := []string{
			"localhost:63401",
			"localhost:63402",
			"localhost:63403",
		}
		for i := 0; i < w.numNodes; i++ {
			cnf := conf.NewDefaultConfig()
			cnf.NodeID = i
			cnf.ClusterID = TestClusterID
			cnf.RaftAddresses = raftAddresses
			cnf.NumShards = 30
			cnf.ReplicationFactor = 3
			cnf.DataDir = w.dataDir
			cnf.TestServer = false
			cnf.KafkaBrokers = brokerConfigs
			cnf.NotifListenAddresses = notifAddresses
			cnf.Debug = true
			cnf.EnableSourceStats = true
			cnf.EnableAPIServer = true
			cnf.APIServerListenAddresses = apiServerListenAddresses
			cnf.ProtobufDescriptorDir = ProtoDescriptorDir

			// We set snapshot settings to low values so we can trigger more snapshots and exercise the
			// snapshotting - in real life these would be much higher
			//cnf.DataSnapshotEntries = 10
			//cnf.DataCompactionOverhead = 5
			//cnf.SequenceSnapshotEntries = 10
			//cnf.SequenceCompactionOverhead = 5

			s, err := server.NewServer(*cnf)
			if err != nil {
				log.Fatal(err)
			}
			w.pranaCluster[i] = s
		}
	}

	w.startCluster()
}

func (w *sqlTestsuite) setup(fakeCluster bool, numNodes int) {
	w.fakeCluster = fakeCluster
	w.numNodes = numNodes
	protoRegistry, err := protolib.NewDirBackedRegistry(ProtoDescriptorDir)
	require.NoError(w.t, err)
	w.registerEncoders(protoRegistry)
	w.fakeKafka = kafka.NewFakeKafka()

	dataDir, err := ioutil.TempDir("", "sql-test")
	if err != nil {
		log.Fatal(err)
	}
	w.dataDir = dataDir

	w.setupPranaCluster()

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
		if TestPrefix != "" && (strings.Index(fileName, TestPrefix) != 0) {
			continue
		}
		if ExcludedTestPrefix != "" && (strings.Index(fileName, ExcludedTestPrefix) == 0) {
			continue
		}
		if file.IsDir() {
			continue
		}
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

func (w *sqlTestsuite) registerEncoders(registry protolib.Resolver) {
	w.registerEncoder(&kafka.JSONKeyJSONValueEncoder{})
	w.registerEncoder(&kafka.StringKeyTLJSONValueEncoder{})
	w.registerEncoder(&kafka.Int64BEKeyTLJSONValueEncoder{})
	w.registerEncoder(&kafka.Int32BEKeyTLJSONValueEncoder{})
	w.registerEncoder(&kafka.Int16BEKeyTLJSONValueEncoder{})
	w.registerEncoder(&kafka.Float64BEKeyTLJSONValueEncoder{})
	w.registerEncoder(&kafka.Float32BEKeyTLJSONValueEncoder{})
	w.registerEncoder(&kafka.NestedJSONKeyNestedJSONValueEncoder{})
	w.registerEncoder(&kafka.JSONHeadersEncoder{})
	w.registerEncoderFactory(kafka.NewStringKeyProtobufValueEncoderFactory(registry), &kafka.StringKeyProtobufValueEncoder{})
}

func (w *sqlTestsuite) registerEncoder(encoder kafka.MessageEncoder) {
	if _, ok := w.encoders[encoder.Name()]; ok {
		panic(fmt.Sprintf("encoder with name %s already registered", encoder.Name()))
	}
	w.encoders[encoder.Name()] = staticEncoderFactory(encoder)
}

func (w *sqlTestsuite) registerEncoderFactory(factory encoderFactory, typ kafka.MessageEncoder) {
	name := typ.Name()
	if _, ok := w.encoders[name]; ok {
		panic(fmt.Sprintf("encoder with name %s already registered", name))
	}
	w.encoders[name] = factory
}

func (w *sqlTestsuite) stopCluster() {
	for _, prana := range w.pranaCluster {
		err := prana.Stop()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (w *sqlTestsuite) startCluster() {
	// The nodes need to be started in parallel, as cluster.start() shouldn't return until the cluster is
	// available - i.e. all nodes are up. (Currently that is broken and there is a time.sleep but we should fix that)
	wg := sync.WaitGroup{}
	for i, prana := range w.pranaCluster {
		wg.Add(1)
		prana := prana
		ind := i
		go func() {
			log.Infof("Starting prana node %d", ind)
			err := prana.Start()
			if err != nil {
				log.Fatalf("Failed to start cluster %+v", err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func (w *sqlTestsuite) teardown() {
	w.stopCluster()
	if w.dataDir != "" {
		err := os.RemoveAll(w.dataDir)
		if err != nil {
			log.Fatal(err)
		}
	}
}

type sqlTest struct {
	testSuite     *sqlTestsuite
	testName      string
	scriptFile    string
	testDataFile  string
	outFile       string
	output        *strings.Builder
	rnd           *rand.Rand
	prana         *server.Server
	topics        []*kafka.Topic
	cli           *client.Client
	sessionID     string
	currentSchema string
}

func (st *sqlTest) run() {

	// Only run one test in the suite at a time
	st.testSuite.lock.Lock()
	defer st.testSuite.lock.Unlock()

	log.Infof("Running sql test %s", st.testName)

	require := st.testSuite.suite.Require()

	require.NotEmpty(st.scriptFile, fmt.Sprintf("sql test %s is missing script file %s", st.testName, fmt.Sprintf("%s_test_script.txt", st.testName)))
	require.NotEmpty(st.testDataFile, fmt.Sprintf("sql test %s is missing test data file %s", st.testName, fmt.Sprintf("%s_test_data.txt", st.testName)))
	require.NotEmpty(st.outFile, fmt.Sprintf("sql test %s is missing out file %s", st.testName, fmt.Sprintf("%s_test_out.txt", st.testName)))

	scriptFile, closeFunc := openFile("./testdata/" + st.scriptFile)
	defer closeFunc()

	b, err := ioutil.ReadAll(scriptFile)
	require.NoError(err)
	scriptContents := string(b)

	// All executable script commands whether they are special comments or sql statements must end with semi-colon followed by newline
	if scriptContents[len(scriptContents)-1] == ';' {
		scriptContents = scriptContents[:len(scriptContents)-1]
	}
	commands := strings.Split(scriptContents, ";\n")
	numIters := 1
	for iter := 0; iter < numIters; iter++ {
		numIts := st.runTestIteration(require, commands, iter)
		if iter == 0 {
			numIters = numIts
		}
	}
}

//nolint:gocyclo
func (st *sqlTest) runTestIteration(require *require.Assertions, commands []string, iter int) int {
	log.Infof("Running test iteration %d", iter)
	start := time.Now()
	st.prana = st.choosePrana()
	st.output = &strings.Builder{}
	st.cli = st.createCli(require)
	numIters := 1
	for i, command := range commands {
		st.output.WriteString(command + ";\n")
		command = trimBothEnds(command)
		if command == "" {
			continue
		}
		log.Infof("Executing line: %s", command)
		if strings.HasPrefix(command, "--load data") {
			st.executeLoadData(require, command)
		} else if strings.HasPrefix(command, "--close session") {
			st.executeCloseSession(require)
		} else if strings.HasPrefix(command, "--repeat") {
			if i > 0 {
				require.Fail("--repeat command must be first line in script")
			}
			var err error
			n, err := strconv.ParseInt(command[9:], 10, 32)
			require.NoError(err)
			numIters = int(n)
		} else if strings.HasPrefix(command, "--create topic") {
			st.executeCreateTopic(require, command)
		} else if strings.HasPrefix(command, "--delete topic") {
			st.executeDeleteTopic(require, command)
		} else if strings.HasPrefix(command, "--restart cluster") {
			st.executeRestartCluster(require)
		} else if strings.HasPrefix(command, "--kafka fail") {
			st.executeKafkaFail(require, command)
		} else if strings.HasPrefix(command, "--wait for rows") {
			st.executeWaitForRows(require, command)
		} else if strings.HasPrefix(command, "--wait for duplicates") {
			st.executeWaitForDuplicates(require, command)
		} else if strings.HasPrefix(command, "--wait for committed") {
			st.executeWaitForCommitted(require, command)
		} else if strings.HasPrefix(command, "--enable commit offsets") {
			st.executeEnableCommitOffsets(require, command)
		} else if strings.HasPrefix(command, "--disable commit offsets") {
			st.executeDisableCommitOffsets(require, command)
		} else if strings.HasPrefix(command, "--register protobuf") {
			st.executeRegisterProtobufCommand(require, command)
		}
		if strings.HasPrefix(command, "--") {
			// Just a normal comment - ignore
		} else {
			// sql statement
			st.executeSQLStatement(require, command)
		}
	}

	fmt.Println("TEST OUTPUT=========================\n" + st.output.String())
	fmt.Println("END TEST OUTPUT=====================")

	st.closeClient(require)

	for _, topic := range st.topics {
		err := st.testSuite.fakeKafka.DeleteTopic(topic.Name)
		require.NoError(err)
	}

	// Now we verify that the test has left the database in a clean state - there should be no user sources
	// or materialized views and no data in the database and nothing in the remote session caches
	for _, prana := range st.testSuite.pranaCluster {

		// Script should delete all it's table and MV. Once they are all deleted the schema will automatically
		// be removed, so should not exist at end of test
		schemaNames := prana.GetMetaController().GetSchemaNames()
		for _, schemaName := range schemaNames {
			if schemaName != "sys" {
				sch, ok := prana.GetMetaController().GetSchema(schemaName)
				if ok {
					// Must be empty
					require.Equal(0, sch.LenTables(), fmt.Sprintf("Schema %s is not empty at end of test. Did you drop all the tables and materialized views?", sch.Name))
				} else {
					require.False(ok, fmt.Sprintf("Test schema exists: %v. Did you drop all the tables and materialized views?", sch))
				}
			}
		}

		require.True(prana.GetPushEngine().IsEmpty(), "PushEngine state not empty at end of test")

		// This can be async - a replica can be taken off line and snap-shotted while the delete range is occurring
		// and the query can look at it's stale data - it will eventually come right once it has caught up
		ok, err := commontest.WaitUntilWithError(func() (bool, error) {
			return st.tableDataLeft(require, prana, false)
		}, 5*time.Second, 100*time.Millisecond)
		require.NoError(err)
		if !ok {
			_, _ = st.tableDataLeft(require, prana, true)
		}
		require.True(ok, "table data left at end of test")

		ok, err = commontest.WaitUntilWithError(func() (bool, error) {
			num, err := prana.GetPullEngine().NumCachedSessions()
			if err != nil {
				return false, err
			}
			return num == 0, nil
		}, 5*time.Second, 10*time.Millisecond)
		require.NoError(err)
		require.True(ok, "timed out waiting for num remote sessions to get to zero")

		topicNames := st.testSuite.fakeKafka.GetTopicNames()
		if len(topicNames) > 0 {
			log.Infof("Topics left at end of test run - please make sure you delete them at the end of your script")
			for _, name := range topicNames {
				log.Infof("Topic %s", name)
			}
		}
		require.Equal(0, len(topicNames), "Topics left at end of test run")

		rows, err := prana.GetPullEngine().ExecuteQuery("sys", "select * from tables ")
		require.NoError(err)
		if rows.RowCount() != 0 {
			for i := 0; i < rows.RowCount(); i++ {
				row := rows.GetRow(i)
				log.Println(row.String())
			}
		}
		require.Equal(0, rows.RowCount(), "Rows in sys.tables at end of test run")

		require.Equal(0, prana.GetCommandExecutor().RunningCommands(), "DDL commands left at end of test run")

		require.Equal(0, prana.GetAPIServer().SessionCount(), "API Server sessions left at end of test run")
	}

	outfile, closeFunc := openFile("./testdata/" + st.outFile)
	defer closeFunc()
	b, err := ioutil.ReadAll(outfile)
	require.NoError(err)
	expectedOutput := string(b)
	actualOutput := st.output.String()
	require.Equal(trimBothEnds(expectedOutput), trimBothEnds(actualOutput))

	dur := time.Now().Sub(start)
	log.Infof("Finished running sql test %s time taken %d ms", st.testName, dur.Milliseconds())
	return numIters
}

func (st *sqlTest) waitUntilRowsInTable(require *require.Assertions, tableName string, numRows int) {
	ok, err := commontest.WaitUntilWithError(func() (bool, error) {
		ch, err := st.cli.ExecuteStatement(st.sessionID, fmt.Sprintf("select * from %s", tableName))
		if err != nil {
			return false, err
		}
		lineCount := -2 // There's a header and a footer
		for range ch {
			lineCount++
		}
		return lineCount == numRows, nil
	}, 10*time.Second, 100*time.Millisecond)
	require.NoError(err)
	require.True(ok)
}

func (st *sqlTest) getAllRowsInTable(tableID uint64) (int, error) {
	totRows := 0
	for _, prana := range st.testSuite.pranaCluster {
		scheds, err := prana.GetPushEngine().GetLocalLeaderSchedulers()
		if err != nil {
			return 0, err
		}
		for shardID := range scheds {
			keyStartPrefix := table.EncodeTableKeyPrefix(tableID, shardID, 16)
			keyEndPrefix := table.EncodeTableKeyPrefix(tableID+1, shardID, 16)
			pairs, err := prana.GetCluster().LocalScan(keyStartPrefix, keyEndPrefix, 100000)
			if err != nil {
				return 0, err
			}
			totRows += len(pairs)
		}
	}
	return totRows, nil
}

func (st *sqlTest) tableDataLeft(require *require.Assertions, prana *server.Server, displayRows bool) (bool, error) {
	localShards := prana.GetCluster().GetLocalShardIDs()
	for _, shardID := range localShards {
		keyStartPrefix := table.EncodeTableKeyPrefix(common.UserTableIDBase, shardID, 16)
		keyEndPrefix := table.EncodeTableKeyPrefix(0, shardID+1, 16)
		pairs, err := prana.GetCluster().LocalScan(keyStartPrefix, keyEndPrefix, 100)
		require.NoError(err)
		if displayRows && len(pairs) > 0 {
			for _, pair := range pairs {
				log.Infof("%s v:%v", common.DumpDataKey(pair.Key), pair.Value)
			}
			require.Equal(0, len(pairs), fmt.Sprintf("Table data left at end of test for shard %d", shardID))
		}
		if len(pairs) != 0 {
			return false, nil
		}
	}
	return true, nil
}

type dataset struct {
	name       string
	sourceInfo *common.SourceInfo
	colTypes   []common.ColumnType
	rows       *common.Rows
}

func (st *sqlTest) loadDataset(require *require.Assertions, fileName string, dsName string) (*dataset, kafka.MessageEncoder) {
	dataFile, closeFunc := openFile("./testdata/" + st.testDataFile)
	defer closeFunc()
	scanner := bufio.NewScanner(dataFile)
	var (
		encoder     kafka.MessageEncoder
		currDataSet *dataset
		err         error
	)
	lineNum := 1
	for scanner.Scan() {
		line := scanner.Text()
		line = trimBothEnds(line)
		if strings.HasPrefix(line, "dataset:") {
			line = line[8:]
			parts := strings.Split(line, " ")
			lp := len(parts)
			require.True(lp == 2 || lp == 3, fmt.Sprintf("invalid dataset line in file %s: %s", fileName, line))
			dataSetName := parts[0]
			if dsName != dataSetName {
				if currDataSet != nil {
					return currDataSet, encoder
				}
				continue
			}
			sourceName := parts[1]
			require.NotEmpty(st.currentSchema, "no schema selected")
			sourceInfo, ok := st.prana.GetMetaController().GetSource(st.currentSchema, sourceName)
			require.True(ok, fmt.Sprintf("unknown source %s", sourceName))
			if lp == 3 {
				encoderName := parts[2]
				options := ""
				if strings.Contains(encoderName, ":") {
					parts := strings.SplitN(encoderName, ":", 2)
					encoderName, options = parts[0], parts[1]
				}
				factory, ok := st.testSuite.encoders[encoderName]
				require.True(ok, fmt.Sprintf("unknown encoder %s", encoderName))
				encoder, err = factory(options)
				require.NoError(err)
			} else {
				encoder = defaultEncoder
			}
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
				if part == "null" {
					currDataSet.rows.AppendNullToColumn(i)
				} else {
					switch colType.Type {
					case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
						val, intErr := strconv.ParseInt(part, 10, 64)
						if intErr != nil {
							// bools are ints
							boolVal, err := strconv.ParseBool(part)
							if err != nil {
								require.NoError(intErr)
							}
							if boolVal {
								val = 1
							} else {
								val = 0
							}
						}
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
					case common.TypeTimestamp:
						val := common.NewTimestampFromString(part)
						currDataSet.rows.AppendTimestampToColumn(i, val)
					default:
						require.Fail(fmt.Sprintf("unexpected data type %d", colType.Type))
					}
				}
			}
		}
		lineNum++
	}
	require.NotNil(currDataSet, fmt.Sprintf("Cannot find dataset %s in test data file %s", dsName, fileName))
	return currDataSet, encoder
}

func (st *sqlTest) executeLoadData(require *require.Assertions, command string) {
	noWait := strings.HasSuffix(command, "no wait")
	if noWait {
		command = command[:len(command)-8]
	}
	// If no wait we run the load data asynchronously
	if noWait {
		go st.doLoadData(require, command, true)
	} else {
		st.doLoadData(require, command, false)
	}
}

func (st *sqlTest) doLoadData(require *require.Assertions, command string, noWait bool) {
	start := time.Now()
	datasetName := command[12:]
	dataset, encoder := st.loadDataset(require, st.testDataFile, datasetName)
	fakeKafka := st.testSuite.fakeKafka
	initialCommitted := st.getNumCommitted(require, dataset.sourceInfo.ID)
	err := kafka.IngestRows(fakeKafka, dataset.sourceInfo, dataset.rows, encoder)
	require.NoError(err)
	if !noWait {
		st.waitForCommitted(require, dataset.rows.RowCount()+initialCommitted, dataset.sourceInfo.ID)
		st.waitForProcessingToComplete(require)
	}
	end := time.Now()
	dur := end.Sub(start)
	log.Infof("Load data %s execute time ms %d", command, dur.Milliseconds())
}

func (st *sqlTest) executeCloseSession(require *require.Assertions) {
	// Closes then recreates the cli
	st.closeClient(require)
	st.cli = st.createCli(require)
}

func (st *sqlTest) closeClient(require *require.Assertions) {
	err := st.cli.CloseSession(st.sessionID)
	require.NoError(err)
	err = st.cli.Stop()
	require.NoError(err)
}

func (st *sqlTest) executeCreateTopic(require *require.Assertions, command string) {
	parts := strings.Split(command, " ")
	lp := len(parts)
	require.True(lp == 3 || lp == 4, "Invalid create topic, should be --create topic topic_name [partitions]")
	topicName := parts[2]
	var partitions int64 = 10
	if len(parts) > 3 {
		var err error
		partitions, err = strconv.ParseInt(parts[3], 10, 64)
		require.NoError(err)
	}
	_, err := st.testSuite.fakeKafka.CreateTopic(topicName, int(partitions))
	require.NoError(err)
	log.Infof("Created topic %s partitions %d", topicName, partitions)
}

func (st *sqlTest) executeDeleteTopic(require *require.Assertions, command string) {
	parts := strings.Split(command, " ")
	lp := len(parts)
	require.True(lp == 3, "Invalid delete topic, should be --delete topic topic_name")
	topicName := parts[2]
	err := st.testSuite.fakeKafka.DeleteTopic(topicName)
	require.NoError(err)
	log.Infof("Deleted topic %s ", topicName)
}

func (st *sqlTest) executeRestartCluster(require *require.Assertions) {
	st.closeClient(require)
	st.testSuite.restartCluster()
	st.prana = st.choosePrana()
	st.cli = st.createCli(require)
	st.topics = make([]*kafka.Topic, 0)
}

func (st *sqlTest) executeKafkaFail(require *require.Assertions, command string) {
	log.Infof("Executing kafka fail")
	parts := strings.Split(command, " ")
	lp := len(parts)
	require.True(lp == 5, "Invalid kafka fail, should be --kafka fail topic_name source_name fail_time")
	topicName := parts[2]
	sourceNae := parts[3]
	sFailTime := parts[4]
	failTime, err := strconv.ParseInt(sFailTime, 10, 64)
	require.NoError(err)
	dur := time.Millisecond * time.Duration(failTime)
	require.NotEmpty(st.currentSchema, "no schema selected")
	srcInfo, ok := st.prana.GetMetaController().GetSource(st.currentSchema, sourceNae)
	require.True(ok)
	groupID := source.GenerateGroupID(TestClusterID, srcInfo)
	err = st.testSuite.fakeKafka.InjectFailure(topicName, groupID, dur)
	require.NoError(err)
}

func (st *sqlTest) executeWaitForRows(require *require.Assertions, command string) {
	command = command[16:]
	parts := strings.Split(command, " ")
	lp := len(parts)
	require.True(lp == 2, "Invalid wait for rows, should be --wait for rows table_name num_rows")
	tableName := parts[0]
	sNumRows := parts[1]
	numrows, err := strconv.ParseInt(sNumRows, 10, 32)
	require.NoError(err)
	st.waitUntilRowsInTable(require, tableName, int(numrows))
}

func (st *sqlTest) getNumCommitted(require *require.Assertions, sourceID uint64) int {
	totCommitted := 0
	for _, server := range st.testSuite.pranaCluster {
		source, err := server.GetPushEngine().GetSource(sourceID)
		require.NoError(err)
		totCommitted += int(source.GetCommittedCount())
	}
	return totCommitted
}

func (st *sqlTest) executeWaitForCommitted(require *require.Assertions, command string) {
	command = command[21:]
	parts := strings.Split(command, " ")
	lp := len(parts)
	require.True(lp == 2, "Invalid wait for committed, should be --wait for committed source_name num_rows")
	sourceName := parts[0]
	sNumRows := parts[1]
	numrows, err := strconv.ParseInt(sNumRows, 10, 32)
	require.NoError(err)
	require.NotEmpty(st.currentSchema, "no schema selected")
	sourceInfo, ok := st.testSuite.pranaCluster[0].GetMetaController().GetSource(st.currentSchema, sourceName)
	require.True(ok, fmt.Sprintf("no such source %s", sourceName))
	st.waitForCommitted(require, int(numrows), sourceInfo.ID)
}

func (st *sqlTest) waitForCommitted(require *require.Assertions, numrows int, sourceID uint64) {
	ok, err := commontest.WaitUntilWithError(func() (bool, error) {
		totCommitted := st.getNumCommitted(require, sourceID)
		if totCommitted == numrows {
			return true, nil
		}
		return false, nil
	}, 5*time.Second, 100*time.Millisecond)
	require.NoError(err)
	require.True(ok, fmt.Sprintf("timed out waiting for %d rows to be committed, actual committed %d", numrows, st.getNumCommitted(require, sourceID)))
}

func (st *sqlTest) executeWaitForDuplicates(require *require.Assertions, command string) {
	command = command[22:]
	parts := strings.Split(command, " ")
	lp := len(parts)
	require.True(lp == 2, "Invalid wait for duplicates, should be --wait for duplicates source_name num_rows")
	sourceName := parts[0]
	sNumRows := parts[1]
	numrows, err := strconv.ParseInt(sNumRows, 10, 32)
	require.NoError(err)

	require.NotEmpty(st.currentSchema, "no schema selected")
	sourceInfo, ok := st.testSuite.pranaCluster[0].GetMetaController().GetSource(st.currentSchema, sourceName)
	require.True(ok, fmt.Sprintf("no such source %s", sourceName))

	ok, err = commontest.WaitUntilWithError(func() (bool, error) {
		totDuplicates := 0
		for _, server := range st.testSuite.pranaCluster {
			source, err := server.GetPushEngine().GetSource(sourceInfo.ID)
			require.NoError(err)
			totDuplicates += int(source.GetDuplicateCount())
		}
		// We wait for at least numrows - we can have more than this number in the case that
		// a rebalance occurred after failure injection stopped so consumer might receive dups then rebalance
		// then two consumers receive dups again
		if totDuplicates >= int(numrows) {
			return true, nil
		}
		return false, nil
	}, 5*time.Second, 100*time.Millisecond)
	require.NoError(err)
	require.True(ok, "timed out waiting for duplicates")
}

func (st *sqlTest) executeEnableCommitOffsets(require *require.Assertions, command string) {
	sourceName := command[24:]
	st.setCommitOffsets(require, sourceName, true)
}

func (st *sqlTest) executeDisableCommitOffsets(require *require.Assertions, command string) {
	sourceName := command[25:]
	st.setCommitOffsets(require, sourceName, false)
}

func (st *sqlTest) setCommitOffsets(require *require.Assertions, sourceName string, enable bool) {
	require.NotEmpty(st.currentSchema, "no schema selected")
	sourceInfo, ok := st.testSuite.pranaCluster[0].GetMetaController().GetSource(st.currentSchema, sourceName)
	require.True(ok, fmt.Sprintf("no such source %s", sourceName))
	for _, server := range st.testSuite.pranaCluster {
		source, err := server.GetPushEngine().GetSource(sourceInfo.ID)
		require.NoError(err)
		source.SetCommitOffsets(enable)
	}
}

func (st *sqlTest) executeRegisterProtobufCommand(require *require.Assertions, cmd string) {
	parts := strings.Split(cmd, " ")
	fileName := parts[len(parts)-1]
	text, err := ioutil.ReadFile(filepath.Join("testdata/protodesc", fileName))
	require.NoError(err)

	fd := &descriptorpb.FileDescriptorSet{}
	require.NoError(proto.UnmarshalText(string(text), fd))

	err = st.cli.RegisterProtobufs(context.Background(), &service.RegisterProtobufsRequest{Descriptors: fd})
	require.NoError(err)
}

func (st *sqlTest) waitForProcessingToComplete(require *require.Assertions) {
	for _, prana := range st.testSuite.pranaCluster {
		err := prana.GetPushEngine().WaitForProcessingToComplete()
		require.NoError(err)
	}
}

func (st *sqlTest) executeSQLStatement(require *require.Assertions, statement string) {
	start := time.Now()
	isUse := strings.HasPrefix(statement, "use ")
	resChan, err := st.cli.ExecuteStatement(st.sessionID, statement)
	require.NoError(err)
	lastLine := ""
	for line := range resChan {
		log.Infof("output:%s", line)
		st.output.WriteString(line + "\n")
		lastLine = line
	}
	successful := lastLine == "0 rows returned"
	if isUse && successful {
		st.currentSchema = statement[4:]
	}
	// In the case of a create mv with an aggregation, rows can be forwarded to other shards so it might not be fully
	// filled by the time the command returns
	st.waitForProcessingToComplete(require)
	end := time.Now()
	dur := end.Sub(start)
	log.Infof("Statement execute time ms %d", dur.Milliseconds())
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

func (st *sqlTest) createCli(require *require.Assertions) *client.Client {
	// We connect to a random Prana
	prana := st.choosePrana()
	id := prana.GetCluster().GetNodeID()
	apiServerAddress := fmt.Sprintf("localhost:%d", apiServerListenAddressBase+id)
	cli := client.NewClient(apiServerAddress, 5*time.Second)
	err := cli.Start()
	require.NoError(err)
	sessID, err := cli.CreateSession()
	require.NoError(err)
	st.sessionID = sessID
	return cli
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
