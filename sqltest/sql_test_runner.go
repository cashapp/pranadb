package sqltest

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/squareup/pranadb/command"
	"github.com/squareup/pranadb/interruptor"
	pranalog "github.com/squareup/pranadb/log"
	"github.com/squareup/pranadb/push"

	"github.com/golang/protobuf/proto"
	"github.com/squareup/pranadb/client"
	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protolib"
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

	_ "net/http/pprof" //nolint:gosec
)

const (
	TestPrefix           = "" // Set this to the name of a test if you want to only run that test, e.g. during development
	ExcludedTestPrefixes = ""
	TestClusterID        = 12345678
	ProtoDescriptorDir   = "../protos"
)

var (
	lock           sync.Mutex
	defaultEncoder = &kafka.JSONKeyJSONValueEncoder{}
	updateFlag     = flag.Bool("update", false, "If set, expected test results in *test_out.txt will be updated")
)

const (
	apiServerListenAddressBase = 63401
	clientPageSize             = 3 // We set this to a small value to exercise the paging logic
)

type sqlTestsuite struct {
	fakeCluster       bool
	numNodes          int
	replicationFactor int
	useHTTPAPI        bool
	tlsKeysInfo       *TLSKeysInfo
	fakeKafka         *kafka.FakeKafka
	pranaCluster      []*server.Server
	suite             suite.Suite
	tests             map[string]*sqlTest
	t                 *testing.T
	dataDir           string
	lock              sync.Mutex
	encoders          map[string]encoderFactory
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

type TLSKeysInfo struct {
	ServerCertPath string
	ServerKeyPath  string
	ClientCertPath string
	ClientKeyPath  string
}

func testSQL(t *testing.T, fakeCluster bool, numNodes int, replicationFactor int, useHTTPAPI bool, tlsKeysInfo *TLSKeysInfo) {
	t.Helper()

	go func() {
		// Start a profiling server
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	log.SetFormatter(&log.TextFormatter{
		ForceColors:            true,
		DisableQuote:           true,
		FullTimestamp:          true,
		DisableLevelTruncation: true,
		TimestampFormat:        pranalog.TimestampFormat,
	})
	log.SetLevel(log.TraceLevel)

	// Make sure we don't run tests in parallel
	lock.Lock()
	defer lock.Unlock()

	ts := &sqlTestsuite{tests: make(map[string]*sqlTest), t: t, encoders: make(map[string]encoderFactory)}
	ts.setup(fakeCluster, numNodes, replicationFactor, useHTTPAPI, tlsKeysInfo)
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
		"genbroker": {
			ClientType: conf.BrokerClientGenerator,
		},
	}
	// We set the table initialisation batch size to be a small number to exercise the batching logic
	push.SetInitBatchSize(3)
	w.pranaCluster = make([]*server.Server, w.numNodes)
	if w.fakeCluster {
		cnf := conf.NewDefaultConfig()
		cnf.NodeID = 0
		cnf.ClusterID = TestClusterID
		cnf.NumShards = 10
		cnf.TestServer = true
		cnf.KafkaBrokers = brokerConfigs
		if w.useHTTPAPI {
			cnf.HTTPAPIServerEnabled = true
			cnf.HTTPAPIServerTLSConfig.Enabled = true
			cnf.HTTPAPIServerTLSConfig.CertPath = w.tlsKeysInfo.ServerCertPath
			cnf.HTTPAPIServerTLSConfig.KeyPath = w.tlsKeysInfo.ServerKeyPath
			cnf.HTTPAPIServerTLSConfig.ClientCertsPath = w.tlsKeysInfo.ClientCertPath
			cnf.HTTPAPIServerTLSConfig.ClientAuth = conf.ClientAuthModeRequireAndVerifyClientCert
			cnf.HTTPAPIServerListenAddresses = []string{
				"127.0.0.1:63401",
			}
		} else {
			cnf.GRPCAPIServerEnabled = true
			cnf.GRPCAPIServerTLSConfig.Enabled = true
			cnf.GRPCAPIServerTLSConfig.CertPath = w.tlsKeysInfo.ServerCertPath
			cnf.GRPCAPIServerTLSConfig.KeyPath = w.tlsKeysInfo.ServerKeyPath
			cnf.GRPCAPIServerTLSConfig.ClientCertsPath = w.tlsKeysInfo.ClientCertPath
			cnf.GRPCAPIServerTLSConfig.ClientAuth = conf.ClientAuthModeRequireAndVerifyClientCert
			cnf.GRPCAPIServerListenAddresses = []string{
				"127.0.0.1:63401",
			}
		}
		cnf.SourceStatsEnabled = true
		cnf.ProtobufDescriptorDir = ProtoDescriptorDir
		s, err := server.NewServer(*cnf)
		if err != nil {
			log.Fatal(err)
		}
		w.pranaCluster[0] = s
	} else {
		var raftListenAddresses []string
		var remotingListenAddresses []string
		var apiServerListenAddresses []string
		for i := 0; i < w.numNodes; i++ {
			raftListenAddresses = append(raftListenAddresses, fmt.Sprintf("127.0.0.1:%d", i+63201))
			remotingListenAddresses = append(remotingListenAddresses, fmt.Sprintf("127.0.0.1:%d", i+63301))
			apiServerListenAddresses = append(apiServerListenAddresses, fmt.Sprintf("127.0.0.1:%d", i+63401))
		}
		for i := 0; i < w.numNodes; i++ {
			cnf := conf.NewDefaultConfig()
			cnf.NodeID = i
			cnf.ClusterID = TestClusterID
			cnf.RaftListenAddresses = raftListenAddresses
			cnf.NumShards = 30
			cnf.ReplicationFactor = w.replicationFactor
			cnf.DataDir = w.dataDir
			cnf.TestServer = false
			cnf.KafkaBrokers = brokerConfigs
			cnf.RemotingListenAddresses = remotingListenAddresses
			cnf.SourceStatsEnabled = true
			if w.useHTTPAPI {
				cnf.HTTPAPIServerEnabled = true
				cnf.HTTPAPIServerTLSConfig.Enabled = true
				cnf.HTTPAPIServerTLSConfig.CertPath = w.tlsKeysInfo.ServerCertPath
				cnf.HTTPAPIServerTLSConfig.KeyPath = w.tlsKeysInfo.ServerKeyPath
				cnf.HTTPAPIServerTLSConfig.ClientCertsPath = w.tlsKeysInfo.ClientCertPath
				cnf.HTTPAPIServerTLSConfig.ClientAuth = conf.ClientAuthModeRequireAndVerifyClientCert
				cnf.HTTPAPIServerListenAddresses = apiServerListenAddresses
			} else {
				cnf.GRPCAPIServerEnabled = true
				cnf.GRPCAPIServerTLSConfig.Enabled = true
				cnf.GRPCAPIServerTLSConfig.CertPath = w.tlsKeysInfo.ServerCertPath
				cnf.GRPCAPIServerTLSConfig.KeyPath = w.tlsKeysInfo.ServerKeyPath
				cnf.GRPCAPIServerTLSConfig.ClientCertsPath = w.tlsKeysInfo.ClientCertPath
				cnf.GRPCAPIServerTLSConfig.ClientAuth = conf.ClientAuthModeRequireAndVerifyClientCert
				cnf.GRPCAPIServerListenAddresses = apiServerListenAddresses
			}
			cnf.ProtobufDescriptorDir = ProtoDescriptorDir
			cnf.FailureInjectorEnabled = true
			cnf.ScreenDragonLogSpam = true
			cnf.RaftRTTMs = 25
			cnf.DisableFsync = true // for performance
			s, err := server.NewServer(*cnf)
			if err != nil {
				log.Fatal(err)
			}
			w.pranaCluster[i] = s
		}
	}

	// We override MaxVarCharLength in tests so we can test ingesting varchar fields that are too big
	source.MaxVarCharOverride = 1000

	interruptor.ActivateTestInterruptManager()

	w.startCluster()
}

func (w *sqlTestsuite) setup(fakeCluster bool, numNodes int, replicationFactor int, useHTTPAPI bool, tlsKeysInfo *TLSKeysInfo) {
	w.fakeCluster = fakeCluster
	w.numNodes = numNodes
	w.replicationFactor = replicationFactor
	w.useHTTPAPI = useHTTPAPI
	w.tlsKeysInfo = tlsKeysInfo
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
		if ExcludedTestPrefixes != "" {
			exclude := false
			excluded := strings.Split(ExcludedTestPrefixes, ",")
			for _, excl := range excluded {
				if strings.Index(fileName, excl) == 0 {
					exclude = true
					break
				}
			}
			if exclude {
				continue
			}
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
			currSQLTest.outFile = currTestName + "_test_out.txt"
		}
		if strings.HasSuffix(fileName, "_test_data.txt") {
			currSQLTest.testDataFile = fileName
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
	log.Info("**** stopping cluster")
	for _, prana := range w.pranaCluster {
		err := prana.Stop()
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Info("**** stopped cluster")
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
	clientNodeID  int
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
	if !*updateFlag {
		require.NotEmpty(st.outFile, fmt.Sprintf("sql test %s is missing out file %s", st.testName, fmt.Sprintf("%s_test_out.txt", st.testName)))
	}

	b, err := os.ReadFile("./testdata/" + st.scriptFile)
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
		waitForInd := strings.Index(command, "wait for results")
		if waitForInd != -1 {
			st.output.WriteString(command[:waitForInd-1] + ";\n")
		} else {
			st.output.WriteString(command + ";\n")
		}
		command = trimBothEnds(command)
		if command == "" {
			continue
		}
		log.Infof("Executing line: %s", command)
		if strings.HasPrefix(command, "--load data") {
			st.executeLoadData(require, command)
		} else if strings.HasPrefix(command, "--close client") {
			st.executeCloseClient(require)
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
		} else if strings.HasPrefix(command, "--wait for schedulers") {
			st.waitForSchedulers(require)
		} else if strings.HasPrefix(command, "--wait for committed") {
			st.executeWaitForCommitted(require, command)
		} else if strings.HasPrefix(command, "--enable commit offsets") {
			st.executeEnableCommitOffsets(require, command)
		} else if strings.HasPrefix(command, "--disable commit offsets") {
			st.executeDisableCommitOffsets(require, command)
		} else if strings.HasPrefix(command, "--register protobuf") {
			st.executeRegisterProtobufCommand(require, command)
		} else if strings.HasPrefix(command, "--activate failpoint") {
			st.executeActivateFailpoint(require, command)
		} else if strings.HasPrefix(command, "--deactivate failpoint") {
			st.executeDeactivateFailpoint(require, command)
		} else if strings.HasPrefix(command, "--cluster only") {
			if st.testSuite.numNodes == 1 {
				st.closeClient(require)
				return 1
			}
		} else if strings.HasPrefix(command, "--local only") {
			if st.testSuite.numNodes > 1 {
				st.closeClient(require)
				return 1
			}
		} else if strings.HasPrefix(command, "--pause") {
			st.executePause(require, command)
		} else if strings.HasPrefix(command, "--get ddl lock") {
			st.executeGetDdlLock(require)
		} else if strings.HasPrefix(command, "--set ddl lock timeout") {
			st.executeSetDdlLockTimeout(require, command)
		} else if strings.HasPrefix(command, "--activate interrupt") {
			st.executeActivateInterrupt(command)
		} else if strings.HasPrefix(command, "--deactivate interrupt") {
			st.executeDeactivateInterrupt(command)
		} else if strings.HasPrefix(command, "--async reset ddl") {
			st.executeAsyncResetDdl(require, command)
		}
		if strings.HasPrefix(command, "--") {
			// Just a normal comment - ignore
		} else {
			if strings.HasPrefix(command, "execps ") {
				st.executePreparedStatement(require, command)
			} else {
				st.executeSQLStatement(require, command)
			}
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
	// or materialized views and no data in the database and nothing in the remote exec ctx caches
	for _, prana := range st.testSuite.pranaCluster {

		// This ensures no rows in receiver, forwarder tables etc
		err := prana.GetPushEngine().WaitForProcessingToComplete()
		require.NoError(err)

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
			num, err := prana.GetPullEngine().NumCachedExecCtxs()
			if err != nil {
				return false, errors.WithStack(err)
			}
			return num == 0, nil
		}, 5*time.Second, 10*time.Millisecond)
		require.NoError(err)
		require.True(ok, "timed out waiting for num remote execution contexts to get to zero")

		topicNames := st.testSuite.fakeKafka.GetTopicNames()
		if len(topicNames) > 0 {
			log.Infof("Topics left at end of test run - please make sure you delete them at the end of your script")
			for _, name := range topicNames {
				log.Infof("Topic %s", name)
			}
		}
		require.Equal(0, len(topicNames), "Topics left at end of test run")

		tableRows, err := prana.GetPullEngine().ExecuteQuery("sys", "select * from tables")
		require.NoError(err)
		if tableRows.RowCount() != 0 {
			for i := 0; i < tableRows.RowCount(); i++ {
				row := tableRows.GetRow(i)
				log.Println(row.String())
			}
		}
		require.Equal(0, tableRows.RowCount(), "Rows in sys.tables at end of test run")

		indexRows, err := prana.GetPullEngine().ExecuteQuery("sys", "select * from indexes")
		require.NoError(err)
		if indexRows.RowCount() != 0 {
			for i := 0; i < indexRows.RowCount(); i++ {
				row := indexRows.GetRow(i)
				log.Println(row.String())
			}
		}
		require.Equal(0, indexRows.RowCount(), "Rows in sys.indexes at end of test run")

		require.True(prana.GetCommandExecutor().Empty(), "DDL state left at end of test run")
	}

	if *updateFlag {
		f, err := os.Create("./testdata/" + st.outFile)
		defer f.Close()
		require.NoError(err)
		_, err = f.WriteString(st.output.String())
		require.NoError(err)
	} else {
		b, err := os.ReadFile("./testdata/" + st.outFile)
		require.NoError(err)
		expectedLines := strings.Split(string(b), "\n")
		actualLines := strings.Split(st.output.String(), "\n")
		ok := true
		if len(expectedLines) != len(actualLines) {
			ok = false
		} else {
			// We compare the lines one by one because there are special lines that we compare by prefix not exactly
			// E.g. internal error contains a UUID which is different each time so we can't exact compare
			for i, expected := range expectedLines {
				actual := actualLines[i]
				hasPrefix := false
				for _, prefix := range prefixCompareLines {
					if strings.HasPrefix(expected, prefix) {
						if !strings.HasPrefix(actual, prefix) {
							ok = false
						}
						hasPrefix = true
						break
					}
				}
				if !ok {
					break
				}
				a := strings.TrimRight(actual, " ")
				e := strings.TrimRight(expected, " ")
				if !hasPrefix && a != e {
					ok = false
					break
				}
			}
		}
		if !ok {
			require.Equal(string(b), st.output.String())
		}
	}
	dur := time.Now().Sub(start)
	log.Infof("Finished running sql test %s time taken %d ms", st.testName, dur.Milliseconds())
	return numIters
}

var prefixCompareLines = []string{"Failed to execute statement: PDB5000 - Internal error - reference:"}

func (st *sqlTest) waitUntilRowsInTable(require *require.Assertions, tableName string, numRows int) {
	lineExpected := fmt.Sprintf("%d rows returned", numRows)
	ok, err := commontest.WaitUntilWithError(func() (bool, error) {
		ch, err := st.cli.ExecuteStatement(fmt.Sprintf("select * from %s", tableName), nil, nil)
		if err != nil {
			return false, errors.WithStack(err)
		}
		lastLine := ""
		for l := range ch {
			lastLine = l
		}
		return lineExpected == lastLine, nil
	}, 10*time.Second, 100*time.Millisecond)
	require.NoError(err)
	require.True(ok)
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
			require.True(lp >= 2 && lp <= 4, fmt.Sprintf("invalid dataset line in file %s: %s", fileName, line))
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
			if lp >= 3 {
				encoderName := parts[2]
				options := ""
				if strings.Contains(encoderName, ":") {
					encParts := strings.SplitN(encoderName, ":", 2)
					encoderName, options = encParts[0], encParts[1]
				}
				factory, ok := st.testSuite.encoders[encoderName]
				require.True(ok, fmt.Sprintf("unknown encoder %s", encoderName))
				encoder, err = factory(options)
				require.NoError(err)
			} else {
				encoder = defaultEncoder
			}
			colTypes := sourceInfo.TableInfo.ColumnTypes
			if lp >= 4 {
				colTypes, err = parseColumnTypes(parts[3])
				require.NoError(err)
			}
			rf := common.NewRowsFactory(colTypes)
			rows := rf.NewRows(100)
			currDataSet = &dataset{name: dataSetName, sourceInfo: sourceInfo, rows: rows, colTypes: colTypes}
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
						val, err := common.NewTimestampFromString(part)
						require.NoError(err)
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
	var initialCommitted int
	if !noWait {
		initialCommitted = st.getNumCommitted(require, dataset.sourceInfo.ID)
	}
	err := kafka.IngestRows(fakeKafka, dataset.sourceInfo, dataset.colTypes, dataset.rows, encoder)
	require.NoError(err)
	if !noWait {
		st.waitForCommitted(require, dataset.rows.RowCount()+initialCommitted, dataset.sourceInfo.ID)
	}
	end := time.Now()
	dur := end.Sub(start)
	log.Infof("Load data %s execute time ms %d", command, dur.Milliseconds())
}

func (st *sqlTest) executeCloseClient(require *require.Assertions) {
	// Closes then recreates the cli
	st.closeClient(require)
	st.cli = st.createCli(require)
}

func (st *sqlTest) closeClient(require *require.Assertions) {
	err := st.cli.Stop()
	require.NoError(err)
}

func (st *sqlTest) executeCreateTopic(require *require.Assertions, command string) {
	parts := strings.Split(command, " ")
	lp := len(parts)
	require.True(lp == 3 || lp == 4, "Invalid create topic, should be --create topic topic_name [partitions]")
	topicName := parts[2]
	var partitions int64 = 20
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
	groupID := srcInfo.OriginInfo.ConsumerGroupID
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
	log.Printf("Waiting for committed %d", numrows)
	ok, err := commontest.WaitUntilWithError(func() (bool, error) {
		totCommitted := st.getNumCommitted(require, sourceID)
		if totCommitted == numrows {
			return true, nil
		}
		return false, nil
	}, 30*time.Second, 100*time.Millisecond)
	require.NoError(err)
	require.True(ok, fmt.Sprintf("timed out waiting for %d rows to be committed, actual committed %d", numrows, st.getNumCommitted(require, sourceID)))
	st.waitForProcessingToComplete(require)
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

	err = st.cli.RegisterProtobufs(context.Background(), fd)
	require.NoError(err)
}

func (st *sqlTest) executeActivateFailpoint(require *require.Assertions, cmd string) {
	st.activateFailpoint(require, cmd, true)
}

func (st *sqlTest) executeDeactivateFailpoint(require *require.Assertions, cmd string) {
	st.activateFailpoint(require, cmd, false)
}

func (st *sqlTest) activateFailpoint(require *require.Assertions, cmd string, activate bool) {
	parts := strings.Split(cmd, " ")
	sNodeID := parts[len(parts)-1]
	fpName := parts[len(parts)-2]
	nodeID, err := strconv.ParseInt(sNodeID, 10, 64)
	require.NoError(err)
	var prana *server.Server
	if int(nodeID) >= st.testSuite.numNodes {
		// Ignore - this is so we can write a test that runs with multiple number of nodes
		return
	}
	if nodeID == -1 {
		// This represents choose the node the client is currently connected to - this will be the originating node
		// for the DDL command
		prana = st.testSuite.pranaCluster[st.clientNodeID]
	} else {
		prana = st.testSuite.pranaCluster[nodeID]
	}
	fi := prana.GetFailureInjector()
	fp := fi.GetFailpoint(fpName)
	if activate {
		fp.SetFailAction(func() error {
			return errors.Errorf("failpoint %s triggered in prana %d", fpName, nodeID)
		})
	} else {
		fp.Deactivate()
	}
}

func (st *sqlTest) executePause(require *require.Assertions, command string) {
	command = command[8:]
	pauseTime, err := strconv.ParseInt(command, 10, 32)
	log.Debugf("Pausing for %d ms", pauseTime)
	time.Sleep(time.Duration(pauseTime) * time.Millisecond)
	require.NoError(err)
}

func (st *sqlTest) executeGetDdlLock(require *require.Assertions) {
	//choose a random Prana
	prana := st.choosePrana()
	ok, err := prana.GetCluster().GetLock(st.currentSchema + "/")
	require.NoError(err)
	st.output.WriteString(fmt.Sprintf("Get lock returned: %t", ok))
}

func (st *sqlTest) executeSetDdlLockTimeout(require *require.Assertions, com string) {
	timeout, err := strconv.ParseInt(com[23:], 10, 64)
	require.NoError(err)
	command.SetSchemaLockAttemptTimeout(time.Duration(timeout) * time.Second)
}

func (st *sqlTest) executeActivateInterrupt(com string) {
	parts := strings.Split(com, " ")
	interruptName := parts[2]
	st.interruptManager().ActivateDelay(interruptName)
}

func (st *sqlTest) executeDeactivateInterrupt(com string) {
	parts := strings.Split(com, " ")
	interruptName := parts[2]
	st.interruptManager().DeactivateDelay(interruptName)
}

func (st *sqlTest) interruptManager() *interruptor.DelayingInterruptManager {
	i, ok := interruptor.GetInterruptManager().(*interruptor.DelayingInterruptManager)
	if !ok {
		panic("not a *interruptor.DelayingInterruptManager")
	}
	return i
}

func (st *sqlTest) executeAsyncResetDdl(require *require.Assertions, com string) {
	parts := strings.Split(com, " ")
	schemaName := parts[3]
	go func() {
		// Execute the reset ddl command asynchronously after a short delay
		time.Sleep(1 * time.Second)
		resChan, err := st.cli.ExecuteStatement(fmt.Sprintf("reset ddl %s", schemaName), nil, nil)
		require.NoError(err)
		for range resChan {
		}
	}()
}

func (st *sqlTest) executePreparedStatement(require *require.Assertions, command string) {
	//--execps num_args argType argVal ... query
	command = command[7:]
	parts := split(command)
	numArgs, err := strconv.Atoi(parts[0])
	require.NoError(err)
	sargTypes := make([]string, numArgs)
	sargs := make([]string, numArgs)
	pos := 1
	for i := 0; i < numArgs; i++ {
		sargTypes[i] = parts[pos]
		pos++
		sargs[i] = parts[pos]
		pos++
	}
	start := time.Now()
	resChan, err := st.cli.ExecuteStatement(parts[pos], sargTypes, sargs)
	require.NoError(err)
	for line := range resChan {
		log.Infof("output:%s", line)
		st.output.WriteString(line + "\n")
	}
	end := time.Now()
	dur := end.Sub(start)
	log.Infof("Prepared statement execute time ms %d", dur.Milliseconds())
}

func split(str string) []string {
	var res []string
	var cb *strings.Builder
	inQuote := false
	for _, b := range str {
		if b == ' ' && !inQuote {
			if cb != nil {
				res = append(res, cb.String())
				cb = nil
			}
		} else if b == '"' {
			if cb == nil {
				inQuote = true
			} else {
				inQuote = false
			}
		} else {
			if cb == nil {
				cb = &strings.Builder{}
			}
			cb.WriteRune(b)
		}
	}
	if cb != nil {
		res = append(res, cb.String())
	}
	return res
}

func (st *sqlTest) waitForProcessingToComplete(require *require.Assertions) {
	log.Debug("Waiting for processing to complete")
	for _, prana := range st.testSuite.pranaCluster {
		err := prana.GetPushEngine().WaitForProcessingToComplete()
		require.NoError(err)
	}
	log.Debug("Waited for processing to complete")
}

func (st *sqlTest) waitForSchedulers(require *require.Assertions) {
	log.Debug("Waiting for schedulers to complete")
	for _, prana := range st.testSuite.pranaCluster {
		err := prana.GetPushEngine().WaitForSchedulers()
		require.NoError(err)
	}
	log.Debug("Waited for schedulers to complete")
}

func (st *sqlTest) executeSQLStatement(require *require.Assertions, statement string) {
	start := time.Now()
	ind := strings.Index(statement, "wait for results")
	var waitUntilResults string
	if ind != -1 {
		waitUntilResults = statement[ind+17:] + "\n"
		statement = statement[:ind]
	}
	isUse := strings.HasPrefix(strings.ToLower(statement), "use ")

	var res string
	if waitUntilResults == "" {
		res = st.execStatement(require, statement)
		log.Infof("output:%s", res)
		st.output.WriteString(res)
	} else {
		ok, err := commontest.WaitUntilWithError(func() (bool, error) {
			res = st.execStatement(require, statement)
			return res == waitUntilResults, nil
		}, 1*time.Minute, 100*time.Millisecond)
		require.NoError(err)
		res = st.execStatement(require, statement)
		if !ok {
			st.output.WriteString("DOES NOT MATCH EXPECTED\n")
		}
		st.output.WriteString(res)
	}

	if isUse && strings.HasSuffix(res, "0 rows returned\n") {
		st.currentSchema = strings.ToLower(statement[4:])
	}
	end := time.Now()
	dur := end.Sub(start)
	log.Infof("Statement execute time ms %d", dur.Milliseconds())
}

func (st *sqlTest) execStatement(require *require.Assertions, statement string) string {
	resChan, err := st.cli.ExecuteStatement(statement, nil, nil)
	require.NoError(err)
	sb := strings.Builder{}
	for line := range resChan {
		sb.WriteString(line)
		sb.WriteRune('\n')
	}
	return sb.String()
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
	apiServerAddress := fmt.Sprintf("127.0.0.1:%d", apiServerListenAddressBase+id)
	var cli *client.Client
	tlsConf := client.TLSConfig{
		EnableTLS:        true,
		TrustedCertsPath: st.testSuite.tlsKeysInfo.ServerCertPath,
		CertPath:         st.testSuite.tlsKeysInfo.ClientCertPath,
		KeyPath:          st.testSuite.tlsKeysInfo.ClientKeyPath,
	}
	if st.testSuite.useHTTPAPI {
		cli = client.NewClientUsingHTTP(apiServerAddress, tlsConf)
	} else {
		cli = client.NewClientUsingGRPC(apiServerAddress, tlsConf)
	}
	cli.SetPageSize(clientPageSize)
	err := cli.Start()
	require.NoError(err)
	st.clientNodeID = id
	return cli
}

func trimBothEnds(str string) string {
	str = strings.TrimLeft(str, " \t\n")
	str = strings.TrimRight(str, " \t\n")
	return str
}

func parseColumnTypes(str string) ([]common.ColumnType, error) {
	parts := strings.Split(str, ",")
	cols := make([]common.ColumnType, 0, len(parts))
	for _, p := range parts {
		t := common.Type(0)
		if err := t.Capture([]string{p}); err != nil {
			return nil, errors.WithStack(err)
		}
		def := parser.ColumnDef{Type: t}
		ct, err := def.ToColumnType()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		cols = append(cols, ct)
	}
	return cols, nil
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
