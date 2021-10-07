//go:build integration
// +build integration

package kafkatest

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/squareup/pranadb/client"
	"github.com/squareup/pranadb/common/commontest"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/msggen"
	"github.com/squareup/pranadb/server"
	"github.com/squareup/pranadb/table"
	"github.com/stretchr/testify/require"
)

const numPartitions = 25
const paymentTopicName = "payments"

var kafkaProvider = NewKafkaProviderFlag()

func TestKafkaIntegration(t *testing.T) {
	RequireKafka(t, KafkaOpts{
		Topic:         paymentTopicName,
		NumPartitions: numPartitions,
		Provider:      *kafkaProvider,
	})

	dataDir, err := ioutil.TempDir("", "kafka-int-test")
	require.NoError(t, err)
	defer func() {
		err := os.RemoveAll(dataDir)
		if err != nil {
			log.Errorf("failed to delete datadir %v", err)
		}
	}()

	cluster := startPranaCluster(t, dataDir)
	defer stopPranaCluster(t, cluster)

	cli := client.NewClient(cluster[0].GetAPIServer().GetListenAddress(), time.Second*5)
	err = cli.Start()
	require.NoError(t, err)
	defer func() {
		err := cli.Stop()
		if err != nil {
			log.Errorf("Failed to close client %v", err)
		}
	}()

	gm, err := msggen.NewGenManager()
	require.NoError(t, err)
	props := map[string]string{
		"bootstrap.servers": "localhost:9092",
	}

	sessionID, err := cli.CreateSession()
	require.NoError(t, err)
	require.NotNil(t, sessionID)

	ch, err := cli.ExecuteStatement(sessionID, "use test")
	require.NoError(t, err)
	res := <-ch
	require.Equal(t, "0 rows returned", res)

	createSourceSQL := fmt.Sprintf(`
create source payments(
    payment_id varchar,
    customer_id bigint,
    payment_time timestamp,
    amount decimal(10, 2),
    payment_type varchar,
    currency varchar,
    fraud_score double,
    primary key (payment_id)
) with (
    brokername = "testbroker",
    topicname = "%s",
    headerencoding = "stringbytes",
    keyencoding = "stringbytes",
    valueencoding = "json",
    columnselectors = (
        k,
        v.customer_id,
        t,
        v.amount,
        v.payment_type,
        v.currency,
        h.fraud_score
    ),
    properties = ()
)
`, paymentTopicName)
	ch, err = cli.ExecuteStatement(sessionID, createSourceSQL)
	require.NoError(t, err)
	res = <-ch
	require.Equal(t, "0 rows returned", res)

	ch, err = cli.ExecuteStatement(sessionID, "select * from payments order by payment_id")
	require.NoError(t, err)
	res = <-ch
	require.Equal(t, "|payment_id|customer_id|payment_time|amount|payment_type|currency|fraud_score|", res)

	var numPayments int64 = 1500

	err = gm.ProduceMessages("payments", paymentTopicName, numPartitions, 0, numPayments, 0, props)
	require.NoError(t, err)

	waitUntilRowsInTable(t, "payments", int(numPayments), cluster)

	ch, err = cli.ExecuteStatement(sessionID, "select * from payments order by payment_id")
	require.NoError(t, err)
	lineCount := 0
	for range ch {
		lineCount++
	}
	require.Equal(t, int(numPayments+2), lineCount)

	// Send more messages
	err = gm.ProduceMessages("payments", paymentTopicName, numPartitions, 0, numPayments, numPayments, props)
	require.NoError(t, err)

	waitUntilRowsInTable(t, "payments", int(numPayments*2), cluster)

	ch, err = cli.ExecuteStatement(sessionID, "select * from payments order by payment_id")
	require.NoError(t, err)

	lineCount = 0
	for range ch {
		lineCount++
	}
	require.Equal(t, int(2*numPayments+2), lineCount)
}

func startPranaCluster(t *testing.T, dataDir string) []*server.Server {
	t.Helper()
	numNodes := 3
	pranaCluster := make([]*server.Server, numNodes)
	brokerConfigs := map[string]conf.BrokerConfig{
		"testbroker": {
			ClientType: conf.BrokerClientDefault,
			Properties: map[string]string{
				"bootstrap.servers": "localhost:9092",
			},
		},
	}
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
		"localhost:6584",
		"localhost:6585",
		"localhost:6586",
	}
	for i := 0; i < numNodes; i++ {
		cnf := conf.NewDefaultConfig()
		cnf.NodeID = i
		cnf.ClusterID = 12345
		cnf.RaftAddresses = raftAddresses
		cnf.NumShards = 30
		cnf.ReplicationFactor = 3
		cnf.DataDir = dataDir
		cnf.TestServer = false
		cnf.KafkaBrokers = brokerConfigs
		cnf.NotifListenAddresses = notifAddresses
		cnf.Debug = true
		cnf.EnableAPIServer = true
		cnf.APIServerListenAddresses = apiServerListenAddresses

		// We set snapshot settings to low values so we can trigger more snapshots and exercise the
		// snapshotting - in real life these would be much higher
		//cnf.DataSnapshotEntries = 10
		//cnf.DataCompactionOverhead = 5
		//cnf.SequenceSnapshotEntries = 10
		//cnf.SequenceCompactionOverhead = 5

		s, err := server.NewServer(*cnf)
		require.NoError(t, err)
		pranaCluster[i] = s
	}
	startCluster(t, pranaCluster)
	return pranaCluster
}

func startCluster(t *testing.T, cluster []*server.Server) {
	t.Helper()
	wg := sync.WaitGroup{}
	for _, prana := range cluster {
		wg.Add(1)
		prana := prana
		go func() {
			err := prana.Start()
			require.NoError(t, err)
			wg.Done()
		}()
	}
	wg.Wait()
}

func stopPranaCluster(t *testing.T, cluster []*server.Server) {
	t.Helper()
	for _, s := range cluster {
		err := s.Stop()
		if err != nil {
			log.Errorf("Failed to stop cluster %v", err)
		}
	}
}

func waitUntilRowsInTable(t *testing.T, tableName string, numRows int, cluster []*server.Server) {
	t.Helper()
	schema, ok := cluster[0].GetMetaController().GetSchema("test")
	require.True(t, ok, "can't find test schema")
	tab, ok := schema.GetTable(tableName)
	require.True(t, ok, fmt.Sprintf("can't find table %s", tableName))
	tabInfo := tab.GetTableInfo()
	totRows := 0
	ok, err := commontest.WaitUntilWithError(func() (bool, error) {
		var err error
		totRows, err = getAllRowsInTable(tabInfo.ID, cluster)
		require.NoError(t, err)
		return totRows == numRows, nil
	}, 30*time.Second, 100*time.Millisecond)
	require.NoError(t, err)
	if !ok {
		for _, prana := range cluster {
			lls, err := prana.GetPushEngine().GetLocalLeaderSchedulers()
			require.NoError(t, err)
			var ss []uint64
			for sid := range lls {
				ss = append(ss, sid)
			}
		}
	}
	require.True(t, ok, "Timed out waiting for %d rows in table %s there are %d", numRows, tableName, totRows)
}

func getAllRowsInTable(tableID uint64, cluster []*server.Server) (int, error) {
	totRows := 0
	for _, prana := range cluster {
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
