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
	"github.com/squareup/pranadb/common/commontest"

	"github.com/squareup/pranadb/client"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/msggen"
	"github.com/squareup/pranadb/server"
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

	cli := client.NewClientUsingGRPC(cluster[0].GetGRPCServer().GetListenAddress(), client.TLSConfig{})
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

	ch, err := cli.ExecuteStatement("use test", nil, nil)
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
        meta("key"),
        customer_id,
        meta("timestamp"),
        amount,
        payment_type,
        currency,
        meta("header").fraud_score
    ),
    properties = ()
)
`, paymentTopicName)
	ch, err = cli.ExecuteStatement(createSourceSQL, nil, nil)
	require.NoError(t, err)
	res = <-ch
	require.Equal(t, "0 rows returned", res)

	ch, err = cli.ExecuteStatement("select * from payments order by payment_id", nil, nil)
	require.NoError(t, err)
	<-ch
	res = <-ch
	require.Equal(t, "| payment_id | customer_id          | payment_time               | amount     | payment_.. | currency   | fraud_sc.. |", res)

	var numPayments int64 = 1500

	err = gm.ProduceMessages("payments", paymentTopicName, numPartitions, 0, numPayments, 0, props)
	require.NoError(t, err)

	waitUntilRowsInPayments(t, int(numPayments), cli)

	// Send more messages
	err = gm.ProduceMessages("payments", paymentTopicName, numPartitions, 0, numPayments, numPayments, props)
	require.NoError(t, err)

	waitUntilRowsInPayments(t, 2*int(numPayments), cli)
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
		cnf.EnableGRPCAPIServer = true
		cnf.GRPCAPIServerListenAddresses = apiServerListenAddresses

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

func waitUntilRowsInPayments(t *testing.T, numRows int, cli *client.Client) {
	ok, err := commontest.WaitUntilWithError(func() (bool, error) {
		ch, err := cli.ExecuteStatement("select * from payments order by payment_id", nil, nil)
		require.NoError(t, err)
		lastLine := ""
		for line := range ch {
			lastLine = line
		}
		return lastLine == fmt.Sprintf("%d rows returned", numRows), nil
	}, 10*time.Second, 100*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
}
