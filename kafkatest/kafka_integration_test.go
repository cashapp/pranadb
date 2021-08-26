package kafkatest

import (
	json2 "encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/squareup/pranadb/client"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/common/commontest"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/server"
	"github.com/squareup/pranadb/sharder"
	"github.com/squareup/pranadb/table"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

const numPartitions uint32 = 25

func TestKafkaIntegration(t *testing.T) {

	dataDir, err := ioutil.TempDir("", "kafka-int-test")
	require.NoError(t, err)
	defer func() {
		err := os.RemoveAll(dataDir)
		require.NoError(t, err)
	}()

	cluster := startPranaCluster(t, dataDir)
	defer stopPranaCluster(t, cluster)

	cli := client.NewClient(cluster[0].GetAPIServer().GetListenAddress(), time.Second*5)
	err = cli.Start()
	require.NoError(t, err)
	defer func() {
		err := cli.Stop()
		require.NoError(t, err)
	}()

	cm := &kafka.ConfigMap{}
	err = cm.SetKey("bootstrap.servers", "localhost:9092")
	require.NoError(t, err)
	producer, err := kafka.NewProducer(cm)
	require.NoError(t, err)

	go func() {
		// This appears to be necessary to stop the producer hanging
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	log.Println("Servers started")

	sessionID, err := cli.CreateSession()
	require.NoError(t, err)
	require.NotNil(t, sessionID)

	ch, err := cli.ExecuteStatement(sessionID, "use test")
	require.NoError(t, err)
	res := <-ch
	require.Equal(t, "0 rows returned", res)

	createSourceSQL := `
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
    topicname = "testtopic",
    headerencoding = "stringbytes",
    keyencoding = "stringbytes",
    valueencoding = "json",
    columnselectors = (
        "k",
        "v.customer_id",
        "t",
        "v.amount",
        "v.payment_type",
        "v.currency",
        "h.fraud_score"
    )
    properties = ()
)
`
	ch, err = cli.ExecuteStatement(sessionID, createSourceSQL)
	require.NoError(t, err)
	res = <-ch
	require.Equal(t, "0 rows returned", res)

	ch, err = cli.ExecuteStatement(sessionID, "select * from payments order by payment_id")
	require.NoError(t, err)
	res = <-ch
	require.Equal(t, "|payment_id|customer_id|payment_time|amount|payment_type|currency|fraud_score|", res)

	log.Println("Executed create source")

	numPayments := 1500

	sendMessages(t, 0, numPayments, 17, "testtopic", producer)

	log.Println("sent messages")

	start := time.Now()
	waitUntilRowsInTable(t, "payments", numPayments, cluster)
	dur := time.Now().Sub(start)
	log.Printf("Waiting for rows took %d ms", dur.Milliseconds())

	log.Println("Executing query again")
	ch, err = cli.ExecuteStatement(sessionID, "select * from payments order by payment_id")
	require.NoError(t, err)
	lineCount := 0
	for line := range ch {
		log.Println(line)
		lineCount++
	}
	require.Equal(t, numPayments+2, lineCount)

	//for i := 0; i < 100; i++ {

	//log.Printf("************* ITERATION %d", i)

	// Send more messages
	sendMessages(t, numPayments, numPayments, 17, "testtopic", producer)

	waitUntilRowsInTable(t, "payments", numPayments*2, cluster)

	log.Println("Sent more messages")
	ch, err = cli.ExecuteStatement(sessionID, "select * from payments order by payment_id")
	require.NoError(t, err)

	lineCount = 0
	for line := range ch {
		log.Println(line)
		lineCount++
	}
	require.Equal(t, 2*numPayments+2, lineCount)

	log.Println("Test completed ok")
	//}

	producer.Close()
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
	log.Println("Stopping cluster")
	for _, s := range cluster {
		err := s.Stop()
		require.NoError(t, err)
	}
}

func sendMessages(t *testing.T, startID int, numPayments int, numCustomers int, topicName string, producer *kafka.Producer) {
	t.Helper()
	//time.Sleep(5 * time.Second)
	log.Println("Sending messages")
	rnd := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	paymentTypes := []string{"btc", "p2p", "other"}
	currencies := []string{"gbp", "usd", "eur", "aud"}
	timestamp := time.Date(2021, time.Month(4), 12, 9, 0, 0, 0, time.UTC)
	for i := startID; i < numPayments+startID; i++ {
		m := make(map[string]interface{})
		paymentID := fmt.Sprintf("payment%06d", i)
		customerID := i % numCustomers
		m["customer_id"] = customerID
		m["amount"] = fmt.Sprintf("%.2f", float64(rnd.Int31n(1000000))/10)
		m["payment_type"] = paymentTypes[i%len(paymentTypes)]
		m["currency"] = currencies[i%len(currencies)]
		json, err := json2.Marshal(&m)
		require.NoError(t, err)
		var headers []kafka.Header
		fs := rnd.Float64()
		headers = append(headers, kafka.Header{
			Key:   "fraud_score",
			Value: []byte(fmt.Sprintf("%.2f", fs)),
		})

		// The cflt client doesn't choose partition itself, so we hash the customer id to choose the partition
		kv := make([]byte, 0, 8)
		kv = common.AppendUint64ToBufferBE(kv, uint64(customerID))
		hash, err := sharder.Hash(kv)
		require.NoError(t, err)
		partition := hash % numPartitions

		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: int32(partition)},
			Value:          json,
			Key:            []byte(paymentID),
			Timestamp:      timestamp,
			Headers:        headers,
		}
		err = producer.Produce(msg, nil)
		require.NoError(t, err)
		timestamp = timestamp.Add(1 * time.Second)
	}

	outstanding := producer.Flush(10000)
	require.Equal(t, 0, outstanding)
	//producer.Close()
	log.Println("*********Sent messages")
}

func waitUntilRowsInTable(t *testing.T, tableName string, numRows int, cluster []*server.Server) {
	t.Helper()
	log.Printf("Waiting for %d rows in table %s", numRows, tableName)
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
	}, 60*time.Second, 100*time.Millisecond)
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
	log.Println("Rows arrived ok")
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
