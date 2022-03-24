package source

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/squareup/pranadb/metrics"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/kafka"
	"github.com/squareup/pranadb/protolib"
	"github.com/squareup/pranadb/push/exec"
	"github.com/squareup/pranadb/push/mover"
	"github.com/squareup/pranadb/push/sched"
	"github.com/squareup/pranadb/sharder"
)

const (
	defaultNumConsumersPerSource  = 2
	defaultPollTimeoutMs          = 20
	defaultMaxPollMessages        = 1000
	maxRetryDelay                 = time.Second * 30
	initialRestartDelay           = time.Millisecond * 100
	numConsumersPerSourcePropName = "prana.source.numconsumers"
	pollTimeoutPropName           = "prana.source.polltimeoutms"
	maxPollMessagesPropName       = "prana.source.maxpollmessages"
)

type RowProcessor interface {
}

type SchedulerSelector interface {
	ChooseLocalScheduler(key []byte) (*sched.ShardScheduler, error)
}

type IngestLimiter interface {
	Limit()
}

type Source struct {
	sourceInfo              *common.SourceInfo
	tableExecutor           *exec.TableExecutor
	sharder                 *sharder.Sharder
	cluster                 cluster.Cluster
	mover                   *mover.Mover
	protoRegistry           protolib.Resolver
	schedSelector           SchedulerSelector
	msgProvFact             kafka.MessageProviderFactory
	msgConsumers            []*MessageConsumer
	queryExec               common.SimpleQueryExec
	lock                    sync.Mutex
	lastRestartDelay        time.Duration
	started                 bool
	numConsumersPerSource   int
	pollTimeoutMs           int
	maxPollMessages         int
	committedCount          int64
	enableStats             bool
	commitOffsets           common.AtomicBool
	rowsIngestedCounter     metrics.Counter
	batchesIngestedCounter  metrics.Counter
	bytesIngestedCounter    metrics.Counter
	ingestDurationHistogram metrics.Observer
	ingestRowSizeHistogram  metrics.Observer
	globalRateLimiter       IngestLimiter

	totIngested uint64
}

var (
	rowsIngestedVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pranadb_rows_ingested_total",
		Help: "counter for number of rows ingested, segmented by source name",
	}, []string{"source"})
	batchesIngestedVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pranadb_batches_ingested_total",
		Help: "counter for number of row batches ingested, segmented by source name",
	}, []string{"source"})
	bytesIngestedVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pranadb_bytes_ingested_total",
		Help: "counter for number of row bytes ingested, segmented by source name",
	}, []string{"source"})
	ingestBatchTimeVec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "pranadb_ingest_batch_time_nanos",
		Help: "histogram measuring time to ingest batches of rows in sources in nanoseconds",
	}, []string{"source"})
	ingestRowSizeVec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "pranadb_ingest_row_size",
		Help: "histogram measuring size of ingested rows in bytes",
	}, []string{"source"})
)

func NewSource(sourceInfo *common.SourceInfo, tableExec *exec.TableExecutor, sharder *sharder.Sharder,
	cluster cluster.Cluster, mover *mover.Mover, schedSelector SchedulerSelector, cfg *conf.Config,
	queryExec common.SimpleQueryExec, registry protolib.Resolver, globalRateLimiter IngestLimiter) (*Source, error) {
	// TODO we should validate the sourceinfo - e.g. check that number of col selectors, column names and column types are the same
	var msgProvFact kafka.MessageProviderFactory
	ti := sourceInfo.TopicInfo
	if ti == nil {
		// TODO not sure if we need this... parser should catch it?
		return nil, errors.NewPranaErrorf(errors.MissingTopicInfo, "No topic info configured for source %s", sourceInfo.Name)
	}
	if cfg.KafkaBrokers == nil {
		return nil, errors.NewPranaError(errors.MissingKafkaBrokers, "No Kafka brokers configured")
	}
	brokerConf, ok := cfg.KafkaBrokers[ti.BrokerName]
	if !ok {
		return nil, errors.NewPranaErrorf(errors.UnknownBrokerName, "Unknown broker. Name: %s", ti.BrokerName)
	}
	props := copyAndAddAll(brokerConf.Properties, ti.Properties)
	groupID := GenerateGroupID(cfg.ClusterID, sourceInfo)
	switch brokerConf.ClientType {
	case conf.BrokerClientFake:
		var err error
		msgProvFact, err = kafka.NewFakeMessageProviderFactory(ti.TopicName, props, groupID)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	case conf.BrokerClientDefault:
		msgProvFact = kafka.NewMessageProviderFactory(ti.TopicName, props, groupID)
	default:
		return nil, errors.NewPranaErrorf(errors.UnsupportedBrokerClientType, "Unsupported broker client type %d", brokerConf.ClientType)
	}
	numConsumers, err := getOrDefaultIntValue(numConsumersPerSourcePropName, sourceInfo.TopicInfo.Properties, defaultNumConsumersPerSource)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	pollTimeoutMs, err := getOrDefaultIntValue(pollTimeoutPropName, sourceInfo.TopicInfo.Properties, defaultPollTimeoutMs)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	maxPollMessages, err := getOrDefaultIntValue(maxPollMessagesPropName, sourceInfo.TopicInfo.Properties, defaultMaxPollMessages)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	rowsIngestedCounter := rowsIngestedVec.WithLabelValues(sourceInfo.Name)
	batchesIngestedCounter := batchesIngestedVec.WithLabelValues(sourceInfo.Name)
	bytesIngestedCounter := bytesIngestedVec.WithLabelValues(sourceInfo.Name)
	ingestDurationHistogram := ingestBatchTimeVec.WithLabelValues(sourceInfo.Name)
	ingestRowSizeHistogram := ingestRowSizeVec.WithLabelValues(sourceInfo.Name)
	source := &Source{
		sourceInfo:              sourceInfo,
		tableExecutor:           tableExec,
		sharder:                 sharder,
		cluster:                 cluster,
		mover:                   mover,
		protoRegistry:           registry,
		schedSelector:           schedSelector,
		msgProvFact:             msgProvFact,
		queryExec:               queryExec,
		numConsumersPerSource:   numConsumers,
		pollTimeoutMs:           pollTimeoutMs,
		maxPollMessages:         maxPollMessages,
		enableStats:             cfg.EnableSourceStats,
		rowsIngestedCounter:     rowsIngestedCounter,
		batchesIngestedCounter:  batchesIngestedCounter,
		bytesIngestedCounter:    bytesIngestedCounter,
		ingestDurationHistogram: ingestDurationHistogram,
		ingestRowSizeHistogram:  ingestRowSizeHistogram,
		globalRateLimiter:       globalRateLimiter,
	}
	source.commitOffsets.Set(true)
	return source, nil
}

func (s *Source) Start() error {
	log.Infof("Starting source %s.%s", s.sourceInfo.SchemaName, s.sourceInfo.Name)
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.started {
		return nil
	}

	if len(s.msgConsumers) != 0 {
		panic("more than zero consumers!")
	}

	for i := 0; i < s.numConsumersPerSource; i++ {
		msgProvider, err := s.msgProvFact.NewMessageProvider()
		if err != nil {
			return errors.WithStack(err)
		}
		// We choose a local scheduler based on the name of the schema, name of source and ordinal of the message consumer
		// The shard of that scheduler is where ingested rows will be staged, ready to be forwarded to their
		// destination shards. Having a message consumer pinned to a shard ensures all messages for a particular
		// Kafka partition are forwarded in order. If different batches used different shards, we couldn't guarantee that.
		// We can't write ingested rows directly into the target shards as we can't commit offsets atomically that way unless
		// we write each in a batch with a single row - this is because messages for the same Kafka partition would be hashed
		// to different target Prana shards.
		// A particular message consumer will always stage all its messages in the same local shard for the life of the consumer
		// We can vary the number of consumers to scale the forwarding.
		sKey := fmt.Sprintf("%s-%s-%d", s.sourceInfo.SchemaName, s.sourceInfo.Name, i)
		scheduler, err := s.schedSelector.ChooseLocalScheduler([]byte(sKey))
		if err != nil {
			return errors.WithStack(err)
		}

		consumer, err := NewMessageConsumer(msgProvider, time.Duration(s.pollTimeoutMs)*time.Millisecond,
			s.maxPollMessages, s, scheduler)
		if err != nil {
			return errors.WithStack(err)
		}
		s.msgConsumers = append(s.msgConsumers, consumer)
	}

	s.started = true
	return nil
}

func (s *Source) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.stop()
}

func (s *Source) IsRunning() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.started
}

func (s *Source) Drop() error {
	// Delete the deduplication ids for the source
	startPrefix := common.AppendUint64ToBufferBE(nil, common.ForwardDedupTableID)
	endPrefix := common.IncrementBytesBigEndian(startPrefix)
	if err := s.cluster.DeleteAllDataInRangeForAllShards(startPrefix, endPrefix); err != nil {
		return errors.WithStack(err)
	}

	// Delete the table data
	tableStartPrefix := common.AppendUint64ToBufferBE(nil, s.sourceInfo.ID)
	tableEndPrefix := common.AppendUint64ToBufferBE(nil, s.sourceInfo.ID+1)
	return s.cluster.DeleteAllDataInRangeForAllShards(tableStartPrefix, tableEndPrefix)
}

func (s *Source) AddConsumingExecutor(mvName string, executor exec.PushExecutor) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.tableExecutor.AddConsumingNode(mvName, executor)
}

func (s *Source) RemoveConsumingExecutor(mvName string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.tableExecutor.RemoveConsumingNode(mvName)
}

func (s *Source) GetConsumingMVs() []string {
	return s.tableExecutor.GetConsumingMvNames()
}

// An error occurred in the consumer
func (s *Source) consumerError(err error, clientError bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		return
		//panic("Got consumer error but souce is not started")
	}
	log.Errorf("Failure in consumer, source will be stopped: %+v. ", err)
	if err2 := s.stop(); err2 != nil {
		return
	}
	if clientError {
		var delay time.Duration
		if s.lastRestartDelay != 0 {
			delay = s.lastRestartDelay
			if delay < maxRetryDelay {
				delay *= 2
			}
		} else {
			delay = initialRestartDelay
		}
		log.Warnf("Will attempt restart of source after delay of %d ms", delay.Milliseconds())
		time.AfterFunc(delay, func() {
			err := s.Start()
			if err != nil {
				log.Errorf("Failed to start source %+v", err)
			}
		})
	}
}

func (s *Source) stop() error {
	if !s.started {
		return nil
	}
	for _, consumer := range s.msgConsumers {
		if err := consumer.Stop(); err != nil {
			return errors.WithStack(err)
		}
	}
	for _, consumer := range s.msgConsumers {
		if err := consumer.Close(); err != nil {
			return errors.WithStack(err)
		}
	}
	s.msgConsumers = nil
	s.started = false
	return nil
}

func (s *Source) handleMessages(messages []*kafka.Message, scheduler *sched.ShardScheduler,
	mp *MessageParser) error {
	errChan := scheduler.ScheduleAction(func() error {
		return s.ingestMessages(messages, scheduler.ShardID(), mp)
	})
	err, ok := <-errChan
	if !ok {
		panic("channel closed")
	}
	return errors.WithStack(err)
}

func (s *Source) ingestMessages(messages []*kafka.Message, shardID uint64, mp *MessageParser) error {

	start := time.Now()

	rows, err := mp.ParseMessages(messages)
	if err != nil {
		return errors.WithStack(err)
	}

	// TODO where Source has no key - need to create one

	// Partition the rows and send them to the appropriate shards
	info := s.sourceInfo.TableInfo
	pkCols := info.PrimaryKeyCols
	colTypes := info.ColumnTypes
	tableID := info.ID

	log.Debugf("Ingesting batch of %d", len(messages))

	forwardBatches := make(map[uint64]*cluster.WriteBatch)

	totBatchSizeBytes := 0
	for i := 0; i < rows.RowCount(); i++ {
		// We throttle the global ingest to prevent the node getting overloaded - it's easy otherwise to saturate the
		// disk throughput which can make the node unstable
		s.globalRateLimiter.Limit()

		row := rows.GetRow(i)
		key := make([]byte, 0, 8)
		key, err := common.EncodeKeyCols(&row, pkCols, colTypes, key)
		if err != nil {
			return errors.WithStack(err)
		}
		destShardID, err := s.sharder.CalculateShard(sharder.ShardTypeHash, key)
		if err != nil {
			return errors.WithStack(err)
		}

		forwardBatch, ok := forwardBatches[destShardID]
		if !ok {
			forwardBatch = cluster.NewWriteBatch(destShardID, true)
			forwardBatches[destShardID] = forwardBatch
		}

		kMsg := messages[i]
		dedupKey := make([]byte, 0, 24)
		dedupKey = common.AppendUint64ToBufferBE(dedupKey, tableID)
		dedupKey = common.AppendUint64ToBufferBE(dedupKey, uint64(kMsg.PartInfo.PartitionID))
		dedupKey = common.AppendUint64ToBufferBE(dedupKey, uint64(kMsg.PartInfo.Offset))

		log.Printf("partition id is %d", kMsg.PartInfo.PartitionID)

		receiverKey := s.mover.EncodeReceiverForIngestKey(destShardID, dedupKey, tableID)

		valueBuff := make([]byte, 0, 32)
		var encodedRow []byte
		encodedRow, err = common.EncodeRow(&row, colTypes, valueBuff)
		if err != nil {
			return err
		}

		forwardBatch.AddPut(receiverKey, s.mover.EncodePrevAndCurrentRow(nil, encodedRow))

		l := len(valueBuff)
		totBatchSizeBytes += l
		s.ingestRowSizeHistogram.Observe(float64(l))
	}

	lb := len(forwardBatches)
	chs := make([]chan error, 0, lb)

	for _, b := range forwardBatches {
		ch := make(chan error, 1)
		chs = append(chs, ch)
		theBatch := b
		go func() {
			err := s.cluster.WriteBatchWithDedup(theBatch)
			if err != nil {
				ch <- err
				return
			}
			ch <- nil
		}()
	}
	for i := 0; i < lb; i++ {
		err, ok := <-chs[i]
		if !ok {
			panic("channel closed")
		}
		if err != nil {
			return errors.WithStack(err)
		}
	}

	ingestTimeNanos := time.Now().Sub(start).Nanoseconds()
	s.ingestDurationHistogram.Observe(float64(ingestTimeNanos))
	s.rowsIngestedCounter.Add(float64(rows.RowCount()))
	s.batchesIngestedCounter.Add(1)
	s.bytesIngestedCounter.Add(float64(totBatchSizeBytes))

	err = s.mover.TransferData(shardID, true)

	tot := atomic.AddUint64(&s.totIngested, uint64(len(messages)))

	log.Infof("Source %s.%s ingested batch of %d total ingested %d", s.sourceInfo.SchemaName, s.sourceInfo.Name, len(messages), tot)

	return err
}

func (s *Source) TableExecutor() *exec.TableExecutor {
	return s.tableExecutor
}

func copyAndAddAll(p1 map[string]string, p2 map[string]string) map[string]string {
	m := make(map[string]string, len(p1)+len(p2))
	for k, v := range p2 {
		m[k] = v
	}
	// p1 properties override p2 so we add them last
	for k, v := range p1 {
		m[k] = v
	}
	return m
}

func GenerateGroupID(clusterID int, sourceInfo *common.SourceInfo) string {
	return fmt.Sprintf("prana-source-%d-%s-%s-%d", clusterID, sourceInfo.SchemaName, sourceInfo.Name, sourceInfo.ID)
}

func getOrDefaultIntValue(propName string, props map[string]string, def int) (int, error) {
	ncs, ok := props[propName]
	var res int
	if ok {
		nc, err := strconv.ParseInt(ncs, 10, 32)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		res = int(nc)
	} else {
		res = def
	}
	return res, nil
}

func (s *Source) addCommittedCount(val int64) {
	atomic.AddInt64(&s.committedCount, val)
}

func (s *Source) GetCommittedCount() int64 {
	return atomic.LoadInt64(&s.committedCount)
}

func (s *Source) SetCommitOffsets(enable bool) {
	s.commitOffsets.Set(enable)
}
