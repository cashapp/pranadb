package source

import (
	"github.com/squareup/pranadb/kafka/load"

	"github.com/squareup/pranadb/push/util"
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

type Source struct {
	sourceInfo              *common.SourceInfo
	tableExecutor           *exec.TableExecutor
	sharder                 *sharder.Sharder
	cluster                 cluster.Cluster
	protoRegistry           protolib.Resolver
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
	ingestExpressions       []*common.Expression
	cfg                     *conf.Config
	restartTimer            *time.Timer
	stopped                 bool // represents a hard stop - not a stop then a restart after delay
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

func NewSource(sourceInfo *common.SourceInfo, tableExec *exec.TableExecutor, ingestExpressions []*common.Expression, sharder *sharder.Sharder,
	cluster cluster.Cluster, cfg *conf.Config, queryExec common.SimpleQueryExec, registry protolib.Resolver) (*Source, error) {
	numConsumers, err := common.GetOrDefaultIntProperty(numConsumersPerSourcePropName, sourceInfo.OriginInfo.Properties, defaultNumConsumersPerSource)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	pollTimeoutMs, err := common.GetOrDefaultIntProperty(pollTimeoutPropName, sourceInfo.OriginInfo.Properties, defaultPollTimeoutMs)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	maxPollMessages, err := common.GetOrDefaultIntProperty(maxPollMessagesPropName, sourceInfo.OriginInfo.Properties, defaultMaxPollMessages)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ti := sourceInfo.OriginInfo
	var brokerConf conf.BrokerConfig
	var ok bool
	if cfg.KafkaBrokers != nil {
		brokerConf, ok = cfg.KafkaBrokers[ti.BrokerName]
	}
	if !ok || cfg.KafkaBrokers == nil {
		return nil, errors.NewPranaErrorf(errors.InvalidStatement, "Unknown broker %s - has it been configured in the server config?", ti.BrokerName)
	}
	props := copyAndAddAll(brokerConf.Properties, ti.Properties)
	groupID := sourceInfo.OriginInfo.ConsumerGroupID
	var msgProvFact kafka.MessageProviderFactory
	switch brokerConf.ClientType {
	case conf.BrokerClientFake:
		var err error
		msgProvFact, err = kafka.NewFakeMessageProviderFactory(ti.TopicName, props, groupID)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	case conf.BrokerClientDefault:
		msgProvFact = kafka.NewMessageProviderFactory(ti.TopicName, props, groupID)
	case conf.BrokerClientGenerator:
		msgProvFact, err = load.NewMessageProviderFactory(10000, numConsumers, cluster.GetNodeID(), sourceInfo.OriginInfo.Properties)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.NewPranaErrorf(errors.InvalidStatement, "Unsupported broker client type %d", brokerConf.ClientType)
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
		protoRegistry:           registry,
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
		ingestExpressions:       ingestExpressions,
		cfg:                     cfg,
	}
	source.commitOffsets.Set(true)
	return source, nil
}

func (s *Source) Start() error {

	s.lock.Lock()
	defer s.lock.Unlock()

	return s.start()
}

func (s *Source) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.stopped = true // hard stop - no restart
	if s.restartTimer != nil {
		s.restartTimer.Stop()
	}
	return s.stop()
}

func (s *Source) IsRunning() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.started
}

func (s *Source) Drop() error {
	if s.tableExecutor.IsTransient() {
		return nil
	}
	// Delete the deduplication ids for the source
	log.Printf("dropping source %s %d", s.sourceInfo.Name, s.sourceInfo.ID)
	startPrefix := common.AppendUint64ToBufferBE(nil, common.ForwardDedupTableID)
	startPrefix = common.AppendUint64ToBufferBE(startPrefix, s.sourceInfo.ID)
	endPrefix := common.IncrementBytesBigEndian(startPrefix)
	if err := s.cluster.DeleteAllDataInRangeForAllShardsLocally(startPrefix, endPrefix); err != nil {
		return errors.WithStack(err)
	}

	// Delete the table data
	tableStartPrefix := common.AppendUint64ToBufferBE(nil, s.sourceInfo.ID)
	tableEndPrefix := common.AppendUint64ToBufferBE(nil, s.sourceInfo.ID+1)
	return s.cluster.DeleteAllDataInRangeForAllShardsLocally(tableStartPrefix, tableEndPrefix)
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

func (s *Source) ingestError(err error, clientError bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		return
	}
	if clientError {
		// Probably Kafka is unavailable
		log.Warnf("Failure in Kafka client, source %s.%s will be stopped: %+v", s.sourceInfo.SchemaName, s.sourceInfo.Name, err)
		if err2 := s.stop(); err2 != nil {
			return
		}
		// We retry connecting with exponentially increasing delay
		var delay time.Duration
		if s.lastRestartDelay != 0 {
			delay = s.lastRestartDelay
			if delay < maxRetryDelay {
				delay *= 2
			}
		} else {
			delay = initialRestartDelay
		}
		s.restartAfterDelay(delay)
		return
	}

	// Unexpected error in ingest, log and stop source.
	log.Errorf("Failure in ingest, source %s.%s will be stopped: %+v", s.sourceInfo.SchemaName, s.sourceInfo.Name, err)
	if err2 := s.stop(); err2 != nil {
		return
	}
}

func (s *Source) restartAfterDelay(delay time.Duration) {
	log.Warnf("Will attempt restart of source %s.%s after delay of %d ms", s.sourceInfo.SchemaName, s.sourceInfo.Name, delay.Milliseconds())
	s.restartTimer = time.AfterFunc(delay, func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		if s.stopped {
			return
		}
		err := s.start()
		if err != nil {
			log.Errorf("Failed to start source %+v", err)
		}
	})
}

func (s *Source) start() error {
	log.Infof("Starting source %s.%s", s.sourceInfo.SchemaName, s.sourceInfo.Name)

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
		consumer, err := NewMessageConsumer(msgProvider, time.Duration(s.pollTimeoutMs)*time.Millisecond,
			s.maxPollMessages, s)
		if err != nil {
			return errors.WithStack(err)
		}
		s.msgConsumers = append(s.msgConsumers, consumer)
	}

	s.started = true
	return nil
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

func (s *Source) ingestMessages(messages []*kafka.Message, mp *MessageParser) error {

	start := time.Now()

	rows, err := mp.ParseMessages(messages)
	if err != nil {
		return errors.WithStack(err)
	}

	// Partition the rows and send them to the appropriate shards
	info := s.sourceInfo.TableInfo
	pkCols := info.PrimaryKeyCols
	colTypes := info.ColumnTypes
	tableID := info.ID

	forwardBatches := make(map[uint64]*cluster.WriteBatch)

	totBatchSizeBytes := 0
	rowsIngested := 0
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)

		filtered := false
		if s.ingestExpressions != nil {
			// We filter out any rows which don't match the optional ingest filter
			for _, predicate := range s.ingestExpressions {
				accept, isNull, err := predicate.EvalBoolean(&row)
				if err != nil {
					return errors.WithStack(err)
				}
				if isNull {
					return errors.Error("null returned from evaluating select predicate")
				}
				if !accept {
					filtered = true
					break
				}
			}
		}
		if filtered {
			continue
		}

		for _, pkCol := range s.sourceInfo.PrimaryKeyCols {
			if row.IsNull(pkCol) {
				return errors.New("cannot ingest message, null value in PK col(s)")
			}
		}

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
			forwardBatch = cluster.NewWriteBatch(destShardID)
			forwardBatches[destShardID] = forwardBatch
		}

		kMsg := messages[i]
		forwardKey := util.EncodeKeyForForwardIngest(tableID, uint64(kMsg.PartInfo.PartitionID),
			uint64(kMsg.PartInfo.Offset+1), tableID)

		valueBuff := make([]byte, 0, 32)
		var encodedRow []byte
		encodedRow, err = common.EncodeRow(&row, colTypes, valueBuff)
		if err != nil {
			return err
		}

		forwardBatch.AddPut(forwardKey, util.EncodePrevAndCurrentRow(nil, encodedRow))

		l := len(valueBuff)
		totBatchSizeBytes += l
		s.ingestRowSizeHistogram.Observe(float64(l))
		rowsIngested++
	}

	if err := util.SendForwardBatches(forwardBatches, s.cluster); err != nil {
		log.Errorf("failed to send ingest forward batches %+v", err)
		return err
	}

	ingestTimeNanos := time.Now().Sub(start).Nanoseconds()
	s.ingestDurationHistogram.Observe(float64(ingestTimeNanos))
	s.rowsIngestedCounter.Add(float64(rowsIngested))
	s.batchesIngestedCounter.Add(1)
	s.bytesIngestedCounter.Add(float64(totBatchSizeBytes))

	log.Infof("ingested batch of %d", rowsIngested)

	return nil
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

func (s *Source) addCommittedCount(val int64) {
	atomic.AddInt64(&s.committedCount, val)
}

func (s *Source) GetCommittedCount() int64 {
	return atomic.LoadInt64(&s.committedCount)
}

func (s *Source) SetCommitOffsets(enable bool) {
	s.commitOffsets.Set(enable)
}

func (s *Source) SetMaxConsumerRate(rate int) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, consumer := range s.msgConsumers {
		provider := consumer.msgProvider
		provider.SetMaxRate(rate)
	}
}
