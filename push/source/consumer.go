package source

import (
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/kafka"
	"github.com/squareup/pranadb/push/sched"
)

type MessageConsumer struct {
	msgProvider             kafka.MessageProvider
	pollTimeout             time.Duration
	maxMessages             int
	source                  *Source
	loopCh                  chan struct{}
	scheduler               *sched.ShardScheduler
	startupCommittedOffsets map[int32]int64
	running                 common.AtomicBool
	messageParser           *MessageParser
	msgBatch                []*kafka.Message
	offsetsToCommit         map[int32]int64
}

func NewMessageConsumer(msgProvider kafka.MessageProvider, pollTimeout time.Duration, maxMessages int, source *Source, scheduler *sched.ShardScheduler, startupCommitOffsets map[int32]int64) (*MessageConsumer, error) {
	lcm := make(map[int32]int64)
	for k, v := range startupCommitOffsets {
		lcm[k] = v
	}
	messageParser, err := NewMessageParser(source.sourceInfo, source.protoRegistry)
	if err != nil {
		return nil, err
	}
	mc := &MessageConsumer{
		msgProvider:             msgProvider,
		pollTimeout:             pollTimeout,
		maxMessages:             maxMessages,
		source:                  source,
		scheduler:               scheduler,
		startupCommittedOffsets: lcm,
		loopCh:                  make(chan struct{}, 1),
		messageParser:           messageParser,
		offsetsToCommit:         make(map[int32]int64),
	}

	msgProvider.SetRebalanceCallback(mc.rebalanceOccurring)

	if err := msgProvider.Start(); err != nil {
		return nil, err
	}

	mc.start()

	return mc, nil
}

func (m *MessageConsumer) start() {
	m.running.Set(true)
	go m.pollLoop()
}

func (m *MessageConsumer) Stop() error {
	if !m.running.CompareAndSet(true, false) {
		return nil
	}
	<-m.loopCh
	return m.msgProvider.Stop()
}

func (m *MessageConsumer) Close() error {
	// Actually unsubscribes
	return m.msgProvider.Close()
}

func (m *MessageConsumer) consumerError(err error, clientError bool) {
	if err := m.msgProvider.Stop(); err != nil {
		log.Errorf("failed to stop message provider %v", err)
	}
	go func() {
		m.source.consumerError(err, clientError)
	}()
}

func (m *MessageConsumer) rebalanceOccurring() error {
	log.Info("Rebalance occurring")
	// This wil be called on the message loop when the consumer calls in to getMessage, so we can simply ignore
	// the current unprocessed batch of messages
	m.msgBatch = nil
	m.offsetsToCommit = make(map[int32]int64)
	return nil
}

func (m *MessageConsumer) pollLoop() {
	defer func() {
		m.loopCh <- struct{}{}
	}()
	for m.running.Get() {
		messages, offsetsToCommit, err := m.getBatch(m.pollTimeout, m.maxMessages)
		if err != nil {
			m.consumerError(err, true)
			return
		}
		if len(messages) != 0 {
			// This blocks until messages were actually ingested
			err := m.source.handleMessages(messages, offsetsToCommit, m.scheduler, m.messageParser)
			if err != nil {
				m.consumerError(err, false)
				return
			}
		}
		// Commit the offsets - note there may be more offsets than messages in the case of duplicates
		if len(offsetsToCommit) != 0 {
			if m.source.commitOffsets.Get() {
				if err := m.msgProvider.CommitOffsets(offsetsToCommit); err != nil {
					m.consumerError(err, true)
					return
				}
			}
			if m.source.enableStats {
				m.source.addCommittedCount(int64(len(messages)))
			}
		}
	}
}

func (m *MessageConsumer) getBatch(pollTimeout time.Duration, maxRecords int) ([]*kafka.Message, map[int32]int64, error) {
	start := time.Now()
	remaining := pollTimeout

	m.msgBatch = nil
	m.offsetsToCommit = make(map[int32]int64)

	// The golang Kafka consumer API returns single messages, not batches, but it's more efficient for us to
	// process in batches. So we attempt to return more than one message at a time.
	for len(m.msgBatch) <= maxRecords {
		msg, err := m.msgProvider.GetMessage(remaining)
		if err != nil {
			return nil, nil, err
		}
		if msg == nil {
			break
		}
		partID := msg.PartInfo.PartitionID
		var lastOffset int64
		var ok bool
		lastOffset, ok = m.startupCommittedOffsets[partID]
		if !ok {
			lastOffset = -1
		} else {
			// The committed offset is actually one more than the last offset seen. Yes this is weird, but
			// it's consistent with how you commit offsets in Kafka (you specify 1 + the last offset you processed)
			// So, to get the last offset actually seen, we subtract 1
			lastOffset--
		}

		m.offsetsToCommit[partID] = msg.PartInfo.Offset + 1
		if msg.PartInfo.Offset <= lastOffset {
			// We've seen the message before - this can be the case if a node crashed after offset was committed in
			// Prana but before offset was committed in Kafka.
			// In this case we log a warning, and ignore the message, the offset will be committed
			log.Warnf("mc: %p Duplicate message delivery attempted on node %d schema %s source %s topic %s partition %d offset %d"+
				" Message will be ignored", m, m.source.cluster.GetNodeID(), m.source.sourceInfo.SchemaName, m.source.sourceInfo.Name, m.source.sourceInfo.TopicInfo.TopicName, partID, msg.PartInfo.Offset)
			if m.source.enableStats {
				m.source.incrementDuplicateCount()
			}
			continue
		}

		m.msgBatch = append(m.msgBatch, msg)
		remaining = pollTimeout - time.Now().Sub(start)
		if remaining <= 0 {
			break
		}
	}
	return m.msgBatch, m.offsetsToCommit, nil
}
