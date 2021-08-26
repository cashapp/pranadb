package source

import (
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/kafka"
	"github.com/squareup/pranadb/push/sched"
	"time"
)

type MessageConsumer struct {
	msgProvider             kafka.MessageProvider
	pollTimeout             time.Duration
	maxMessages             int
	source                  *Source
	loopCh                  chan struct{}
	scheduler               *sched.ShardScheduler
	startupCommittedOffsets map[int32]int64
	started                 common.AtomicBool
	running                 common.AtomicBool
	messageParser           *MessageParser

	rebalancing common.AtomicBool
}

func NewMessageConsumer(msgProvider kafka.MessageProvider, pollTimeout time.Duration, maxMessages int, source *Source,
	scheduler *sched.ShardScheduler, startupCommitOffsets map[int32]int64) (*MessageConsumer, error) {
	lcm := make(map[int32]int64)
	for k, v := range startupCommitOffsets {
		lcm[k] = v
	}
	messageParser, err := NewMessageParser(source.sourceInfo)
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
	}
	msgProvider.SetPartitionsAssignedCb(mc.partitionsAssigned)
	msgProvider.SetPartitionsRevokedCb(mc.partitionsRevoked)

	// It's important that the message consumer is started before the message provider
	// Otherwise there is a race condition where rebalancing starts (but not finishes) before the consumer is started,
	// and then the consumer is started, which starts the poll loop, but rebalancing is still in progress.
	// Messages can then be consumed and committed before rebalancing is complete which can result in out of order
	// commits, if another consumer has started and has committed around the same time.
	// If the message provider is not set by the time the consumer has called getMessage first time, it will simply
	// return nil, which is ok
	mc.start()

	// Starting the provider actually subscribes
	if err := msgProvider.Start(); err != nil {
		return nil, err
	}
	return mc, nil
}

func (m *MessageConsumer) partitionsAssigned() error {
	if !m.rebalancing.Get() {
		panic("not rebalancing")
	}
	if !m.running.CompareAndSet(false, true) {
		return nil
	}
	// We start the poll loop again
	go m.pollLoop()
	return nil
}

/*
partitions revoked and assigned are called on the poll loop with the confluent go client

we should assume they're always called this way, this is good in a way as cb will only be invoked when getMessage is called
so we know there are no messages waiting to be committed.

so we don't need to do anything in partition assigned and revoked

and in the fake consumer, if a consumer is added or removed, then we first set revoking=true on each consumer, we allow
offsets to be committed at this point, then when the next call to getMessage comes in on each consumer and all are calling
getMessage then we block them on a waitgroup, we now know offsets have been committed, then we can do the rebalance
*/

func (m *MessageConsumer) partitionsRevoked() error {
	// Re-balancing is about to occur - the Kafka client should guarantee this is called before rebalancing actually
	// happens - it gives the consumer a chance to finish processing any messages and to commit any offsets.
	// Without this it would be possible for another consumer to take over processing for the the same set of
	// partitions and commit an offset in the same partition before this one had committed theirs.

	// We wait for any processing to complete on the poll loop
	m.rebalancing.Set(true)

	// We have to wait for poll loop to stop on a different goroutine as this callback is called from the poll loop
	// with the confluent client
	if m.running.CompareAndSet(true, false) {
		go func() {
			_, ok := <-m.loopCh
			if !ok {
				panic("channel closed")
			}
			m.rebalancing.Set(false)
		}()
	}

	// TODO - it is not sufficient to just wait for the pool loop to stop.
	// Some time after the poll loop has stopped messages wil be forwarded async to other nodes.
	// If owner of partitions changes during rebalance then messages processed from the new owner consumer
	// could be forwarded before the messages processed by the earlier consumer.
	// To deal with this case we should add an extra delay in here

	return nil
}

func (m *MessageConsumer) start() {
	m.started.Set(true)
	m.running.Set(true)
	go m.pollLoop()
}

func (m *MessageConsumer) Stop() error {
	if !m.started.CompareAndSet(true, false) {
		return nil
	}

	// If called from a consumer error it will be called on the poll loop and the consumer error will
	// have already set running to false
	m.waitForPollLoopToStop()

	// unsubscribe happens here
	return m.msgProvider.Stop()
}

func (m *MessageConsumer) waitForPollLoopToStop() {
	if m.running.CompareAndSet(true, false) {
		_, ok := <-m.loopCh
		if !ok {
			panic("channel closed")
		}
	}
}

func (m *MessageConsumer) consumerError(err error, clientError bool) {
	m.running.Set(false)
	go func() {
		m.source.consumerError(err, clientError)
	}()
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

			log.Printf("Got batch of %d messages", len(messages))

			// This blocks until messages were actually ingested
			err := m.source.handleMessages(messages, offsetsToCommit, m.scheduler, m.messageParser)
			if err != nil {
				m.consumerError(err, false)
				return
			}
		}
		// Commit the offsets - note there may be more offsets than messages in the case of duplicates

		if len(offsetsToCommit) != 0 {
			if err := m.msgProvider.CommitOffsets(offsetsToCommit); err != nil {
				m.consumerError(err, true)
				return
			}
		}
	}
}

func (m *MessageConsumer) getBatch(pollTimeout time.Duration, maxRecords int) ([]*kafka.Message, map[int32]int64, error) {
	start := time.Now()
	remaining := pollTimeout
	var msgs []*kafka.Message
	offsetsToCommit := make(map[int32]int64)
	// The golang Kafka consumer API returns single messages, not batches, but it's more efficient for us to
	// process in batches. So we attempt to return more than one message at a time.
	for len(msgs) <= maxRecords {
		msg, err := m.msgProvider.GetMessage(remaining)
		if err != nil {
			return nil, nil, err
		}
		if msg == nil {
			break
		}
		log.Printf("Got message from message provider %s", string(msg.Key))

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

		offsetsToCommit[partID] = msg.PartInfo.Offset + 1
		if msg.PartInfo.Offset <= lastOffset {
			// We've seen the message before - this can be the case if a node crashed after offset was committed in
			// Prana but before offset was committed in Kafka.
			// In this case we log a warning, and ignore the message, the offset will be committed
			log.Warnf("Duplicate message delivery attempted on node %d schema %s source %s topic %s partition %d offset %d"+
				" Message will be ignored", m.source.cluster.GetNodeID(), m.source.sourceInfo.SchemaName, m.source.sourceInfo.Name, m.source.sourceInfo.TopicInfo.TopicName, partID, msg.PartInfo.Offset)
			continue
		}

		msgs = append(msgs, msg)
		remaining = pollTimeout - time.Now().Sub(start)
		if remaining <= 0 {
			break
		}
	}
	return msgs, offsetsToCommit, nil
}
