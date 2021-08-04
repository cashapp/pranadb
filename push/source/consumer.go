package source

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/kafka"
	"github.com/squareup/pranadb/push/sched"
	"log"
	"time"
)

type MessageConsumer struct {
	msgProvider kafka.MessageProvider
	pollTimeout time.Duration
	maxMessages int
	source      *Source
	started     common.AtomicBool
	loopCh      chan struct{}
	scheduler   *sched.ShardScheduler
}

func NewMessageConsumer(msgProvider kafka.MessageProvider, pollTimeout time.Duration, maxMessages int, source *Source,
	scheduler *sched.ShardScheduler) *MessageConsumer {
	return &MessageConsumer{
		msgProvider: msgProvider,
		pollTimeout: pollTimeout,
		maxMessages: maxMessages,
		source:      source,
		scheduler:   scheduler,
	}
}

func (m *MessageConsumer) Start() {
	if m.started.Get() {
		return
	}
	m.loopCh = make(chan struct{}, 1)
	m.started.Set(true)
	go m.pollLoop()
}

func (m *MessageConsumer) Stop() {
	if !m.started.Get() {
		return
	}
	m.started.Set(false)
	_, ok := <-m.loopCh
	if !ok {
		panic("channel closed")
	}
}

func (m *MessageConsumer) pollLoop() {
	defer func() {
		m.loopCh <- struct{}{}
	}()
	for m.started.Get() {
		messages, offsetsToCommit, err := m.getBatch(m.pollTimeout, m.maxMessages)
		if err != nil {
			log.Printf("Failed to get batch of messages %v", err)
			return
		}
		//
		if len(messages) != 0 {
			// This blocks until messages were actually ingested
			err := m.source.handleMessages(messages, offsetsToCommit, m.scheduler)
			if err != nil {
				log.Printf("Failed to process messages %v", err)
				return
			}

		}
		// Commit the offsets - note there may be more offsets than messages in the case of duplicates
		if len(offsetsToCommit) != 0 {
			if err := m.msgProvider.CommitOffsets(offsetsToCommit); err != nil {
				log.Printf("Failed to commit Kafka offsets %v", err)
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
		partID := msg.PartInfo.PartitionID
		startupLastOffset := m.source.startupLastOffset(partID)

		offsetsToCommit[partID] = msg.PartInfo.Offset
		if msg.PartInfo.Offset <= startupLastOffset {
			// We've seen the message before - this can be the case if a node crashed after offset was committed in
			// Prana but before offset was committed in Kafka.
			// In this case we log a warning, and ignore the message, the offset will be committed
			log.Printf("Duplicate message delivery attempted on schema %s source %s topic %s partition %d - is the server recovering after failure?"+
				" Message will be ignoreed", m.source.sourceInfo.SchemaName, m.source.sourceInfo.Name, m.source.sourceInfo.TopicInfo.TopicName, partID)
			continue
		}

		msgs = append(msgs, msg)
		remaining = pollTimeout - time.Now().Sub(start)
		if remaining <= 0 {
			break
		}
	}
	log.Printf("Got a batch of %d messages", len(msgs))
	return msgs, offsetsToCommit, nil
}
