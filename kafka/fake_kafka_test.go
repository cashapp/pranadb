package kafka

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common/commontest"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGetFakeKafkas(t *testing.T) {
	fk1 := NewFakeKafka()
	fk2 := NewFakeKafka()
	fk3 := NewFakeKafka()

	ffk1, ok := GetFakeKafka(fk1.ID)
	require.True(t, ok)
	require.Equal(t, fk1, ffk1)

	ffk2, ok := GetFakeKafka(fk2.ID)
	require.True(t, ok)
	require.Equal(t, fk2, ffk2)

	ffk3, ok := GetFakeKafka(fk3.ID)
	require.True(t, ok)
	require.Equal(t, fk3, ffk3)

	_, ok = GetFakeKafka(12345)
	require.False(t, ok)
}

func TestCreateDeleteTopic(t *testing.T) {

	fk := NewFakeKafka()

	t1, err := fk.CreateTopic("topic1", 10)
	require.NoError(t, err)
	require.Equal(t, "topic1", t1.Name)
	require.Equal(t, 10, len(t1.partitions))

	names := fk.GetTopicNames()
	require.Equal(t, 1, len(names))
	require.Equal(t, "topic1", names[0])

	t2, err := fk.CreateTopic("topic2", 20)
	require.NoError(t, err)
	require.Equal(t, "topic2", t2.Name)
	require.Equal(t, 20, len(t2.partitions))

	names = fk.GetTopicNames()
	require.Equal(t, 2, len(names))
	m := map[string]struct{}{}
	for _, name := range names {
		m[name] = struct{}{}
	}
	_, ok := m["topic2"]
	require.True(t, ok)
	_, ok = m["topic1"]
	require.True(t, ok)

	err = fk.DeleteTopic("topic1")
	require.NoError(t, err)
	names = fk.GetTopicNames()
	require.Equal(t, 1, len(names))
	require.Equal(t, "topic2", names[0])

	err = fk.DeleteTopic("topic2")
	require.NoError(t, err)
	names = fk.GetTopicNames()
	require.Equal(t, 0, len(names))
}

func TestIngestConsumeOneSubscriber(t *testing.T) {
	fk := NewFakeKafka()
	parts := 10
	topic, err := fk.CreateTopic("topic1", parts)
	require.NoError(t, err)

	numMessages := 1000
	sentMsgs := sendMessages(t, fk, numMessages, topic.Name)

	groupID := "group1"
	sub, err := topic.CreateSubscriber(groupID, nil)
	require.NoError(t, err)

	receivedMsgs := map[string]*Message{}
	for i := 0; i < numMessages; i++ {
		msg, err := sub.GetMessage(5 * time.Second)
		require.NoError(t, err)
		require.NotNil(t, msg)
		receivedMsgs[string(msg.Key)] = msg
	}

	for _, msg := range sentMsgs {
		rec, ok := receivedMsgs[string(msg.Key)]
		require.True(t, ok)
		require.Equal(t, msg, rec)
	}

	group, ok := topic.getGroup(groupID)
	require.True(t, ok)
	require.Equal(t, 1, len(group.subscribers))
}

func TestIngestConsumeTwoSubscribersOneGroup(t *testing.T) {
	fk := NewFakeKafka()
	parts := 1000
	topic, err := fk.CreateTopic("topic1", parts)
	require.NoError(t, err)
	var numMessages int64 = 10
	groupID := "group1"
	var msgCounter int64
	consumer1 := newConsumer(groupID, topic, &msgCounter, numMessages)
	consumer2 := newConsumer(groupID, topic, &msgCounter, numMessages)
	consumer1.start()
	consumer2.start()

	sentMsgs := sendMessages(t, fk, int(numMessages), topic.Name)

	commontest.WaitUntil(t, func() (bool, error) {
		return atomic.LoadInt64(&msgCounter) == numMessages, nil
	})

	recvMsgs := make(map[string][]byte)
	for _, msg := range consumer1.getMessages() {
		recvMsgs[string(msg.Key)] = msg.Value
	}
	for _, msg := range consumer2.getMessages() {
		recvMsgs[string(msg.Key)] = msg.Value
	}

	for _, sentMsg := range sentMsgs {
		_, ok := recvMsgs[string(sentMsg.Key)]
		require.True(t, ok, fmt.Sprintf("did not receive msg %s", string(sentMsg.Key)))
	}
}

func newConsumer(groupID string, topic *Topic, msgCounter *int64, maxMessages int64) *consumer {
	return &consumer{
		groupID:     groupID,
		topic:       topic,
		msgCounter:  msgCounter,
		maxMessages: maxMessages,
	}
}

type consumer struct {
	lock        sync.Mutex
	groupID     string
	topic       *Topic
	msgCounter  *int64
	msgs        []*Message
	maxMessages int64
}

func (c *consumer) start() {
	go func() {
		err := c.runLoop()
		if err != nil {
			log.Fatal(err)
		}
	}()
}

func (c *consumer) runLoop() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	subscriber, err := c.topic.CreateSubscriber(c.groupID, c.rebalance)
	if err != nil {
		return err
	}
	for {
		msg, err := subscriber.GetMessage(10 * time.Millisecond)
		if err != nil {
			return err
		}
		if msg != nil {
			c.msgs = append(c.msgs, msg)
			offsets := make(map[int32]int64)
			offsets[msg.PartInfo.PartitionID] = msg.PartInfo.Offset + 1
			if err := subscriber.commitOffsets(offsets); err != nil {
				return err
			}
			atomic.AddInt64(c.msgCounter, 1)
		}
		if atomic.LoadInt64(c.msgCounter) == c.maxMessages {
			return nil
		}
	}
}

func (c *consumer) rebalance() error {
	log.Println("rebalance occurred")
	return nil
}

func (c *consumer) getMessages() []*Message {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.msgs
}

func sendMessages(t *testing.T, fk *FakeKafka, numMessages int, topicName string) []*Message {
	t.Helper()
	var sentMsgs []*Message
	for i := 0; i < numMessages; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		var headers []MessageHeader
		for j := 0; j < 10; j++ {
			headerKey := fmt.Sprintf("header-key-%d", i)
			headerValue := []byte(fmt.Sprintf("header-value-%d", i))
			headers = append(headers, MessageHeader{
				Key:   headerKey,
				Value: headerValue,
			})
		}
		msg := &Message{
			Key:     key,
			Value:   value,
			Headers: headers,
		}
		err := fk.IngestMessage(topicName, msg)
		require.NoError(t, err)
		sentMsgs = append(sentMsgs, msg)
	}
	return sentMsgs
}
