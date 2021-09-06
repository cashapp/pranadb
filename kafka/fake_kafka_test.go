package kafka

import (
	"fmt"
	"github.com/stretchr/testify/require"
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
