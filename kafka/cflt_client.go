//go:build !segmentio
// +build !segmentio

package kafka

import (
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/errors"
)

// Kafka Message Provider implementation that uses the standard Confluent golang client

func NewMessageProviderFactory(topicName string, props map[string]string, groupID string) MessageClient {
	return &ConfluentMessageProviderFactory{
		topicName: topicName,
		props:     props,
		groupID:   groupID,
	}
}

type ConfluentMessageProviderFactory struct {
	topicName string
	props     map[string]string
	groupID   string
}

func (cmpf *ConfluentMessageProviderFactory) NewMessageProvider() (MessageProvider, error) {
	kmp := &ConfluentMessageProvider{}
	kmp.krpf = cmpf
	kmp.topicName = cmpf.topicName
	return kmp, nil
}

func (cmpf *ConfluentMessageProviderFactory) NewMessageProducer() (MessageProducer, error) {
	kmp := &ConfluentMessageProducer{}
	kmp.krpf = cmpf
	kmp.topicName = cmpf.topicName
	return kmp, nil
}

type ConfluentMessageProducer struct {
	krpf      *ConfluentMessageProviderFactory
	topicName string
	producer  *kafka.Producer
}

func (c *ConfluentMessageProducer) Stop() error {
	c.producer.Close()
	return nil
}

func (c *ConfluentMessageProducer) Start() error {
	cm := &kafka.ConfigMap{
		"acks": "all",
	}
	for k, v := range c.krpf.props {
		if err := cm.SetKey(k, v); err != nil {
			return errors.WithStack(err)
		}
	}
	producer, err := kafka.NewProducer(cm)
	if err != nil {
		return err
	}
	c.producer = producer
	return nil
}

func (c *ConfluentMessageProducer) SendMessages(messages []Message) error {
	deliveryChan := make(chan kafka.Event, len(messages))
	for _, msg := range messages {
		hdrs := make([]kafka.Header, len(msg.Headers))
		for i, hdr := range msg.Headers {
			hdrs[i] = kafka.Header{
				Key:   hdr.Key,
				Value: hdr.Value,
			}
		}
		kmsg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &c.topicName, Partition: msg.PartInfo.PartitionID},
			Value:          msg.Value,
			Key:            msg.Key,
			Timestamp:      msg.TimeStamp,
			Headers:        hdrs,
		}
		if err := c.producer.Produce(kmsg, deliveryChan); err != nil {
			return err
		}
	}
	for i := 0; i < len(messages); i++ {
		ev, ok := <-deliveryChan
		if !ok {
			return errors.New("channel was closed")
		}
		m := ev.(*kafka.Message) //nolint:forcetypeassert
		if m.TopicPartition.Error != nil {
			return m.TopicPartition.Error
		}
	}
	return nil
}

var _ MessageProducer = &ConfluentMessageProducer{}

type ConfluentMessageProvider struct {
	lock        sync.Mutex
	consumer    *kafka.Consumer
	topicName   string
	krpf        *ConfluentMessageProviderFactory
	rebalanceCB RebalanceCallback
}

var _ MessageProvider = &ConfluentMessageProvider{}

func (cmp *ConfluentMessageProvider) SetRebalanceCallback(callback RebalanceCallback) {
	cmp.rebalanceCB = callback
}

func (cmp *ConfluentMessageProvider) RebalanceOccurred(cons *kafka.Consumer, event kafka.Event) error {
	log.Debugf("rebalance event received in consumer %v %p", event, cmp)
	_, ok := event.(kafka.RevokedPartitions)
	if ok {
		if err := cmp.rebalanceCB(); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (cmp *ConfluentMessageProvider) GetMessage(pollTimeout time.Duration) (*Message, error) {
	cmp.lock.Lock()
	defer cmp.lock.Unlock()
	if cmp.consumer == nil {
		return nil, nil
	}

	ev := cmp.consumer.Poll(int(pollTimeout.Milliseconds()))
	if ev == nil {
		return nil, nil
	}
	switch e := ev.(type) {
	case *kafka.Message:
		msg := e
		headers := make([]MessageHeader, len(msg.Headers))
		for i, hdr := range msg.Headers {
			headers[i] = MessageHeader{
				Key:   hdr.Key,
				Value: hdr.Value,
			}
		}
		m := &Message{
			PartInfo: PartInfo{
				PartitionID: msg.TopicPartition.Partition,
				Offset:      int64(msg.TopicPartition.Offset),
			},
			TimeStamp: msg.Timestamp,
			Key:       msg.Key,
			Value:     msg.Value,
			Headers:   headers,
		}
		return m, nil
	case kafka.Error:
		return nil, e
	default:
		return nil, errors.Errorf("unexpected result from poll %+v", e)
	}
}

func (cmp *ConfluentMessageProvider) CommitOffsets(offsetsMap map[int32]int64) error {
	cmp.lock.Lock()
	defer cmp.lock.Unlock()
	if cmp.consumer == nil {
		return nil
	}
	offsets := make([]kafka.TopicPartition, len(offsetsMap))
	i := 0
	for partID, offset := range offsetsMap {
		offsets[i] = kafka.TopicPartition{
			Topic:     &cmp.topicName,
			Partition: partID,
			Offset:    kafka.Offset(offset),
		}
		i++
	}
	_, err := cmp.consumer.CommitOffsets(offsets)
	return errors.WithStack(err)
}

func (cmp *ConfluentMessageProvider) Stop() error {
	return nil
}

func (cmp *ConfluentMessageProvider) Close() error {
	cmp.lock.Lock()
	defer cmp.lock.Unlock()
	err := cmp.consumer.Close()
	cmp.consumer = nil
	return errors.WithStack(err)
}

func (cmp *ConfluentMessageProvider) Start() error {
	cmp.lock.Lock()
	defer cmp.lock.Unlock()

	cm := &kafka.ConfigMap{
		"group.id":             cmp.krpf.groupID,
		"auto.offset.reset":    "earliest",
		"enable.auto.commit":   false,
		"session.timeout.ms":   30 * 1000,
		"max.poll.interval.ms": 60 * 1000,
	}
	for k, v := range cmp.krpf.props {
		if err := cm.SetKey(k, v); err != nil {
			return errors.WithStack(err)
		}
	}
	consumer, err := kafka.NewConsumer(cm)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := consumer.Subscribe(cmp.krpf.topicName, cmp.RebalanceOccurred); err != nil {
		return errors.WithStack(err)
	}
	cmp.consumer = consumer
	return nil
}
