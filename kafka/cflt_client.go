//go:build confluent
// +build confluent

package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/perrors"
	"sync"
	"time"
)

// Kafka Message Provider implementation that uses the standard Confluent golang client

func NewMessageProviderFactory(topicName string, props map[string]string, groupID string) MessageProviderFactory {
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
			return err
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
		return nil, perrors.Errorf("unexpected result from poll %+v", e)
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
	return err
}

func (cmp *ConfluentMessageProvider) Stop() error {
	return nil
}

func (cmp *ConfluentMessageProvider) Close() error {
	cmp.lock.Lock()
	defer cmp.lock.Unlock()
	err := cmp.consumer.Close()
	cmp.consumer = nil
	return err
}

func (cmp *ConfluentMessageProvider) Start() error {
	cmp.lock.Lock()
	defer cmp.lock.Unlock()

	cm := &kafka.ConfigMap{
		"group.id":           cmp.krpf.groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	}
	for k, v := range cmp.krpf.props {
		if err := cm.SetKey(k, v); err != nil {
			return err
		}
	}
	consumer, err := kafka.NewConsumer(cm)
	if err != nil {
		return err
	}
	if err := consumer.Subscribe(cmp.krpf.topicName, cmp.RebalanceOccurred); err != nil {
		return err
	}
	cmp.consumer = consumer
	return nil
}
