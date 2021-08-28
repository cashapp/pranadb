package kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"sync"
	"time"
)

// Kafka Message Provider implementation that uses the standard Confluent golang client

func NewCfltMessageProviderFactory(topicName string, props map[string]string, groupID string) MessageProviderFactory {
	return &CfltMessageProviderFactory{
		topicName: topicName,
		props:     props,
		groupID:   groupID,
	}
}

type CfltMessageProviderFactory struct {
	topicName string
	props     map[string]string
	groupID   string
}

func (krpf *CfltMessageProviderFactory) NewMessageProvider() (MessageProvider, error) {
	kmp := &KafkaMessageProvider{}
	kmp.krpf = krpf
	kmp.topicName = krpf.topicName
	return kmp, nil
}

type KafkaMessageProvider struct {
	lock      sync.Mutex
	consumer  *kafka.Consumer
	topicName string
	krpf      *CfltMessageProviderFactory
}

func (k *KafkaMessageProvider) RebalanceOccurred(cons *kafka.Consumer, event kafka.Event) error {
	log.Printf("rebalance event received in consumer %v %p", event, k)
	return nil
}

func (k *KafkaMessageProvider) GetMessage(pollTimeout time.Duration) (*Message, error) {
	k.lock.Lock()
	defer k.lock.Unlock()
	if k.consumer == nil {
		return nil, nil
	}

	ev := k.consumer.Poll(int(pollTimeout.Milliseconds()))
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
		return nil, fmt.Errorf("unexpected result from poll %v", e)
	}
}

func (k *KafkaMessageProvider) CommitOffsets(offsetsMap map[int32]int64) error {
	k.lock.Lock()
	defer k.lock.Unlock()
	if k.consumer == nil {
		return nil
	}
	offsets := make([]kafka.TopicPartition, len(offsetsMap))
	i := 0
	for partID, offset := range offsetsMap {
		offsets[i] = kafka.TopicPartition{
			Topic:     &k.topicName,
			Partition: partID,
			Offset:    kafka.Offset(offset),
		}
		i++
	}
	_, err := k.consumer.CommitOffsets(offsets)
	return err
}

func (k *KafkaMessageProvider) Stop() error {
	return nil
}

func (k *KafkaMessageProvider) Close() error {
	k.lock.Lock()
	defer k.lock.Unlock()
	err := k.consumer.Close()
	k.consumer = nil
	return err
}

func (k *KafkaMessageProvider) Start() error {
	k.lock.Lock()
	defer k.lock.Unlock()

	cm := &kafka.ConfigMap{
		"group.id":           k.krpf.groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	}
	for k, v := range k.krpf.props {
		if err := cm.SetKey(k, v); err != nil {
			return err
		}
	}
	consumer, err := kafka.NewConsumer(cm)
	if err != nil {
		return err
	}
	if err := consumer.Subscribe(k.krpf.topicName, k.RebalanceOccurred); err != nil {
		return err
	}
	k.consumer = consumer
	return nil
}
