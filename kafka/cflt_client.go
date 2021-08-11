package kafka

import (
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
	cm := &kafka.ConfigMap{}
	for k, v := range krpf.props {
		if err := cm.SetKey(k, v); err != nil {
			return nil, err
		}
	}
	if err := cm.SetKey("group.id", krpf.groupID); err != nil {
		return nil, err
	}
	consumer, err := kafka.NewConsumer(cm)
	if err != nil {
		return nil, err
	}
	kmp := &KafkaMessageProvider{}
	if err := consumer.Subscribe(krpf.topicName, kmp.RebalanceOccurred); err != nil {
		return nil, err
	}
	kmp.consumer = consumer
	return kmp, nil
}

type KafkaMessageProvider struct {
	lock      sync.Mutex
	consumer  *kafka.Consumer
	topicName string
	paCb      PartitionsCallback
	prCb      PartitionsCallback
}

func (k *KafkaMessageProvider) SetPartitionsAssignedCb(cb PartitionsCallback) {
	k.paCb = cb
}

func (k *KafkaMessageProvider) SetPartitionsRevokedCb(cb PartitionsCallback) {
	k.prCb = cb
}

func (k *KafkaMessageProvider) RebalanceOccurred(cons *kafka.Consumer, event kafka.Event) error {
	k.lock.Lock()
	defer k.lock.Unlock()
	// TODO prevent getMessage until some time after rebalance event
	log.Printf("Rebalance occurred %s", event.String())

	switch event.(type) {
	case kafka.AssignedPartitions:
		if k.paCb == nil {
			panic("No PartitionsAssignedCallback set")
		}
		if err := k.paCb(); err != nil {
			return err
		}
	case kafka.RevokedPartitions:
		if k.prCb == nil {
			panic("No PartitionsRevokedCallback set")
		}
		if err := k.prCb(); err != nil {
			return err
		}
	}

	return nil
}

func (k *KafkaMessageProvider) GetMessage(pollTimeout time.Duration) (*Message, error) {
	k.lock.Lock()
	defer k.lock.Unlock()
	msg, err := k.consumer.ReadMessage(pollTimeout)
	if err != nil {
		return nil, err
	}
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
}

func (k *KafkaMessageProvider) CommitOffsets(offsetsMap map[int32]int64) error {
	// TODO a bit clunky - can this be optimised to avoid the copying?
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
	return k.consumer.Close()
}
