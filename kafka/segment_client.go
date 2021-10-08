//go:build !confluent
// +build !confluent

package kafka

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/squareup/pranadb/errors"
)

// Kafka Message Provider implementation that uses the SegmentIO golang client
// Important note - the SegmentIO client does not handle the Kafka rebalance protocol and does not allow callbacks
// to be set which are called when partitions are revoked and assigned. In the Kafka rebalance protocol these callbacks
// gives the user of the client an opportunity to commit any messages or reset other state before partitions are assigned
// to other consumers. The protocol guarantees, under normal conditions, that no other consumers will consume from
// those partitions until the callbacks have been executed on all consumers.
// Without this it's possible that consumers could concurrently process duplicate messages.
// DO NOT USE this client in production. We leave it here for use during development as it's easier to build on newer
// Macbooks than the Confluent client.

func NewMessageProviderFactory(topicName string, props map[string]string, groupID string) MessageProviderFactory {
	return &SegmentMessageProviderFactory{
		topicName: topicName,
		props:     props,
		groupID:   groupID,
	}
}

type SegmentMessageProviderFactory struct {
	topicName string
	props     map[string]string
	groupID   string
}

func (smpf *SegmentMessageProviderFactory) NewMessageProvider() (MessageProvider, error) {
	mp := &SegmentKafkaMessageProvider{}
	mp.krpf = smpf
	mp.topicName = smpf.topicName
	return mp, nil
}

type SegmentKafkaMessageProvider struct {
	lock      sync.Mutex
	reader    *kafka.Reader
	topicName string
	krpf      *SegmentMessageProviderFactory
}

var _ MessageProvider = &SegmentKafkaMessageProvider{}

func (smp *SegmentKafkaMessageProvider) SetRebalanceCallback(callback RebalanceCallback) {
}

func (smp *SegmentKafkaMessageProvider) GetMessage(pollTimeout time.Duration) (*Message, error) {
	smp.lock.Lock()
	defer smp.lock.Unlock()
	if smp.reader == nil {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), pollTimeout)
	defer cancel()

	msg, err := smp.reader.FetchMessage(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, nil
		}
		return nil, errors.WithStack(err)
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
			PartitionID: int32(msg.Partition),
			Offset:      msg.Offset,
		},
		TimeStamp: msg.Time,
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   headers,
	}
	return m, nil
}

func (smp *SegmentKafkaMessageProvider) CommitOffsets(offsets map[int32]int64) error {
	smp.lock.Lock()
	defer smp.lock.Unlock()
	if smp.reader == nil {
		return nil
	}
	kmsgs := make([]kafka.Message, 0, len(offsets))
	for partition, offset := range offsets {
		kmsgs = append(kmsgs, kafka.Message{
			Topic:     smp.topicName,
			Partition: int(partition),
			// The offset passed to commit is 1 higher than the offset of the original message.
			Offset: offset - 1,
		})
	}

	return smp.reader.CommitMessages(context.Background(), kmsgs...)
}

func (smp *SegmentKafkaMessageProvider) Stop() error {
	return nil
}

func (smp *SegmentKafkaMessageProvider) Close() error {
	smp.lock.Lock()
	defer smp.lock.Unlock()
	err := smp.reader.Close()
	smp.reader = nil
	return errors.WithStack(err)
}

func (smp *SegmentKafkaMessageProvider) Start() error {
	smp.lock.Lock()
	defer smp.lock.Unlock()

	cfg := &kafka.ReaderConfig{
		GroupID:     smp.krpf.groupID,
		Topic:       smp.krpf.topicName,
		StartOffset: kafka.FirstOffset,
	}
	for k, v := range smp.krpf.props {
		if err := setProperty(cfg, k, v); err != nil {
			return errors.WithStack(err)
		}
	}
	reader := kafka.NewReader(*cfg)
	smp.reader = reader
	return nil
}

func setProperty(cfg *kafka.ReaderConfig, k, v string) error {
	switch k {
	case "bootstrap.servers":
		cfg.Brokers = strings.Split(v, ",")
	default:
		return errors.NewInvalidConfigurationError(fmt.Sprintf("unsupported segmentio/kafka-go client option: %s", v))
	}
	return nil
}
