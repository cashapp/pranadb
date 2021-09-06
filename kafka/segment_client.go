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
	"github.com/squareup/pranadb/perrors"
)

// Kafka Message Provider implementation that uses the SegmentIO golang client

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

func (p *SegmentKafkaMessageProvider) GetMessage(pollTimeout time.Duration) (*Message, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.reader == nil {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), pollTimeout)
	defer cancel()

	msg, err := p.reader.FetchMessage(ctx)
	if err != nil {
		if err == context.DeadlineExceeded {
			return nil, nil
		}
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
			PartitionID: int32(msg.Partition),
			Offset:      msg.Offset,
		},
		TimeStamp: msg.Time,
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   headers,
	}
	//log.Infof("recv %d %d", msg.Partition, atomic.AddInt32(&count, 1))
	return m, nil
}

func (p *SegmentKafkaMessageProvider) CommitOffsets(offsets map[int32]int64) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.reader == nil {
		return nil
	}
	kmsgs := make([]kafka.Message, 0, len(offsets))
	for partition, offset := range offsets {
		kmsgs = append(kmsgs, kafka.Message{
			Topic:     p.topicName,
			Partition: int(partition),
			// The offset passed to commit is 1 higher than the offset of the original message.
			Offset: offset - 1,
		})
	}

	return p.reader.CommitMessages(context.Background(), kmsgs...)
}

func (p *SegmentKafkaMessageProvider) Stop() error {
	return nil
}

func (p *SegmentKafkaMessageProvider) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	err := p.reader.Close()
	p.reader = nil
	return err
}

func (p *SegmentKafkaMessageProvider) Start() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	cfg := &kafka.ReaderConfig{
		GroupID:     p.krpf.groupID,
		Topic:       p.krpf.topicName,
		StartOffset: kafka.FirstOffset,
	}
	for k, v := range p.krpf.props {
		if err := setProperty(cfg, k, v); err != nil {
			return err
		}
	}
	reader := kafka.NewReader(*cfg)
	p.reader = reader
	return nil
}

func setProperty(cfg *kafka.ReaderConfig, k, v string) error {
	switch k {
	case "bootstrap.servers":
		cfg.Brokers = strings.Split(v, ",")
	default:
		return perrors.NewInvalidConfigurationError(fmt.Sprintf("unsupported segmentio/kafka-go client option: %s", v))
	}
	return nil
}
