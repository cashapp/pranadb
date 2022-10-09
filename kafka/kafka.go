package kafka

import "time"

type ClientFactory func(topicName string, props map[string]string, groupID string) MessageClient

type MessageClient interface {
	NewMessageProvider() (MessageProvider, error)
	NewMessageProducer() (MessageProducer, error)
}

type MessageProvider interface {
	GetMessage(pollTimeout time.Duration) (*Message, error)
	CommitOffsets(offsets map[int32]int64) error
	Stop() error
	Start() error
	Close() error
	SetRebalanceCallback(callback RebalanceCallback)
}

type MessageProducer interface {
	SendMessages(messages []Message) error
	Stop() error
	Start() error
}

type Message struct {
	PartInfo  PartInfo
	TimeStamp time.Time
	Key       []byte
	Value     []byte
	Headers   []MessageHeader
}

type RebalanceCallback func() error

type MessageHeader struct {
	Key   string
	Value []byte
}

type PartInfo struct {
	PartitionID int32
	Offset      int64
}
