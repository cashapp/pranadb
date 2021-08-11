package kafka

import "time"

type MessageProviderFactory interface {
	NewMessageProvider() (MessageProvider, error)
}

type MessageProvider interface {
	GetMessage(pollTimeout time.Duration) (*Message, error)
	CommitOffsets(offsets map[int32]int64) error
	Stop() error
	SetPartitionsAssignedCb(cb PartitionsCallback)
	SetPartitionsRevokedCb(cb PartitionsCallback)
}

type PartitionsCallback func() error

type Message struct {
	PartInfo  PartInfo
	TimeStamp time.Time
	Key       []byte
	Value     []byte
	Headers   []MessageHeader
}

type MessageHeader struct {
	Key   string
	Value []byte
}

type PartInfo struct {
	PartitionID int32
	Offset      int64
}
