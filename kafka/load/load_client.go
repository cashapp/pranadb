package load

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/kafka"
	"github.com/squareup/pranadb/msggen"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"
)

var _ kafka.MessageClient = &LoadClientMessageProviderFactory{}

type LoadClientMessageProviderFactory struct {
	bufferSize             int
	partitionsPerConsumer  int
	consumersPerSource     int
	partitionsStart        int
	nextPartition          int
	properties             map[string]string
	maxMessagesPerConsumer int64
	uniqueIDsPerPartition  int64
	messageGeneratorName   string
	committedOffsets       map[int32]int64
	committedOffsetsLock   sync.Mutex
}

const (
	produceTimeout                 = 100 * time.Millisecond
	partitionsPerConsumerPropName  = "prana.loadclient.partitionsperconsumer"
	uniqueIDsPerPartitionPropName  = "prana.loadclient.uniqueidsperpartition"
	maxMessagesPerConsumerPropName = "prana.loadclient.maxmessagesperconsumer"
	messageGeneratorPropName       = "prana.loadclient.messagegenerator"
	defaultPartitionsPerConsumer   = 4
	defaultMessageGeneratorName    = "simple"
)

func NewMessageProviderFactory(bufferSize int, numConsumersPerSource int, nodeID int,
	properties map[string]string) (*LoadClientMessageProviderFactory, error) {
	partitionsPerConsumer, err := common.GetOrDefaultIntProperty(partitionsPerConsumerPropName, properties, defaultPartitionsPerConsumer)
	if err != nil {
		return nil, err
	}
	partitionsPerNode := numConsumersPerSource * partitionsPerConsumer
	partitionsStart := nodeID * partitionsPerNode
	uniqueIDsPerPartition, err := common.GetOrDefaultIntProperty(uniqueIDsPerPartitionPropName, properties, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	maxMessagesPerConsumer, err := common.GetOrDefaultIntProperty(maxMessagesPerConsumerPropName, properties, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	msgGeneratorName, ok := properties[messageGeneratorPropName]
	if !ok {
		msgGeneratorName = defaultMessageGeneratorName
	}
	return &LoadClientMessageProviderFactory{
		bufferSize:             bufferSize,
		partitionsPerConsumer:  partitionsPerConsumer,
		consumersPerSource:     numConsumersPerSource,
		partitionsStart:        partitionsStart,
		nextPartition:          partitionsStart,
		properties:             properties,
		uniqueIDsPerPartition:  int64(uniqueIDsPerPartition),
		maxMessagesPerConsumer: int64(maxMessagesPerConsumer),
		messageGeneratorName:   msgGeneratorName,
		committedOffsets:       map[int32]int64{},
	}, nil
}

func (l *LoadClientMessageProviderFactory) NewMessageProvider() (kafka.MessageProvider, error) {
	l.committedOffsetsLock.Lock()
	defer l.committedOffsetsLock.Unlock()
	msgs := make(chan *kafka.Message, l.bufferSize)
	partitions := make([]int32, l.partitionsPerConsumer)
	sb := strings.Builder{}
	sb.WriteString("creating message provider, partitions are: ")
	for i := range partitions {
		partitions[i] = int32(l.nextPartition)
		sb.WriteString(fmt.Sprintf("%d,", l.nextPartition))
		l.nextPartition++
	}
	log.Info(sb.String())
	if l.nextPartition == l.partitionsStart+(l.consumersPerSource*l.partitionsPerConsumer) {
		// Wrap around - consumers for a source can get closed when lags time out, and we need to make sure partitions
		// go back to the right value next time they are created
		l.nextPartition = l.partitionsStart
	}
	offsets := make([]int64, len(partitions))
	for i, partitionID := range partitions {
		offsets[i] = l.committedOffsets[partitionID] + 1
	}
	rnd := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	msgGen, err := l.getMessageGenerator(l.messageGeneratorName)
	if err != nil {
		return nil, err
	}
	msgGen.Init()
	return &LoadClientMessageProvider{
		factory:               l,
		msgs:                  msgs,
		partitions:            partitions,
		numPartitions:         len(partitions),
		offsets:               offsets,
		uniqueIDsPerPartition: l.uniqueIDsPerPartition,
		maxMessages:           l.maxMessagesPerConsumer,
		rnd:                   rnd,
		msgGenerator:          msgGen,
		committedOffsets:      map[int32]int64{},
	}, nil
}

func (l *LoadClientMessageProviderFactory) NewMessageProducer() (kafka.MessageProducer, error) {
	panic("not implemented")
}

func (l *LoadClientMessageProviderFactory) getMessageGenerator(name string) (msggen.MessageGenerator, error) {
	switch name {
	case "simple":
		return &simpleGenerator{uniqueIDsPerPartition: l.uniqueIDsPerPartition}, nil
	case "payments":
		return &paymentsGenerator{uniqueIDsPerPartition: l.uniqueIDsPerPartition}, nil
	default:
		return nil, errors.Errorf("unknown message generator name %s", name)
	}
}

func (l *LoadClientMessageProviderFactory) updateCommittedOffsets(offsets map[int32]int64) {
	l.committedOffsetsLock.Lock()
	defer l.committedOffsetsLock.Unlock()
	for p, off := range offsets {
		l.committedOffsets[p] = off
	}
}

type LoadClientMessageProvider struct {
	factory               *LoadClientMessageProviderFactory
	msgs                  chan *kafka.Message
	running               common.AtomicBool
	numPartitions         int
	partitions            []int32
	offsets               []int64
	sequence              int64
	uniqueIDsPerPartition int64
	maxMessages           int64
	msgGenerator          msggen.MessageGenerator
	rnd                   *rand.Rand
	msgLock               sync.Mutex
	committedOffsets      map[int32]int64
}

func (l *LoadClientMessageProvider) GetMessage(pollTimeout time.Duration) (*kafka.Message, error) {
	select {
	case msg := <-l.msgs:
		if msg == nil {
			// Messages channel was closed - probably max number of configured messages was exceeded
			// In this case we don't want to busy loop, so we introduce a delay
			time.Sleep(pollTimeout)
		}
		return msg, nil
	case <-time.After(pollTimeout):
		return nil, nil
	}
}

func (l *LoadClientMessageProvider) CommitOffsets(offsets map[int32]int64) error {
	l.msgLock.Lock()
	defer l.msgLock.Unlock()
	for p, off := range offsets {
		l.committedOffsets[p] = off
	}
	return nil
}

func (l *LoadClientMessageProvider) Stop() error {
	return nil
}

func (l *LoadClientMessageProvider) Start() error {
	l.running.Set(true)
	go l.genLoop()
	return nil
}

func (l *LoadClientMessageProvider) Close() error {
	l.msgLock.Lock()
	defer l.msgLock.Unlock()
	l.running.Set(false)
	// message providers can get stopped and restarted - we need to store the last offsets so when they get restarted
	// don't get duplicate messages - this isn't persistent though - if node is rolled then will get duplicates!
	l.factory.updateCommittedOffsets(l.committedOffsets)
	return nil
}

func (l *LoadClientMessageProvider) SetRebalanceCallback(callback kafka.RebalanceCallback) {
}

func (l *LoadClientMessageProvider) genLoop() {
	var msgCount int64
	var msg *kafka.Message
	for l.running.Get() && msgCount < l.maxMessages {
		if msg == nil {
			var err error
			msg, err = l.genMessage()
			if err != nil {
				log.Errorf("failed to generate message %+v", err)
				return
			}
		}
		select {
		case l.msgs <- msg:
			msg = nil
		case <-time.After(produceTimeout):
		}
		msgCount++
	}
	close(l.msgs)
}

func (l *LoadClientMessageProvider) genMessage() (*kafka.Message, error) {
	index := l.sequence % int64(l.numPartitions)
	partition := l.partitions[index]
	offset := l.offsets[index]

	msg, err := l.msgGenerator.GenerateMessage(partition, offset, l.rnd)
	if err != nil {
		return nil, err
	}
	l.offsets[index]++
	l.sequence++

	return msg, nil
}
