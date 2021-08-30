package msggen

import (
	"fmt"
	kafkaclient "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/kafka"
	"github.com/squareup/pranadb/sharder"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// MessageGenerator - quick and dirty Kafka message generator for demos, tests etc
type MessageGenerator interface {
	GenerateMessage(index int64, rnd *rand.Rand) (*kafka.Message, error)
	Name() string
}

type GenManager struct {
	lock       sync.Mutex
	generators map[string]MessageGenerator
}

func NewGenManager() (*GenManager, error) {
	gm := &GenManager{generators: make(map[string]MessageGenerator)}
	if err := gm.RegisterGenerators(); err != nil {
		return nil, err
	}
	return gm, nil
}

func (gm *GenManager) RegisterGenerator(gen MessageGenerator) error {
	gm.lock.Lock()
	defer gm.lock.Unlock()
	if _, ok := gm.generators[gen.Name()]; ok {
		return fmt.Errorf("generator already registered with name %s", gen.Name())
	}
	gm.generators[gen.Name()] = gen
	return nil
}

func (gm *GenManager) RegisterGenerators() error {
	return gm.RegisterGenerator(&PaymentGenerator{})
}

func (gm *GenManager) ProduceMessages(genName string, topicName string, partitions int, delay time.Duration,
	numMessages int64, indexStart int64, kafkaProps map[string]string) error {
	gm.lock.Lock()
	defer gm.lock.Unlock()

	gen, ok := gm.generators[genName]
	if !ok {
		return fmt.Errorf("no generator with registered with name %s", genName)
	}

	cm := &kafkaclient.ConfigMap{}
	for k, v := range kafkaProps {
		if err := cm.SetKey(k, v); err != nil {
			return err
		}
	}
	producer, err := kafkaclient.NewProducer(cm)
	if err != nil {
		return err
	}
	wg := &sync.WaitGroup{}
	wg.Add(int(numMessages))
	var errValue atomic.Value
	go func() {
		// This appears to be necessary to stop the producer hanging
		for e := range producer.Events() {
			msg, ok := e.(*kafkaclient.Message)
			if ok {
				if msg.TopicPartition.Error != nil {
					err := fmt.Errorf("Delivery failed: %v\n", msg.TopicPartition)
					errValue.Store(err)
				} else {
					wg.Done()
				}
			}
		}
	}()
	rnd := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	status := newSendStatus(topicName)
	status.start()
	for i := indexStart; i < indexStart+numMessages; i++ {
		msg, err := gen.GenerateMessage(i, rnd)
		if err != nil {
			return err
		}
		hash, err := sharder.Hash(msg.Key)
		if err != nil {
			return err
		}
		part := hash % uint32(partitions)
		kheaders := make([]kafkaclient.Header, len(msg.Headers))
		for i, hdr := range msg.Headers {
			kheaders[i] = kafkaclient.Header{
				Key:   hdr.Key,
				Value: hdr.Value,
			}
		}
		kmsg := &kafkaclient.Message{
			TopicPartition: kafkaclient.TopicPartition{
				Topic:     &topicName,
				Partition: int32(part),
			},
			Value:         msg.Value,
			Key:           msg.Key,
			Timestamp:     msg.TimeStamp,
			TimestampType: 0,
			Opaque:        nil,
			Headers:       kheaders,
		}
		if err := producer.Produce(kmsg, nil); err != nil {
			return err
		}
		status.incMessagesSent()
		if delay != 0 {
			time.Sleep(delay)
		}
	}
	outstanding := producer.Flush(10000)
	if outstanding != 0 {
		return fmt.Errorf("producer failed to flush %d messages", outstanding)
	}
	v := errValue.Load()
	if v != nil {
		err, ok := v.(error)
		if !ok {
			panic("not an error")
		}
		return err
	}
	wg.Wait()
	producer.Close()
	status.stop()
	log.Println("Messages sent ok")
	return nil
}

type sendStatus struct {
	msgsSent  int64
	running   common.AtomicBool
	interval  time.Duration
	topicName string
	closeChan chan struct{}
}

func newSendStatus(topicName string) *sendStatus {
	return &sendStatus{
		interval:  1 * time.Second,
		topicName: topicName,
		closeChan: make(chan struct{}, 1),
	}
}

func (s *sendStatus) incMessagesSent() {
	atomic.AddInt64(&s.msgsSent, 1)
}

func (s *sendStatus) start() {
	s.running.Set(true)
	go s.runLoop()
}

func (s *sendStatus) stop() {
	s.running.Set(false)
	<-s.closeChan
}

func (s *sendStatus) runLoop() {
	last := time.Now()
	for s.running.Get() {
		time.Sleep(100 * time.Millisecond)
		now := time.Now()
		if now.Sub(last) >= s.interval {
			log.Printf("%d messages sent to topic %s", atomic.LoadInt64(&s.msgsSent), s.topicName)
			last = now
		}
	}
	s.closeChan <- struct{}{}
}
