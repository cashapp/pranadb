package kafka

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/squareup/pranadb/sharder"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const maxBufferedMessagesPerPartition = 10000
const FakeKafkaIDPropName = "fakeKafkaID"

var fakeKafkaSeq int64 = -1

// We store the fake kafkas in a top level map so the ids can be passed in test config and used to look up the
// real broker in the source connection logic
var fakeKafkas sync.Map

func GetFakeKafka(id int64) (*FakeKafka, bool) {
	f, ok := fakeKafkas.Load(id)
	if !ok {
		return nil, false
	}
	return f.(*FakeKafka), true
}

func NewFakeKafka() *FakeKafka {
	id := atomic.AddInt64(&fakeKafkaSeq, 1)
	fk := &FakeKafka{ID: id}
	fakeKafkas.Store(id, fk)
	return fk
}

type FakeKafka struct {
	ID        int64
	topicLock sync.Mutex
	topics    sync.Map
}

func (f *FakeKafka) CreateTopic(name string, partitions int) (*Topic, error) {
	f.topicLock.Lock()
	defer f.topicLock.Unlock()
	if _, ok := f.getTopic(name); ok {
		return nil, fmt.Errorf("topic with name %s already exists", name)
	}
	parts := make([]*Partition, partitions)
	for i := 0; i < partitions; i++ {
		parts[i] = &Partition{
			id:       int32(i),
			messages: make(chan *Message, maxBufferedMessagesPerPartition),
		}
	}
	topic := &Topic{
		Name:       name,
		partitions: parts,
		groups:     make(map[string]*Group),
	}
	f.topics.Store(name, topic)
	return topic, nil
}

func (f *FakeKafka) GetTopic(name string) (*Topic, bool) {
	f.topicLock.Lock()
	defer f.topicLock.Unlock()
	return f.getTopic(name)
}

func (f *FakeKafka) DeleteTopic(name string) error {
	f.topicLock.Lock()
	defer f.topicLock.Unlock()
	topic, ok := f.getTopic(name)
	if !ok {
		return fmt.Errorf("no such topic %s", name)
	}
	topic.close()
	f.topics.Delete(name)
	return nil
}

func (f *FakeKafka) IngestMessage(topicName string, message *Message) error {
	topic, ok := f.getTopic(topicName)
	if !ok {
		return fmt.Errorf("no such topic %s", topicName)
	}
	return topic.push(message)
}

func (f *FakeKafka) GetTopicNames() []string {
	f.topicLock.Lock()
	defer f.topicLock.Unlock()
	var names []string
	f.topics.Range(func(key, _ interface{}) bool {
		names = append(names, key.(string))
		return true
	})
	return names
}

func (f *FakeKafka) getTopic(name string) (*Topic, bool) {
	t, ok := f.topics.Load(name)
	if !ok {
		return nil, false
	}
	return t.(*Topic), true
}

func hash(key []byte) (uint32, error) {
	return sharder.Hash(key)
}

type Topic struct {
	Name       string
	lock       sync.RWMutex
	groups     map[string]*Group
	partitions []*Partition
}

type Partition struct {
	id         int32
	messages   MessageQueue
	highOffset int64
}

type MessageQueue chan *Message

func (p *Partition) push(message *Message) {
	offset := atomic.AddInt64(&p.highOffset, 1)
	message.PartInfo = PartInfo{
		PartitionID: p.id,
		Offset:      offset,
	}
	p.messages <- message
}

func (t *Topic) push(message *Message) error {
	part, err := t.calcPartition(message)
	if err != nil {
		return err
	}
	t.partitions[part].push(message)
	return nil
}

func (t *Topic) calcPartition(message *Message) (int, error) {
	h, err := hash(message.Key)
	if err != nil {
		return 0, err
	}
	partID := int(h % uint32(len(t.partitions)))
	return partID, nil
}

func (t *Topic) CreateSubscriber(groupID string) (*Subscriber, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	group, ok := t.groups[groupID]
	if !ok {
		group = newGroup(groupID, t)
		t.groups[groupID] = group
	}
	subscriber := group.createSubscriber(t, group)
	if err := group.rebalance(); err != nil {
		return nil, err
	}
	return subscriber, nil
}

func (t *Topic) unsubscribe(subscriber *Subscriber) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	group := t.groups[subscriber.group.id]
	group.unsubscribe(subscriber)
	return group.rebalance()
}

func (t *Topic) close() {
}

func (t *Topic) TotalCommittedMessages(groupID string) int {
	t.lock.Lock()
	defer t.lock.Unlock()
	group, ok := t.groups[groupID]
	if !ok {
		return 0
	}
	return group.totalCommittedMessages()
}

type Group struct {
	id          string
	lock        sync.Mutex
	topic       *Topic
	offsets     map[int32]int64
	subscribers []*Subscriber
}

func newGroup(id string, topic *Topic) *Group {
	return &Group{
		id:      id,
		topic:   topic,
		offsets: make(map[int32]int64),
	}
}

func (g *Group) createSubscriber(t *Topic, group *Group) *Subscriber {
	subscriber := &Subscriber{
		topic: t,
		group: group,
	}
	g.subscribers = append(g.subscribers, subscriber)
	return subscriber
}

func (g *Group) commitOffsets(offsets map[int32]int64) {
	g.lock.Lock()
	defer g.lock.Unlock()
	for partID, offset := range offsets {
		g.offsets[partID] = offset
	}
}

func (g *Group) rebalance() error {
	g.lock.Lock()
	defer g.lock.Unlock()
	if len(g.subscribers) > len(g.topic.partitions) {
		return errors.New("too many subscribers")
	}
	for _, subscriber := range g.subscribers {
		subscriber.partitions = []*Partition{}
	}
	for i, part := range g.topic.partitions {
		subscriber := g.subscribers[i%len(g.subscribers)]
		subscriber.partitions = append(subscriber.partitions, part)
	}
	return nil
}

func (g *Group) unsubscribe(subscriber *Subscriber) {
	g.lock.Lock()
	defer g.lock.Unlock()

	var newSubscribers []*Subscriber
	for _, c := range g.subscribers {
		if c != subscriber {
			newSubscribers = append(newSubscribers, c)
		}
	}
	g.subscribers = newSubscribers
}

func (g *Group) totalCommittedMessages() int {
	g.lock.Lock()
	defer g.lock.Unlock()
	var tot int64
	for _, offsets := range g.offsets {
		tot += offsets
	}
	return int(tot)
}

type Subscriber struct {
	topic      *Topic
	lock       sync.Mutex
	partitions []*Partition
	group      *Group
}

func (c *Subscriber) commitOffsets(offsets map[int32]int64) {
	c.group.commitOffsets(offsets)
}

func (c *Subscriber) copyParts() []*Partition {
	// Need to hold the group rebalance read lock too
	c.group.lock.Lock()
	defer c.group.lock.Unlock()
	c.lock.Lock()
	defer c.lock.Unlock()

	res := make([]*Partition, len(c.partitions))
	copy(res, c.partitions)
	return res
}

func (c *Subscriber) GetMessage(pollTimeout time.Duration) (*Message, error) {
	// We copy the parts in case a rebalance happens while we're waiting for a message
	partsCopy := c.copyParts()

	// We have to do a bit of reflective wibbling to select from a dynamic set of channels with a timeout
	var set []reflect.SelectCase
	for _, part := range partsCopy {
		set = append(set, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(part.messages),
		})
	}
	set = append(set, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(time.After(pollTimeout)),
	})

	index, valValue, ok := reflect.Select(set)
	if !ok {
		return nil, errors.New("channel was closed")
	}
	if index == len(partsCopy) {
		// timeout fired
		return nil, nil
	}
	return valValue.Interface().(*Message), nil
}

func (c *Subscriber) Unsubscribe() error {
	return c.topic.unsubscribe(c)
}

func NewFakeMessageProviderFactory(topicName string, props map[string]string, groupName string) (MessageProviderFactory, error) {
	sFakeKafkaID, ok := props[FakeKafkaIDPropName]
	if !ok {
		return nil, errors.New("no fakeKafkaID property in broker configuration")
	}
	fakeKafkaID, err := strconv.ParseInt(sFakeKafkaID, 10, 64)
	if err != nil {
		return nil, err
	}
	fk, ok := GetFakeKafka(fakeKafkaID)
	if !ok {
		return nil, fmt.Errorf("cannot find fake kafka with id %d", fakeKafkaID)
	}
	return &FakeMessageProviderFactory{
		fk:        fk,
		topicName: topicName,
		props:     props,
		groupID:   groupName,
	}, nil
}

type FakeMessageProviderFactory struct {
	fk        *FakeKafka
	topicName string
	props     map[string]string
	groupID   string
}

func (fmpf *FakeMessageProviderFactory) NewMessageProvider() (MessageProvider, error) {
	topic, ok := fmpf.fk.GetTopic(fmpf.topicName)
	if !ok {
		return nil, fmt.Errorf("no such topic %s", fmpf.topicName)
	}
	subscriber, err := topic.CreateSubscriber(fmpf.groupID)
	if err != nil {
		return nil, err
	}
	return &FakeMessageProvider{
		subscriber: subscriber,
		topicName:  fmpf.topicName,
	}, nil
}

type FakeMessageProvider struct {
	subscriber *Subscriber
	topicName  string
}

func (f *FakeMessageProvider) GetMessage(pollTimeout time.Duration) (*Message, error) {
	return f.subscriber.GetMessage(pollTimeout)
}

func (f *FakeMessageProvider) CommitOffsets(offsets map[int32]int64) error {
	f.subscriber.commitOffsets(offsets)
	return nil
}

func (f *FakeMessageProvider) Stop() error {
	return f.subscriber.Unsubscribe()
}
