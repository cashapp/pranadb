package kafka

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/squareup/pranadb/sharder"
	"reflect"
	"strconv"
	"strings"
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

func (f *FakeKafka) InjectFailure(topicName string, groupID string, failTime time.Duration) error {
	f.topicLock.Lock()
	defer f.topicLock.Unlock()
	t, ok := f.getTopic(topicName)
	if !ok {
		return fmt.Errorf("no such topic %s", topicName)
	}
	return t.injectFailure(groupID, failTime)
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

func (p *Partition) resetOffset() {
	atomic.StoreInt64(&p.highOffset, 0)
}

func (t *Topic) injectFailure(groupID string, failTime time.Duration) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	group, ok := t.groups[groupID]
	if !ok {
		return fmt.Errorf("no such group %s", groupID)
	}
	group.injectFailure(failTime)
	return nil
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

func (t *Topic) RestOffsets() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	for _, part := range t.partitions {
		part.resetOffset()
	}
	for _, group := range t.groups {
		group.resetOffsets()
	}
	return nil
}

func (t *Topic) unsubscribe(subscriber *Subscriber) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	group := t.groups[subscriber.group.id]
	return group.unsubscribe(subscriber)
}

func (t *Topic) close() {
}

func (t *Topic) TotalMessages(groupID string) (int, int) {
	t.lock.Lock()
	defer t.lock.Unlock()
	group, ok := t.groups[groupID]
	if !ok {
		return 0, 0
	}
	totMsgs := 0
	totCommitted := 0
	for _, part := range t.partitions {
		highOff := atomic.LoadInt64(&part.highOffset)
		totMsgs += int(highOff)
		committed := group.offsetsForPartition(part.id)
		totCommitted += int(committed)
	}
	return totMsgs, totCommitted
}

type Group struct {
	id          string
	lock        sync.Mutex
	topic       *Topic
	offsets     map[int32]int64
	subscribers []*Subscriber
	failureEnd  *time.Time
}

func newGroup(id string, topic *Topic) *Group {
	return &Group{
		id:      id,
		topic:   topic,
		offsets: make(map[int32]int64),
	}
}

func (g *Group) injectFailure(failTime time.Duration) {
	g.lock.Lock()
	defer g.lock.Unlock()
	if g.failureEnd != nil {
		panic("already in failure mode")
	}
	tEnd := time.Now().Add(failTime)
	g.failureEnd = &tEnd
}

func (g *Group) checkInjectFailure() error {
	if g.failureEnd != nil {
		if time.Now().Sub(*g.failureEnd) >= 0 {
			g.failureEnd = nil
			return nil
		}
		return errors.New("Injected failure")
	}
	return nil
}

func (g *Group) createSubscriber(t *Topic, group *Group) *Subscriber {
	subscriber := &Subscriber{
		topic: t,
		group: group,
	}
	g.subscribers = append(g.subscribers, subscriber)
	return subscriber
}

func (g *Group) commitOffsets(offsets map[int32]int64) error {
	g.lock.Lock()
	defer g.lock.Unlock()
	if err := g.checkInjectFailure(); err != nil {
		return err
	}
	for partID, offset := range offsets {
		currOff, ok := g.offsets[partID]
		if ok && (currOff >= offset) {
			return fmt.Errorf("offset committed out of order on group %s partId %d curr offset %d offset %d", g.id, partID, currOff, offset)
		}
		g.offsets[partID] = offset
	}
	return nil
}

func (g *Group) rebalance() error {
	// We first call the subscribers that rebalance is about to occur - this gives them a chance to
	// commit offsets - this must be done outside of the group lock
	for _, subscriber := range g.subscribers {
		if subscriber.prCB != nil {
			if err := subscriber.prCB(); err != nil {
				return err
			}
		}
	}
	g.lock.Lock()
	defer g.lock.Unlock()
	if len(g.subscribers) == 0 {
		return nil
	}
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
	for _, sub := range g.subscribers {
		builder := strings.Builder{}
		for _, part := range sub.partitions {
			builder.WriteString(fmt.Sprintf("%d,", part.id))
		}
	}
	for _, subscriber := range g.subscribers {
		if subscriber.paCB != nil {
			if err := subscriber.paCB(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *Group) unsubscribe(subscriber *Subscriber) error {
	g.lock.Lock()
	defer g.lock.Unlock()
	var newSubscribers []*Subscriber
	for _, c := range g.subscribers {
		if c != subscriber {
			newSubscribers = append(newSubscribers, c)
		}
	}
	g.subscribers = newSubscribers
	return nil
}

func (g *Group) offsetsForPartition(partID int32) int64 {
	g.lock.Lock()
	defer g.lock.Unlock()
	return g.offsets[partID]
}

func (g *Group) resetOffsets() {
	g.lock.Lock()
	defer g.lock.Unlock()
	g.offsets = make(map[int32]int64)
}

type Subscriber struct {
	topic      *Topic
	lock       sync.Mutex
	partitions []*Partition
	group      *Group
	prCB       PartitionsCallback
	paCB       PartitionsCallback
}

func (c *Subscriber) setPartitionsAssignedCb(cb PartitionsCallback) {
	c.paCB = cb
}

func (c *Subscriber) setPartitionsRevokedCb(cb PartitionsCallback) {
	c.prCB = cb
}

func (c *Subscriber) commitOffsets(offsets map[int32]int64) error {
	return c.group.commitOffsets(offsets)
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

	// We need to lock the group too when this occurs to prevent a rebalance happening at the same time

	c.group.lock.Lock()
	defer c.group.lock.Unlock()

	if err := c.group.checkInjectFailure(); err != nil {
		return nil, err
	}

	// We have to do a bit of reflective wibbling to select from a dynamic set of channels with a timeout
	var set []reflect.SelectCase
	for _, part := range c.partitions {
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
	if index == len(c.partitions) {
		// timeout fired
		return nil, nil
	}
	builder := strings.Builder{}
	for _, part := range c.partitions {
		builder.WriteString(fmt.Sprintf("%d,", part.id))
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

func (f *FakeMessageProvider) SetPartitionsAssignedCb(cb PartitionsCallback) {
	f.subscriber.setPartitionsAssignedCb(cb)
}

func (f *FakeMessageProvider) SetPartitionsRevokedCb(cb PartitionsCallback) {
	f.subscriber.setPartitionsRevokedCb(cb)
}

func (f *FakeMessageProvider) GetMessage(pollTimeout time.Duration) (*Message, error) {
	return f.subscriber.GetMessage(pollTimeout)
}

func (f *FakeMessageProvider) CommitOffsets(offsets map[int32]int64) error {
	return f.subscriber.commitOffsets(offsets)
}

func (f *FakeMessageProvider) Stop() error {
	return f.subscriber.Unsubscribe()
}
