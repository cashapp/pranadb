package kafka

import (
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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
	}
	f.topics.Store(name, topic)
	return topic, nil
}

func (f *FakeKafka) GetTopic(name string) (*Topic, bool) {
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
	groups     sync.Map
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
	group, ok := t.getGroup(groupID)
	if !ok {
		return fmt.Errorf("no such group %s", groupID)
	}
	return group.injectFailure(failTime)
}

func (t *Topic) push(message *Message) error {
	part, err := t.calcPartition(message)
	if err != nil {
		return err
	}
	t.partitions[part].push(message)
	return nil
}

func (t *Topic) getGroup(groupID string) (*Group, bool) {
	v, ok := t.groups.Load(groupID)
	if !ok {
		return nil, false
	}
	return v.(*Group), true
}

func (t *Topic) calcPartition(message *Message) (int, error) {
	h, err := hash(message.Key)
	if err != nil {
		return 0, err
	}
	partID := int(h % uint32(len(t.partitions)))
	return partID, nil
}

func (t *Topic) CreateSubscriber(groupID string, paCB PartitionsCallback, prCB PartitionsCallback) (*Subscriber, error) {
	group, ok := t.getGroup(groupID)
	if !ok {
		t.lock.Lock()
		group, ok = t.getGroup(groupID)
		if !ok {
			group = newGroup(groupID, t)
			t.groups.Store(groupID, group)
		}
		t.lock.Unlock()
	}
	return group.createSubscriber(t, group, paCB, prCB)
}

func (t *Topic) ResetOffsets() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	for _, part := range t.partitions {
		part.resetOffset()
	}
	t.groups.Range(func(_, v interface{}) bool {
		v.(*Group).resetOffsets()
		return true
	})
	return nil
}

func (t *Topic) close() {
}

func (t *Topic) TotalMessages(groupID string) (int, int) {
	group, ok := t.getGroup(groupID)
	if !ok {
		return 0, 0
	}
	totMsgs := 0
	totCommitted := 0
	offs := group.getOffsets()
	for _, part := range t.partitions {
		highOff := atomic.LoadInt64(&part.highOffset)
		totMsgs += int(highOff)
		committed := offs[part.id]
		totCommitted += int(committed)
	}
	return totMsgs, totCommitted
}

type Group struct {
	id              string
	subscribersLock sync.Mutex
	topic           *Topic
	offsets         sync.Map
	subscribers     []*Subscriber
	failureEnd      *time.Time
	feLock          sync.Mutex
}

func newGroup(id string, topic *Topic) *Group {
	return &Group{
		id:    id,
		topic: topic,
	}
}

func (g *Group) injectFailure(failTime time.Duration) error {
	g.subscribersLock.Lock()
	defer g.subscribersLock.Unlock()
	if err := g.quiesceConsumers(); err != nil {
		return err
	}
	tEnd := time.Now().Add(failTime)
	g.feLock.Lock()
	g.failureEnd = &tEnd
	g.feLock.Unlock()
	return g.wakeConsumers()
}

func (g *Group) checkInjectFailure() error {

	g.feLock.Lock()
	defer g.feLock.Unlock()

	if g.failureEnd != nil {

		if time.Now().Sub(*g.failureEnd) >= 0 {
			log.Infof("Failure injection has ended")
			g.failureEnd = nil
			return nil
		}
		return errors.New("Injected failure")
	}
	return nil
}

func (g *Group) createSubscriber(t *Topic, group *Group, paCB PartitionsCallback, prCB PartitionsCallback) (*Subscriber, error) {
	g.subscribersLock.Lock()
	defer g.subscribersLock.Unlock()
	if err := g.quiesceConsumers(); err != nil {
		return nil, err
	}
	subscriber := &Subscriber{
		topic: t,
		group: group,
		paCB:  paCB,
		prCB:  prCB,
	}
	g.subscribers = append(g.subscribers, subscriber)
	if err := g.rebalance(); err != nil {
		return nil, err
	}
	return subscriber, nil
}

func (g *Group) commitOffsets(offsets map[int32]int64) error {
	if err := g.checkInjectFailure(); err != nil {
		return err
	}
	for partID, offset := range offsets {
		co, ok := g.offsets.Load(partID)
		if ok {
			currOff, ok := co.(int64)
			if !ok {
				panic("not an int64")
			}
			if currOff >= offset {
				return fmt.Errorf("offset committed out of order on group %s partId %d curr offset %d offset %d", g.id, partID, currOff, offset)
			}
		}
		g.offsets.Store(partID, offset)
	}
	return nil
}

func (g *Group) quiesceConsumers() error {
	// We first call the subscribers that rebalance is about to occur - this gives them a chance to
	// commit offsets
	for _, subscriber := range g.subscribers {
		if subscriber.prCB != nil {
			if err := subscriber.prCB(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *Group) rebalance() error {
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
	return g.wakeConsumers()
}

func (g *Group) wakeConsumers() error {
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

	g.subscribersLock.Lock()
	defer g.subscribersLock.Unlock()

	if err := g.quiesceConsumers(); err != nil {
		return err
	}

	var newSubscribers []*Subscriber
	for _, c := range g.subscribers {
		if c != subscriber {
			newSubscribers = append(newSubscribers, c)
		}
	}
	g.subscribers = newSubscribers
	return g.rebalance()
}

func (g *Group) getOffsets() map[int32]int64 {
	m := make(map[int32]int64)
	g.offsets.Range(func(key, value interface{}) bool {
		v, ok := value.(int64)
		if !ok {
			panic("not anint64")
		}
		m[key.(int32)] = v
		return true
	})

	return m
}

func (g *Group) resetOffsets() {
	g.offsets.Range(func(key, _ interface{}) bool {
		g.offsets.Delete(key)
		return true
	})
}

type Subscriber struct {
	topic      *Topic
	partitions []*Partition
	group      *Group
	prCB       PartitionsCallback
	paCB       PartitionsCallback
}

func (c *Subscriber) commitOffsets(offsets map[int32]int64) error {
	return c.group.commitOffsets(offsets)
}

func (c *Subscriber) GetMessage(pollTimeout time.Duration) (*Message, error) {

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
	return c.group.unsubscribe(c)
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
	return &FakeMessageProvider{
		topic:   topic,
		groupID: fmpf.groupID,
	}, nil
}

type FakeMessageProvider struct {
	subscriber *Subscriber
	topic      *Topic
	groupID    string
	started    bool
	lock       sync.Mutex
	prCB       PartitionsCallback
	paCB       PartitionsCallback
}

func (f *FakeMessageProvider) SetPartitionsAssignedCb(cb PartitionsCallback) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.paCB = cb
}

func (f *FakeMessageProvider) SetPartitionsRevokedCb(cb PartitionsCallback) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.prCB = cb
}

func (f *FakeMessageProvider) GetMessage(pollTimeout time.Duration) (*Message, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.subscriber == nil {
		// This is ok, we must start the message consumer before the we start the message provider
		// so there is a window where the subscriber is not set.
		return nil, nil
	}
	return f.subscriber.GetMessage(pollTimeout)
}

func (f *FakeMessageProvider) CommitOffsets(offsets map[int32]int64) error {
	if f.subscriber == nil {
		return errors.New("not started")
	}
	return f.subscriber.commitOffsets(offsets)
}

func (f *FakeMessageProvider) Start() error {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.started {
		return nil
	}
	if f.prCB == nil || f.paCB == nil {
		return errors.New("callbacks must be set before message provider is started")
	}
	subscriber, err := f.topic.CreateSubscriber(f.groupID, f.paCB, f.prCB)
	if err != nil {
		return err
	}
	f.subscriber = subscriber
	return nil
}

func (f *FakeMessageProvider) Stop() error {
	return f.subscriber.Unsubscribe()
}
