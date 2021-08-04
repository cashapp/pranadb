package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/common/commontest"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const maxBufferedMessagesPerPartition = 10000

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
	topic.push(message)
	return nil
}

// IngestRows ingests rows given schema and source name - convenience method for use in tests
func (f *FakeKafka) IngestRows(sourceInfo *common.SourceInfo, rows *common.Rows,
	keyEncoding common.KafkaEncoding, valueEncoding common.KafkaEncoding, groupID string) error {
	topicName := sourceInfo.TopicInfo.TopicName

	topic, ok := f.GetTopic(topicName)
	if !ok {
		return fmt.Errorf("cannot find topic %s", topicName)
	}
	startCommitted := topic.TotalCommittedMessages(groupID)

	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		if err := f.IngestRow(topicName, &row, sourceInfo.ColumnTypes, sourceInfo.PrimaryKeyCols, keyEncoding, valueEncoding); err != nil {
			return err
		}
	}
	// And we wait for all offsets to be committed
	wanted := startCommitted + rows.RowCount()
	ok, err := commontest.WaitUntilWithError(func() (bool, error) {
		return topic.TotalCommittedMessages(groupID) == wanted, nil
	}, 5*time.Second, 10*time.Millisecond)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("messages not committed within timeout")
	}
	return nil
}

// IngestRow is a convenience method which encodes the row into a Kafka message first, then ingests it
func (f *FakeKafka) IngestRow(topicName string, row *common.Row, colTypes []common.ColumnType, keyCols []int,
	keyEncoding common.KafkaEncoding, valueEncoding common.KafkaEncoding) error {
	topic, ok := f.getTopic(topicName)
	if !ok {
		return fmt.Errorf("no such topic %s", topicName)
	}

	// TODO support other encodings so we can test them from SQLTest
	if keyEncoding != common.EncodingJSON || valueEncoding != common.EncodingJSON {
		return errors.New("Only JSON key and value encodings for ingesting currently supported")
	}

	keyMap := map[string]interface{}{}
	for i, keyCol := range keyCols {
		colType := colTypes[keyCol]
		colVal := getColVal(keyCol, colType, row)
		keyMap[fmt.Sprintf("k%d", i)] = colVal
	}

	valMap := map[string]interface{}{}
	for i, colType := range colTypes {
		colVal := getColVal(i, colType, row)
		valMap[fmt.Sprintf("v%d", i)] = colVal
	}

	keyBytes, err := json.Marshal(keyMap)
	if err != nil {
		return err
	}

	valBytes, err := json.Marshal(valMap)
	if err != nil {
		return err
	}

	message := &Message{
		Key:   keyBytes,
		Value: valBytes,
	}
	topic.push(message)
	return nil
}

func getColVal(colIndex int, colType common.ColumnType, row *common.Row) interface{} {
	var colVal interface{}
	switch colType.Type {
	case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
		colVal = row.GetInt64(colIndex)
	case common.TypeDouble:
		colVal = row.GetFloat64(colIndex)
	case common.TypeVarchar:
		colVal = row.GetString(colIndex)
	case common.TypeDecimal:
		dec := row.GetDecimal(colIndex)
		colVal = dec.String()
	case common.TypeTimestamp:
		panic("TODO")
	case common.TypeUnknown:
		panic("unknown type")
	}
	return colVal
}

func (f *FakeKafka) getTopic(name string) (*Topic, bool) {
	t, ok := f.topics.Load(name)
	if !ok {
		return nil, false
	}
	return t.(*Topic), true
}

func hash(key []byte) uint32 {
	hash := uint32(31)
	for _, b := range key {
		hash = 31*hash + uint32(b)
	}
	return hash
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

func (t *Topic) push(message *Message) {
	part := t.calcPartition(message)
	t.partitions[part].push(message)
}

func (t *Topic) calcPartition(message *Message) int {
	hash := hash(message.Key)
	return int(hash % uint32(len(t.partitions)))
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
	group := t.groups[groupID]
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
	partIndex  int
	partitions []*Partition
	group      *Group
}

func (c *Subscriber) commitOffsets(offsets map[int32]int64) {
	c.group.commitOffsets(offsets)
}

func (c *Subscriber) GetMessage(pollTimeout time.Duration) *Message {
	// Need to hold the topic rebalance read lock too
	c.topic.lock.RLock()
	defer c.topic.lock.RUnlock()
	c.lock.Lock()
	defer c.lock.Unlock()
	lp := len(c.partitions)

	start := time.Now()

	timeout := time.Duration(int64(pollTimeout) / int64(lp))
	if timeout == 0 {
		timeout = 1
	}
	for {
		part := c.partitions[c.partIndex]
		c.partIndex++
		if c.partIndex == lp {
			c.partIndex = 0
		}
		select {
		case msg := <-part.messages:
			return msg
		case <-time.After(timeout):
		}
		if time.Now().Sub(start) >= pollTimeout {
			return nil
		}
	}
}

func (c *Subscriber) Unsubscribe() error {
	return c.topic.unsubscribe(c)
}

func NewFakeMessageProviderFactory(topicName string, props map[string]string, groupName string) (MessageProviderFactory, error) {
	sFakeKafkaID, ok := props["fakeKafkaID"]
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
	msg := f.subscriber.GetMessage(pollTimeout)
	return msg, nil
}

func (f *FakeMessageProvider) CommitOffsets(offsets map[int32]int64) error {
	f.subscriber.commitOffsets(offsets)
	return nil
}

func (f *FakeMessageProvider) Stop() error {
	return f.subscriber.Unsubscribe()
}
