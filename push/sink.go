package push

import (
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/kafka"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/push/codec"
	"github.com/squareup/pranadb/push/exec"
	"github.com/squareup/pranadb/push/util"
	"github.com/squareup/pranadb/sharder"
	"github.com/squareup/pranadb/table"
	"reflect"
	"sync"
	"time"
)

const sendCheckInterval = 10 * time.Millisecond

type Sink struct {
	startStopLock sync.Mutex
	started       bool
	Info          *common.SinkInfo
	schema        *common.Schema
	pe            *Engine
	toSend        sync.Map
	sendTimer     *time.Timer
	producer      kafka.MessageProducer
	headerCodec   codec.Codec
	keyCodec      codec.Codec
	valueCodec    codec.Codec
	children      []exec.PushExecutor
}

func CreateSink(pe *Engine, pl *parplan.Planner, schema *common.Schema, sinkName string, query string,
	tableID uint64, seqGenerator common.SeqGenerator, originInfo *common.SinkTargetInfo) (*Sink, error) {

	var brokerConf conf.BrokerConfig
	var ok bool
	if pe.cfg.KafkaBrokers != nil {
		brokerConf, ok = pe.cfg.KafkaBrokers[originInfo.BrokerName]
	}
	if !ok || pe.cfg.KafkaBrokers == nil {
		return nil, errors.NewPranaErrorf(errors.InvalidStatement, "Unknown broker %s - has it been configured in the server config?", originInfo.BrokerName)
	}
	props := util.CopyAndAddAllProperties(brokerConf.Properties, originInfo.Properties)

	var msgProvFact kafka.MessageClient
	switch brokerConf.ClientType {
	case conf.BrokerClientFake:
		var err error
		msgProvFact, err = kafka.NewFakeMessageProviderFactory(originInfo.TopicName, props, "")
		if err != nil {
			return nil, errors.WithStack(err)
		}
	case conf.BrokerClientDefault:
		msgProvFact = kafka.NewMessageProviderFactory(originInfo.TopicName, props, "")
	default:
		return nil, errors.NewPranaErrorf(errors.InvalidStatement, "Unsupported broker client type %d", brokerConf.ClientType)
	}

	producer, err := msgProvFact.NewMessageProducer()
	if err != nil {
		return nil, err
	}

	headerCodec, keyCodec, valueCodec, err := util.GetCodecs(pe.protoRegistry, originInfo.HeaderEncoding,
		originInfo.KeyEncoding, originInfo.ValueEncoding, originInfo.Injectors)
	if err != nil {
		return nil, err
	}

	dag, _, err := pe.buildPushQueryExecution(pl, schema, query, sinkName, seqGenerator)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(originInfo.Injectors) != len(dag.ColNames()) {
		return nil, errors.NewPranaErrorf(errors.InvalidStatement, "Number of injectors must match number of columns in sink query")
	}
	tableInfo := common.NewTableInfo(
		tableID,
		schema.Name,
		sinkName,
		dag.KeyCols(),
		dag.ColNames(),
		dag.ColTypes(),
		0,
		0,
	)
	tableInfo.ColsVisible = dag.ColsVisible()
	sinkInfo := common.SinkInfo{
		Name:       sinkName,
		Query:      query,
		TableInfo:  tableInfo,
		TargetInfo: originInfo,
	}
	sink := Sink{
		schema:      schema,
		pe:          pe,
		Info:        &sinkInfo,
		producer:    producer,
		headerCodec: headerCodec,
		keyCodec:    keyCodec,
		valueCodec:  valueCodec,
	}
	exec.ConnectPushExecutors([]exec.PushExecutor{dag}, &sink)
	if err := sink.validateDag(&sink); err != nil {
		return nil, err
	}
	return &sink, nil
}

func (s *Sink) Start() error {
	s.startStopLock.Lock()
	defer s.startStopLock.Unlock()
	if s.started {
		return nil
	}
	if err := s.producer.Start(); err != nil {
		return err
	}
	s.started = true
	s.scheduleTimer()
	return nil
}

func (s *Sink) scheduleTimer() {
	s.sendTimer = time.AfterFunc(sendCheckInterval, s.checkBatchTimeouts)
}

func (s *Sink) Stop() error {
	s.startStopLock.Lock()
	defer s.startStopLock.Unlock()
	if !s.started {
		return nil
	}
	s.started = false
	if err := s.producer.Stop(); err != nil {
		return err
	}
	if s.sendTimer != nil {
		s.sendTimer.Stop()
		s.sendTimer = nil
	}
	return nil
}

type toSendBatch struct {
	createTime time.Time
	rowsToSend map[string]*common.Row
	sent       bool
}

func (s *Sink) HandleRows(rowsBatch exec.RowsBatch, ctx *exec.ExecutionContext) error {

	numEntries := rowsBatch.Len()

	shardID := ctx.WriteBatch.ShardID
	var rowsToSend map[string]*common.Row
	var tsb *toSendBatch
	if s.Info.TargetInfo.EmitAfter == 0 {
		rowsToSend = map[string]*common.Row{}
	} else {
		rr, ok := s.toSend.Load(shardID)
		if ok {
			tsb = rr.(*toSendBatch) //nolint:forcetypeassert
			rowsToSend = tsb.rowsToSend
		} else {
			tsb = &toSendBatch{
				createTime: time.Now(),
				rowsToSend: map[string]*common.Row{},
			}
			rowsToSend = tsb.rowsToSend
			s.toSend.Store(shardID, tsb)
		}
	}

	toPut := make(map[string][]byte, numEntries)

	for i := 0; i < numEntries; i++ {
		prevRow := rowsBatch.PreviousRow(i)
		currentRow := rowsBatch.CurrentRow(i)

		if currentRow != nil {
			keyBuff := table.EncodeTableKeyPrefix(s.Info.ID, ctx.WriteBatch.ShardID, 32)
			keyBuff, err := common.EncodeKeyCols(currentRow, s.Info.PrimaryKeyCols, s.Info.ColumnTypes, keyBuff)
			if err != nil {
				return err
			}
			if s.Info.TargetInfo.EmitAfter > 0 {
				valueBuff, err := common.EncodeRow(currentRow, s.Info.ColumnTypes, nil)
				if err != nil {
					return err
				}
				//ctx.WriteBatch.AddPut(keyBuff, valueBuff)
				toPut[common.ByteSliceToStringZeroCopy(keyBuff)] = valueBuff
			}
			rowsToSend[common.ByteSliceToStringZeroCopy(keyBuff)] = currentRow
		} else {
			// It's a delete
			keyBuff := table.EncodeTableKeyPrefix(s.Info.ID, ctx.WriteBatch.ShardID, 32)
			keyBuff, err := common.EncodeKeyCols(prevRow, s.Info.PrimaryKeyCols, s.Info.ColumnTypes, keyBuff)
			if err != nil {
				return errors.WithStack(err)
			}
			if s.Info.TargetInfo.EmitAfter > 0 {
				// We don't delete the row, we store a tombstone - this is used in a sink where we need to keep track
				// of deleted rows too
				//ctx.WriteBatch.AddPut(keyBuff, nil)
				toPut[common.ByteSliceToStringZeroCopy(keyBuff)] = nil
			}
			rowsToSend[common.ByteSliceToStringZeroCopy(keyBuff)] = nil
		}
	}
	if s.Info.TargetInfo.EmitAfter == 0 {
		return s.sendMessageBatch(shardID, rowsToSend, false, nil)
	} else if len(rowsToSend) >= s.Info.TargetInfo.MaxBufferedMessages {
		// We pass toPut in as we don't delete rows in this current handle batch as they won't be put anyway
		if err := s.sendMessageBatch(shardID, rowsToSend, true, toPut); err != nil {
			return err
		}
		tsb.sent = true
		s.toSend.Delete(shardID)
	} else {
		// we're going to send async so actually put the rows
		for k, v := range toPut {
			ctx.WriteBatch.AddPut(common.StringToByteSliceZeroCopy(k), v)
		}
	}
	return nil
}

func (s *Sink) checkBatchTimeouts() {
	s.startStopLock.Lock()
	defer s.startStopLock.Unlock()
	if !s.started {
		return
	}
	s.toSend.Range(func(key, value interface{}) bool {
		shardID := key.(uint64)     //nolint:forcetypeassert
		tsb := value.(*toSendBatch) //nolint:forcetypeassert
		if time.Now().Sub(tsb.createTime) >= s.Info.TargetInfo.EmitAfter {
			sched := s.pe.getScheduler(shardID)
			if sched == nil {
				log.Warnf("checking batch timeouts, can't find scheduler %d", shardID)
				return false
			}
			// We send the batch on the scheduler
			ch, err := sched.AddAction(func() error {
				if tsb.sent {
					// It's possible it could be sent already
					return nil
				}
				if err := s.sendMessageBatch(shardID, tsb.rowsToSend, true, nil); err != nil {
					return err
				}
				tsb.sent = true
				s.toSend.Delete(shardID)
				return nil
			}, 1)
			if err != nil {
				log.Errorf("failed to schedule message batch %+v", err)
				return false
			}
			err = <-ch
			if err != nil {
				log.Errorf("failed to send message batch %+v", err)
				return false
			}
		}
		return true
	})
	s.scheduleTimer()
}

func (s *Sink) sendMessageBatch(shardID uint64, rows map[string]*common.Row, persistent bool, ignoreDeletes map[string][]byte) error {
	var kmsgs []kafka.Message
	var wb *cluster.WriteBatch
	if persistent {
		wb = cluster.NewWriteBatch(shardID)
	}
	for sKey, row := range rows {
		key := common.StringToByteSliceZeroCopy(sKey)
		kmsg, err := s.createKafkaMessage(row)
		if err != nil {
			return err
		}
		kmsgs = append(kmsgs, *kmsg)
		if persistent {
			if ignoreDeletes != nil {
				_, ok := ignoreDeletes[sKey]
				if !ok {
					wb.AddDelete(key)
				}
			} else {
				wb.AddDelete(key)
			}
		}
	}
	log.Debugf("producer sending %d messages", len(kmsgs))
	if err := s.producer.SendMessages(kmsgs); err != nil {
		return err
	}
	if persistent {
		return s.pe.cluster.WriteBatch(wb, true)
	}
	return nil
}

func (s *Sink) createKafkaMessage(row *common.Row) (*kafka.Message, error) { //nolint:gocyclo

	var timestamp time.Time
	var key interface{}
	var header map[string]interface{}

	// TODO protos
	value := map[string]interface{}{}

	for i, colType := range s.Info.ColumnTypes {
		isNull := row.IsNull(i)
		if !isNull {
			injector := s.Info.TargetInfo.Injectors[i]
			var val interface{}
			switch colType.Type {
			case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
				val = row.GetInt64(i)
			case common.TypeDouble:
				val = row.GetFloat64(i)
			case common.TypeVarchar:
				val = row.GetString(i)
			case common.TypeDecimal:
				dec := row.GetDecimal(i)
				val = dec.String()
			case common.TypeTimestamp:
				ts := row.GetTimestamp(i)
				gt, err := ts.GoTime(time.UTC)
				if err != nil {
					return nil, err
				}
				val = gt
			default:
				panic("unknown type")
			}
			if injector.MetaKey != nil {
				if *injector.MetaKey == "timestamp" {
					ts, ok := val.(time.Time)
					if !ok {
						return nil, errors.NewPranaErrorf(errors.InvalidStatement, "meta timestamp injector must inject a value of type timestamp")
					}
					timestamp = ts
				} else if *injector.MetaKey == "key" {
					if len(injector.Selector) == 0 {
						key = val
					} else {
						if key == nil {
							// TODO protos
							key = map[string]interface{}{}
						}
						if err := injector.Selector.Inject(key, val); err != nil {
							return nil, err
						}
					}
				} else if *injector.MetaKey == "header" {
					if header == nil {
						header = map[string]interface{}{}
					}
					if err := injector.Selector.Inject(header, val); err != nil {
						return nil, err
					}
				}
			} else if err := injector.Selector.Inject(value, val); err != nil {
				return nil, err
			}
		}
	}

	encodedKey, err := s.keyCodec.Encode(key)
	if err != nil {
		return nil, err
	}

	hash, err := sharder.Hash(encodedKey)
	if err != nil {
		return nil, err
	}
	part := hash % uint32(s.Info.TargetInfo.NumPartitions)

	var kheaders []kafka.MessageHeader
	if header != nil {
		kheaders = make([]kafka.MessageHeader, len(header))
		i := 0
		for k, v := range header {
			encoded, err := s.headerCodec.Encode(v)
			if err != nil {
				return nil, err
			}
			kheaders[i] = kafka.MessageHeader{
				Key:   k,
				Value: encoded,
			}
			i++
		}
	}

	if timestamp.IsZero() {
		timestamp = time.Now()
	}

	encodedBody, err := s.valueCodec.Encode(value)
	if err != nil {
		return nil, err
	}

	return &kafka.Message{
		PartInfo: kafka.PartInfo{
			PartitionID: int32(part),
		},
		Value:     encodedBody,
		Key:       encodedKey,
		TimeStamp: timestamp,
		Headers:   kheaders,
	}, nil
}

func (s *Sink) Connect() error {
	return s.connect(s)
}

func (s *Sink) validateDag(executor exec.PushExecutor) error {
	for _, child := range executor.GetChildren() {
		err := s.validateDag(child)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	_, ok := executor.(*exec.Aggregator)
	if ok {
		return errors.NewPranaErrorf(errors.InvalidStatement, "Sink queries cannot contain aggregations")
	}
	return nil
}

func (s *Sink) connect(executor exec.PushExecutor) error {
	for _, child := range executor.GetChildren() {
		err := s.connect(child)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	scan, ok := executor.(*exec.Scan)
	if ok {
		tableName := scan.TableName
		tbl, ok := s.schema.GetTable(tableName)
		if !ok {
			return errors.Errorf("unknown source or materialized view %s", tableName)
		}
		switch tbl := tbl.(type) {
		case *common.SourceInfo:
			source, err := s.pe.GetSource(tbl.ID)
			if err != nil {
				return errors.WithStack(err)
			}
			source.AddConsumingNode(s.Info.Name, executor)
		case *common.MaterializedViewInfo:
			mv, err := s.pe.GetMaterializedView(tbl.ID)
			if err != nil {
				return errors.WithStack(err)
			}
			mv.addConsumingExecutor(s.Info.Name, executor)
		default:
			return errors.Errorf("table scan on %s is not supported", reflect.TypeOf(tbl))
		}
	}
	return nil
}

func (s *Sink) Disconnect() error {
	return s.disconnect(s)
}

func (s *Sink) Drop() error {
	s.pe.cluster.TableDropped(s.Info.ID)
	startPrefix := common.AppendUint64ToBufferBE(nil, s.Info.ID)
	endPrefix := common.AppendUint64ToBufferBE(nil, s.Info.ID+1)
	return s.pe.cluster.DeleteAllDataInRangeForAllShardsLocally(startPrefix, endPrefix)
}

func (s *Sink) disconnect(node exec.PushExecutor) error {

	scan, ok := node.(*exec.Scan)
	if ok {
		tableName := scan.TableName
		tbl, ok := s.schema.GetTable(tableName)
		if !ok {
			return errors.Errorf("unknown source or materialized view %s", tableName)
		}
		switch tbl := tbl.(type) {
		case *common.SourceInfo:
			source, err := s.pe.GetSource(tbl.ID)
			if err != nil {
				return errors.WithStack(err)
			}
			source.RemoveConsumingNode(s.Info.Name)
		case *common.MaterializedViewInfo:
			mv, err := s.pe.GetMaterializedView(tbl.ID)
			if err != nil {
				return errors.WithStack(err)
			}
			mv.removeConsumingExecutor(s.Info.Name)
		default:
			return errors.Errorf("cannot disconnect %s: invalid table type", tbl)
		}
	}
	for _, child := range node.GetChildren() {
		err := s.disconnect(child)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (s *Sink) loadPersistentState(shardID uint64) error {
	keyStart := table.EncodeTableKeyPrefix(s.Info.ID, shardID, 32)
	keyEnd := table.EncodeTableKeyPrefix(s.Info.ID+1, shardID, 32)
	kvp, err := s.pe.cluster.LocalScan(keyStart, keyEnd, -1)
	if err != nil {
		return err
	}
	rowsMap := map[string]*common.Row{}
	rf := common.NewRowsFactory(s.Info.ColumnTypes)
	rows := rf.NewRows(len(kvp))
	for i, kv := range kvp {
		skey := common.ByteSliceToStringZeroCopy(kv.Key)
		if err := common.DecodeRow(kv.Value, s.Info.ColumnTypes, rows); err != nil {
			return err
		}
		row := rows.GetRow(i)
		rowsMap[skey] = &row
	}
	return s.sendMessageBatch(shardID, rowsMap, true, nil)
}

func (s *Sink) SetParent(parent exec.PushExecutor) {
}

func (s *Sink) GetParent() exec.PushExecutor {
	return nil
}

func (s *Sink) AddChild(child exec.PushExecutor) {
	if len(s.children) != 0 {
		panic("child already set")
	}
	s.children = []exec.PushExecutor{child}
}

func (s *Sink) GetChildren() []exec.PushExecutor {
	return s.children
}

func (s *Sink) ClearChildren() {
}

func (s *Sink) ReCalcSchemaFromChildren() error {
	return nil
}

func (s *Sink) ColNames() []string {
	return nil
}

func (s *Sink) SetColNames(colNames []string) {
}

func (s *Sink) ColTypes() []common.ColumnType {
	return nil
}

func (s *Sink) KeyCols() []int {
	return nil
}

func (s *Sink) ColsVisible() []bool {
	return nil
}
