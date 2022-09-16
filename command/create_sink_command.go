package command

import (
	"fmt"
	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/command/parser/selector"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/interruptor"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/push"
	"strings"
	"sync"
	"time"
)

const defaultMaxBufferedMessages = 1000

type CreateSinkCommand struct {
	lock           sync.Mutex
	e              *Executor
	pl             *parplan.Planner
	schema         *common.Schema
	createSinkSQL  string
	tableSequences []uint64
	sink           *push.Sink
	ast            *parser.CreateSink
	interruptor    interruptor.Interruptor
}

func (c *CreateSinkCommand) CommandType() DDLCommandType {
	return DDLCommandTypeCreateSink
}

func (c *CreateSinkCommand) SchemaName() string {
	return c.schema.Name
}

func (c *CreateSinkCommand) SQL() string {
	return c.createSinkSQL
}

func (c *CreateSinkCommand) TableSequences() []uint64 {
	return c.tableSequences
}

func (c *CreateSinkCommand) Cancel() {
	c.interruptor.Interrupt()
}

func NewOriginatingCreateSinkCommand(e *Executor, pl *parplan.Planner, schema *common.Schema, sql string,
	tableSequences []uint64, ast *parser.CreateSink) (*CreateSinkCommand, error) {
	return &CreateSinkCommand{
		e:              e,
		schema:         schema,
		pl:             pl,
		ast:            ast,
		createSinkSQL:  sql,
		tableSequences: tableSequences,
	}, nil
}

func NewCreateSinkCommand(e *Executor, schemaName string, createSinkSQL string, tableSequences []uint64) *CreateSinkCommand {
	schema := e.metaController.GetOrCreateSchema(schemaName)
	pl := parplan.NewPlanner(schema)
	return &CreateSinkCommand{
		e:              e,
		schema:         schema,
		pl:             pl,
		createSinkSQL:  createSinkSQL,
		tableSequences: tableSequences,
	}
}

func (c *CreateSinkCommand) OnPhase(phase int32) error {
	if phase == 0 {
		return c.onPhase0()
	}
	panic("invalid phase")
}

func (c *CreateSinkCommand) NumPhases() int {
	return 1
}

func (c *CreateSinkCommand) Before() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Mainly validation

	sink, err := c.createSinkFromAST(c.ast)
	if err != nil {
		return errors.WithStack(err)
	}
	c.sink = sink

	if err := c.e.metaController.ExistsTable(c.schema, c.sink.Info.Name); err != nil {
		return err
	}

	rows, err := c.e.pullEngine.ExecuteQuery("sys",
		fmt.Sprintf("select id from tables where schema_name='%s' and name='%s' and kind='%s'", c.sink.Info.SchemaName,
			c.sink.Info.Name, meta.TableKindSink))
	if err != nil {
		return errors.WithStack(err)
	}
	if rows.RowCount() != 0 {
		return errors.Errorf("sink with name %s.%s already exists in storage", c.sink.Info.SchemaName, c.sink.Info.Name)
	}
	return c.e.metaController.PersistSink(c.sink.Info)
}

func (c *CreateSinkCommand) onPhase0() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// If phase0 on the originating node, sink will already be set
	// this means we do not have to parse the ast twice
	if c.sink == nil {
		sink, err := c.createSink()
		if err != nil {
			return errors.WithStack(err)
		}
		c.sink = sink
	}
	if err := c.e.pushEngine.RegisterSink(c.sink); err != nil {
		return err
	}
	if err := c.e.metaController.RegisterSink(c.sink.Info); err != nil {
		return err
	}
	if err := c.sink.Start(); err != nil {
		return err
	}
	return c.sink.Connect()
}

func (c *CreateSinkCommand) AfterPhase(phase int32) error {
	return nil
}

func (c *CreateSinkCommand) Cleanup() {
	if c.sink != nil {
		if err := c.e.pushEngine.RemoveSink(c.sink.Info.ID); err != nil {
			// Ignore
		}
		if err := c.e.metaController.UnregisterSink(c.sink.Info.SchemaName, c.sink.Info.Name); err != nil {
			// Ignore
		}
		if err := c.e.metaController.DeleteSink(c.sink.Info.ID); err != nil {
			// Ignore
		}
	}
}

func (c *CreateSinkCommand) createSinkFromAST(ast *parser.CreateSink) (*push.Sink, error) { //nolint:gocyclo
	sinkName := strings.ToLower(ast.Name)
	querySQL := ast.Query.String()
	seqGenerator := common.NewPreallocSeqGen(c.tableSequences)
	tableID := seqGenerator.GenerateSequence()

	var (
		headerEncoding, keyEncoding, valueEncoding common.KafkaEncoding
		propsMap                                   map[string]string
		injectors                                  []selector.ColumnSelector
		brokerName, topicName                      string
		numPartitions, maxBufferedMessages         int
		sEmitAfter                                 string
	)
	for _, opt := range ast.TargetInformation {
		switch {
		case opt.HeaderEncoding != "":
			headerEncoding = common.KafkaEncodingFromString(opt.HeaderEncoding)
			if headerEncoding.Encoding == common.EncodingUnknown {
				return nil, errors.NewPranaErrorf(errors.InvalidStatement, "Unknown topic encoding %s", opt.HeaderEncoding)
			}
		case opt.KeyEncoding != "":
			keyEncoding = common.KafkaEncodingFromString(opt.KeyEncoding)
			if keyEncoding.Encoding == common.EncodingUnknown {
				return nil, errors.NewPranaErrorf(errors.InvalidStatement, "Unknown topic encoding %s", opt.KeyEncoding)
			}
		case opt.ValueEncoding != "":
			valueEncoding = common.KafkaEncodingFromString(opt.ValueEncoding)
			if valueEncoding.Encoding == common.EncodingUnknown {
				return nil, errors.NewPranaErrorf(errors.InvalidStatement, "Unknown topic encoding %s", opt.ValueEncoding)
			}
		case opt.Properties != nil:
			propsMap = make(map[string]string, len(opt.Properties))
			for _, prop := range opt.Properties {
				propsMap[prop.Key] = prop.Value
			}
		case opt.Injectors != nil:
			cs := opt.Injectors
			injectors = make([]selector.ColumnSelector, len(cs))
			for i := 0; i < len(cs); i++ {
				injectors[i] = cs[i].ToSelector()
			}
		case opt.BrokerName != "":
			brokerName = opt.BrokerName
		case opt.TopicName != "":
			topicName = opt.TopicName
		case opt.NumPartitions != 0:
			numPartitions = opt.NumPartitions
		case opt.MaxBufferedMessages != 0:
			maxBufferedMessages = opt.MaxBufferedMessages
		case opt.EmitAfter != "":
			sEmitAfter = opt.EmitAfter
		}
	}
	if headerEncoding == common.KafkaEncodingUnknown {
		return nil, errors.NewPranaErrorf(errors.InvalidStatement, "headerEncoding is required")
	}
	if keyEncoding == common.KafkaEncodingUnknown {
		return nil, errors.NewPranaErrorf(errors.InvalidStatement, "keyEncoding is required")
	}
	if valueEncoding == common.KafkaEncodingUnknown {
		return nil, errors.NewPranaErrorf(errors.InvalidStatement, "valueEncoding is required")
	}
	if brokerName == "" {
		return nil, errors.NewPranaErrorf(errors.InvalidStatement, "brokerName is required")
	}
	if topicName == "" {
		return nil, errors.NewPranaErrorf(errors.InvalidStatement, "topicName is required")
	}
	if numPartitions <= 0 {
		return nil, errors.NewPranaErrorf(errors.InvalidStatement, "numPartitions must be > 0")
	}
	var emitAfter time.Duration
	if sEmitAfter != "" {
		var err error
		emitAfter, err = parseEmitAfter(sEmitAfter)
		if err != nil {
			return nil, err
		}
	}
	if maxBufferedMessages == 0 {
		maxBufferedMessages = defaultMaxBufferedMessages
	}
	if maxBufferedMessages < 0 {
		return nil, errors.NewPranaErrorf(errors.InvalidStatement, "maxBufferedMessages must be >= 0")
	}
	if len(injectors) == 0 {
		return nil, errors.NewPranaErrorf(errors.InvalidStatement, "injectors must be specified")
	}
	originInfo := &common.SinkTargetInfo{
		BrokerName:          brokerName,
		TopicName:           topicName,
		NumPartitions:       numPartitions,
		MaxBufferedMessages: maxBufferedMessages,
		EmitAfter:           emitAfter,
		HeaderEncoding:      headerEncoding,
		KeyEncoding:         keyEncoding,
		ValueEncoding:       valueEncoding,
		Injectors:           injectors,
		Properties:          propsMap,
	}
	return push.CreateSink(c.e.pushEngine, c.pl, c.schema, sinkName, querySQL, tableID, seqGenerator, originInfo)
}

func parseEmitAfter(sEmitAfter string) (time.Duration, error) {
	sr := strings.Trim(sEmitAfter, " \t")
	var dur time.Duration
	var err error
	if strings.HasSuffix(sr, "ms") || strings.HasSuffix(sr, "s") || strings.HasSuffix(sr, "m") ||
		strings.HasSuffix(sr, "h") {
		dur, err = time.ParseDuration(sr)
		if err == nil && dur >= 0 {
			return dur, nil
		}
	}
	return 0, errors.NewPranaErrorf(errors.InvalidStatement, "Invalid EmitAfter %s. Must be an integer >= 0 "+
		"followed by a unit. Valid units are \"ms\", \"s\", \"m\", \"h\"", sEmitAfter)
}

func (c *CreateSinkCommand) createSink() (*push.Sink, error) {
	ast, err := parser.Parse(c.createSinkSQL)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if ast.Create == nil || ast.Create.Sink == nil {
		return nil, errors.Errorf("not a create sink %s", c.createSinkSQL)
	}
	return c.createSinkFromAST(ast.Create.Sink)
}

func (c *CreateSinkCommand) GetExtraData() []byte {
	return nil
}
