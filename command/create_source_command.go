package command

import (
	"fmt"
	"strings"
	"sync"

	"github.com/alecthomas/repr"
	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/command/parser/selector"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/push/source"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type CreateSourceCommand struct {
	lock           sync.Mutex
	e              *Executor
	schemaName     string
	sql            string
	tableSequences []uint64
	ast            *parser.CreateSource
	sourceInfo     *common.SourceInfo
	source         *source.Source
}

func (c *CreateSourceCommand) CommandType() DDLCommandType {
	return DDLCommandTypeCreateSource
}

func (c *CreateSourceCommand) SchemaName() string {
	return c.schemaName
}

func (c *CreateSourceCommand) SQL() string {
	return c.sql
}

func (c *CreateSourceCommand) TableSequences() []uint64 {
	return c.tableSequences
}

func (c *CreateSourceCommand) LockName() string {
	return c.schemaName + "/"
}

func NewOriginatingCreateSourceCommand(e *Executor, schemaName string, sql string, tableSequences []uint64, ast *parser.CreateSource) *CreateSourceCommand {
	ast.Name = strings.ToLower(ast.Name)
	return &CreateSourceCommand{
		e:              e,
		schemaName:     schemaName,
		sql:            sql,
		tableSequences: tableSequences,
		ast:            ast,
	}
}

func NewCreateSourceCommand(e *Executor, schemaName string, sql string, tableSequences []uint64) *CreateSourceCommand {
	return &CreateSourceCommand{
		e:              e,
		schemaName:     schemaName,
		sql:            sql,
		tableSequences: tableSequences,
	}
}

func (c *CreateSourceCommand) Before() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	var err error
	c.sourceInfo, err = c.getSourceInfo(c.ast)
	if err != nil {
		return errors.WithStack(err)
	}
	return c.validate()
}

func (c *CreateSourceCommand) validate() error {
	schema := c.e.metaController.GetOrCreateSchema(c.schemaName)
	if err := c.e.metaController.ExistsMvOrSource(schema, c.sourceInfo.Name); err != nil {
		return err
	}
	rows, err := c.e.pullEngine.ExecuteQuery("sys",
		fmt.Sprintf("select id from tables where schema_name='%s' and name='%s' and kind='%s'", c.sourceInfo.SchemaName, c.sourceInfo.Name, meta.TableKindSource))
	if err != nil {
		return errors.WithStack(err)
	}
	if rows.RowCount() != 0 {
		return errors.Errorf("source with name %s.%s already exists in storage", c.sourceInfo.SchemaName, c.sourceInfo.Name)
	}

	origInfo := c.sourceInfo.OriginInfo
	if origInfo.InitialState != "" {
		err := validateInitState(origInfo.InitialState, c.sourceInfo.TableInfo, c.e.metaController)
		if err != nil {
			return err
		}
	}

	for _, enc := range []common.KafkaEncoding{origInfo.HeaderEncoding, origInfo.KeyEncoding, origInfo.ValueEncoding} {
		if enc.Encoding != common.EncodingProtobuf {
			continue
		}
		_, err := c.e.protoRegistry.FindDescriptorByName(protoreflect.FullName(enc.SchemaName))
		if err != nil {
			return errors.NewPranaErrorf(errors.InvalidStatement, "Proto message %q not registered", enc.SchemaName)
		}
	}

	for _, sel := range origInfo.ColSelectors {
		if sel.MetaKey == nil && len(sel.Selector) == 0 {
			return errors.NewPranaErrorf(errors.InvalidStatement, "Invalid column selector %q", sel)
		}
		if sel.MetaKey != nil {
			f := *sel.MetaKey
			if !(f == "header" || f == "key" || f == "timestamp") {
				return errors.NewPranaErrorf(errors.InvalidStatement, `Invalid metadata key in column selector %q. Valid values are "header", "key", "timestamp".`, sel)
			}
		}
	}

	return nil
}

func (c *CreateSourceCommand) OnPhase(phase int32) error {
	switch phase {
	case 0:
		return c.onPhase0()
	case 1:
		return c.onPhase1()
	default:
		panic("invalid phase")
	}
}

func (c *CreateSourceCommand) NumPhases() int {
	return 2
}

func (c *CreateSourceCommand) onPhase0() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// If phase0 on the originating node, mvInfo will already be set
	// this means we do not have to parse the ast twice
	if c.sourceInfo == nil {
		ast, err := parser.Parse(c.sql)
		if err != nil {
			return errors.WithStack(err)
		}
		if ast.Create == nil || ast.Create.Source == nil {
			return errors.Errorf("not a create source %s", c.sql)
		}
		c.sourceInfo, err = c.getSourceInfo(ast.Create.Source)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Create source in push engine so it can receive forwarded rows, do not activate consumers yet
	var initTable *common.TableInfo
	if c.sourceInfo.OriginInfo.InitialState != "" {
		var err error
		initTable, err = getInitialiseFromTable(c.schemaName, c.sourceInfo.OriginInfo.InitialState, c.e.metaController)
		if err != nil {
			return err
		}
	}
	src, err := c.e.pushEngine.CreateSource(c.sourceInfo, initTable)
	if err != nil {
		return errors.WithStack(err)
	}
	if initTable != nil {
		if err := c.e.pushEngine.LoadInitialStateForTable(c.e.cluster.GetLocalShardIDs(), initTable.ID, c.sourceInfo.ID); err != nil {
			return err
		}
	}
	c.source = src
	return err
}

func (c *CreateSourceCommand) onPhase1() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Activate the message consumers for the source
	if err := c.source.Start(); err != nil {
		return errors.WithStack(err)
	}

	// Register the source in the in memory meta data
	return c.e.metaController.RegisterSource(c.sourceInfo)
}

func (c *CreateSourceCommand) AfterPhase(phase int32) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if phase == 0 {
		// We persist the source *before* it is registered - otherwise if failure occurs source can disappear after
		// being used
		return c.e.metaController.PersistSource(c.sourceInfo)
	}
	return nil
}

// nolint: gocyclo
func (c *CreateSourceCommand) getSourceInfo(ast *parser.CreateSource) (*common.SourceInfo, error) {
	ast.Name = strings.ToLower(ast.Name)
	var (
		colNames []string
		colTypes []common.ColumnType
		colIndex = map[string]int{}
		pkCols   []int
	)
	for i, option := range ast.Options {
		switch {
		case option.Column != nil:
			// Convert AST column definition to a ColumnType.
			col := option.Column
			cName := strings.ToLower(col.Name)
			colIndex[cName] = i
			colNames = append(colNames, cName)
			colType, err := col.ToColumnType()
			if err != nil {
				return nil, errors.WithStack(err)
			}
			colTypes = append(colTypes, colType)

		case len(option.PrimaryKey) > 0:
			for _, pk := range option.PrimaryKey {
				index, ok := colIndex[strings.ToLower(pk)]
				if !ok {
					return nil, errors.NewPranaErrorf(errors.InvalidStatement, "Invalid primary key column %q", option.PrimaryKey)
				}
				pkCols = append(pkCols, index)
			}
		default:
			panic(repr.String(option))
		}
	}

	var (
		headerEncoding, keyEncoding, valueEncoding common.KafkaEncoding
		ingestFilter                               string
		propsMap                                   map[string]string
		colSelectors                               []selector.ColumnSelector
		brokerName, topicName                      string
		initialiseFrom                             string
	)
	for _, opt := range ast.OriginInformation {
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
		case opt.IngestFilter != "":
			ingestFilter = opt.IngestFilter
		case opt.Properties != nil:
			propsMap = make(map[string]string, len(opt.Properties))
			for _, prop := range opt.Properties {
				propsMap[prop.Key] = prop.Value
			}
		case opt.ColSelectors != nil:
			cs := opt.ColSelectors
			colSelectors = make([]selector.ColumnSelector, len(cs))
			for i := 0; i < len(cs); i++ {
				colSelectors[i] = cs[i].ToSelector()
			}
		case opt.BrokerName != "":
			brokerName = opt.BrokerName
		case opt.TopicName != "":
			topicName = opt.TopicName
		case opt.InitialState != "":
			initialiseFrom = opt.InitialState
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
	if len(colSelectors) != len(colTypes) {
		return nil, errors.NewPranaErrorf(errors.InvalidStatement,
			"Number of column selectors (%d) must match number of columns (%d)", len(colSelectors), len(colTypes))
	}
	if len(pkCols) == 0 {
		return nil, errors.NewPranaErrorf(errors.InvalidStatement, "Primary key is required")
	}
	if len(colIndex) != len(colNames) {
		return nil, errors.NewPranaErrorf(errors.InvalidStatement, "Duplicate column names")
	}

	pkMap := make(map[int]struct{}, len(pkCols))
	for _, pkCol := range pkCols {
		pkMap[pkCol] = struct{}{}
	}
	if len(pkMap) != len(pkCols) {
		return nil, errors.NewPranaErrorf(errors.InvalidStatement, "Primary key cannot contain same column multiple times")
	}

	originInfo := &common.SourceOriginInfo{
		BrokerName:     brokerName,
		TopicName:      topicName,
		HeaderEncoding: headerEncoding,
		KeyEncoding:    keyEncoding,
		ValueEncoding:  valueEncoding,
		IngestFilter:   ingestFilter,
		ColSelectors:   colSelectors,
		Properties:     propsMap,
		InitialState:   initialiseFrom,
	}
	tableInfo := common.NewTableInfo(c.tableSequences[0], c.schemaName, ast.Name, pkCols, colNames, colTypes, nil)
	return &common.SourceInfo{
		TableInfo:  tableInfo,
		OriginInfo: originInfo,
	}, nil
}
