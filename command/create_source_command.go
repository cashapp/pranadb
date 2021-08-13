package command

import (
	"fmt"
	"github.com/alecthomas/repr"
	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"sync"
)

type CreateSourceCommand struct {
	lock           sync.Mutex
	e              *Executor
	schemaName     string
	sql            string
	tableSequences []uint64
	ast            *parser.CreateSource
	sourceInfo     *common.SourceInfo
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

func (c *CreateSourceCommand) GetLockName() string {
	return c.schemaName + "/"
}

func NewOriginatingCreateSourceCommand(e *Executor, schemaName string, sql string, tableSequences []uint64, ast *parser.CreateSource) *CreateSourceCommand {
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

func (c *CreateSourceCommand) BeforePrepare() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Before prepare we just persist the source info in the tables table
	var err error
	c.sourceInfo, err = c.getSourceInfo(c.ast)
	if err != nil {
		return err
	}
	return c.e.metaController.PersistSource(c.sourceInfo, true)
}

func (c *CreateSourceCommand) OnPrepare() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// If receiving on prepare from broadcast on the originating node, sourceInfo will already be set
	// this means we do not have to parse the ast twice!
	if c.sourceInfo == nil {
		ast, err := parser.Parse(c.sql)
		if err != nil {
			return errors.MaybeAddStack(err)
		}
		if ast.Create == nil || ast.Create.Source == nil {
			return fmt.Errorf("not a create source %s", c.sql)
		}
		c.sourceInfo, err = c.getSourceInfo(ast.Create.Source)
		if err != nil {
			return err
		}
	}
	// Create source in push engine so it can receive forwarded rows, do not activate consumers yet
	return c.e.pushEngine.CreateSource(c.sourceInfo)
}

func (c *CreateSourceCommand) OnCommit() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Activate the message consumers for the source
	if err := c.e.pushEngine.StartSource(c.sourceInfo.ID); err != nil {
		return err
	}

	// Register the source in the in memory meta data
	return c.e.metaController.RegisterSource(c.sourceInfo)
}

func (c *CreateSourceCommand) AfterCommit() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Update row in metadata tables table to prepare=false
	return c.e.metaController.PersistSource(c.sourceInfo, false)
}

func (c *CreateSourceCommand) getSourceInfo(ast *parser.CreateSource) (*common.SourceInfo, error) {
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
			colIndex[col.Name] = i
			colNames = append(colNames, col.Name)
			colType, err := col.ToColumnType()
			if err != nil {
				return nil, errors.MaybeAddStack(err)
			}
			colTypes = append(colTypes, colType)

		case option.PrimaryKey != "":
			index, ok := colIndex[option.PrimaryKey]
			if !ok {
				return nil, fmt.Errorf("invalid primary key column %q", option.PrimaryKey)
			}
			pkCols = append(pkCols, index)

		default:
			panic(repr.String(option))
		}
	}

	headerEncoding := common.KafkaEncodingFromString(ast.TopicInformation.HeaderEncoding)
	if headerEncoding == common.EncodingUnknown {
		return nil, errors.NewUserErrorF(errors.UnknownTopicEncoding, "Unknown topic encoding %s", ast.TopicInformation.HeaderEncoding)
	}
	keyEncoding := common.KafkaEncodingFromString(ast.TopicInformation.KeyEncoding)
	if keyEncoding == common.EncodingUnknown {
		return nil, errors.NewUserErrorF(errors.UnknownTopicEncoding, "Unknown topic encoding %s", ast.TopicInformation.KeyEncoding)
	}
	valueEncoding := common.KafkaEncodingFromString(ast.TopicInformation.ValueEncoding)
	if valueEncoding == common.EncodingUnknown {
		return nil, errors.NewUserErrorF(errors.UnknownTopicEncoding, "Unknown topic encoding %s", ast.TopicInformation.ValueEncoding)
	}
	props := ast.TopicInformation.Properties
	propsMap := make(map[string]string, len(props))
	for _, prop := range props {
		propsMap[prop.Key] = prop.Value
	}

	cs := ast.TopicInformation.ColSelectors
	colSelectors := make([]string, len(cs))
	for i := 0; i < len(cs); i++ {
		colSelectors[i] = cs[i]
	}
	lc := len(colSelectors)
	if lc > 0 && lc != len(colTypes) {
		return nil, errors.NewUserErrorF(errors.WrongNumberColumnSelectors,
			"if specified, number of column selectors (%d) must match number of columns (%d)", lc, len(colTypes))
	}

	topicInfo := &common.TopicInfo{
		BrokerName:     ast.TopicInformation.BrokerName,
		TopicName:      ast.TopicInformation.TopicName,
		HeaderEncoding: headerEncoding,
		KeyEncoding:    keyEncoding,
		ValueEncoding:  valueEncoding,
		ColSelectors:   colSelectors,
		Properties:     propsMap,
	}

	tableInfo := common.TableInfo{
		ID:             c.tableSequences[0],
		SchemaName:     c.schemaName,
		Name:           ast.Name,
		PrimaryKeyCols: pkCols,
		ColumnNames:    colNames,
		ColumnTypes:    colTypes,
		IndexInfos:     nil,
	}
	return &common.SourceInfo{
		TableInfo: &tableInfo,
		TopicInfo: topicInfo,
	}, nil
}
