package command

import (
	"fmt"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/interruptor"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/push"
	"strings"
	"sync"
)

type CreateMVCommand struct {
	lock           sync.Mutex
	e              *Executor
	pl             *parplan.Planner
	schema         *common.Schema
	createMVSQL    string
	tableSequences []uint64
	mv             *push.MaterializedView
	ast            *parser.CreateMaterializedView
	toDeleteBatch  *cluster.ToDeleteBatch
	interruptor    interruptor.Interruptor
	leadersMap     map[uint64]uint64
}

func (c *CreateMVCommand) CommandType() DDLCommandType {
	return DDLCommandTypeCreateMV
}

func (c *CreateMVCommand) SchemaName() string {
	return c.schema.Name
}

func (c *CreateMVCommand) SQL() string {
	return c.createMVSQL
}

func (c *CreateMVCommand) TableSequences() []uint64 {
	return c.tableSequences
}

func (c *CreateMVCommand) Cancel() {
	c.interruptor.Interrupt()
}

func NewOriginatingCreateMVCommand(e *Executor, pl *parplan.Planner, schema *common.Schema, sql string,
	tableSequences []uint64, ast *parser.CreateMaterializedView) (*CreateMVCommand, error) {
	leadersMap, err := e.cluster.GetLeadersMap()
	if err != nil {
		return nil, err
	}
	return &CreateMVCommand{
		e:              e,
		schema:         schema,
		pl:             pl,
		ast:            ast,
		createMVSQL:    sql,
		tableSequences: tableSequences,
		leadersMap:     leadersMap,
	}, nil
}

func NewCreateMVCommand(e *Executor, schemaName string, createMVSQL string, tableSequences []uint64, extraData []byte) *CreateMVCommand {
	schema := e.metaController.GetOrCreateSchema(schemaName)
	pl := parplan.NewPlanner(schema)
	return &CreateMVCommand{
		e:              e,
		schema:         schema,
		pl:             pl,
		createMVSQL:    createMVSQL,
		tableSequences: tableSequences,
		leadersMap:     deserializeLeadersMap(extraData),
	}
}

func (c *CreateMVCommand) OnPhase(phase int32) error {
	switch phase {
	case 0:
		return c.onPhase0()
	case 1:
		return c.onPhase1()
	case 2:
		return c.onPhase2()
	default:
		panic("invalid phase")
	}
}

func (c *CreateMVCommand) NumPhases() int {
	return 3
}

func (c *CreateMVCommand) Before() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Mainly validation

	mv, err := c.createMVFromAST(c.ast)
	if err != nil {
		return errors.WithStack(err)
	}
	c.mv = mv

	if err := c.e.metaController.ExistsMvOrSource(c.schema, mv.Info.Name); err != nil {
		return err
	}

	rows, err := c.e.pullEngine.ExecuteQuery("sys",
		fmt.Sprintf("select id from tables where schema_name='%s' and name='%s' and kind='%s'", c.mv.Info.SchemaName, c.mv.Info.Name, meta.TableKindMaterializedView))
	if err != nil {
		return errors.WithStack(err)
	}
	if rows.RowCount() != 0 {
		return errors.Errorf("materialized view with name %s.%s already exists in storage", c.mv.Info.SchemaName, c.mv.Info.Name)
	}
	if mv.Info.OriginInfo.InitialState != "" {
		err := validateInitState(mv.Info.OriginInfo.InitialState, mv.Info.TableInfo, c.e.metaController)
		if err != nil {
			return err
		}
	}

	return err
}

func (c *CreateMVCommand) onPhase0() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// If phase0 on the originating node, mv will already be set
	// this means we do not have to parse the ast twice
	if c.mv == nil {
		mv, err := c.createMV()
		if err != nil {
			return errors.WithStack(err)
		}
		c.mv = mv
	}

	// We store rows in the to_delete table - if MV creation fails (e.g. node crashes) then on restart the MV state will
	// be cleaned up - we have to add a prefix for each shard as the shard id comes first in the key
	var err error
	c.toDeleteBatch, err = storeToDeleteBatch(c.mv.Info.ID, c.e.cluster)
	if err != nil {
		return err
	}

	// We can now load initial state from initial state table (if any)
	var initTable *common.TableInfo
	if c.mv.Info.OriginInfo.InitialState != "" {
		var err error
		initTable, err = getInitialiseFromTable(c.mv.Info.SchemaName, c.mv.Info.OriginInfo.InitialState, c.e.metaController)
		if err != nil {
			return err
		}
		shardIDs := c.e.cluster.GetLocalShardIDs()
		if err := c.e.pushEngine.LoadInitialStateForTable(shardIDs, initTable.ID, c.mv.Info.ID, &c.interruptor); err != nil {
			return err
		}
	}

	// We must first connect any aggregations in the MV as remote consumers as they might have rows forwarded to them
	// during the MV fill process. This must be done on all nodes before we start the fill
	// We do not join the MV up to it's feeding sources or MVs at this point
	return c.mv.Connect(false, true)
}

func (c *CreateMVCommand) onPhase1() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	var localLeaderShards []uint64
	for shardID, nodeID := range c.leadersMap {
		if nodeID == uint64(c.e.cluster.GetNodeID()) {
			localLeaderShards = append(localLeaderShards, shardID)
		}
	}

	if err := c.e.cluster.RegisterStartFill(c.leadersMap, &c.interruptor); err != nil {
		return err
	}

	// Fill the MV from it's feeding sources and MVs
	if err := c.mv.Fill(localLeaderShards, &c.interruptor); err != nil {
		return err
	}

	c.e.cluster.RegisterEndFill()
	return nil
}

func (c *CreateMVCommand) onPhase2() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// The fill can cause rows to be forwarded - to make sure they're all processed we must wait for all schedulers
	// on all nodes - this must be done after fill has completed on all nodes
	if err := c.e.pushEngine.WaitForSchedulers(); err != nil {
		return err
	}

	// The MV is now created and filled on all nodes but it isn't currently registered so it can't be used by clients
	// We register it now
	if err := c.e.pushEngine.RegisterMV(c.mv); err != nil {
		return errors.WithStack(err)
	}
	if err := c.e.metaController.RegisterMaterializedView(c.mv.Info, c.mv.InternalTables); err != nil {
		return err
	}
	// Maybe inject an error after fill and after row in tables table is persisted but before to_delete rows removed
	if err := c.e.FailureInjector().GetFailpoint("create_mv_2").CheckFail(); err != nil {
		return err
	}

	// Now delete rows from the to_delete table
	return c.e.cluster.RemoveToDeleteBatch(c.toDeleteBatch)
}

func (c *CreateMVCommand) AfterPhase(phase int32) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if phase == 1 {
		// Maybe inject an error after fill but before row in tables table is persisted
		if err := c.e.FailureInjector().GetFailpoint("create_mv_1").CheckFail(); err != nil {
			return err
		}

		// We add the MV to the tables table once the fill phase is complete
		// We only do this on the originating node
		// We need to do this *before* the MV is available to clients otherwise a node failure and restart could cause
		// the MV to disappear after it's been used
		return c.e.metaController.PersistMaterializedView(c.mv.Info, c.mv.InternalTables)
	}
	return nil
}

func (c *CreateMVCommand) Cleanup() {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.mv == nil {
		return
	}
	if err := c.mv.Disconnect(); err != nil {
		// Ignore
	}
	if err := c.e.pushEngine.RemoveMV(c.mv.Info.ID); err != nil {
		// Ignore
	}
	c.e.cluster.RegisterEndFill()
}

func (c *CreateMVCommand) createMVFromAST(ast *parser.CreateMaterializedView) (*push.MaterializedView, error) {
	mvName := strings.ToLower(ast.Name.String())
	querySQL := ast.Query.String()
	seqGenerator := common.NewPreallocSeqGen(c.tableSequences)
	tableID := seqGenerator.GenerateSequence()
	var initTable string
	if ast.OriginInformation != nil {
		for _, info := range ast.OriginInformation {
			if info.InitialState != "" {
				initTable = info.InitialState
				break
			}
		}
	}
	return push.CreateMaterializedView(c.e.pushEngine, c.pl, c.schema, mvName, querySQL, initTable, tableID, seqGenerator)
}

func (c *CreateMVCommand) createMV() (*push.MaterializedView, error) {
	ast, err := parser.Parse(c.createMVSQL)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if ast.Create == nil || ast.Create.MaterializedView == nil {
		return nil, errors.Errorf("not a create materialized view %s", c.createMVSQL)
	}
	return c.createMVFromAST(ast.Create.MaterializedView)
}

func (c *CreateMVCommand) GetExtraData() []byte {
	return serializeLeadersMap(c.leadersMap)
}

func serializeLeadersMap(m map[uint64]uint64) []byte {
	var buff []byte
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(m)))
	for shardID, nodeID := range m {
		buff = common.AppendUint64ToBufferLE(buff, shardID)
		buff = common.AppendUint64ToBufferLE(buff, nodeID)
	}
	return buff
}

func deserializeLeadersMap(buff []byte) map[uint64]uint64 {
	l, offset := common.ReadUint32FromBufferLE(buff, 0)
	m := make(map[uint64]uint64, l)
	for i := 0; i < int(l); i++ {
		var shardID, nodeID uint64
		shardID, offset = common.ReadUint64FromBufferLE(buff, offset)
		nodeID, offset = common.ReadUint64FromBufferLE(buff, offset)
		m[shardID] = nodeID
	}
	return m
}
