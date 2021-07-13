package command

import (
	"github.com/alecthomas/repr"
	"github.com/pkg/errors"
	"log"
	"sync/atomic"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/pull"
	"github.com/squareup/pranadb/pull/exec"
	"github.com/squareup/pranadb/push"
)

type Executor struct {
	cluster         cluster.Cluster
	metaController  *meta.Controller
	pushEngine      *push.PushEngine
	pullEngine      *pull.PullEngine
	ddlStatementSeq int64
	notifActions    map[int64]chan statementExecutionResult
}

func NewCommandExecutor(
	metaController *meta.Controller,
	pushEngine *push.PushEngine,
	pullEngine *pull.PullEngine,
	cluster cluster.Cluster,
) *Executor {
	return &Executor{
		cluster:        cluster,
		metaController: metaController,
		pushEngine:     pushEngine,
		pullEngine:     pullEngine,
		notifActions:   make(map[int64]chan statementExecutionResult),
	}
}

func (p *Executor) createSource(
	schemaName string,
	name string,
	colNames []string,
	colTypes []common.ColumnType,
	pkCols []int,
	topicInfo *common.TopicInfo,
	seqGenerator common.SeqGenerator,
) error {
	log.Printf("creating source %s on node %d", name, p.cluster.GetNodeID())
	id := seqGenerator.GenerateSequence()

	tableInfo := common.TableInfo{
		ID:             id,
		TableName:      name,
		PrimaryKeyCols: pkCols,
		ColumnNames:    colNames,
		ColumnTypes:    colTypes,
		IndexInfos:     nil,
	}
	sourceInfo := common.SourceInfo{
		SchemaName: schemaName,
		Name:       name,
		TableInfo:  &tableInfo,
		TopicInfo:  topicInfo,
	}
	err := p.metaController.RegisterSource(&sourceInfo)
	if err != nil {
		return err
	}
	return p.pushEngine.CreateSource(&sourceInfo)
}

func (p *Executor) createMaterializedView(schemaName string, name string, query string, seqGenerator common.SeqGenerator) error {
	log.Printf("creating mv %s on node %d", name, p.cluster.GetNodeID())
	id := seqGenerator.GenerateSequence()
	schema := p.metaController.GetOrCreateSchema(schemaName)
	mvInfo, err := p.pushEngine.CreateMaterializedView(schema, name, query, id, seqGenerator)
	if err != nil {
		return err
	}
	err = p.metaController.RegisterMaterializedView(mvInfo)
	if err != nil {
		return err
	}
	return nil
}

func (p *Executor) CreateSink(schemaName string, sinkInfo *common.SinkInfo) error {
	panic("implement me")
}

func (p *Executor) DropSource(schemaName string, name string) error {
	panic("implement me")
}

func (p *Executor) DropMaterializedView(schemaName string, name string) error {
	panic("implement me")
}

func (p *Executor) DropSink(schemaName string, name string) error {
	panic("implement me")
}

func (p *Executor) CreatePushQuery(sql string) error {
	panic("implement me")
}

// GetPushEngine is only used in testing
func (p *Executor) GetPushEngine() *push.PushEngine {
	return p.pushEngine
}

// ExecuteSQLStatement executes a synchronous SQL statement.
func (p *Executor) ExecuteSQLStatement(schemaName string, sql string) (exec.PullExecutor, error) {
	return p.executeSQLStatementInternal(schemaName, sql, true, nil)
}

func (p *Executor) executeSQLStatementInternal(schemaName string, sql string, local bool, seqGenerator common.SeqGenerator) (exec.PullExecutor, error) {
	ast, err := parser.Parse(sql)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	switch {
	case ast.Select != "":
		return p.execSelect(schemaName, ast.Select)

	case ast.Create != nil && ast.Create.MaterializedView != nil:
		if local {
			// Materialized view needs 2 sequences
			sequences, err := p.generateSequences(2)
			if err != nil {
				return nil, err
			}
			return p.executeInGlobalOrder(schemaName, sql, sequences)
		}
		return p.execCreateMaterializedView(schemaName, ast.Create.MaterializedView, seqGenerator)

	case ast.Create != nil && ast.Create.Source != nil:
		if local {
			// Source needs 1 sequence
			sequences, err := p.generateSequences(1)
			if err != nil {
				return nil, err
			}
			return p.executeInGlobalOrder(schemaName, sql, sequences)
		}
		return p.execCreateSource(schemaName, ast.Create.Source, seqGenerator)

	default:
		panic("unsupported query " + sql)
	}
}

func (p *Executor) generateSequences(numValues int) ([]uint64, error) {
	sequences := make([]uint64, numValues)
	for i := 0; i < numValues; i++ {
		v, err := p.cluster.GenerateTableID()
		if err != nil {
			return nil, err
		}
		sequences[i] = v
	}
	return sequences, nil
}

// We need to reserve the table sequences required for the DDL statement *before* we broadcast the DDL across the
// cluster, and those same table sequence values have to be used on every node for consistency.
// So we create a sequence generator that returns value based on already obtained sequence values
type preallocSeqGen struct {
	sequences []uint64
	index     int
}

func (p preallocSeqGen) GenerateSequence() uint64 {
	if p.index >= len(p.sequences) {
		panic("not enough sequence values")
	}
	res := p.sequences[p.index]
	p.index++
	return res
}

func (p *Executor) execSelect(schemaName string, sql string) (exec.PullExecutor, error) {
	schema, ok := p.metaController.GetSchema(schemaName)
	if !ok {
		return nil, errors.Errorf("unknown schema %s", schemaName)
	}
	dag, err := p.pullEngine.BuildPullQuery(schema, sql)
	return dag, errors.WithStack(err)
}

func (p *Executor) execCreateMaterializedView(schemaName string, mv *parser.CreateMaterializedView, seqGenerator common.SeqGenerator) (exec.PullExecutor, error) {
	err := p.createMaterializedView(schemaName, mv.Name.String(), mv.Query.String(), seqGenerator)
	return exec.Empty, errors.WithStack(err)
}

func (p *Executor) execCreateSource(schemaName string, src *parser.CreateSource, seqGenerator common.SeqGenerator) (exec.PullExecutor, error) {
	var (
		colNames []string
		colTypes []common.ColumnType
		colIndex = map[string]int{}
		pkCols   []int
	)
	for i, option := range src.Options {
		switch {
		case option.Column != nil:
			// Convert AST column definition to a ColumnType.
			col := option.Column
			colIndex[col.Name] = i
			colNames = append(colNames, col.Name)
			colType, err := col.ToColumnType()
			if err != nil {
				return nil, errors.WithStack(err)
			}
			colTypes = append(colTypes, colType)

		case option.PrimaryKey != "":
			index, ok := colIndex[option.PrimaryKey]
			if !ok {
				return nil, errors.Errorf("invalid primary key column %q", option.PrimaryKey)
			}
			pkCols = append(pkCols, index)

		default:
			panic(repr.String(option))
		}
	}
	if err := p.createSource(schemaName, src.Name, colNames, colTypes, pkCols, nil, seqGenerator); err != nil {
		return nil, errors.WithStack(err)
	}
	return exec.Empty, nil
}

// DDL statements such as create materialized view, create source etc need to broadcast to every node in the cluster
// so they can be installed in memory on each node, and we also need to ensure that all statements are executed in
// the exact same order on each node irrespective of where the command originated from.
// In order to do this, we first broadcast the command across the cluster via a raft group which ensures a global
// ordering and we don't process the command locally until we receive it from the cluster.
func (p *Executor) executeInGlobalOrder(schemaName string, sql string, sequences []uint64) (exec.PullExecutor, error) {
	nextSeq := atomic.AddInt64(&p.ddlStatementSeq, 1)
	statementInfo := ddlStatementInfo{
		originatingNodeID: p.cluster.GetNodeID(),
		sequence:          nextSeq,
		schemaName:        schemaName,
		sql:               sql,
		tableSequences:    sequences,
	}
	var buff []byte
	notif := cluster.Notification{
		Type: cluster.NotificationTypeDDLStatement,
		Data: statementInfo.Serialize(buff),
	}
	ch := make(chan statementExecutionResult)
	p.notifActions[nextSeq] = ch

	go func() {
		err := p.cluster.BroadcastNotification(&notif)
		if err != nil {
			ch <- statementExecutionResult{err: err}
		}
	}()

	res, ok := <-ch
	if !ok {
		panic("channel was closed")
	}
	return res.exec, res.err
}

func (p *Executor) HandleNotification(notification *cluster.Notification) {
	ddlStmt := ddlStatementInfo{}
	ddlStmt.Deserialize(notification.Data, 0)
	seqGenerator := &preallocSeqGen{sequences: ddlStmt.tableSequences}
	if ddlStmt.originatingNodeID == p.cluster.GetNodeID() {
		ch, ok := p.notifActions[ddlStmt.sequence]
		if !ok {
			panic("cannot find notification")
		}
		ex, err := p.executeSQLStatementInternal(ddlStmt.schemaName, ddlStmt.sql, false, seqGenerator)
		res := statementExecutionResult{
			exec: ex,
			err:  err,
		}
		ch <- res
	} else {
		_, err := p.executeSQLStatementInternal(ddlStmt.schemaName, ddlStmt.sql, false, seqGenerator)
		if err != nil {
			log.Printf("Failed to execute broadcast DDL %s for %s %v", ddlStmt.sql, ddlStmt.schemaName, err)
		}
	}
}

type statementExecutionResult struct {
	exec exec.PullExecutor
	err  error
}

type ddlStatementInfo struct {
	originatingNodeID int
	sequence          int64
	schemaName        string
	sql               string
	tableSequences    []uint64
}

func (d *ddlStatementInfo) Serialize(buff []byte) []byte {
	buff = common.AppendUint64ToBufferLittleEndian(buff, uint64(d.originatingNodeID))
	buff = common.AppendUint64ToBufferLittleEndian(buff, uint64(d.sequence))
	buff = common.EncodeString(d.schemaName, buff)
	buff = common.EncodeString(d.sql, buff)
	buff = common.AppendUint32ToBufferLittleEndian(buff, uint32(len(d.tableSequences)))
	for _, seq := range d.tableSequences {
		buff = common.AppendUint64ToBufferLittleEndian(buff, seq)
	}
	return buff
}

func (d *ddlStatementInfo) Deserialize(buff []byte, offset int) int {
	d.originatingNodeID = int(common.ReadUint64FromBufferLittleEndian(buff, offset))
	offset += 8
	d.sequence = int64(common.ReadUint64FromBufferLittleEndian(buff, offset))
	offset += 8
	d.schemaName, offset = common.DecodeString(buff, offset)
	d.sql, offset = common.DecodeString(buff, offset)
	sqlLen := common.ReadUint32FromBufferLittleEndian(buff, offset)
	offset += 4
	d.tableSequences = make([]uint64, sqlLen)
	for i := 0; i < int(sqlLen); i++ {
		d.tableSequences[i] = common.ReadUint64FromBufferLittleEndian(buff, offset)
		offset += 8
	}
	return offset
}
