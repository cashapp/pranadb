package command

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/clustermsgs"
	"sync"
	"sync/atomic"
	"time"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/remoting"
)

const (
	defaultSchemaLockAttemptTimeout = 10 * time.Second
	schemaLockRetryDelay            = 1 * time.Second
)

var schemaLockAttemptTimeout = defaultSchemaLockAttemptTimeout
var schemaLockAttemptTimeoutLock = sync.Mutex{}

func getSchemaLockAttemptTimeout() time.Duration {
	schemaLockAttemptTimeoutLock.Lock()
	defer schemaLockAttemptTimeoutLock.Unlock()
	return schemaLockAttemptTimeout
}

func SetSchemaLockAttemptTimeout(timeout time.Duration) {
	schemaLockAttemptTimeoutLock.Lock()
	defer schemaLockAttemptTimeoutLock.Unlock()
	schemaLockAttemptTimeout = timeout
}

type DDLCommand interface {
	CommandType() DDLCommandType

	SchemaName() string

	SQL() string

	TableSequences() []uint64

	// Before is called on the originating node before the first phase
	Before() error

	// OnPhase is called on every node in the cluster passing in the phase
	OnPhase(phase int32) error

	// AfterPhase is called on the originating node once successful responses from the specified phase have been returned
	AfterPhase(phase int32) error

	// NumPhases returns the number of phases in the command
	NumPhases() int

	// Cancel cancels the command
	Cancel()

	// Cleanup will be called if an error occurs during execution of the command, it should perform any clean up logic
	// to leave the system in a clean state
	Cleanup()

	GetExtraData() []byte
}

type DDLCommandType int

const (
	DDLCommandTypeCreateSource = iota
	DDLCommandTypeDropSource
	DDLCommandTypeCreateMV
	DDLCommandTypeDropMV
	DDLCommandTypeCreateIndex
	DDLCommandTypeDropIndex
)

func NewDDLCommandRunner(ce *Executor) *DDLCommandRunner {
	return &DDLCommandRunner{
		ce:                    ce,
		idSeq:                 -1,
		cancellationSequences: make(map[cancelledCommandsKey]int64),
	}
}

func NewDDLCommand(e *Executor, commandType DDLCommandType, schemaName string, sql string, tableSequences []uint64,
	extraData []byte) DDLCommand {
	switch commandType {
	case DDLCommandTypeCreateSource:
		return NewCreateSourceCommand(e, schemaName, sql, tableSequences, extraData)
	case DDLCommandTypeCreateMV:
		return NewCreateMVCommand(e, schemaName, sql, tableSequences, extraData)
	case DDLCommandTypeDropSource:
		return NewDropSourceCommand(e, schemaName, sql)
	case DDLCommandTypeDropMV:
		return NewDropMVCommand(e, schemaName, sql)
	case DDLCommandTypeCreateIndex:
		return NewCreateIndexCommand(e, schemaName, sql, tableSequences, extraData)
	case DDLCommandTypeDropIndex:
		return NewDropIndexCommand(e, schemaName, sql)
	default:
		panic("invalid ddl command")
	}
}

type DDLCommandRunner struct {
	ce       *Executor
	commands sync.Map
	idSeq    int64

	handleCancelLock sync.Mutex
	// Cancel can be received on a node during or *before* the command has been received
	// so we keep a map of the sequences for each schema and originating node before which commands should be cancelled
	cancellationSequences map[cancelledCommandsKey]int64
}

type cancelledCommandsKey struct {
	schemaName        string
	originatingNodeID int64
}

func (d *DDLCommandRunner) generateCommandKey(origNodeID uint64, commandID uint64) string {
	key := make([]byte, 0, 16)
	key = common.AppendUint64ToBufferLE(key, origNodeID)
	key = common.AppendUint64ToBufferLE(key, commandID)
	return string(key)
}

type ddlHandler struct {
	runner *DDLCommandRunner
}

func (d *ddlHandler) HandleMessage(clusterMsg remoting.ClusterMessage) (remoting.ClusterMessage, error) {
	return nil, d.runner.HandleDdlMessage(clusterMsg)
}

type cancelHandler struct {
	runner *DDLCommandRunner
}

func (a *cancelHandler) HandleMessage(clusterMsg remoting.ClusterMessage) (remoting.ClusterMessage, error) {
	return nil, a.runner.HandleCancelMessage(clusterMsg)
}

func (d *DDLCommandRunner) DdlHandler() remoting.ClusterMessageHandler {
	return &ddlHandler{runner: d}
}

func (d *DDLCommandRunner) CancelHandler() remoting.ClusterMessageHandler {
	return &cancelHandler{runner: d}
}

func (d *DDLCommandRunner) HandleCancelMessage(clusterMsg remoting.ClusterMessage) error {
	cancelMsg, ok := clusterMsg.(*clustermsgs.DDLCancelMessage)
	if !ok {
		panic("not a cancel msg")
	}
	return d.cancelCommandsForSchema(cancelMsg.SchemaName, cancelMsg.CommandId, cancelMsg.OriginatingNodeId)
}

// cancel commands for the schema up to and including the commandID
func (d *DDLCommandRunner) cancelCommandsForSchema(schemaName string, commandID int64, originatingNodeID int64) error {
	found := false
	d.commands.Range(func(key, value interface{}) bool {
		command, ok := value.(DDLCommand)
		if !ok {
			panic("not a ddl command")
		}
		if command.SchemaName() == schemaName {
			d.commands.Delete(key)
			command.Cancel()
			command.Cleanup()
			found = true
		}
		return true
	})
	if !found {
		d.handleCancelLock.Lock()
		defer d.handleCancelLock.Unlock()
		// If we didn't find the command to delete it's possible that the cancel has arrived before the original command
		// so we add it to a map so we know to ignore the command if it arrives later
		key := cancelledCommandsKey{
			schemaName:        schemaName,
			originatingNodeID: originatingNodeID,
		}
		d.cancellationSequences[key] = commandID
	}
	return nil
}

// Cancel stops any running DDL and broadcasts all nodes to do the same
func (d *DDLCommandRunner) Cancel(schemaName string) error {

	lockName := getLockName(schemaName)

	// Get the lock if it isn't already held
	_, err := d.ce.cluster.GetLock(lockName)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		_, err := d.ce.cluster.ReleaseLock(getLockName(schemaName))
		if err != nil {
			log.Errorf("failed to release ddl lock %+v", err)
		}
	}()

	// Broadcast cancel to other nodes
	// We need to use a different client as broadcast on a remoting client currently only supports one broadcast at
	// a time, and one could be in progress
	return d.broadcastCancel(schemaName)
}

func getLockName(schemaName string) string {
	return schemaName + "/"
}

func (d *DDLCommandRunner) HandleDdlMessage(ddlMsg remoting.ClusterMessage) error {
	if !d.ce.pushEngine.IsStarted() {
		return errors.NewPranaErrorf(errors.DdlRetry, "push engine is not started")
	}
	ddlInfo, ok := ddlMsg.(*clustermsgs.DDLStatementInfo)
	if !ok {
		panic("not a ddl statement info")
	}
	skey := d.generateCommandKey(uint64(ddlInfo.GetOriginatingNodeId()), uint64(ddlInfo.GetCommandId()))
	c, ok := d.commands.Load(skey)
	var com DDLCommand
	if ok {
		com, ok = c.(DDLCommand)
		if !ok {
			panic("not a ddl command")
		}
	}
	phase := ddlInfo.GetPhase()
	originatingNode := ddlInfo.GetOriginatingNodeId() == int64(d.ce.cluster.GetNodeID())
	log.Debugf("Handling DDL message %d %s on node %d from node %d", ddlInfo.CommandType, ddlInfo.Sql,
		d.ce.cluster.GetNodeID(), ddlInfo.GetOriginatingNodeId())
	if phase == 0 {
		if !ok {
			if originatingNode {
				// Can happen if the command is cancelled - we just ignore
				return nil
			}
			com = NewDDLCommand(d.ce, DDLCommandType(ddlInfo.CommandType), ddlInfo.GetSchemaName(), ddlInfo.GetSql(),
				ddlInfo.GetTableSequences(), ddlInfo.GetExtraData())
			if !d.storeIfNotCancelled(skey, ddlInfo.CommandId, ddlInfo.GetOriginatingNodeId(), com) {
				return nil
			}
		}
	} else if !ok {
		// This can happen if ddlMsg comes in after commands are cancelled
		log.Warnf("cannot find command with id %d:%d", ddlInfo.GetOriginatingNodeId(), ddlInfo.GetCommandId())
		return nil
	}
	log.Debugf("Running phase %d for DDL message %d %s", phase, com.CommandType(), ddlInfo.Sql)
	err := com.OnPhase(phase)
	if err != nil {
		com.Cleanup()
	}
	log.Debugf("Running phase %d for DDL message %d %s returned err %v", phase, com.CommandType(), ddlInfo.Sql, err)
	if phase == int32(com.NumPhases()-1) || err != nil {
		// Final phase or err so delete the command
		d.commands.Delete(skey)
	}
	return err
}

func (d *DDLCommandRunner) storeIfNotCancelled(skey string, commandID int64, originatingNodeID int64, com DDLCommand) bool {
	d.handleCancelLock.Lock()
	defer d.handleCancelLock.Unlock()
	// We first check if we have already received a cancel for commands up to this id - cancels can come in before the
	// original command was fielded
	key := cancelledCommandsKey{
		schemaName:        com.SchemaName(),
		originatingNodeID: originatingNodeID,
	}
	cid, ok := d.cancellationSequences[key]
	if ok && cid >= commandID {
		log.Debugf("ddl command arrived after cancellation, it will be ignored command id %d cid %d", commandID, cid)
		return false
	}

	d.commands.Store(skey, com)
	return true
}

func (d *DDLCommandRunner) RunCommand(ctx context.Context, command DDLCommand) error {
	log.Debugf("Attempting to run DDL command %d", command.CommandType())
	lockName := getLockName(command.SchemaName())
	if err := d.getLock(lockName); err != nil {
		return errors.WithStack(err)
	}
	id := atomic.AddInt64(&d.idSeq, 1)
	commandKey := d.generateCommandKey(uint64(d.ce.cluster.GetNodeID()), uint64(id))
	d.commands.Store(commandKey, command)
	ddlInfo := &clustermsgs.DDLStatementInfo{
		OriginatingNodeId: int64(d.ce.cluster.GetNodeID()), // TODO do we need this?
		CommandId:         id,
		CommandType:       int32(command.CommandType()),
		SchemaName:        command.SchemaName(),
		Sql:               command.SQL(),
		TableSequences:    command.TableSequences(),
		ExtraData:         command.GetExtraData(),
	}
	d.cancelIfContextCancelled(ctx, commandKey, command.SchemaName())
	err := d.RunWithLock(commandKey, command, ddlInfo)
	// We release the lock even if we got an error
	if _, err2 := d.ce.cluster.ReleaseLock(getLockName(command.SchemaName())); err2 != nil {
		log.Errorf("failed to release lock %+v", err2)
	}
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (d *DDLCommandRunner) cancelIfContextCancelled(ctx context.Context, commandKey string, schemaName string) {
	go func() {
		<-ctx.Done()
		// If command is still in the map then this context is being cancelled - we need to cancel
		_, ok := d.commands.Load(commandKey)
		if ok {
			if err := d.Cancel(schemaName); err != nil {
				log.Errorf("failed to cancel ddl %v", err)
			}
		}
	}()
}

func (d *DDLCommandRunner) RunWithLock(commandKey string, command DDLCommand, ddlInfo *clustermsgs.DDLStatementInfo) error {
	log.Debugf("Running DDL command %d %s", command.CommandType(), ddlInfo.Sql)
	if err := command.Before(); err != nil {
		d.commands.Delete(commandKey)
		return errors.WithStack(err)
	}
	log.Debugf("Executed before for DDL command %d %s", command.CommandType(), ddlInfo.Sql)
	for phase := 0; phase < command.NumPhases(); phase++ {
		log.Debugf("Broadcasting phase %d for DDL command %d %s", phase, command.CommandType(), ddlInfo.Sql)
		err := d.broadcastDDL(int32(phase), ddlInfo)
		if err == nil {
			err = command.AfterPhase(int32(phase))
		}
		if err != nil {
			log.Debugf("Error return from broadcasting phase %d for DDL command %d %s %v cancel will be broadcast", phase, command.CommandType(), ddlInfo.Sql, err)
			d.commands.Delete(commandKey)
			// Broadcast a cancel to clean up command state across the cluster
			if err2 := d.broadcastCancel(command.SchemaName()); err2 != nil {
				// Ignore
			}
			log.Debugf("Broadcast of cancel returned for DDL command %d %s", command.CommandType(), ddlInfo.Sql)
			return errors.WithStack(err)
		}
	}
	return nil
}

func (d *DDLCommandRunner) broadcastCancel(schemaName string) error {
	return d.ce.ddlResetClient.Broadcast(&clustermsgs.DDLCancelMessage{
		SchemaName:        schemaName,
		CommandId:         atomic.LoadInt64(&d.idSeq),
		OriginatingNodeId: int64(d.ce.config.NodeID),
	})
}

func (d *DDLCommandRunner) broadcastDDL(phase int32, ddlInfo *clustermsgs.DDLStatementInfo) error {
	// Broadcast DDL and wait for responses
	ddlInfo.Phase = phase
	return d.ce.ddlClient.Broadcast(ddlInfo)
}

func (d *DDLCommandRunner) getLock(lockName string) error {
	log.Debugf("Attempting to get DDL lock %s", lockName)
	timeout := getSchemaLockAttemptTimeout()
	start := time.Now()
	for {
		ok, err := d.ce.cluster.GetLock(lockName)
		if err != nil {
			return errors.WithStack(err)
		}
		if ok {
			log.Debugf("DDL lock %s obtained", lockName)
			return nil
		}
		if time.Now().Sub(start) > timeout {
			return errors.NewPranaErrorf(errors.Timeout,
				"Timed out waiting to execute DDL on schema: %s. Is there another DDL operation running?", lockName[:len(lockName)-1])
		}
		time.Sleep(schemaLockRetryDelay)
	}
}

func (d *DDLCommandRunner) empty() bool {
	log.Debug("DDLCommand Runner state:")
	count := 0
	d.commands.Range(func(key, value interface{}) bool {
		log.Debugf("DDLCommand runner has command: %s", value.(DDLCommand).SQL()) //nolint:forcetypeassert
		count++
		return true
	})
	return count == 0
}
