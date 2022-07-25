package command

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"sync/atomic"
	"time"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/notifications"
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
		ce:    ce,
		idSeq: -1,
	}
}

func NewDDLCommand(e *Executor, commandType DDLCommandType, schemaName string, sql string, tableSequences []uint64,
	extraData []byte) DDLCommand {
	switch commandType {
	case DDLCommandTypeCreateSource:
		return NewCreateSourceCommand(e, schemaName, sql, tableSequences, extraData)
	case DDLCommandTypeCreateMV:
		return NewCreateMVCommand(e, schemaName, sql, tableSequences)
	case DDLCommandTypeDropSource:
		return NewDropSourceCommand(e, schemaName, sql)
	case DDLCommandTypeDropMV:
		return NewDropMVCommand(e, schemaName, sql)
	case DDLCommandTypeCreateIndex:
		return NewCreateIndexCommand(e, schemaName, sql, tableSequences)
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

func (d *ddlHandler) HandleMessage(notification remoting.ClusterMessage) (remoting.ClusterMessage, error) {
	return nil, d.runner.HandleDdlMessage(notification)
}

type cancelHandler struct {
	runner *DDLCommandRunner
}

func (a *cancelHandler) HandleMessage(notification remoting.ClusterMessage) (remoting.ClusterMessage, error) {
	return nil, a.runner.HandleCancelMessage(notification)
}

func (d *DDLCommandRunner) DdlHandler() remoting.ClusterMessageHandler {
	return &ddlHandler{runner: d}
}

func (d *DDLCommandRunner) CancelHandler() remoting.ClusterMessageHandler {
	return &cancelHandler{runner: d}
}

func (d *DDLCommandRunner) HandleCancelMessage(notification remoting.ClusterMessage) error {
	cancelMsg, ok := notification.(*notifications.DDLCancelMessage)
	if !ok {
		panic("not a cancel msg")
	}
	return d.cancelCommandsForSchema(cancelMsg.SchemaName)
}

func (d *DDLCommandRunner) cancelCommandsForSchema(schemaName string) error {
	d.commands.Range(func(key, value interface{}) bool {
		command, ok := value.(DDLCommand)
		if !ok {
			panic("not a ddl command")
		}
		if command.SchemaName() == schemaName {
			d.commands.Delete(key)
			command.Cancel()
			command.Cleanup()
		}
		return true
	})
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

func (d *DDLCommandRunner) HandleDdlMessage(notification remoting.ClusterMessage) error {
	ddlInfo, ok := notification.(*notifications.DDLStatementInfo)
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
	if phase == 0 {
		if !ok {
			if originatingNode {
				// Can happen if the command is cancelled - we just ignore
				return nil
			}
			com = NewDDLCommand(d.ce, DDLCommandType(ddlInfo.CommandType), ddlInfo.GetSchemaName(), ddlInfo.GetSql(),
				ddlInfo.GetTableSequences(), ddlInfo.GetExtraData())
			d.commands.Store(skey, com)
		}
	} else if !ok {
		// This can happen if notification comes in after commands are cancelled
		log.Warnf("cannot find command with id %d:%d", ddlInfo.GetOriginatingNodeId(), ddlInfo.GetCommandId())
		return nil
	}
	err := com.OnPhase(phase)
	if err != nil {
		com.Cleanup()
	}
	if phase == int32(com.NumPhases()-1) {
		// Final phase so delete the command
		d.commands.Delete(skey)
	}
	return err
}

func (d *DDLCommandRunner) RunCommand(command DDLCommand) error {
	lockName := getLockName(command.SchemaName())
	if err := d.getLock(lockName); err != nil {
		return errors.WithStack(err)
	}
	id := atomic.AddInt64(&d.idSeq, 1)
	commandKey := d.generateCommandKey(uint64(d.ce.cluster.GetNodeID()), uint64(id))
	d.commands.Store(commandKey, command)
	ddlInfo := &notifications.DDLStatementInfo{
		OriginatingNodeId: int64(d.ce.cluster.GetNodeID()), // TODO do we need this?
		CommandId:         id,
		CommandType:       int32(command.CommandType()),
		SchemaName:        command.SchemaName(),
		Sql:               command.SQL(),
		TableSequences:    command.TableSequences(),
		ExtraData:         command.GetExtraData(),
	}
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

func (d *DDLCommandRunner) RunWithLock(commandKey string, command DDLCommand, ddlInfo *notifications.DDLStatementInfo) error {
	if err := command.Before(); err != nil {
		d.commands.Delete(commandKey)
		return errors.WithStack(err)
	}
	for phase := 0; phase < command.NumPhases(); phase++ {
		err := d.broadcastDDL(int32(phase), ddlInfo)
		if err == nil {
			err = command.AfterPhase(int32(phase))
		}
		if err != nil {
			// Broadcast a cancel to clean up command state across the cluster
			if err2 := d.broadcastCancel(command.SchemaName()); err2 != nil {
				// Ignore
			}
			return errors.WithStack(err)
		}
	}
	return nil
}

func (d *DDLCommandRunner) broadcastCancel(schemaName string) error {
	return d.ce.ddlResetClient.BroadcastSync(&notifications.DDLCancelMessage{SchemaName: schemaName})
}

func (d *DDLCommandRunner) broadcastDDL(phase int32, ddlInfo *notifications.DDLStatementInfo) error {
	// Broadcast DDL and wait for responses
	ddlInfo.Phase = phase
	return d.ce.notifClient.BroadcastSync(ddlInfo)
}

func (d *DDLCommandRunner) getLock(lockName string) error {
	timeout := getSchemaLockAttemptTimeout()
	start := time.Now()
	for {
		ok, err := d.ce.cluster.GetLock(lockName)
		if err != nil {
			return errors.WithStack(err)
		}
		if ok {
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
	count := 0
	d.commands.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count == 0
}
