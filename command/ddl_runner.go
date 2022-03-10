package command

import (
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/notifier"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/notifications"
)

const (
	schemaLockAttemptTimeout = 30 * time.Second
	schemaLockRetryDelay     = 1 * time.Second
)

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

	LockName() string

	// NumPhases returns the number of phases in the command
	NumPhases() int
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
		ce:       ce,
		commands: make(map[string]DDLCommand),
		idSeq:    -1,
	}
}

func NewDDLCommand(e *Executor, commandType DDLCommandType, schemaName string, sql string, tableSequences []uint64) DDLCommand {
	switch commandType {
	case DDLCommandTypeCreateSource:
		return NewCreateSourceCommand(e, schemaName, sql, tableSequences)
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
	lock     sync.Mutex
	ce       *Executor
	commands map[string]DDLCommand
	idSeq    int64
}

func (d *DDLCommandRunner) generateCommandKey(origNodeID uint64, commandID uint64) string {
	key := make([]byte, 0, 16)
	key = common.AppendUint64ToBufferLE(key, origNodeID)
	key = common.AppendUint64ToBufferLE(key, commandID)
	return string(key)
}

func (d *DDLCommandRunner) HandleNotification(notification notifier.Notification) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	ddlInfo, ok := notification.(*notifications.DDLStatementInfo)
	if !ok {
		panic("not a ddl statement info")
	}
	skey := d.generateCommandKey(uint64(ddlInfo.GetOriginatingNodeId()), uint64(ddlInfo.GetCommandId()))
	com, ok := d.commands[skey]
	phase := ddlInfo.GetPhase()
	originatingNode := ddlInfo.GetOriginatingNodeId() == int64(d.ce.cluster.GetNodeID())
	if phase == 0 {
		if !ok {
			if originatingNode {
				return errors.Errorf("cannot find command with id %d:%d on originating node", ddlInfo.GetOriginatingNodeId(), ddlInfo.GetCommandId())
			}
			com = NewDDLCommand(d.ce, DDLCommandType(ddlInfo.CommandType), ddlInfo.GetSchemaName(), ddlInfo.GetSql(), ddlInfo.GetTableSequences())
			d.commands[skey] = com
		}
	} else if !ok {
		return errors.Errorf("cannot find command with id %d:%d", ddlInfo.GetOriginatingNodeId(), ddlInfo.GetCommandId())
	}
	err := com.OnPhase(phase)
	if phase == int32(com.NumPhases()-1) {
		// Final phase so delete the command
		delete(d.commands, skey)
	}
	return err
}

func (d *DDLCommandRunner) RunCommand(command DDLCommand) error {
	lockName := command.LockName()
	id := atomic.AddInt64(&d.idSeq, 1)
	commandKey := d.generateCommandKey(uint64(d.ce.cluster.GetNodeID()), uint64(id))
	d.lock.Lock()
	d.commands[commandKey] = command
	d.lock.Unlock()
	defer func() {
		delete(d.commands, commandKey)
	}()
	ddlInfo := &notifications.DDLStatementInfo{
		OriginatingNodeId: int64(d.ce.cluster.GetNodeID()), // TODO do we need this?
		CommandId:         id,
		CommandType:       int32(command.CommandType()),
		SchemaName:        command.SchemaName(),
		Sql:               command.SQL(),
		TableSequences:    command.TableSequences(),
	}
	if err := d.getLock(lockName); err != nil {
		return errors.WithStack(err)
	}
	err := d.RunWithLock(command, ddlInfo)
	// We release the lock even if we got an error
	d.releaseLock(lockName)
	return errors.WithStack(err)
}

func (d *DDLCommandRunner) releaseLock(lockName string) {
	ok, err := d.ce.cluster.ReleaseLock(lockName)
	if err != nil {
		log.Errorf("error in releasing DDL lock %+v", err)
	}
	if !ok {
		log.Errorf("failed to release ddl lock %s", lockName)
	}
}

func (d *DDLCommandRunner) RunWithLock(command DDLCommand, ddlInfo *notifications.DDLStatementInfo) error {
	if err := command.Before(); err != nil {
		return errors.WithStack(err)
	}
	for phase := 0; phase < command.NumPhases(); phase++ {
		if err := d.broadcastDDL(int32(phase), ddlInfo); err != nil {
			return errors.WithStack(err)
		}
		if err := command.AfterPhase(int32(phase)); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (d *DDLCommandRunner) broadcastDDL(phase int32, ddlInfo *notifications.DDLStatementInfo) error {
	// Broadcast DDL and wait for responses
	ddlInfo.Phase = phase
	return d.ce.notifClient.BroadcastSync(ddlInfo)
}

func (d *DDLCommandRunner) getLock(lockName string) error {
	start := time.Now()
	for {
		ok, err := d.ce.cluster.GetLock(lockName)
		if err != nil {
			return errors.WithStack(err)
		}
		if ok {
			return nil
		}
		if time.Now().Sub(start) > schemaLockAttemptTimeout {
			return errors.Errorf("timed out waiting to get ddl schema lock for schema %s", lockName)
		}
		time.Sleep(schemaLockRetryDelay)
	}
}

func (d *DDLCommandRunner) runningCommands() int {
	d.lock.Lock()
	defer d.lock.Unlock()
	return len(d.commands)
}

func (d *DDLCommandRunner) clear() {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.commands = make(map[string]DDLCommand)
}
