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

	// BeforePrepare is called on the originating node only before prepare is broadcast
	BeforePrepare() error

	// OnPrepare is called on every node on receipt of prepare DDL notification
	OnPrepare() error

	// OnCommit is called on every node on receipt of commit DDL notification
	OnCommit() error

	// AfterCommit is called on the originating node only after all responses from commit have returned
	AfterCommit() error

	LockName() string
}

type DDLCommandType int

const (
	DDLCommandTypeCreateSource = iota
	DDLCommandTypeDropSource
	DDLCommandTypeCreateMV
	DDLCommandTypeDropMV
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
	if ddlInfo.GetPrepare() {
		// If this comes in on the originating node the command will already be there
		com, ok := d.commands[skey]
		if !ok {
			com = NewDDLCommand(d.ce, DDLCommandType(ddlInfo.CommandType), ddlInfo.GetSchemaName(), ddlInfo.GetSql(), ddlInfo.GetTableSequences())
			d.commands[skey] = com
		}
		if err := com.OnPrepare(); err != nil {
			return err
		}
	} else {
		com, ok := d.commands[skey]
		if !ok {
			return errors.Errorf("cannot find command with id %d:%d", ddlInfo.GetOriginatingNodeId(), ddlInfo.GetCommandId())
		}
		if err := com.OnCommit(); err != nil {
			return err
		}
		delete(d.commands, skey)
	}
	return nil
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
		return err
	}
	err := d.RunWithLock(command, ddlInfo)
	// We release the lock even if we got an error
	d.releaseLock(lockName)
	return err
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
	if err := command.BeforePrepare(); err != nil {
		return err
	}
	if err := d.broadcastDDL(true, ddlInfo); err != nil {
		return err
	}
	if err := d.broadcastDDL(false, ddlInfo); err != nil {
		return err
	}
	return command.AfterCommit()
}

func (d *DDLCommandRunner) broadcastDDL(prepare bool, ddlInfo *notifications.DDLStatementInfo) error {
	// Broadcast DDL and wait for responses

	// Broadcast should reach all nodes in the cluster, if a node has died, no DDL can be processed until it is
	// brought back up. This can be some minutes later so we retry indefinitely. It should not be aborted unless
	// the command is aborted by the user
	ddlInfo.Prepare = prepare
	return d.ce.notifClient.BroadcastSync(ddlInfo)
}

func (d *DDLCommandRunner) getLock(lockName string) error {
	start := time.Now()
	for {
		ok, err := d.ce.cluster.GetLock(lockName)
		if err != nil {
			return err
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
