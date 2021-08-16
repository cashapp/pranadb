package command

import (
	"fmt"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/notifier"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/notifications"
	"log"
	"sync"
	"sync/atomic"
	"time"
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
	log.Printf("Creating ddl command with type: %v", commandType)
	switch commandType {
	case DDLCommandTypeCreateSource:
		return NewCreateSourceCommand(e, schemaName, sql, tableSequences)
	case DDLCommandTypeCreateMV:
	case DDLCommandTypeDropSource:
		return NewDropSourceCommand(e, schemaName, sql)
	case DDLCommandTypeDropMV:
		return NewDropMVCommand(e, schemaName, sql)
	default:
		panic("invalid ddl command")
	}
	return nil
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

func (d *DDLCommandRunner) HandleNotification(notification notifier.Notification) {
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
			log.Printf("Failed to prepare ddl command: %v", err)
			// TODO do we really want to send back a response here -caller will think it succeeded
			// We should probably send back pass/fail in sync broadcast response
			return
		}
	} else {
		com, ok := d.commands[skey]
		if !ok {
			log.Printf("cannot find command with id %d:%d", ddlInfo.GetOriginatingNodeId(), ddlInfo.GetCommandId())
			return
		}
		if err := com.OnCommit(); err != nil {
			log.Printf("Failed to commit ddl command: %v", err)
			// TODO do we really want to send back a response here -caller will think it succeeded
			// We should probably send back pass/fail in sync broadcast response
			return
		}
		delete(d.commands, skey)
	}
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
	if err := command.BeforePrepare(); err != nil {
		return err
	}
	if err := d.broadcastDDL(true, ddlInfo); err != nil {
		return err
	}
	if err := d.broadcastDDL(false, ddlInfo); err != nil {
		return err
	}
	if err := command.AfterCommit(); err != nil {
		return err
	}
	ok, err := d.ce.cluster.ReleaseLock(lockName)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("failed to release ddl lock %s", lockName)
	}
	return nil
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
			return fmt.Errorf("timed out waiting to get ddl schema lock for schema %s", lockName)
		}
		time.Sleep(schemaLockRetryDelay)
	}
}
