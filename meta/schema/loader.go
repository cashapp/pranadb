package schema

import (
	"fmt"
	"log"

	"github.com/squareup/pranadb/command"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/push"
)

// Loader is a service that loads existing table schemas from disk and applies them to the metadata
// controller and the push engine.
type Loader struct {
	meta       *meta.Controller
	executor   *command.Executor
	pushEngine *push.PushEngine
}

func NewLoader(m *meta.Controller, exec *command.Executor, push *push.PushEngine) *Loader {
	return &Loader{
		meta:       m,
		executor:   exec,
		pushEngine: push,
	}
}

func (l *Loader) Start() error {
	session := l.executor.CreateSession("sys")
	defer func() {
		if err := session.Close(); err != nil {
			log.Printf("failed to close session: %v", err)
		}
	}()
	pull, err := l.executor.ExecuteSQLStatement(session, "select * from tables")
	if err != nil {
		return err
	}
	rows, err := pull.GetRows(-1)
	if err != nil {
		return err
	}
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		kind := row.GetString(1)
		switch kind {
		case meta.TableKindSource:
			info := meta.DecodeSourceInfoRow(&row)
			if err := l.meta.RegisterSource(info, false); err != nil {
				return err
			}
			if err := l.pushEngine.CreateSource(info); err != nil {
				return err
			}
		case meta.TableKindMaterializedView:
			// TODO: Load materialized view
		default:
			return fmt.Errorf("unknown table kind %s", kind)
		}
	}
	return nil
}
