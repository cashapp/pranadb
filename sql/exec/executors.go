package exec

import "github.com/squareup/pranadb/sql"

type PushExecutor interface {

	handleRows(rows sql.Rows) error

}

type PullExecutor interface {

	getRows() (sql.Rows, error)

}

type PushSelect struct {


}

type Expression interface {



}


