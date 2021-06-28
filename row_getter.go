package pranadb

import "github.com/squareup/pranadb/exec"

type LocalRowGetter struct {
}

func (l *LocalRowGetter) GetRows(limit int, queryID string) (resultChan chan exec.AsyncRowGetterResult) {
	// TODO lookup running pull query and call GetRows on the parent most executor
	return nil
}
