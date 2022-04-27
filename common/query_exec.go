package common

type SimpleQueryExec interface {
	ExecuteQuery(schemaName string, query string) (rows *Rows, err error)
}
