package command

// A Ref to a view, table, column, etc.
type Ref struct {
	Path []string `@Ident ("." @Ident)*`
}

// CreateMaterializedView statement.
type CreateMaterializedView struct {
	Name *Ref `@@`
}

// Create statement.
type Create struct {
	MaterializedView *CreateMaterializedView `"CREATE" "MATERIALIZED" "VIEW" @@`
}

// AST root.
type AST struct {
	Select string // Unaltered SELECT statement, if any.

	Create *Create `@@`
}
