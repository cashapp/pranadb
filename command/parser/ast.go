// Package parser contains the command parser.
//
//nolint:govet
package parser

import (
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"

	"github.com/squareup/pranadb/common"
)

// RawQuery represents raw SQL that can be passed through directly.
type RawQuery struct {
	Tokens []lexer.Token
	Query  []string `(!";")+`
}

func (r *RawQuery) String() string {
	out := strings.Builder{}
	for _, token := range r.Tokens {
		out.WriteString(token.Value)
	}
	return out.String()
}

// A Ref to a view, table, column, etc.
type Ref struct {
	Path []string `@Ident ("." @Ident)*`
}

func (r *Ref) String() string {
	return strings.Join(r.Path, ".")
}

// CreateMaterializedView statement.
type CreateMaterializedView struct {
	Name  *Ref      `@@`
	Query *RawQuery `@@`
}
type ColumnDef struct {
	Pos lexer.Position

	Name       string      `@Ident`
	Type       common.Type `@(("TINY"|"BIG")? ("VARCHAR"|"INT"|"TIMESTAMP"|"DOUBLE"))` // Conversion done by common.Type.Capture()
	Parameters []int       `("(" @Number ("," @Number)* ")")?`                         // Optional parameters to the type(x [, x, ...])
	NotNull    bool        `("NULL" | @("NOT" "NULL"))?`                               // Null if omitted.
}

func (c *ColumnDef) ToColumnType() (common.ColumnType, error) {
	switch c.Type {
	case common.TypeDecimal:
		if len(c.Parameters) != 2 {
			return common.ColumnType{}, participle.Errorf(c.Pos, "expected DECIMAL(precision, scale)")
		}
		// TODO: Some validation on precision and scale.
		return common.NewDecimalColumnType(!c.NotNull, c.Parameters[0], c.Parameters[1]), nil

	case common.TypeVarchar:
		if len(c.Parameters) != 1 {
			return common.ColumnType{}, participle.Errorf(c.Pos, "expected VARCHAR(length)")
		}
		if c.Parameters[0] < 1 || c.Parameters[0] > 255 {
			return common.ColumnType{}, participle.Errorf(c.Pos, "length of VARCHAR must be between 1 and 255 not %d", c.Parameters[0])
		}
		return common.NewVarcharColumnType(!c.NotNull, c.Parameters[0]), nil

	default:
		return common.ColumnType{Type: c.Type, NotNullable: c.NotNull}, nil
	}
}

type TableOption struct {
	PrimaryKey string     `  "PRIMARY" "KEY" "(" @Ident ")"`
	Column     *ColumnDef `| @@`
}

type CreateSource struct {
	Name string `@Ident`
	// TODO: Add selection of columns from source. Inline in the column type definitions? Separate clause?
	Options []*TableOption `"(" @@ ("," @@)* ")"` // Table options.
}

// Create statement.
type Create struct {
	MaterializedView *CreateMaterializedView `  "MATERIALIZED" "VIEW" @@`
	Source           *CreateSource           `| "SOURCE" @@`
}

// AST root.
type AST struct {
	Select string // Unaltered SELECT statement, if any.

	Create *Create `"CREATE" @@`
}
