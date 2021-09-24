// Package parser contains the command parser.
//
//nolint:govet
package parser

import (
	"strconv"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"

	"github.com/squareup/pranadb/common"
)

// DefaultFSP is the default fractional seconds precision for a TIMESTAMP field.
const DefaultFSP = 6

// RawQuery represents raw SQL that can be passed through directly.
type RawQuery struct {
	Tokens []lexer.Token
	Query  []string `(!";")+`
}

func (r *RawQuery) String() string {
	out := strings.Builder{}
	for _, token := range r.Tokens {
		v := token.Value
		if token.Type == parser.Lexer().Symbols()["String"] {
			// THIS IS A HACK! Need to fix participle bug that's stripping the quotes from the raw tokens
			v = strconv.Quote(v)
		}
		out.WriteString(v)
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
	Name  *Ref      `@@ "AS"`
	Query *RawQuery `@@`
}

type ColumnDef struct {
	Pos lexer.Position

	Name string `@Ident`

	Type       common.Type `@(("VARCHAR"|"TINYINT"|"INT"|"BIGINT"|"TIMESTAMP"|"DOUBLE"|"DECIMAL"))` // Conversion done by common.Type.Capture()
	Parameters []int       `("(" @Number ("," @Number)* ")")?`                                      // Optional parameters to the type(x [, x, ...])
}

func (c *ColumnDef) ToColumnType() (common.ColumnType, error) {
	ct, ok := common.ColumnTypesByType[c.Type]
	if ok {
		if len(c.Parameters) != 0 {
			return common.ColumnType{}, participle.Errorf(c.Pos, "")
		}
		return ct, nil
	}
	switch c.Type {
	case common.TypeDecimal:
		if len(c.Parameters) != 2 {
			return common.ColumnType{}, participle.Errorf(c.Pos, "expected DECIMAL(precision, scale)")
		}
		return common.NewDecimalColumnType(c.Parameters[0], c.Parameters[1]), nil
	case common.TypeTimestamp:
		var fsp int8 = DefaultFSP
		if len(c.Parameters) == 1 {
			fsp = int8(c.Parameters[0])
		}
		return common.NewTimestampColumnType(fsp), nil
	default:
		panic(c.Type) // If this happens there's something wrong with the parser and/or validation.
	}
}

type TableOption struct {
	PrimaryKey []string   `  "PRIMARY" "KEY" "(" @Ident ( "," @Ident )* ")"`
	Column     *ColumnDef `| @@`
}

type CreateSource struct {
	Name string `@Ident`
	// TODO: Add selection of columns from source. Inline in the column type definitions? Separate clause?
	Options          []*TableOption    `"(" @@ ("," @@)* ")"` // Table options.
	TopicInformation *TopicInformation `"WITH" "(" @@ ")"`
}

type TopicInformation struct {
	BrokerName     string               `"BrokerName" "=" @String ","`
	TopicName      string               `"TopicName" "=" @String ","`
	HeaderEncoding string               `"HeaderEncoding" "=" @String ","`
	KeyEncoding    string               `"KeyEncoding" "=" @String ","`
	ValueEncoding  string               `"ValueEncoding" "=" @String ","`
	ColSelectors   []string             `"ColumnSelectors" "=" "(" (@String ("," @String)*)? ")"`
	Properties     []*TopicInfoProperty `"Properties" "=" "(" (@@ ("," @@)*)? ")"`
}

type ColSelector struct {
	Selector string `@String`
}

type TopicInfoProperty struct {
	Key   string `@String "="`
	Value string `@String`
}

// Create statement.
type Create struct {
	MaterializedView *CreateMaterializedView `  "MATERIALIZED" "VIEW" @@`
	Source           *CreateSource           `| "SOURCE" @@`
}

// Drop statement
type Drop struct {
	MaterializedView bool   `(   @"MATERIALIZED" "VIEW"`
	Source           bool   `  | @"SOURCE" )`
	Name             string `@Ident `
}

// Execute statement.
type Execute struct {
	PsID int64    `@Number`
	Args []string `(@String | @Number)*`
}

// AST root.
type AST struct {
	Select  string // Unaltered SELECT statement, if any.
	Prepare string // Unaltered PREPARE statement, if any.

	Use     string   `(  "USE" @Ident`
	Execute *Execute ` | "EXECUTE" @@`
	Drop    *Drop    ` | "DROP" @@ `
	Create  *Create  ` | "CREATE" @@ ) ";"?`
}
