package parser

import (
	"testing"

	"github.com/alecthomas/participle/v2/lexer"
	"github.com/alecthomas/repr"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected *AST
		err      string
	}{
		{"Select", "SELECT * FROM table WHERE foo = `bar`",
			&AST{Select: "SELECT * FROM table WHERE foo = `bar`"}, ""},
		{"CreateMV", `CREATE MATERIALIZED VIEW myview SELECT * FROM table`, &AST{
			Create: &Create{
				MaterializedView: &CreateMaterializedView{
					Name: &Ref{Path: []string{"myview"}},
					Query: &RawQuery{
						Tokens: []lexer.Token{
							{Type: -6, Value: " ", Pos: lexer.Position{Offset: 31, Line: 1, Column: 32}},
							{Type: -2, Value: "SELECT", Pos: lexer.Position{Offset: 32, Line: 1, Column: 33}},
							{Type: -6, Value: " ", Pos: lexer.Position{Offset: 38, Line: 1, Column: 39}},
							{Type: -5, Value: "*", Pos: lexer.Position{Offset: 39, Line: 1, Column: 40}},
							{Type: -6, Value: " ", Pos: lexer.Position{Offset: 40, Line: 1, Column: 41}},
							{Type: -2, Value: "FROM", Pos: lexer.Position{Offset: 41, Line: 1, Column: 42}},
							{Type: -6, Value: " ", Pos: lexer.Position{Offset: 45, Line: 1, Column: 46}},
							{Type: -2, Value: "table", Pos: lexer.Position{Offset: 46, Line: 1, Column: 47}},
						},
					},
				},
			},
		}, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := Parse(test.sql)
			if test.err != "" {
				require.EqualError(t, err, test.err)
			} else {
				require.NoError(t, err)
				require.Equal(t,
					repr.String(test.expected, repr.IgnoreGoStringer(), repr.Indent("  ")),
					repr.String(actual, repr.IgnoreGoStringer(), repr.Indent("  ")),
					repr.String(actual, repr.IgnoreGoStringer(), repr.Indent("  ")))
			}
		})
	}
}
