package command

import (
	"testing"

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
		{"CreateMV", `CREATE MATERIALIZED VIEW myview`, &AST{
			Create: &Create{
				MaterializedView: &CreateMaterializedView{
					Name: &Ref{Path: []string{"myview"}},
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
					repr.String(actual, repr.IgnoreGoStringer(), repr.Indent("  ")))
			}
		})
	}
}
