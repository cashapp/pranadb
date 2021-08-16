package parser

import (
	"testing"

	"github.com/alecthomas/participle/v2/lexer"
	"github.com/alecthomas/repr"
	"github.com/squareup/pranadb/common"
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
		{"CreateMV", `CREATE MATERIALIZED VIEW myview AS SELECT * FROM table`, &AST{
			Create: &Create{
				MaterializedView: &CreateMaterializedView{
					Name: &Ref{Path: []string{"myview"}},
					Query: &RawQuery{
						Tokens: []lexer.Token{
							{Type: -6, Value: " ", Pos: lexer.Position{Offset: 34, Line: 1, Column: 35}},
							{Type: -2, Value: "SELECT", Pos: lexer.Position{Offset: 35, Line: 1, Column: 36}},
							{Type: -6, Value: " ", Pos: lexer.Position{Offset: 41, Line: 1, Column: 42}},
							{Type: -5, Value: "*", Pos: lexer.Position{Offset: 42, Line: 1, Column: 43}},
							{Type: -6, Value: " ", Pos: lexer.Position{Offset: 43, Line: 1, Column: 44}},
							{Type: -2, Value: "FROM", Pos: lexer.Position{Offset: 44, Line: 1, Column: 45}},
							{Type: -6, Value: " ", Pos: lexer.Position{Offset: 48, Line: 1, Column: 49}},
							{Type: -2, Value: "table", Pos: lexer.Position{Offset: 49, Line: 1, Column: 50}},
						},
					},
				},
			},
		}, ""},
		{"CreateSource", `
			create source sensor_readings(
			sensor_id bigint,
			location varchar,
			temperature double,
			primary key (sensor_id)
		) with (
			brokername = "testbroker",
			topicname = "testtopic",
			headerencoding = "json",
			keyencoding = "json",
			valueencoding = "json",
			columnselectors = (
				"k.k0",
				"v.v1",
				"v.v2"
			)
			properties = (
			"prop1" = "val1",
			"prop2" = "val2"
			)
		)`, &AST{Create: &Create{
			Source: &CreateSource{
				Name: "sensor_readings",
				Options: []*TableOption{
					{Column: &ColumnDef{Pos: lexer.Position{Offset: 38, Line: 3, Column: 4}, Name: "sensor_id", Type: common.Type(3)}},
					{Column: &ColumnDef{Pos: lexer.Position{Offset: 59, Line: 4, Column: 4}, Name: "location", Type: common.Type(6)}},
					{Column: &ColumnDef{Pos: lexer.Position{Offset: 80, Line: 5, Column: 4}, Name: "temperature", Type: common.Type(4)}},
					{PrimaryKey: "sensor_id"},
				},
				TopicInformation: &TopicInformation{
					BrokerName:     "testbroker",
					TopicName:      "testtopic",
					HeaderEncoding: "json",
					KeyEncoding:    "json",
					ValueEncoding:  "json",
					ColSelectors:   []string{"k.k0", "v.v1", "v.v2"},
					Properties: []*TopicInfoProperty{
						{Key: "prop1", Value: "val1"},
						{Key: "prop2", Value: "val2"},
					},
				},
			},
		}}, ""},
		{
			"DropSource", "DROP SOURCE test_source_1",
			&AST{Drop: &Drop{Source: true, Name: "test_source_1"}}, "",
		},
		{
			"DropMaterializedView", "DROP MATERIALIZED VIEW test_mv_1",
			&AST{Drop: &Drop{MaterializedView: true, Name: "test_mv_1"}}, "",
		},
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
