package command

import (
	"regexp"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
	"github.com/alecthomas/participle/v2/lexer/stateful"
)

var (
	lex = stateful.MustSimple([]stateful.Rule{
		{`Keyword`, `(?i)\b(SELECT|FROM|TOP|DISTINCT|ALL|WHERE|GROUP|BY|HAVING|UNION|MINUS|EXCEPT|INTERSECT|ORDER|LIMIT|OFFSET|TRUE|FALSE|NULL|IS|NOT|ANY|SOME|BETWEEN|AND|OR|LIKE|AS|IN|MATERIALIZED|VIEW)\b`, nil},
		{`Ident`, "[a-zA-Z_][a-zA-Z_0-9]*|`[^`]*`", nil},
		{`Number`, `[-+]?\d*\.?\d+([eE][-+]?\d+)?`, nil},
		{`String`, `'[^']*'|"[^"]*"`, nil},
		{`Operators`, `<>|!=|<=|>=|[-+*/%,.()=<>]`, nil},
		{`Whitespace`, `\s+`, nil},
	})
	parser = participle.MustBuild(&AST{},
		participle.Lexer(lex),
		participle.CaseInsensitive("Keyword"),
		participle.Elide("Whitespace"),
		participle.UseLookahead(3),
		participle.Unquote(),
		participle.Map(func(token lexer.Token) (lexer.Token, error) {
			if strings.HasPrefix(token.Value, "`") {
				token.Value = token.Value[1 : len(token.Value)-1]
			}
			return token, nil
		}, "Ident"),
	)
	selectPrefix = regexp.MustCompile(`(?i)^select\s+`)
)

// Parse an SQL statement.
func Parse(sql string) (*AST, error) {
	if selectPrefix.MatchString(sql) {
		return &AST{Select: sql}, nil
	}
	ast := &AST{}
	err := parser.ParseString("", sql, ast)
	return ast, err
}
