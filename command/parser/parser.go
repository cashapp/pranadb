package parser

import (
	"github.com/squareup/pranadb/perrors"
	"regexp"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer/stateful"
)

var (
	lex = stateful.MustSimple([]stateful.Rule{
		{`Ident`, "((?i)[a-zA-Z_][a-zA-Z_0-9]*)|`[^`]*`", nil},
		{`Number`, `[-+]?\d*\.?\d+([eE][-+]?\d+)?`, nil},
		{`String`, `'[^']*'|"[^"]*"`, nil},
		{`Punct`, `<>|!=|<=|>=|[-+*/%,.()=<>;]`, nil},
		{`Whitespace`, `\s+`, nil},
	})
	parser = participle.MustBuild(&AST{},
		participle.Lexer(lex),
		participle.CaseInsensitive("Ident"),
		participle.Elide("Whitespace"),
		participle.UseLookahead(2),
		// TODO(aat): There's a bug in Participle that prevents us from using this yet:
		//  any mapping function that mutates a token results in the mutated token being
		//  captured into the `Tokens []lexer.Token` field, while we want the original token.

		// participle.Map(func(token lexer.Token) (lexer.Token, error) {
		// 	if strings.HasPrefix(token.Value, "`") {
		// 		token.Value = token.Value[1 : len(token.Value)-1]
		// 	}
		// 	return token, nil
		// }, "Ident"),
		participle.Unquote("String"),
	)
	selectPrefix  = regexp.MustCompile(`(?i)^select\s+`)
	preparePrefix = regexp.MustCompile(`(?i)^prepare\s+`)
)

// Parse an SQL statement.
func Parse(sql string) (*AST, error) {
	if selectPrefix.MatchString(sql) {
		return &AST{Select: sql}, nil
	}
	if preparePrefix.MatchString(sql) {
		return &AST{Prepare: sql}, nil
	}
	ast := &AST{}
	err := parser.ParseString("", sql, ast)
	return ast, perrors.MaybeAddStack(err)
}
