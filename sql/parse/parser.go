package parse

import (
	"fmt"
	pc_parser "github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

func NewParser() Parser {
	return &parser{pc_parser.New()}
}

type Parser interface {
	Parse(sql string) (stmt ast.StmtNode, err error)
}

type parser struct {
	parser *pc_parser.Parser
}


func (p *parser) Parse(sql string) (stmt ast.StmtNode, err error) {
	stmtNodes, warns, err := p.parser.Parse(sql, charset.CharsetUTF8, "")

	if err != nil {
		return nil, err
	}

	if warns != nil {
		for _, warn := range warns {
			println(warn)
		}
	}

	if len(stmtNodes) != 1 {
		return nil, fmt.Errorf("Expected 1 statement got %d", len(stmtNodes))
	}

	return stmtNodes[0], nil
}
