package parser

import (
	pc_parser "github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

func NewParser() *Parser {
	return &Parser{pc_parser.New()}
}

type Parser struct {
	parser *pc_parser.Parser
}


func (p *Parser) Parse(sql string) error {
	stmtNodes, _, err := p.parser.Parse(sql, charset.CharsetUTF8, "")

	if err != nil {
		return err
	}

	for _, stmtNode := range stmtNodes {
		println("Stmt node: " + stmtNode.Text())
	}

	return nil
}

func (p *Parser) BuildLogicalPlan(stmtNode ast.StmtNode) error {

	//core.PlanBuilder.NewPlanBuilder :=

	return nil
}
