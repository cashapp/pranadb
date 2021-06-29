package parplan

import (
	"testing"
)

func TestParse(t *testing.T) {

	parser := newParser()

	parser.Parse("select t1.col1, t1.col2, t2.col3 from table1 t1 inner join table2 t2 on t1.col1 = t2.col3 order by t1.col1")

}
