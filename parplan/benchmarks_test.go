package parplan

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func BenchmarkParser(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parser := NewParser()
		stmt, err := parser.Parse("select col0, col1, col2 from table1 where col1 = 123456 order by col0, col1")
		require.NoError(b, err)
		require.NotNil(b, stmt)
	}
}

func BenchmarkCreatePlanner(b *testing.B) {
	schema := createTestSchema()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		planner := NewPlanner(schema)
		require.NotNil(b, planner)
	}
}

func BenchmarkPreprocess(b *testing.B) {
	schema := createTestSchema()
	planner := NewPlanner(schema)
	ast, err := planner.parser.Parse("select col0, col1, col2 from table1 where col1 = 123456 order by col0, col1")
	require.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = planner.preprocess(ast.stmt, false)
		require.NoError(b, err)
	}
}

func BenchmarkLogicalPlan(b *testing.B) {
	schema := createTestSchema()
	planner := NewPlanner(schema)
	ast, err := planner.parser.Parse("select col0, col1, col2 from table1 where col1 = 123456 order by col0, col1")
	require.NoError(b, err)
	err = planner.preprocess(ast.stmt, false)
	require.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logicalPlan, err := planner.BuildLogicalPlan(ast, false)
		require.NoError(b, err)
		require.NotNil(b, logicalPlan)
	}
}

func BenchmarkPhysicalPlan(b *testing.B) {
	schema := createTestSchema()
	planner := NewPlanner(schema)
	ast, err := planner.parser.Parse("select col0, col1, col2 from table1 where col1 = 123456 order by col0, col1")
	require.NoError(b, err)
	err = planner.preprocess(ast.stmt, false)
	require.NoError(b, err)
	logicalPlan, err := planner.BuildLogicalPlan(ast, false)
	require.NoError(b, err)
	require.NotNil(b, logicalPlan)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		physicalPlan, err := planner.BuildPhysicalPlan(logicalPlan, true)
		require.NoError(b, err)
		require.NotNil(b, physicalPlan)
	}
}

func BenchmarkFullProcess(b *testing.B) {
	schema := createTestSchema()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		planner := NewPlanner(schema)
		ast, err := planner.parser.Parse("select col0, col1, col2 from table1 where col1 = 123456 order by col0, col1")
		require.NoError(b, err)
		err = planner.preprocess(ast.stmt, false)
		require.NoError(b, err)
		logicalPlan, err := planner.BuildLogicalPlan(ast, false)
		require.NoError(b, err)
		require.NotNil(b, logicalPlan)
		physicalPlan, err := planner.BuildPhysicalPlan(logicalPlan, true)
		require.NoError(b, err)
		require.NotNil(b, physicalPlan)
	}
}
