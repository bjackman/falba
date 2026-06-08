package anal_test

import (
	"database/sql"
	"testing"

	"github.com/bjackman/falba/internal/anal"
	"github.com/bjackman/falba/internal/db"
	"github.com/bjackman/falba/internal/falba"
	"github.com/google/go-cmp/cmp"
	_ "github.com/marcboeker/go-duckdb"
)

func TestGroupByFact_NullFact(t *testing.T) {
	sqlDB, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open DuckDB: %v", err)
	}
	defer sqlDB.Close()

	// We need to construct a db.DB that has some results where the fact we
	// group by is missing (thus NULL in the database).
	falbaDB := &db.DB{
		RootDir: "dummy",
		Results: map[string]*falba.Result{
			"r1": {
				TestName: "test1",
				ResultID: "r1",
				Facts: map[string]falba.Value{
					"my_fact": &falba.StringValue{Value: "value1"},
				},
				Metrics: []*falba.Metric{
					{Name: "my_metric", Value: &falba.IntValue{Value: 10}},
				},
			},
			"r2": {
				TestName: "test1",
				ResultID: "r2",
				Facts:    map[string]falba.Value{
					// my_fact is missing here
				},
				Metrics: []*falba.Metric{
					{Name: "my_metric", Value: &falba.IntValue{Value: 20}},
				},
			},
		},
		FactTypes: map[string]falba.ValueType{
			"my_fact": falba.ValueString,
		},
		MetricTypes: map[string]falba.MetricType{
			"my_metric": {Type: falba.ValueInt},
		},
	}

	err = falbaDB.InsertIntoDuckDB(sqlDB)
	if err != nil {
		t.Fatalf("Failed to insert into DuckDB: %v", err)
	}

	// Call GroupByFact. It should not fail now that we support NULLs.
	groups, err := anal.GroupByFact(sqlDB, falbaDB, "my_fact", "my_metric", "TRUE", 0, nil)
	if err != nil {
		t.Fatalf("GroupByFact failed: %v", err)
	}

	// We expect two groups: "value1" and "<NULL>".
	wantGroups := map[string]*anal.MetricGroup{
		"value1": {
			TestName: "test1",
			Mean:     10,
			Min:      10,
			Max:      10,
		},
		"<NULL>": {
			TestName: "test1",
			Mean:     20,
			Min:      20,
			Max:      20,
		},
	}

	if diff := cmp.Diff(wantGroups, groups, cmp.AllowUnexported(anal.Histogram{})); diff != "" {
		t.Errorf("Unexpected groups (-want +got):\n%s", diff)
	}
}
