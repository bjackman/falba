package db_test

import (
	"database/sql"
	"testing"

	"github.com/bjackman/falba/internal/db"
	"github.com/bjackman/falba/internal/falba"
	"github.com/bjackman/falba/internal/test"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	_ "github.com/marcboeker/go-duckdb"
)

func TestReadDB(t *testing.T) {
	db, err := db.ReadDB("testdata/results")
	if err != nil {
		t.Fatalf("Failed to read DB: %v", err)
	}
	wantResults := []*falba.Result{
		{
			TestName: "my_test",
			ResultID: "1514e610de1e",
			Artifacts: []*falba.Artifact{
				{
					Name: "my_raw_fact",
					Path: test.MustFilepathAbs(t, "testdata/results/my_test:1514e610de1e/artifacts/my_raw_fact"),
				},
				{
					Name: "my_raw_int",
					Path: test.MustFilepathAbs(t, "testdata/results/my_test:1514e610de1e/artifacts/my_raw_int"),
				},
				{
					Name: "my_artifact.json",
					Path: test.MustFilepathAbs(t, "testdata/results/my_test:1514e610de1e/artifacts/my_artifact.json"),
				},
			},
			Metrics: []*falba.Metric{
				{
					Name:  "my_raw_int",
					Value: &falba.IntValue{Value: 1},
				},
				{
					Name:  "my_json_int",
					Value: &falba.IntValue{Value: 1},
				},
				{
					Name:  "my_json_string",
					Value: &falba.StringValue{Value: "foo"},
				},
				{
					Name:  "my_json_float",
					Value: &falba.FloatValue{Value: 2.0},
				},
			},
			Facts: map[string]falba.Value{
				"my_json_fact": &falba.StringValue{Value: "foo"},
				"my_raw_fact":  &falba.StringValue{Value: "GDAY MATE"},
			},
		},
	}

	// Sort stuff so we can compare slices of them without caring about order.
	artifactLess := func(a, b *falba.Artifact) bool {
		return a.Name < b.Name
	}
	metricLess := func(a, b *falba.Metric) bool {
		return a.Name < b.Name
	}
	ignoreOrder := []cmp.Option{
		cmpopts.SortSlices(artifactLess),
		cmpopts.SortSlices(metricLess),
	}

	if diff := cmp.Diff(db.Results, wantResults, ignoreOrder...); diff != "" {
		t.Errorf("Unexpected results when reading DB (-got +want): %v", diff)
	}
}

// This test was written by Claude Code.
func TestInsertIntoDuckDB(t *testing.T) {
	sqlDB, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open DuckDB: %v", err)
	}
	defer sqlDB.Close()

	db := &db.DB{
		RootDir: "testdata/results",
		Results: []*falba.Result{
			{
				TestName: "test1",
				ResultID: "result1",
				Facts: map[string]falba.Value{
					"fact1": &falba.StringValue{Value: "value1"},
					"fact2": &falba.IntValue{Value: 42},
				},
				Metrics: []*falba.Metric{
					{Name: "metric1", Value: &falba.FloatValue{Value: 3.14}},
					{Name: "metric2", Value: &falba.StringValue{Value: "test"}},
				},
			},
			{
				TestName: "test2",
				ResultID: "result2",
				Facts: map[string]falba.Value{
					"fact3": &falba.StringValue{Value: "true"},
				},
				Metrics: []*falba.Metric{
					{Name: "metric3", Value: &falba.IntValue{Value: 100}},
				},
			},
		},
		FactTypes: map[string]falba.ValueType{
			"fact1": falba.ValueString,
			"fact2": falba.ValueInt,
			"fact3": falba.ValueString,
		},
	}

	err = db.InsertIntoDuckDB(sqlDB)
	if err != nil {
		t.Fatalf("Failed to insert into DuckDB: %v", err)
	}

	rows, err := sqlDB.Query("SELECT test_name, result_id FROM results ORDER BY test_name")
	if err != nil {
		t.Fatalf("Failed to query results: %v", err)
	}
	defer rows.Close()

	var gotResults []struct {
		TestName string
		ResultID string
	}
	for rows.Next() {
		var testName, resultID string
		if err := rows.Scan(&testName, &resultID); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		gotResults = append(gotResults, struct {
			TestName string
			ResultID string
		}{testName, resultID})
	}

	expectedResults := []struct {
		TestName string
		ResultID string
	}{
		{"test1", "result1"},
		{"test2", "result2"},
	}

	if diff := cmp.Diff(gotResults, expectedResults); diff != "" {
		t.Errorf("Unexpected results (-got +want): %v", diff)
	}

	metricsRows, err := sqlDB.Query("SELECT result_id, metric, int_value, float_value, string_value FROM metrics ORDER BY metric")
	if err != nil {
		t.Fatalf("Failed to query metrics: %v", err)
	}
	defer metricsRows.Close()

	var gotMetrics []struct {
		ResultID string
		Metric   string
		Value    interface{}
	}
	for metricsRows.Next() {
		var resultID, metric string
		var intValue sql.NullInt64
		var floatValue sql.NullFloat64
		var stringValue sql.NullString
		if err := metricsRows.Scan(&resultID, &metric, &intValue, &floatValue, &stringValue); err != nil {
			t.Fatalf("Failed to scan metrics row: %v", err)
		}

		var value interface{}
		if intValue.Valid {
			value = intValue.Int64
		} else if floatValue.Valid {
			value = floatValue.Float64
		} else if stringValue.Valid {
			value = stringValue.String
		}

		gotMetrics = append(gotMetrics, struct {
			ResultID string
			Metric   string
			Value    interface{}
		}{resultID, metric, value})
	}

	expectedMetrics := []struct {
		ResultID string
		Metric   string
		Value    interface{}
	}{
		{"result1", "metric1", 3.14},
		{"result1", "metric2", "test"},
		{"result2", "metric3", int64(100)},
	}

	if diff := cmp.Diff(gotMetrics, expectedMetrics); diff != "" {
		t.Errorf("Unexpected metrics (-got +want): %v", diff)
	}
}
