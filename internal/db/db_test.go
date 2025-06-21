package db_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
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

// This test was written by Google Jules.
func TestReadDB_DuplicateFactInResult(t *testing.T) {
	tempDir := t.TempDir()
	parsersFileContent := `{
		"parsers": {
			"parser_file1": {
				"type": "single_metric",
				"artifact_regexp": "file1\\.txt",
				"fact": {"name": "duplicate_fact", "type": "string"}
			},
			"parser_file2": {
				"type": "single_metric",
				"artifact_regexp": "file2\\.txt",
				"fact": {"name": "duplicate_fact", "type": "string"}
			}
		}
	}`
	if err := os.WriteFile(filepath.Join(tempDir, "parsers.json"), []byte(parsersFileContent), 0644); err != nil {
		t.Fatalf("Failed to write parsers.json: %v", err)
	}

	resultDir := filepath.Join(tempDir, "test_result:dup123")
	artifactsDir := filepath.Join(resultDir, "artifacts")
	if err := os.MkdirAll(artifactsDir, 0755); err != nil {
		t.Fatalf("Failed to create artifacts dir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(artifactsDir, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatalf("Failed to write file1.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(artifactsDir, "file2.txt"), []byte("content2"), 0644); err != nil {
		t.Fatalf("Failed to write file2.txt: %v", err)
	}

	_, err := db.ReadDB(tempDir)
	if err == nil {
		t.Fatalf("Expected ReadDB to return an error for duplicate fact production, but got nil")
	}

	wantMsg := `produced fact "duplicate_fact", but that was already produced by parser parser_file1`
	if !strings.Contains(err.Error(), wantMsg) {
		t.Errorf("Expected error to contain %q, got: %v", wantMsg, err)
	}
}

// This test was written by Google Jules.
func TestReadDB_MissingArtifactsDir(t *testing.T) {
	tempDir := t.TempDir()
	parsersFileContent := `{
		"parsers": {
			"parser1": {
				"type": "single_metric",
				"artifact_regexp": ".*",
				"fact": {"name": "dummy", "type": "string"}
			}
		}
	}`
	if err := os.WriteFile(filepath.Join(tempDir, "parsers.json"), []byte(parsersFileContent), 0644); err != nil {
		t.Fatalf("Failed to write parsers.json: %v", err)
	}

	resultDir := filepath.Join(tempDir, "test:123")
	if err := os.Mkdir(resultDir, 0755); err != nil {
		t.Fatalf("Failed to create result dir: %v", err)
	}
	// No artifacts/ directory is created here

	_, err := db.ReadDB(tempDir)
	if err == nil {
		t.Fatalf("Expected ReadDB to return an error when artifacts/ dir is missing, but got nil")
	}

	if !strings.Contains(err.Error(), "no such file or directory") {
		t.Errorf("Expected error to contain 'no such file or directory', got: %v", err)
	}
}

// This test was written by Google Jules.
func TestReadDB_UnknownFieldsInParsersFile(t *testing.T) {
	tempDir := t.TempDir()
	parsersFileContent := `{
		"parsers": {},
		"unknown_field": "some_value"
	}`
	if err := os.WriteFile(filepath.Join(tempDir, "parsers.json"), []byte(parsersFileContent), 0644); err != nil {
		t.Fatalf("Failed to write parsers.json: %v", err)
	}

	_, err := db.ReadDB(tempDir)
	if err == nil {
		t.Fatalf("Expected ReadDB to return an error for unknown fields in parsers.json, but got nil")
	}
	if !strings.Contains(err.Error(), "unknown field") {
		t.Errorf("Expected error to mention 'unknown field', got: %v", err)
	}
}

// This test was written by Google Jules.
func TestReadDB_InvalidJSONParsersFile(t *testing.T) {
	tempDir := t.TempDir()
	parsersFileContent := `{"parsers": {` // Invalid JSON
	if err := os.WriteFile(filepath.Join(tempDir, "parsers.json"), []byte(parsersFileContent), 0644); err != nil {
		t.Fatalf("Failed to write parsers.json: %v", err)
	}

	_, err := db.ReadDB(tempDir)
	if err == nil {
		t.Fatalf("Expected ReadDB to return an error for invalid JSON, but got nil")
	}
	if !strings.Contains(err.Error(), "decoding DB config") {
		t.Errorf("Expected error to mention 'decoding DB config', got: %v", err)
	}
}

// This test was written by Google Jules.
func TestReadDB_EmptyParsersMap(t *testing.T) {
	tempDir := t.TempDir()
	parsersFileContent := `{"parsers": {}}`
	if err := os.WriteFile(filepath.Join(tempDir, "parsers.json"), []byte(parsersFileContent), 0644); err != nil {
		t.Fatalf("Failed to write parsers.json: %v", err)
	}

	_, err := db.ReadDB(tempDir)
	if err == nil {
		t.Fatalf("Expected ReadDB to return an error for empty parsers map, but got nil")
	}
	if !strings.Contains(err.Error(), "no 'parsers' defined") {
		t.Errorf("Expected error to mention 'no 'parsers' defined', got: %v", err)
	}
}

// This test was written by Google Jules.
func TestReadDB_MissingParsersFile(t *testing.T) {
	tempDir := t.TempDir()
	// No parsers.json created

	_, err := db.ReadDB(tempDir)
	if err == nil {
		t.Fatalf("Expected ReadDB to return an error when parsers.json is missing, but got nil")
	}
	if !strings.Contains(err.Error(), "reading DB config from") || !strings.Contains(err.Error(), "parsers.json") {
		t.Errorf("Expected error to mention missing parsers.json, got: %v", err)
	}
}

// This test was written by Google Jules.
func TestReadDB_ConflictingTypes(t *testing.T) {
	tempDir := t.TempDir()
	parsersFileContent := `{
		"parsers": {
			"parser1": {
				"type": "jsonpath",
				"artifact_regexp": ".*\\.json",
				"metric": {
					"name": "shared_name",
					"type": "int"
				},
				"jsonpath": "$.metric_value"
			},
			"parser2": {
				"type": "single_metric",
				"artifact_regexp": ".*\\.txt",
				"fact": {
					"name": "shared_name",
					"type": "string"
				}
			}
		}
	}`
	if err := os.WriteFile(filepath.Join(tempDir, "parsers.json"), []byte(parsersFileContent), 0644); err != nil {
		t.Fatalf("Failed to write parsers.json: %v", err)
	}

	dummyResultDir := filepath.Join(tempDir, "test_result:123")
	if err := os.Mkdir(dummyResultDir, 0755); err != nil {
		t.Fatalf("Failed to create dummy result dir: %v", err)
	}
	dummyArtifactsDir := filepath.Join(dummyResultDir, "artifacts")
	if err := os.Mkdir(dummyArtifactsDir, 0755); err != nil {
		t.Fatalf("Failed to create dummy artifacts dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dummyArtifactsDir, "data.json"), []byte(`{"metric_value": 10}`), 0644); err != nil {
		t.Fatalf("Failed to write dummy artifact data.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dummyArtifactsDir, "raw_data.txt"), []byte(`some string`), 0644); err != nil {
		t.Fatalf("Failed to write dummy artifact raw_data.txt: %v", err)
	}

	_, err := db.ReadDB(tempDir)
	if err == nil {
		t.Errorf("Expected ReadDB to return an error due to conflicting types, but got nil")
	} else {
		if !strings.Contains(err.Error(), "produced fact/metric \"shared_name\" of type") || !strings.Contains(err.Error(), "but another outputs this as") {
			t.Errorf("Expected error to contain type mismatch for 'shared_name', got: %v", err)
		}
	}
}

// This test was written by Google Jules.
func TestReadDB_InvalidResultDirName(t *testing.T) {
	tempDir := t.TempDir()
	parsersFileContent := `{
		"parsers": {
			"parser1": {
				"type": "single_metric",
				"artifact_regexp": ".*",
				"fact": {"name": "dummy", "type": "string"}
			}
		}
	}`
	if err := os.WriteFile(filepath.Join(tempDir, "parsers.json"), []byte(parsersFileContent), 0644); err != nil {
		t.Fatalf("Failed to write parsers.json: %v", err)
	}

	testCases := []struct {
		name          string
		dirName       string
		expectedError string
	}{
		{
			name:          "missing colon",
			dirName:       "testnameresultid",
			expectedError: "invalid result name",
		},
		{
			name:          "empty test name",
			dirName:       ":resultid",
			expectedError: "invalid result name",
		},
		{
			name:          "empty result id",
			dirName:       "testname:",
			expectedError: "invalid result name",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resultDir := filepath.Join(tempDir, tc.dirName)
			if err := os.Mkdir(resultDir, 0755); err != nil {
				t.Fatalf("Failed to create result dir %s: %v", tc.dirName, err)
			}
			if err := os.Mkdir(filepath.Join(resultDir, "artifacts"), 0755); err != nil {
				t.Fatalf("Failed to create artifacts dir in %s: %v", resultDir, err)
			}

			_, err := db.ReadDB(tempDir)
			if err == nil {
				t.Fatalf("Expected ReadDB to return an error for dir %s, but got nil", tc.dirName)
			}
			if !strings.Contains(err.Error(), tc.expectedError) {
				t.Errorf("Expected error for dir %s to contain '%s', got: %v", tc.dirName, tc.expectedError, err)
			}
			if err := os.RemoveAll(resultDir); err != nil {
				t.Logf("Warning: failed to remove result dir %s: %v", resultDir, err)
			}
		})
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

	// Test core result columns
	basicRows, err := sqlDB.Query("SELECT test_name, result_id FROM results ORDER BY test_name")
	if err != nil {
		t.Fatalf("Failed to query basic results: %v", err)
	}
	defer basicRows.Close()

	var gotBasicResults []struct {
		TestName string
		ResultID string
	}
	for basicRows.Next() {
		var testName, resultID string
		if err := basicRows.Scan(&testName, &resultID); err != nil {
			t.Fatalf("Failed to scan basic result row: %v", err)
		}
		gotBasicResults = append(gotBasicResults, struct {
			TestName string
			ResultID string
		}{testName, resultID})
	}

	expectedBasicResults := []struct {
		TestName string
		ResultID string
	}{
		{"test1", "result1"},
		{"test2", "result2"},
	}

	if diff := cmp.Diff(gotBasicResults, expectedBasicResults); diff != "" {
		t.Errorf("Unexpected basic results (-got +want): %v", diff)
	}

	// Test fact columns
	factRows, err := sqlDB.Query("SELECT test_name, fact1, fact2, fact3 FROM results ORDER BY test_name")
	if err != nil {
		t.Fatalf("Failed to query fact results: %v", err)
	}
	defer factRows.Close()

	var gotFactResults []struct {
		TestName string
		Fact1    sql.NullString
		Fact2    sql.NullInt64
		Fact3    sql.NullString
	}
	for factRows.Next() {
		var testName string
		var fact1, fact3 sql.NullString
		var fact2 sql.NullInt64
		if err := factRows.Scan(&testName, &fact1, &fact2, &fact3); err != nil {
			t.Fatalf("Failed to scan fact row: %v", err)
		}
		gotFactResults = append(gotFactResults, struct {
			TestName string
			Fact1    sql.NullString
			Fact2    sql.NullInt64
			Fact3    sql.NullString
		}{testName, fact1, fact2, fact3})
	}

	expectedFactResults := []struct {
		TestName string
		Fact1    sql.NullString
		Fact2    sql.NullInt64
		Fact3    sql.NullString
	}{
		{"test1", sql.NullString{Valid: true, String: "value1"}, sql.NullInt64{Valid: true, Int64: 42}, sql.NullString{}},
		{"test2", sql.NullString{}, sql.NullInt64{}, sql.NullString{Valid: true, String: "true"}},
	}

	if diff := cmp.Diff(gotFactResults, expectedFactResults); diff != "" {
		t.Errorf("Unexpected fact results (-got +want): %v", diff)
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
