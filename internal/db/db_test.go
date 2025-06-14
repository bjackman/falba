package db_test

import (
	"testing"

	"github.com/bjackman/falba/internal/db"
	"github.com/bjackman/falba/internal/falba"
	"github.com/bjackman/falba/internal/parser"
	"github.com/bjackman/falba/internal/test"
	"github.com/google/go-cmp/cmp"
)

func TestReadDB(t *testing.T) {
	db, err := db.ReadDB("testdata/results", []parser.Parser{test.MustNewRegexpParser(t, `\d+`, "my-metric", falba.ValueInt)})
	if err != nil {
		t.Fatalf("Failed to read DB: %v", err)
	}
	wantResults := []*falba.Result{
		{
			TestName: "my_test",
			ResultID: "1514e610de1e",
			Artifacts: []*falba.Artifact{
				{
					Path: test.MustFilepathAbs(t, "testdata/results/my_test:1514e610de1e/artifacts/my_artifact"),
				},
			},
			Metrics: []*falba.Metric{
				{
					Name:  "my-metric",
					Value: &falba.IntValue{Value: 1},
				},
			},
			Facts: map[string]falba.Value{},
		},
	}
	if diff := cmp.Diff(db.Results, wantResults); diff != "" {
		t.Errorf("Unexpected results when reading DB: %v", diff)
	}
}
