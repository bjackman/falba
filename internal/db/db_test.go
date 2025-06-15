package db_test

import (
	"testing"

	"github.com/bjackman/falba/internal/db"
	"github.com/bjackman/falba/internal/falba"
	"github.com/bjackman/falba/internal/test"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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
			Facts: map[string]falba.Value{},
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
