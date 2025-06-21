package parser_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bjackman/falba/internal/falba"
	"github.com/bjackman/falba/internal/parser"
	"github.com/bjackman/falba/internal/test"
	"github.com/google/go-cmp/cmp"
)

func fakeArtifact(t *testing.T, content string) *falba.Artifact {
	path := filepath.Join(t.TempDir(), "artifact")
	err := os.WriteFile(path, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Setting up fake artifact: %v", err)
	}
	return &falba.Artifact{Name: "artifact", Path: path}
}

func TestParser(t *testing.T) {
	// Invalid configurations
	for _, pattern := range []string{
		// Only one match group is allowed.
		"(foo)(bar)",
	} {
		e, err := parser.NewRegexpExtractor(pattern, falba.ValueInt)
		if err == nil {
			t.Errorf("Wanted error for regexp pattern %q, got %v", pattern, e)
		}
	}

	// Parse failures
	for _, tc := range []struct {
		desc    string
		content string
		parser  *parser.Parser
	}{
		{
			desc:    "empty content",
			content: "",
			parser:  test.MustNewRegexpParser(t, ".+", "my-metric", falba.ValueInt),
		},
		{
			desc:    "not int",
			content: "foo",
			parser:  test.MustNewRegexpParser(t, ".+", "my-metric", falba.ValueInt),
		},
		{
			desc:    "float not int",
			content: "1.0",
			parser:  test.MustNewRegexpParser(t, ".+", "my-metric", falba.ValueInt),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			result, err := tc.parser.Parse(fakeArtifact(t, tc.content))
			if err == nil {
				t.Errorf("Expected error, got none, result = %v", result)
			} else if !errors.Is(err, parser.ErrParseFailure) {
				t.Errorf("Expected ErrParseFailure, got %v", err)
			}
		})
	}

	// Happy paths.
	for _, tc := range []struct {
		desc    string
		content string
		parser  *parser.Parser
		want    *falba.Metric
	}{
		{
			desc:    "int",
			content: "1",
			parser:  test.MustNewRegexpParser(t, ".+", "my-metric", falba.ValueInt),
			want:    &falba.Metric{Name: "my-metric", Value: &falba.IntValue{Value: 1}},
		},
		{
			desc:    "int group",
			content: "foo 1",
			parser:  test.MustNewRegexpParser(t, "foo (\\d+)", "my-metric", falba.ValueInt),
			want:    &falba.Metric{Name: "my-metric", Value: &falba.IntValue{Value: 1}},
		},
		{
			desc:    "float int",
			content: "1",
			parser:  test.MustNewRegexpParser(t, ".+", "my-metric", falba.ValueFloat),
			want:    &falba.Metric{Name: "my-metric", Value: &falba.FloatValue{Value: 1.0}},
		},
		{
			desc:    "float",
			content: "1.0",
			parser:  test.MustNewRegexpParser(t, ".+", "my-metric", falba.ValueFloat),
			want:    &falba.Metric{Name: "my-metric", Value: &falba.FloatValue{Value: 1.0}},
		},
		{
			desc:    "string",
			content: "yerp",
			parser:  test.MustNewRegexpParser(t, ".+", "my-metric", falba.ValueString),
			want:    &falba.Metric{Name: "my-metric", Value: &falba.StringValue{Value: "yerp"}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			result, err := tc.parser.Parse(fakeArtifact(t, tc.content))
			if err != nil {
				t.Fatalf("Unexpected failure to parse: %v", err)
			}
			if len(result.Facts) != 0 {
				t.Errorf("Unexpected Facts: %v", result.Facts)
			}
			if diff := cmp.Diff(result.Metrics, []*falba.Metric{tc.want}); diff != "" {
				t.Errorf("Unexpected Metrics, diff: %v", diff)
			}
		})
	}
}

func TestReservedFactNamesRejected(t *testing.T) {
	testCases := []struct {
		name         string
		factName     string
		expectError  bool
	}{
		{"test_name reserved", "test_name", true},
		{"result_id reserved", "result_id", true},
		{"valid fact name", "my_fact", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := `{
				"type": "single_metric",
				"artifact_regexp": "test_artifact",
				"fact": {
					"name": "` + tc.factName + `",
					"type": "string"
				}
			}`

			_, err := parser.FromConfig([]byte(config), "test_parser")

			if tc.expectError {
				if err == nil {
					t.Fatalf("Expected error for reserved fact name %q, but got none", tc.factName)
				}
				if !strings.Contains(err.Error(), "reserved") {
					t.Errorf("Expected error about reserved fact name, got: %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error for valid fact name %q: %v", tc.factName, err)
				}
			}
		})
	}
}
