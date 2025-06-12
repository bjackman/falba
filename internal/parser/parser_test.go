package parser

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bjackman/falba/internal/falba"
	"github.com/google/go-cmp/cmp"
)

func mustNewRegexpParser(t *testing.T, pattern string, metricName string, metricType falba.ValueType) *RegexpParser {
	p, err := NewRegexpParser(pattern, metricName, metricType)
	if err != nil {
		t.Fatalf("Failed to construct parser: %v", err)
	}
	return p
}

func fakeArtifact(t *testing.T, content string) *falba.Artifact {
	path := filepath.Join(t.TempDir(), "artifact")
	err := os.WriteFile(path, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Setting up fake artifact: %v", err)
	}
	return &falba.Artifact{Path: path}
}

func TestParser(t *testing.T) {
	// Invalid configurations
	for _, pattern := range []string{
		// Only one match group is allowed.
		"(foo)(bar)",
	} {
		p, err := NewRegexpParser(pattern, "name", falba.ValueInt)
		if err == nil {
			t.Errorf("Wanted error for regexp pattern %q, got %v", pattern, p)
		}
	}

	// Parse failures
	for _, tc := range []struct {
		desc    string
		content string
		parser  *RegexpParser
	}{
		{
			desc:    "empty content",
			content: "",
			parser:  mustNewRegexpParser(t, ".+", "my-metric", falba.ValueInt),
		},
		{
			desc:    "not int",
			content: "foo",
			parser:  mustNewRegexpParser(t, ".+", "my-metric", falba.ValueInt),
		},
		{
			desc:    "float not int",
			content: "1.0",
			parser:  mustNewRegexpParser(t, ".+", "my-metric", falba.ValueInt),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			result, err := tc.parser.Parse(fakeArtifact(t, tc.content))
			if err == nil {
				t.Errorf("Expected error, got none, result = %v", result)
			} else if !errors.Is(err, ErrParseFailure) {
				t.Errorf("Expected ErrParseFailure, got %v", err)
			}
		})
	}

	// Happy paths.
	for _, tc := range []struct {
		desc    string
		content string
		parser  *RegexpParser
		want    *falba.Metric
	}{
		{
			desc:    "int",
			content: "1",
			parser:  mustNewRegexpParser(t, ".+", "my-metric", falba.ValueInt),
			want:    &falba.Metric{Name: "my-metric", Value: &falba.IntValue{Value: 1}},
		},
		{
			desc:    "int group",
			content: "foo 1",
			parser:  mustNewRegexpParser(t, "foo (\\d+)", "my-metric", falba.ValueInt),
			want:    &falba.Metric{Name: "my-metric", Value: &falba.IntValue{Value: 1}},
		},
		{
			desc:    "float int",
			content: "1",
			parser:  mustNewRegexpParser(t, ".+", "my-metric", falba.ValueFloat),
			want:    &falba.Metric{Name: "my-metric", Value: &falba.FloatValue{Value: 1.0}},
		},
		{
			desc:    "float",
			content: "1.0",
			parser:  mustNewRegexpParser(t, ".+", "my-metric", falba.ValueFloat),
			want:    &falba.Metric{Name: "my-metric", Value: &falba.FloatValue{Value: 1.0}},
		},
		{
			desc:    "string",
			content: "yerp",
			parser:  mustNewRegexpParser(t, ".+", "my-metric", falba.ValueString),
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
