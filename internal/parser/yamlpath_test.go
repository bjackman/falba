package parser

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bjackman/falba/internal/falba"
	"github.com/google/go-cmp/cmp"
)

func fakeYamlArtifact(t *testing.T, content string) *falba.Artifact {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "artifact.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write artifact: %v", err)
	}
	return &falba.Artifact{Name: "artifact.yaml", Path: path}
}

func TestYAMLPathExtractor(t *testing.T) {
	cases := []struct {
		name       string
		expression string
		yaml       string
		valType    falba.ValueType
		want       []falba.Value
		wantErr    bool
	}{
		{
			name:       "simple string",
			expression: "$.foo",
			yaml:       "foo: bar",
			valType:    falba.ValueString,
			want:       []falba.Value{&falba.StringValue{Value: "bar"}},
		},
		{
			name:       "simple int",
			expression: "$.foo",
			yaml:       "foo: 42",
			valType:    falba.ValueInt,
			want:       []falba.Value{&falba.IntValue{Value: 42}},
		},
		{
			name:       "simple float",
			expression: "$.foo",
			yaml:       "foo: 42.5",
			valType:    falba.ValueFloat,
			want:       []falba.Value{&falba.FloatValue{Value: 42.5}},
		},
		{
			name:       "simple bool",
			expression: "$.foo",
			yaml:       "foo: true",
			valType:    falba.ValueBool,
			want:       []falba.Value{&falba.BoolValue{Value: true}},
		},
		{
			name:       "array extraction string",
			expression: "$.foo[*]",
			yaml:       "foo:\n  - a\n  - b",
			valType:    falba.ValueString,
			want:       []falba.Value{&falba.StringValue{Value: "a"}, &falba.StringValue{Value: "b"}},
		},
		{
			name:       "array extraction int",
			expression: "$.foo[*]",
			yaml:       "foo:\n  - 1\n  - 2",
			valType:    falba.ValueInt,
			want:       []falba.Value{&falba.IntValue{Value: 1}, &falba.IntValue{Value: 2}},
		},
		{
			name:       "nested object",
			expression: "$.foo.bar.baz",
			yaml:       "foo:\n  bar:\n    baz: hello",
			valType:    falba.ValueString,
			want:       []falba.Value{&falba.StringValue{Value: "hello"}},
		},
		{
			name:       "wrong type float to string",
			expression: "$.foo",
			yaml:       "foo: 42.5",
			valType:    falba.ValueString,
			wantErr:    true,
		},
		{
			name:       "wrong type string to int",
			expression: "$.foo",
			yaml:       "foo: bar",
			valType:    falba.ValueInt,
			wantErr:    true,
		},
		{
			name:       "missing key",
			expression: "$.bar",
			yaml:       "foo: 42",
			valType:    falba.ValueInt,
			wantErr:    true,
		},
		{
			name:       "invalid expression",
			expression: "$.[",
			yaml:       "foo: 42",
			valType:    falba.ValueInt,
			wantErr:    true, // Fails during Get
		},
		{
			name:       "invalid yaml",
			expression: "$.foo",
			yaml:       "foo: [",
			valType:    falba.ValueInt,
			wantErr:    true, // Fails during Unmarshal
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			extractor, err := NewYAMLPathExtractor(tc.expression, tc.valType)
			if err != nil {
				t.Fatalf("Failed to create extractor: %v", err)
			}

			artifact := fakeYamlArtifact(t, tc.yaml)
			got, err := extractor.Extract(artifact)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("Extract() got no error, wanted error. Result was %v", got)
				}
				return
			}

			if err != nil {
				t.Fatalf("Extract() got unexpected error: %v", err)
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Extract() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestYAMLPathExtractorString(t *testing.T) {
	extractor, _ := NewYAMLPathExtractor("$.foo", falba.ValueString)
	got := extractor.String()
	want := fmt.Sprintf("YAMLPathParser{%q -> %v}", "$.foo", falba.ValueString)
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}
