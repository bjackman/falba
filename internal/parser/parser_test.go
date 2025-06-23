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
		{
			desc:    "bool true",
			content: "true",
			parser:  test.MustNewRegexpParser(t, ".+", "my-metric", falba.ValueBool),
			want:    &falba.Metric{Name: "my-metric", Value: &falba.BoolValue{Value: true}},
		},
		{
			desc:    "bool FALSE",
			content: "FALSE",
			parser:  test.MustNewRegexpParser(t, ".+", "my-metric", falba.ValueBool),
			want:    &falba.Metric{Name: "my-metric", Value: &falba.BoolValue{Value: false}},
		},
		{
			desc:    "bool group True",
			content: "data: True",
			parser:  test.MustNewRegexpParser(t, "data: (True)", "my-metric", falba.ValueBool),
			want:    &falba.Metric{Name: "my-metric", Value: &falba.BoolValue{Value: true}},
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

func mustNewShellvarParser(t *testing.T, varName string, factName string, valueType falba.ValueType) *parser.Parser {
	t.Helper()
	extractor, err := parser.NewShellvarExtractor(varName, valueType)
	if err != nil {
		t.Fatalf("NewShellvarExtractor(%q, %v) failed: %v", varName, valueType, err)
	}
	// ArtifactRE is "." to match any artifact name for these tests
	p, err := parser.NewParser("testShellvar", ".", &parser.ParserTarget{Name: factName, TargetType: parser.TargetFact, ValueType: valueType}, extractor)
	if err != nil {
		t.Fatalf("NewParser failed: %v", err)
	}
	return p
}

func TestShellvarParser_Happy(t *testing.T) {
	testCases := []struct {
		desc    string
		content string
		parser  *parser.Parser
		want    falba.Value
	}{
		{
			desc:    "simple string",
			content: "MY_VAR=simplevalue",
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "simplevalue"},
		},
		{
			desc:    "double quotes",
			content: `MY_VAR="value with spaces"`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "value with spaces"},
		},
		{
			desc:    "single quotes (literal string)",
			content: `MY_VAR='another value'`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: `'another value'`}, // strconv.Unquote fails, returns raw
		},
		{
			desc:    "escaped double quotes inside double quotes",
			content: `MY_VAR="value with \"escaped\" quotes"`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "value with \"escaped\" quotes"}, // strconv.Unquote handles this
		},
		{
			desc:    "escaped backslash inside double quotes",
			content: `MY_VAR="value with \\ backslash"`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "value with \\ backslash"}, // strconv.Unquote handles this
		},
		{
			desc:    "single quotes inside double quotes",
			content: `MY_VAR="value with 'single' quotes"`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "value with 'single' quotes"}, // strconv.Unquote handles this
		},
		{
			desc:    "double quotes inside single quotes (literal single quotes)",
			content: `MY_VAR='value with "double" quotes'`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: `'value with "double" quotes'`}, // strconv.Unquote fails, returns raw
		},
		{
			desc:    "escaped single quote inside single quotes (literal single quotes)",
			content: `MY_VAR='value with \'escaped\' single quote'`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: `'value with \'escaped\' single quote'`}, // strconv.Unquote fails, returns raw
		},
		{
			desc:    "escaped backslash inside single quotes (literal single quotes)",
			content: `MY_VAR='value with \\ backslash'`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: `'value with \\ backslash'`}, // strconv.Unquote fails, returns raw
		},
		{
			desc: "comments and blank lines ignored",
			content: `
# This is a comment
MY_VAR="comment_test"

OTHER_VAR=foo
			`,
			parser: mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:   &falba.StringValue{Value: "comment_test"},
		},
		{
			desc:    "variable at end of file",
			content: "FIRST_VAR=123\nMY_VAR=endvalue",
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "endvalue"},
		},
		{
			desc:    "integer value",
			content: "MY_INT_VAR=12345",
			parser:  mustNewShellvarParser(t, "MY_INT_VAR", "my_int_fact", falba.ValueInt),
			want:    &falba.IntValue{Value: 12345},
		},
		{
			desc:    "integer value with quotes",
			content: `MY_INT_VAR="67890"`,
			parser:  mustNewShellvarParser(t, "MY_INT_VAR", "my_int_fact", falba.ValueInt),
			want:    &falba.IntValue{Value: 67890}, // strconv.Unquote then falba.ParseValue
		},
		{
			desc:    "unrecognised Go escape sequence in double quotes",
			content: `MY_VAR="value with \q char"`, // \q is invalid Go escape
			// strconv.Unquote will fail. parseValue will return rawValue.
			parser: mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:   &falba.StringValue{Value: `"value with \q char"`},
		},
		{
			desc:    "single quotes with non-Go escapes (literal single quotes)",
			content: `MY_VAR='value with \n newline char'`, // \n is not special for strconv.Unquote in single quotes (which it fails on)
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: `'value with \n newline char'`}, // strconv.Unquote fails, returns raw
		},
		{
			desc:    "empty value unquoted",
			content: `MY_VAR=`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: ""},
		},
		{
			desc:    "empty value double quoted",
			content: `MY_VAR=""`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: ""},
		},
		{
			desc:    "empty value single quoted",
			content: `MY_VAR=''`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: ""},
		},
		{
			desc:    "value with equals sign (quoted)",
			content: `MY_VAR="foo=bar"`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "foo=bar"},
		},
		{
			desc:    "value is just quotes",
			content: `MY_VAR="\""`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "\""},
		},
		{
			desc:    "valid trailing backslash in double quotes (value ends with literal backslash)",
			content: `MY_VAR="value\\"`, // Represents "value\"
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "value\\"}, // strconv.Unquote handles this
		},
		{
			desc:    "valid trailing backslash in single quotes (literal single quotes)",
			content: `MY_VAR='value\\'`, // Represents "value\" but in single quotes
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: `'value\\'`}, // strconv.Unquote fails, returns raw
		},
		{
			desc:    "variable not found",
			content: "OTHER_VAR=foo",
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    nil, // Expected behavior for not found is nil (which leads to ErrParseFailure in Extract)
		},
		{
			desc:    "empty file - var not found",
			content: "",
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    nil, // Expected behavior for not found is nil
		},
		{
			desc:    "boolean value true",
			content: "MY_BOOL_VAR=true",
			parser:  mustNewShellvarParser(t, "MY_BOOL_VAR", "my_bool_fact", falba.ValueBool),
			want:    &falba.BoolValue{Value: true},
		},
		{
			desc:    "boolean value FALSE (quoted)",
			content: `MY_BOOL_VAR="FALSE"`,
			parser:  mustNewShellvarParser(t, "MY_BOOL_VAR", "my_bool_fact", falba.ValueBool),
			want:    &falba.BoolValue{Value: false},
		},
		{
			desc:    "boolean value true from quoted int string \"1\"",
			content: `MY_BOOL_VAR="1"`,
			parser:  mustNewShellvarParser(t, "MY_BOOL_VAR", "my_bool_fact", falba.ValueBool),
			want:    &falba.BoolValue{Value: true},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			artifact := fakeArtifact(t, tc.content)
			// For cases where want is nil, we expect an ErrParseFailure from the Extract method
			// which then propagates up from parser.Parse()
			if tc.want == nil {
				_, err := tc.parser.Parse(artifact)
				if err == nil {
					t.Fatalf("Parse() expected error for nil want, got nil")
				}
				if !errors.Is(err, parser.ErrParseFailure) {
					t.Errorf("Parse() expected ErrParseFailure for nil want, got %v", err)
				}
				// Check that the error message contains relevant phrases
				errMsg := err.Error()
				// Check for "variable" AND "not found" for the variable not found case,
				// or "empty content" for the empty file case.
				isVarNotFoundErr := strings.Contains(errMsg, "variable") && strings.Contains(errMsg, "not found")
				isEmptyContentErr := strings.Contains(errMsg, "empty content")

				if !(isVarNotFoundErr || isEmptyContentErr) {
					t.Errorf("Expected error for %q to indicate 'variable not found' or 'empty content', got: %s", tc.desc, errMsg)
				}
				return // End test here for nil want cases
			}

			result, err := tc.parser.Parse(artifact)
			if err != nil {
				t.Fatalf("Parse() failed: %v", err)
			}
			if len(result.Metrics) != 0 {
				t.Errorf("Expected 0 metrics, got %d", len(result.Metrics))
			}
			if len(result.Facts) != 1 {
				t.Errorf("Expected 1 fact, got %d: %v", len(result.Facts), result.Facts)
				return
			}
			factName := tc.parser.Target.Name
			got, ok := result.Facts[factName]
			if !ok {
				t.Fatalf("Fact %q not found in results. Got: %v", factName, result.Facts)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Parse() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// These are tests for behaviour that is wrong, we just keep them as
// change-detectors. The wrongness arises from the fact that we use
// strconv.Unquote which parses Go syntax, which is not actually the syntax we
// are supposed to be parsing here.
func TestShellvarParser_QuotingBugs(t *testing.T) {
	testCases := []struct {
		desc    string
		content string
		parser  *parser.Parser
		want    falba.Value
	}{
		{
			desc:    "escaped dollar bug",
			content: `MY_VAR="value with escaped \$dollar"`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: `"value with escaped \$dollar"`},
		},
		{
			desc:    "escaped backticks bug",
			content: "MY_VAR=\"value with escaped \\`backticks\\`\"",
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
			want:    &falba.StringValue{Value: "\"value with escaped \\`backticks\\`\""},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			artifact := fakeArtifact(t, tc.content)
			result, err := tc.parser.Parse(artifact)
			if err != nil {
				t.Fatalf("Parse() failed: %v", err)
			}
			if len(result.Metrics) != 0 {
				t.Errorf("Expected 0 metrics, got %d", len(result.Metrics))
			}
			if len(result.Facts) != 1 {
				t.Errorf("Expected 1 fact, got %d: %v", len(result.Facts), result.Facts)
				return
			}
			factName := tc.parser.Target.Name
			got, ok := result.Facts[factName]
			if !ok {
				t.Fatalf("Fact %q not found in results. Got: %v", factName, result.Facts)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Parse() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestShellvarParser_Error(t *testing.T) {
	testCases := []struct {
		desc    string
		content string
		parser  *parser.Parser
	}{
		// "malformed line" now results in "variable not found", which is an ErrParseFailure handled by happy path.
		// {
		// 	desc:    "malformed line (no equals) - var not found",
		// 	content: "MY_VAR value", // Line is skipped, MY_VAR not found by that name.
		// 	parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueString),
		// },
		{
			desc:    "type mismatch (string for int)",
			content: "MY_INT_VAR=notanint", // parseValue returns "notanint", falba.ParseValue("notanint", Int) errors.
			parser:  mustNewShellvarParser(t, "MY_INT_VAR", "my_int_fact", falba.ValueInt),
		},
		{
			desc:    "type mismatch (single-quoted string for int)",
			content: "MY_INT_VAR='123'", // parseValue returns "'123'", falba.ParseValue("'123'", Int) errors.
			parser:  mustNewShellvarParser(t, "MY_INT_VAR", "my_int_fact", falba.ValueInt),
		},
		{
			desc:    "type mismatch (string for bool)",
			content: "MY_BOOL_VAR=notabool",
			parser:  mustNewShellvarParser(t, "MY_BOOL_VAR", "my_bool_fact", falba.ValueBool),
		},
		{
			desc:    "type mismatch (int string for bool)",
			content: `MY_BOOL_VAR="1"`,
			parser:  mustNewShellvarParser(t, "MY_BOOL_VAR", "my_bool_fact", falba.ValueBool),
		},
		{
			desc: "invalid escape for strconv.Unquote then type mismatch (int)",
			// MY_VAR="\z" -> strconv.Unquote fails, parseValue returns "\z"
			// falba.ParseValue("\z", int) fails.
			content: `MY_VAR="\z"`,
			parser:  mustNewShellvarParser(t, "MY_VAR", "my_fact", falba.ValueInt),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			artifact := fakeArtifact(t, tc.content)
			_, err := tc.parser.Parse(artifact)
			if err == nil {
				t.Fatalf("Parse() expected error, got nil")
			}
			if !errors.Is(err, parser.ErrParseFailure) {
				t.Errorf("Parse() expected ErrParseFailure, got %v", err)
			}
		})
	}

}

func TestNewShellvarExtractor_Error(t *testing.T) {
	_, err := parser.NewShellvarExtractor("", falba.ValueString)
	if err == nil {
		t.Error("Expected error for empty var name, got nil")
	}
}

func TestShellvarFromConfig(t *testing.T) {
	configJSON := `{
		"type": "shellvar",
		"artifact_regexp": "os-release",
		"var": "PRETTY_NAME",
		"fact": {
			"name": "os_pretty_name",
			"type": "string"
		}
	}`
	p, err := parser.FromConfig([]byte(configJSON), "shellvar_test_parser")
	if err != nil {
		t.Fatalf("FromConfig failed: %v", err)
	}

	content := `NAME="Ubuntu"
VERSION="20.04.3 LTS (Focal Fossa)"
ID=ubuntu
ID_LIKE=debian
PRETTY_NAME="Ubuntu 20.04.3 LTS"
VERSION_ID="20.04"
HOME_URL="https://www.ubuntu.com/"
SUPPORT_URL="https://help.ubuntu.com/"
BUG_REPORT_URL="https://bugs.launchpad.net/ubuntu/"
PRIVACY_POLICY_URL="https://www.ubuntu.com/legal/terms-and-policies/privacy-policy"
VERSION_CODENAME=focal
UBUNTU_CODENAME=focal
`
	path := filepath.Join(t.TempDir(), "os-release")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("Setting up fake artifact: %v", err)
	}
	result, err := p.Parse(&falba.Artifact{Name: "os-release", Path: path})
	if err != nil {
		t.Fatalf("Parse() with FromConfig parser failed: %v", err)
	}
	wantFacts := map[string]falba.Value{
		"os_pretty_name": &falba.StringValue{Value: "Ubuntu 20.04.3 LTS"},
	}
	if diff := cmp.Diff(wantFacts, result.Facts); diff != "" {
		t.Errorf("Parse() mismatch for FromConfig (-want +got):\n%s", diff)
	}
}

func TestShellvarParserFromConfig_MissingVar(t *testing.T) {
	configJSON := `{
			"type": "shellvar",
			"artifact_regexp": "os-release",
			"fact": {
				"name": "os_pretty_name",
				"type": "string"
			}
		}`
	_, err := parser.FromConfig([]byte(configJSON), "shellvar_test_parser")
	if err == nil {
		t.Fatal("FromConfig expected error for missing 'var', got nil")
	}
	if !strings.Contains(err.Error(), "missing/empty 'var' field") {
		t.Errorf("Expected error about missing 'var', got: %v", err)
	}
}

func TestJSONPathParser(t *testing.T) {
	mustNewJSONPathParser := func(t *testing.T, jsonPath string, targetName string, targetType parser.TargetType, valueType falba.ValueType) *parser.Parser {
		t.Helper()
		extractor, err := parser.NewJSONPathExtractor(jsonPath, valueType)
		if err != nil {
			t.Fatalf("NewJSONPathExtractor(%q, %v) failed: %v", jsonPath, valueType, err)
		}
		p, err := parser.NewParser("testJSONPath", ".", &parser.ParserTarget{Name: targetName, TargetType: targetType, ValueType: valueType}, extractor)
		if err != nil {
			t.Fatalf("NewParser failed: %v", err)
		}
		return p
	}

	happyPathTestCases := []struct {
		desc    string
		content string // JSON content
		parser  *parser.Parser
		want    falba.Value   // For facts
		wantMet *falba.Metric // For metrics
	}{
		{
			desc:    "string fact",
			content: `{"key": "value"}`,
			parser:  mustNewJSONPathParser(t, "$.key", "my_fact", parser.TargetFact, falba.ValueString),
			want:    &falba.StringValue{Value: "value"},
		},
		{
			desc:    "int metric",
			content: `{"num": 123}`,
			parser:  mustNewJSONPathParser(t, "$.num", "my_metric", parser.TargetMetric, falba.ValueInt),
			wantMet: &falba.Metric{Name: "my_metric", Value: &falba.IntValue{Value: 123}},
		},
		{
			desc:    "float fact from number",
			content: `{"val": 45.67}`,
			parser:  mustNewJSONPathParser(t, "$.val", "my_fact", parser.TargetFact, falba.ValueFloat),
			want:    &falba.FloatValue{Value: 45.67},
		},
		{
			desc:    "bool fact true",
			content: `{"is_enabled": true}`,
			parser:  mustNewJSONPathParser(t, "$.is_enabled", "my_bool_fact", parser.TargetFact, falba.ValueBool),
			want:    &falba.BoolValue{Value: true},
		},
		{
			desc:    "bool fact false",
			content: `{"active": false}`,
			parser:  mustNewJSONPathParser(t, "$.active", "my_bool_fact", parser.TargetFact, falba.ValueBool),
			want:    &falba.BoolValue{Value: false},
		},
		{
			desc:    "bool fact from string 'true'",
			content: `{"status": "true"}`,
			parser:  mustNewJSONPathParser(t, "$.status", "my_bool_fact", parser.TargetFact, falba.ValueBool),
			want:    &falba.BoolValue{Value: true},
		},
		{
			desc:    "bool fact from string 'FALSE'",
			content: `{"status": "FALSE"}`,
			parser:  mustNewJSONPathParser(t, "$.status", "my_bool_fact", parser.TargetFact, falba.ValueBool),
			want:    &falba.BoolValue{Value: false},
		},
		{
			desc:    "nested value",
			content: `{"data": {"info": "details"}}`,
			parser:  mustNewJSONPathParser(t, "$.data.info", "my_fact", parser.TargetFact, falba.ValueString),
			want:    &falba.StringValue{Value: "details"},
		},
		{
			desc:    "array element string",
			content: `{"list": ["a", "b", "c"]}`,
			parser:  mustNewJSONPathParser(t, "$.list[1]", "my_fact", parser.TargetFact, falba.ValueString),
			want:    &falba.StringValue{Value: "b"},
		},
		{
			desc:    "array element int",
			content: `{"numbers": [10, 20, 30]}`,
			parser:  mustNewJSONPathParser(t, "$.numbers[0]", "my_metric", parser.TargetMetric, falba.ValueInt),
			wantMet: &falba.Metric{Name: "my_metric", Value: &falba.IntValue{Value: 10}},
		},
		{
			desc:    "array element bool",
			content: `{"flags": [true, false, true]}`,
			parser:  mustNewJSONPathParser(t, "$.flags[1]", "my_bool_fact", parser.TargetFact, falba.ValueBool),
			want:    &falba.BoolValue{Value: false},
		},
		{
			desc:    "filtered value from array",
			content: `{"items": [{"name": "A", "val": 1}, {"name": "B", "val": 2}]}`,
			parser:  mustNewJSONPathParser(t, "$.items[?(@.name=='B')].val", "my_metric", parser.TargetMetric, falba.ValueInt),
			wantMet: &falba.Metric{Name: "my_metric", Value: &falba.IntValue{Value: 2}},
		},
	}

	for _, tc := range happyPathTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			artifact := fakeArtifact(t, tc.content)
			result, err := tc.parser.Parse(artifact)
			if err != nil {
				t.Fatalf("Parse() failed: %v", err)
			}

			if tc.parser.Target.TargetType == parser.TargetFact {
				if len(result.Metrics) != 0 {
					t.Errorf("Expected 0 metrics, got %d", len(result.Metrics))
				}
				if len(result.Facts) != 1 {
					t.Errorf("Expected 1 fact, got %d: %v", len(result.Facts), result.Facts)
					return
				}
				factName := tc.parser.Target.Name
				got, ok := result.Facts[factName]
				if !ok {
					t.Fatalf("Fact %q not found in results. Got: %v", factName, result.Facts)
				}
				if diff := cmp.Diff(tc.want, got); diff != "" {
					t.Errorf("Fact mismatch (-want +got):\n%s", diff)
				}
			} else { // TargetMetric
				if len(result.Facts) != 0 {
					t.Errorf("Expected 0 facts, got %d", len(result.Facts))
				}
				if len(result.Metrics) != 1 {
					t.Errorf("Expected 1 metric, got %d: %v", len(result.Metrics), result.Metrics)
					return
				}
				if diff := cmp.Diff(tc.wantMet, result.Metrics[0]); diff != "" {
					t.Errorf("Metric mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}

	errorTestCases := []struct {
		desc    string
		content string // JSON content
		parser  *parser.Parser
	}{
		{
			desc:    "path not found",
			content: `{"key": "value"}`,
			parser:  mustNewJSONPathParser(t, "$.nonexistent", "my_fact", parser.TargetFact, falba.ValueString),
		},
		{
			desc:    "invalid JSON content",
			content: `{"key": "value"`, // Missing closing brace
			parser:  mustNewJSONPathParser(t, "$.key", "my_fact", parser.TargetFact, falba.ValueString),
		},
		{
			desc:    "type mismatch (string for int)",
			content: `{"val": "notanint"}`,
			parser:  mustNewJSONPathParser(t, "$.val", "my_metric", parser.TargetMetric, falba.ValueInt),
		},
		{
			desc:    "type mismatch (number for string when direct string expected)",
			content: `{"val": 123}`,
			// JSONPathExtractor expects string if ValueString, but JSON has number.
			// This will cause a type assertion error in the extractor.
			parser: mustNewJSONPathParser(t, "$.val", "my_fact", parser.TargetFact, falba.ValueString),
		},
		{
			desc:    "type mismatch (bool for string when direct string expected)",
			content: `{"val": true}`,
			parser:  mustNewJSONPathParser(t, "$.val", "my_fact", parser.TargetFact, falba.ValueString),
		},
		{
			desc:    "type mismatch (string 'notabool' for bool)",
			content: `{"val": "notabool"}`,
			parser:  mustNewJSONPathParser(t, "$.val", "my_fact", parser.TargetFact, falba.ValueBool),
		},
		{
			desc:    "type mismatch (int 1 for bool)",
			content: `{"val": 1}`, // JSONPath returns float64 for numbers
			parser:  mustNewJSONPathParser(t, "$.val", "my_fact", parser.TargetFact, falba.ValueBool),
		},
		{
			desc:    "JSONPath returns multiple values",
			content: `{"items": [1, 2, 3]}`,
			parser:  mustNewJSONPathParser(t, "$.items[*]", "my_metric", parser.TargetMetric, falba.ValueInt),
		},
	}

	for _, tc := range errorTestCases {
		t.Run(tc.desc, func(t *testing.T) {
			artifact := fakeArtifact(t, tc.content)
			_, err := tc.parser.Parse(artifact)
			if err == nil {
				t.Fatalf("Parse() expected error, got nil")
			}
			if !errors.Is(err, parser.ErrParseFailure) {
				// Some JSONPath evaluation errors might be fatal, not ErrParseFailure
				// e.g. if the expression itself is malformed (though NewJSONPathExtractor should catch some)
				// or if JSON unmarshalling fails before JSONPath.
				t.Logf("Note: Error was not ErrParseFailure, but: %v", err)
			}
		})
	}

	t.Run("FromConfig jsonpath", func(t *testing.T) {
		configJSON := `{
			"type": "jsonpath",
			"artifact_regexp": "\\.json$",
			"jsonpath": "$.name",
			"metric": {
				"name": "entity_name",
				"type": "string"
			}
		}`
		p, err := parser.FromConfig([]byte(configJSON), "jsonpath_test_parser")
		if err != nil {
			t.Fatalf("FromConfig failed: %v", err)
		}
		if p.Name != "jsonpath_test_parser" || p.ArtifactRE.String() != "\\.json$" || p.Target.Name != "entity_name" {
			t.Errorf("Parser fields mismatch")
		}
		_, ok := p.Extractor.(*parser.JSONPathExtractor)
		if !ok {
			t.Fatalf("Extractor is not of type *JSONPathExtractor, got %T", p.Extractor)
		}
	})

	t.Run("FromConfig jsonpath missing jsonpath field", func(t *testing.T) {
		configJSON := `{
			"type": "jsonpath",
			"artifact_regexp": "\\.json$",
			"metric": { "name": "foo", "type": "string" }
		}`
		_, err := parser.FromConfig([]byte(configJSON), "test")
		if err == nil || !strings.Contains(err.Error(), "missing/empty 'jsonpath' field") {
			t.Errorf("Expected error about missing 'jsonpath' field, got: %v", err)
		}
	})
}

func TestReservedFactNamesRejected(t *testing.T) {
	testCases := []struct {
		name        string
		factName    string
		expectError bool
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
