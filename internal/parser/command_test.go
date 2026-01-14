package parser

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bjackman/falba/internal/falba"
)

func TestCommandExtractor(t *testing.T) {
	tmpDir := t.TempDir()
	artifactPath := filepath.Join(tmpDir, "test.txt")
	err := os.WriteFile(artifactPath, []byte("hello world\n"), 0644)
	if err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	artifact := &falba.Artifact{
		Name: "test.txt",
		Path: artifactPath,
	}

	t.Run("simple echo", func(t *testing.T) {
		e, err := NewCommandExtractor([]string{"echo", "123"}, falba.ValueInt)
		if err != nil {
			t.Fatalf("NewCommandExtractor failed: %v", err)
		}

		val, err := e.Extract(artifact)
		if err != nil {
			t.Fatalf("Extract failed: %v", err)
		}
		if val.IntValue() != 123 {
			t.Errorf("got %d, want 123", val.IntValue())
		}
	})

	t.Run("piped input", func(t *testing.T) {
		// Use wc -c to count bytes of input "hello world\n" -> 12
		e, err := NewCommandExtractor([]string{"wc", "-c"}, falba.ValueInt)
		if err != nil {
			t.Fatalf("NewCommandExtractor failed: %v", err)
		}

		val, err := e.Extract(artifact)
		if err != nil {
			t.Fatalf("Extract failed: %v", err)
		}
		// "hello world\n" is 12 bytes
		if val.IntValue() != 12 {
			t.Errorf("got %d, want 12", val.IntValue())
		}
	})

	t.Run("parse failure", func(t *testing.T) {
		e, err := NewCommandExtractor([]string{"echo", "notanumber"}, falba.ValueInt)
		if err != nil {
			t.Fatalf("NewCommandExtractor failed: %v", err)
		}

		_, err = e.Extract(artifact)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "parsing output") {
			t.Errorf("error %q should contain 'parsing output'", err.Error())
		}
	})

	t.Run("command failure", func(t *testing.T) {
		e, err := NewCommandExtractor([]string{"sh", "-c", "exit 1"}, falba.ValueInt)
		if err != nil {
			t.Fatalf("NewCommandExtractor failed: %v", err)
		}

		_, err = e.Extract(artifact)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "failed with exit code 1") {
			t.Errorf("error %q should contain 'failed with exit code 1'", err.Error())
		}
	})
}

func TestCommandParserConfig(t *testing.T) {
	// Test full parser configuration
	configJSON := `{
		"type": "command",
		"artifact_regexp": "test.txt",
		"args": ["sh", "-c", "cat | wc -c"],
		"metric": {
			"name": "byte_count",
			"type": "int",
			"unit": "B"
		}
	}`

	p, err := FromConfig(json.RawMessage(configJSON), "test_parser")
	if err != nil {
		t.Fatalf("FromConfig failed: %v", err)
	}
	if p == nil {
		t.Fatal("FromConfig returned nil parser")
	}

	tmpDir := t.TempDir()
	artifactPath := filepath.Join(tmpDir, "test.txt")
	err = os.WriteFile(artifactPath, []byte("12345"), 0644)
	if err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	artifact := &falba.Artifact{
		Name: "test.txt",
		Path: artifactPath,
	}

	res, err := p.Parse(artifact)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(res.Metrics) != 1 {
		t.Fatalf("got %d metrics, want 1", len(res.Metrics))
	}
	if res.Metrics[0].Name != "byte_count" {
		t.Errorf("got metric name %q, want 'byte_count'", res.Metrics[0].Name)
	}
	if res.Metrics[0].Value.IntValue() != 5 {
		t.Errorf("got metric value %d, want 5", res.Metrics[0].Value.IntValue())
	}
}
