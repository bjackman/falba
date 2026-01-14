package parser

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/bjackman/falba/internal/falba"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommandExtractor(t *testing.T) {
	tmpDir := t.TempDir()
	artifactPath := filepath.Join(tmpDir, "test.txt")
	err := os.WriteFile(artifactPath, []byte("hello world\n"), 0644)
	require.NoError(t, err)

	artifact := &falba.Artifact{
		Name: "test.txt",
		Path: artifactPath,
	}

	t.Run("simple echo", func(t *testing.T) {
		e, err := NewCommandExtractor([]string{"echo", "123"}, falba.ValueInt)
		require.NoError(t, err)

		val, err := e.Extract(artifact)
		require.NoError(t, err)
		assert.Equal(t, int64(123), val.IntValue())
	})

	t.Run("piped input", func(t *testing.T) {
		// Use wc -c to count bytes of input "hello world\n" -> 12
		e, err := NewCommandExtractor([]string{"wc", "-c"}, falba.ValueInt)
		require.NoError(t, err)

		val, err := e.Extract(artifact)
		require.NoError(t, err)
		// "hello world\n" is 12 bytes
		assert.Equal(t, int64(12), val.IntValue())
	})

	t.Run("parse failure", func(t *testing.T) {
		e, err := NewCommandExtractor([]string{"echo", "notanumber"}, falba.ValueInt)
		require.NoError(t, err)

		_, err = e.Extract(artifact)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parsing output")
	})

	t.Run("command failure", func(t *testing.T) {
		e, err := NewCommandExtractor([]string{"sh", "-c", "exit 1"}, falba.ValueInt)
		require.NoError(t, err)

		_, err = e.Extract(artifact)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed with exit code 1")
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
	require.NoError(t, err)
	require.NotNil(t, p)

	tmpDir := t.TempDir()
	artifactPath := filepath.Join(tmpDir, "test.txt")
	err = os.WriteFile(artifactPath, []byte("12345"), 0644)
	require.NoError(t, err)

	artifact := &falba.Artifact{
		Name: "test.txt",
		Path: artifactPath,
	}

	res, err := p.Parse(artifact)
	require.NoError(t, err)
	require.Len(t, res.Metrics, 1)
	assert.Equal(t, "byte_count", res.Metrics[0].Name)
	assert.Equal(t, int64(5), res.Metrics[0].Value.IntValue())
}
