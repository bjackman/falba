package parser

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"

	"github.com/bjackman/falba/internal/falba"
)

// ShellCommandExtractor extracts a value by running a shell command
// and piping the artifact content to its stdin.
type ShellCommandExtractor struct {
	Command    string
	ResultType falba.ValueType
}

func NewShellCommandExtractor(command string, resultType falba.ValueType) (*ShellCommandExtractor, error) {
	if command == "" {
		return nil, fmt.Errorf("command cannot be empty")
	}
	return &ShellCommandExtractor{
		Command:    command,
		ResultType: resultType,
	}, nil
}

func (e *ShellCommandExtractor) Extract(artifact *falba.Artifact) (falba.Value, error) {
	content, err := artifact.Content()
	if err != nil {
		return nil, fmt.Errorf("getting artifact content: %v", err)
	}

	cmd := exec.Command("sh", "-c", e.Command)
	cmd.Stdin = bytes.NewReader(content)

	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("%w: command %q failed with exit code %d: %s", ErrParseFailure, e.Command, exitErr.ExitCode(), string(exitErr.Stderr))
		}
		return nil, fmt.Errorf("running command %q: %v", e.Command, err)
	}

	strVal := strings.TrimSpace(string(out))
	val, err := falba.ParseValue(strVal, e.ResultType)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing output %q: %v", ErrParseFailure, strVal, err)
	}

	return val, nil
}

func (e *ShellCommandExtractor) String() string {
	return fmt.Sprintf("ShellCommandExtractor{Command: %q, ResultType: %v}", e.Command, e.ResultType)
}

var _ Extractor = &ShellCommandExtractor{}
