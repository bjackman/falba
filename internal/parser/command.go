package parser

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"

	"github.com/bjackman/falba/internal/falba"
)

// CommandExtractor extracts a value by running an arbitrary command
// and piping the artifact content to its stdin.
type CommandExtractor struct {
	Args       []string
	ResultType falba.ValueType
}

func NewCommandExtractor(args []string, resultType falba.ValueType) (*CommandExtractor, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("command args cannot be empty")
	}
	return &CommandExtractor{
		Args:       args,
		ResultType: resultType,
	}, nil
}

func (e *CommandExtractor) Extract(artifact *falba.Artifact) (falba.Value, error) {
	content, err := artifact.Content()
	if err != nil {
		return nil, fmt.Errorf("getting artifact content: %v", err)
	}

	cmd := exec.Command(e.Args[0], e.Args[1:]...)
	cmd.Stdin = bytes.NewReader(content)

	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("%w: command %v failed with exit code %d: %s", ErrParseFailure, e.Args, exitErr.ExitCode(), string(exitErr.Stderr))
		}
		return nil, fmt.Errorf("running command %v: %v", e.Args, err)
	}

	strVal := strings.TrimSpace(string(out))
	val, err := falba.ParseValue(strVal, e.ResultType)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing output %q: %v", ErrParseFailure, strVal, err)
	}

	return val, nil
}

func (e *CommandExtractor) String() string {
	return fmt.Sprintf("CommandExtractor{Args: %v, ResultType: %v}", e.Args, e.ResultType)
}

var _ Extractor = &CommandExtractor{}
