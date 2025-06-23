package parser

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"

	"github.com/bjackman/falba/internal/falba"
)

// ShellvarExtractor extracts a value from a shell-style variable assignment
// file. This is intended to be like the format of /etc/os-release described
// here: https://www.freedesktop.org/software/systemd/man/latest/os-release.html
// but it isn't really fully implementing that "spec", instead it uses Go's
// strcconv.Unquote to deal with string syntax.
type ShellvarExtractor struct {
	VarName    string
	ResultType falba.ValueType
}

func NewShellvarExtractor(varName string, resultType falba.ValueType) (*ShellvarExtractor, error) {
	if varName == "" {
		return nil, fmt.Errorf("variable name cannot be empty")
	}
	return &ShellvarExtractor{
		VarName:    varName,
		ResultType: resultType,
	}, nil
}

func (e *ShellvarExtractor) Extract(artifact *falba.Artifact) (falba.Value, error) {
	content, err := artifact.Content()
	if err != nil {
		return nil, fmt.Errorf("getting artifact content: %v", err)
	}

	reader := strings.NewReader(string(content))
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("%w: malformed line: %q", ErrParseFailure, line)
		}

		if strings.TrimSpace(parts[0]) != e.VarName {
			continue
		}

		rawValue := strings.TrimSpace(parts[1])
		value, err := e.parseValue(rawValue)
		if err != nil {
			return nil, fmt.Errorf("%w: parsing variable %q: %v", ErrParseFailure, e.VarName, err)
		}

		parsedVal, err := falba.ParseValue(value, e.ResultType)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrParseFailure, err)
		}
		return parsedVal, nil
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning lines: %v", err)
	}

	// If we reach here, the variable was not found in the file.
	// Or the file was empty and thus the variable was not found.
	// Check if content was empty to give a slightly more specific error.
	if len(strings.TrimSpace(string(content))) == 0 {
		return nil, fmt.Errorf("%w: empty content, variable %q not found", ErrParseFailure, e.VarName)
	}
	return nil, fmt.Errorf("%w: variable %q not found", ErrParseFailure, e.VarName)
}

func (e *ShellvarExtractor) parseValue(rawValue string) (string, error) {
	if len(rawValue) == 0 {
		return "", nil
	}

	// TODO: this is not properly parsing the format, it parses a Go string
	// literal which is not actually compatible with the inteded format here.
	unquoted, err := strconv.Unquote(rawValue)
	if err == nil {
		return unquoted, nil
	}

	return rawValue, nil
}

func (e *ShellvarExtractor) String() string {
	return fmt.Sprintf("ShellvarExtractor{VarName: %q, ResultType: %v}", e.VarName, e.ResultType)
}

var _ Extractor = &ShellvarExtractor{}
