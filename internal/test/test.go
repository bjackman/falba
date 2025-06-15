// Package test contains utilities for testing.
package test

import (
	"path/filepath"
	"testing"

	"github.com/bjackman/falba/internal/falba"
	"github.com/bjackman/falba/internal/parser"
)

func MustNewRegexpParser(t *testing.T, pattern string, metricName string, metricType falba.ValueType) *parser.RegexpParser {
	p, err := parser.NewRegexpParser("fake", ".*", pattern, metricName, metricType)
	if err != nil {
		t.Fatalf("Failed to construct parser: %v", err)
	}
	return p
}

func MustFilepathAbs(t *testing.T, path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("Converting %v to absolute path: %v", path, err)
	}
	return abs
}
