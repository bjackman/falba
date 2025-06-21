package falba_test

import (
	"strings"
	"testing"

	"github.com/bjackman/falba/internal/falba"
)

func TestReservedFactNames(t *testing.T) {
	// Test IsReservedFactName
	testCases := []struct {
		name     string
		expected bool
	}{
		{"test_name", true},
		{"result_id", true},
		{"my_fact", false},
		{"another_fact", false},
		{"", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := falba.IsReservedFactName(tc.name); got != tc.expected {
				t.Errorf("IsReservedFactName(%q) = %v, want %v", tc.name, got, tc.expected)
			}
		})
	}

	// Test GetReservedFactNamesString
	reserved := falba.GetReservedFactNamesString()
	if !strings.Contains(reserved, "test_name") {
		t.Errorf("GetReservedFactNamesString() should contain 'test_name', got: %s", reserved)
	}
	if !strings.Contains(reserved, "result_id") {
		t.Errorf("GetReservedFactNamesString() should contain 'result_id', got: %s", reserved)
	}
}