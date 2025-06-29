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

func TestParseValueType(t *testing.T) {
	testCases := []struct {
		name      string
		input     string
		expected  falba.ValueType
		expectErr bool
	}{
		{"int", "int", falba.ValueInt, false},
		{"float", "float", falba.ValueFloat, false},
		{"string", "string", falba.ValueString, false},
		{"bool", "bool", falba.ValueBool, false},
		{"uppercase_bool", "BOOL", 0, true}, // Assuming case-sensitive for now
		{"invalid", "unknown", 0, true},
		{"empty", "", 0, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := falba.ParseValueType(tc.input)
			if tc.expectErr {
				if err == nil {
					t.Errorf("ParseValueType(%q) expected error, got nil", tc.input)
				}
			} else {
				if err != nil {
					t.Errorf("ParseValueType(%q) unexpected error: %v", tc.input, err)
				}
				if got != tc.expected {
					t.Errorf("ParseValueType(%q) = %v, want %v", tc.input, got, tc.expected)
				}
			}
		})
	}
}

func TestParseValue(t *testing.T) {
	testCases := []struct {
		name      string
		inputStr  string
		inputType falba.ValueType
		expected  falba.Value
		expectErr bool
	}{
		// Bool tests
		{"bool_true_lower", "true", falba.ValueBool, &falba.BoolValue{Value: true}, false},
		{"bool_false_lower", "false", falba.ValueBool, &falba.BoolValue{Value: false}, false},
		{"bool_true_upper", "TRUE", falba.ValueBool, &falba.BoolValue{Value: true}, false},
		{"bool_false_upper", "FALSE", falba.ValueBool, &falba.BoolValue{Value: false}, false},
		{"bool_true_mixed", "True", falba.ValueBool, &falba.BoolValue{Value: true}, false},
		{"bool_false_mixed", "False", falba.ValueBool, &falba.BoolValue{Value: false}, false},
		{"bool_invalid_string", "notabool", falba.ValueBool, nil, true},
		// strconv.ParseBool also accepts 1, t, T, TRUE, True, true and 0, f, F, FALSE, False, false.
		{"bool_int_one", "1", falba.ValueBool, &falba.BoolValue{Value: true}, false},
		{"bool_int_zero", "0", falba.ValueBool, &falba.BoolValue{Value: false}, false},
		{"bool_t_lower", "t", falba.ValueBool, &falba.BoolValue{Value: true}, false},
		{"bool_f_upper", "F", falba.ValueBool, &falba.BoolValue{Value: false}, false},

		// Int tests
		{"int_positive", "123", falba.ValueInt, &falba.IntValue{Value: 123}, false},
		{"int_negative", "-456", falba.ValueInt, &falba.IntValue{Value: -456}, false},
		{"int_zero", "0", falba.ValueInt, &falba.IntValue{Value: 0}, false},
		{"int_invalid", "abc", falba.ValueInt, nil, true},

		// Float tests
		{"float_simple", "123.45", falba.ValueFloat, &falba.FloatValue{Value: 123.45}, false},
		{"float_negative", "-0.5", falba.ValueFloat, &falba.FloatValue{Value: -0.5}, false},
		{"float_integer", "789", falba.ValueFloat, &falba.FloatValue{Value: 789}, false},
		{"float_invalid", "def", falba.ValueFloat, nil, true},

		// String tests
		{"string_simple", "hello", falba.ValueString, &falba.StringValue{Value: "hello"}, false},
		{"string_with_spaces", "hello world", falba.ValueString, &falba.StringValue{Value: "hello world"}, false},
		{"string_empty", "", falba.ValueString, &falba.StringValue{Value: ""}, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := falba.ParseValue(tc.inputStr, tc.inputType)
			if tc.expectErr {
				if err == nil {
					t.Errorf("ParseValue(%q, %v) expected error, got nil", tc.inputStr, tc.inputType)
				}
			} else {
				if err != nil {
					t.Errorf("ParseValue(%q, %v) unexpected error: %v", tc.inputStr, tc.inputType, err)
				}
				if !equalValue(got, tc.expected) {
					t.Errorf("ParseValue(%q, %v) = %#v, want %#v", tc.inputStr, tc.inputType, got, tc.expected)
				}
			}
		})
	}
}

// equalValue is a helper to compare falba.Value interfaces.
func equalValue(v1, v2 falba.Value) bool {
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}
	if v1.Type() != v2.Type() {
		return false
	}
	switch v1.Type() {
	case falba.ValueInt:
		return v1.IntValue() == v2.IntValue()
	case falba.ValueFloat:
		return v1.FloatValue() == v2.FloatValue()
	case falba.ValueString:
		return v1.StringValue() == v2.StringValue()
	case falba.ValueBool:
		return v1.BoolValue() == v2.BoolValue()
	default:
		return false
	}
}

func TestValueTypeString(t *testing.T) {
	testCases := []struct {
		valueType falba.ValueType
		expected  string
	}{
		{falba.ValueInt, "int"},
		{falba.ValueFloat, "float"},
		{falba.ValueString, "string"},
		{falba.ValueBool, "bool"},
	}
	for _, tc := range testCases {
		if got := tc.valueType.String(); got != tc.expected {
			t.Errorf("ValueType(%d).String() = %q, want %q", tc.valueType, got, tc.expected)
		}
	}
}

func TestValueTypeMetricsColumn(t *testing.T) {
	testCases := []struct {
		valueType falba.ValueType
		expected  string
	}{
		{falba.ValueInt, "int_value"},
		{falba.ValueFloat, "float_value"},
		{falba.ValueString, "string_value"},
		{falba.ValueBool, "bool_value"},
	}
	for _, tc := range testCases {
		if got := tc.valueType.MetricsColumn(); got != tc.expected {
			t.Errorf("ValueType(%d).MetricsColumn() = %q, want %q", tc.valueType, got, tc.expected)
		}
	}
}

func TestValueInterfaceMethods(t *testing.T) {
	// Test BoolValue
	bvTrue := &falba.BoolValue{Value: true}
	if bvTrue.Type() != falba.ValueBool {
		t.Errorf("bvTrue.Type() = %v, want %v", bvTrue.Type(), falba.ValueBool)
	}
	if !bvTrue.BoolValue() {
		t.Error("bvTrue.BoolValue() = false, want true")
	}
	if bvTrue.IntValue() != 0 {
		t.Errorf("bvTrue.IntValue() = %d, want 0", bvTrue.IntValue())
	}
	if bvTrue.FloatValue() != 0.0 {
		t.Errorf("bvTrue.FloatValue() = %f, want 0.0", bvTrue.FloatValue())
	}
	if bvTrue.StringValue() != "" {
		t.Errorf("bvTrue.StringValue() = %q, want \"\"", bvTrue.StringValue())
	}

	// Test other types return false for BoolValue()
	iv := &falba.IntValue{Value: 1}
	if iv.BoolValue() {
		t.Error("IntValue.BoolValue() returned true, want false")
	}
	fv := &falba.FloatValue{Value: 1.0}
	if fv.BoolValue() {
		t.Error("FloatValue.BoolValue() returned true, want false")
	}
	sv := &falba.StringValue{Value: "true"}
	if sv.BoolValue() {
		t.Error("StringValue.BoolValue() returned true, want false")
	}
}
