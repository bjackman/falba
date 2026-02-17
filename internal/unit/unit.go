// Package unit contains definitions and a registry for units of measurement.
package unit

import "fmt"

// Unit represents a unit of measurement for a metric.
type Unit struct {
	// The full name of the unit, e.g. "nanoseconds", "bytes".
	Name string
	// The short name of the unit, e.g. "ns", "B".
	ShortName string
	// The family of the unit, e.g. "time", "data". This is used to group
	// units for conversion.
	Family string
}

var (
	registry = map[string]Unit{
		"ns":  {Name: "nanosecond", ShortName: "ns", Family: "time"},
		"us":  {Name: "microsecond", ShortName: "us", Family: "time"},
		"ms":  {Name: "millisecond", ShortName: "ms", Family: "time"},
		"s":   {Name: "second", ShortName: "s", Family: "time"},
		"B":   {Name: "byte", ShortName: "B", Family: "data"},
		"KiB": {Name: "kibibyte", ShortName: "KiB", Family: "data"},
		"MiB": {Name: "mebibyte", ShortName: "MiB", Family: "data"},
		"GiB": {Name: "gibibyte", ShortName: "GiB", Family: "data"},
	}
)

// Parse looks up a unit by its short name. An empty short name returns a
// nil unit.
func Parse(shortName string) (*Unit, error) {
	if shortName == "" {
		return nil, nil
	}
	u, ok := registry[shortName]
	if !ok {
		return nil, fmt.Errorf("unknown unit %q", shortName)
	}
	return &u, nil
}
