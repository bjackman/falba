package cmd

import (
	"fmt"
	"log"
	"maps"
	"os"
	"slices"

	"github.com/bjackman/falba/internal/anal"
	"github.com/bjackman/falba/internal/unit"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"golang.org/x/text/number"
)

var (
	cmpFlagMetric    string
	cmpFlagFact      string
	cmpFlagFilter    string
	cmpFlagHistWidth int
)

var printer *message.Printer = message.NewPrinter(language.English)

// transformBigNumber is a text.Transformer for formatting larger numbers
// readably. It rounds to the nearest int and adds commas.
func transformBigNumber(v any) string {
	switch v := v.(type) {
	case float64:
		var opts []number.Option
		if v > 100 {
			opts = append(opts, number.MaxFractionDigits(0))
		}
		return printer.Sprintf("%v", number.Decimal(v, opts...))
	case int:
		return printer.Sprintf("%v", number.Decimal(v))
	default:
		log.Printf("No transformer logic for value of type %T", v)
		return printer.Sprintf("%v", v)
	}
}

func formatTime(v any, u *unit.Unit) string {
	var val float64
	switch t := v.(type) {
	case float64:
		val = t
	case int:
		val = float64(t)
	case int64:
		val = float64(t)
	default:
		return fmt.Sprintf("%v", v) // fallback
	}

	// Convert original value to nanoseconds to have a base unit.
	var ns float64
	switch u.ShortName {
	case "ns":
		ns = val
	case "us":
		ns = val * 1e3
	case "ms":
		ns = val * 1e6
	case "s":
		ns = val * 1e9
	default:
		// Not a time unit we can convert from.
		return printer.Sprintf("%v%s", number.Decimal(val), u.ShortName)
	}

	// Now convert from nanoseconds to a more readable unit.
	if ns < 1000 {
		return printer.Sprintf("%.0fns", number.Decimal(ns))
	}
	us := ns / 1e3
	if us < 1000 {
		return printer.Sprintf("%.2fus", number.Decimal(us))
	}
	ms := us / 1e3
	if ms < 1000 {
		return printer.Sprintf("%.2fms", number.Decimal(ms))
	}
	s := ms / 1e3
	if s < 60 {
		return printer.Sprintf("%.2fs", number.Decimal(s))
	}
	min := s / 60
	if min < 60 {
		return printer.Sprintf("%.2fm", number.Decimal(min))
	}
	hr := min / 60
	return printer.Sprintf("%.2fh", number.Decimal(hr))
}

func transformToPercentage(v any) string {
	delta, ok := v.(float64)
	if !ok {
		return ""
	}
	return printer.Sprintf("%+.1f%%", number.Decimal(delta*100))
}

func newTransformer(unit *unit.Unit) func(v any) string {
	if unit != nil && unit.Family == "time" {
		return func(v any) string {
			return formatTime(v, unit)
		}
	}
	return transformBigNumber
}

func cmdCmp(cmd *cobra.Command, args []string) error {
	falbaDB, sqlDB, err := setupSQL()
	if err != nil {
		log.Fatalf("Setting up SQL DB: %v", err)
	}

	// Just to produce a nice error message, check the fact exists.
	_, ok := falbaDB.FactTypes[cmpFlagFact]
	if !ok {
		return fmt.Errorf("no fact %q (have: %v)", cmpFlagFact, slices.Collect(maps.Keys(falbaDB.FactTypes)))
	}

	groups, err := anal.GroupByFact(sqlDB, falbaDB, cmpFlagFact, cmpFlagMetric, cmpFlagFilter, cmpFlagHistWidth)
	if err != nil {
		return fmt.Errorf("grouping by fact: %v", err)
	}

	if len(groups) == 0 {
		return fmt.Errorf("found no data\n")
	}

	// TODO: It's kinda wrong that we support each group being for a different test...
	// For now, we'll only print one, plus a warning if there are multiple.
	tests := make(map[string]bool)
	for _, g := range groups {
		tests[g.TestName] = true
	}
	allTests := slices.Collect(maps.Keys(tests))
	if len(allTests) != 1 {
		log.Printf("WARNING: Enountered %d tests (%v), this is probably wrong.", len(allTests), allTests)
	}

	metricType := falbaDB.MetricTypes[cmpFlagMetric]
	metricString := cmpFlagMetric
	if metricType.Unit != nil {
		metricString = fmt.Sprintf("%s (%s)", cmpFlagMetric, metricType.Unit.ShortName)
	}

	fmt.Printf("metric: %v   |  test: %v\n", metricString, allTests[0])
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	header := table.Row{cmpFlagFact, "samples", "mean", "min"}
	if cmpFlagHistWidth > 0 {
		header = append(header, "histogram")
	}
	header = append(header, "max", "Δμ")
	t.AppendHeader(header)

	// Sort group keys so we have a consistent baseline.
	groupKeys := slices.Collect(maps.Keys(groups))
	slices.Sort(groupKeys)

	var baselineMean float64
	if len(groupKeys) > 0 {
		baselineMean = groups[groupKeys[0]].Mean
	}

	for _, factVal := range groupKeys {
		group := groups[factVal]
		var delta any
		if group.Mean != baselineMean {
			delta = (group.Mean - baselineMean) / baselineMean
		}

		row := table.Row{
			factVal,
			group.Histogram.TotalSize,
			group.Mean,
			group.Min,
		}
		if cmpFlagHistWidth > 0 {
			row = append(row, group.Histogram.PlotUnicode())
		}
		row = append(row, group.Max, delta)
		t.AppendRow(row)
	}
	t.SetStyle(table.Style{
		Name: "mystyle",
		Box:  table.StyleBoxLight,
		// Needs to be set explicitly for some reason, otherwise the table
		// boxes don't show up.
		Options: table.OptionsDefault,
		Format: table.FormatOptions{
			Header: text.FormatDefault,
			Row:    text.FormatDefault,
		},
	})
	transformer := newTransformer(metricType.Unit)
	t.SetColumnConfigs([]table.ColumnConfig{
		{Name: "mean", Transformer: transformer},
		{Name: "min", Transformer: transformer},
		{Name: "max", Transformer: transformer},
		{Name: "Δμ", Transformer: transformToPercentage},
	})
	t.Render()

	return nil
}

var cmpCmd = &cobra.Command{
	Use:   "cmp",
	Short: "Compare distributions of grouped metrics",
	RunE:  cmdCmp,
}

func init() {
	rootCmd.AddCommand(cmpCmd)

	cmpCmd.Flags().StringVarP(&cmpFlagMetric, "metric", "m", "", "Metric to compare")
	cmpCmd.MarkFlagRequired("metric")
	cmpCmd.Flags().StringVarP(&cmpFlagFact, "fact", "f", "", "Fact to group by")
	cmpCmd.MarkFlagRequired("fact")
	cmpCmd.Flags().StringVarP(&cmpFlagFilter, "filter", "w", "TRUE", "Filter for results. SQL boolean expression.")
	cmpCmd.Flags().IntVar(&cmpFlagHistWidth, "hist-width", 20, "Width of the histogram in characters. Set 0 to disable histogram.")
}
