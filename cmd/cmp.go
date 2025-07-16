package cmd

import (
	"fmt"
	"log"
	"maps"
	"os"
	"slices"

	"github.com/bjackman/falba/internal/anal"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"golang.org/x/text/number"
)

var (
	cmpFlagMetric string
	cmpFlagFact   string
	cmpFlagFilter string
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

	groups, err := anal.GroupByFact(sqlDB, falbaDB, cmpFlagFact, cmpFlagMetric, cmpFlagFilter)
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
	t.AppendHeader(table.Row{cmpFlagFact, "samples", "mean", "min", "histogram", "max"})
	for factVal, group := range groups {
		t.AppendRow(table.Row{
			factVal,
			group.Histogram.TotalSize,
			group.Mean,
			group.Min,
			group.Histogram.PlotUnicode(),
			group.Max})
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
	t.SetColumnConfigs([]table.ColumnConfig{
		{Name: "mean", Transformer: transformBigNumber},
		{Name: "min", Transformer: transformBigNumber},
		{Name: "max", Transformer: transformBigNumber},
	})
	t.SortBy([]table.SortBy{{Name: cmpFlagFact, Mode: table.Asc}})
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
}
