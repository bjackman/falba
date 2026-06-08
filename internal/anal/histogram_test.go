package anal

import (
	"testing"
)

func TestHistogram_PlotUnicode(t *testing.T) {
	testCases := []struct {
		desc      string
		histogram Histogram
		want      string
	}{
		{
			desc: "binary histogram with bug (all empty or full)",
			histogram: Histogram{
				bins: []HistogramBin{
					{boundary: 10, size: 10}, // Peak
					{boundary: 20, size: 0},  // Empty
					{boundary: 30, size: 5},  // Half (5/10 = 0.5 -> '▄')
					{boundary: 40, size: 1},  // Outlier (1/10 = 0.1 -> forced to '_')
				},
				maxSize:   10,
				TotalSize: 16,
			},
			want: "█ ▄_",
		},
		{
			desc: "smooth distribution",
			// If the bug were present, this would produce "  █  "
			histogram: Histogram{
				bins: []HistogramBin{
					{boundary: 10, size: 1}, // 1/5 = 0.2 -> '▂'
					{boundary: 20, size: 2}, // 2/5 = 0.4 -> '▃'
					{boundary: 30, size: 5}, // 5/5 = 1.0 -> '█'
					{boundary: 40, size: 3}, // 3/5 = 0.6 -> '▅'
					{boundary: 50, size: 1}, // 1/5 = 0.2 -> '▂'
				},
				maxSize:   5,
				TotalSize: 12,
			},
			want: "▂▃█▅▂",
		},
		{
			desc: "minimum visibility for small non-empty bins (outliers)",
			// maxSize is 100, so a bin with size 1 is only 1% of max.
			// Mathematically, 1% of 7 levels is 0.07, which rounds down to level 0 (space).
			// We expect it to be rendered as '_' so it remains visible and distinct.
			histogram: Histogram{
				bins: []HistogramBin{
					{boundary: 10, size: 100}, // Peak
					{boundary: 20, size: 0},   // Truly empty (should be ' ')
					{boundary: 30, size: 1},   // Tiny outlier (should be '_')
				},
				maxSize:   100,
				TotalSize: 101,
			},
			want: "█ _",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			got := tc.histogram.PlotUnicode()
			if got != tc.want {
				t.Errorf("PlotUnicode() = %q, want %q", got, tc.want)
			}
		})
	}
}
