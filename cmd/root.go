package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var flagResultDB string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "falba",
	Short: "Fully Automated Luxury Benchmark Analysis",
	Long:  ``,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// "Persistent" means flags that are inherited by subcommands. Persistent
	// flags on the root command are global flags.
	rootCmd.PersistentFlags().StringVar(&flagResultDB, "result-db", "", "Path to Falba DB root")
}
