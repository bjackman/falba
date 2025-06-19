package cmd

import (
	"database/sql"
	"fmt"
	"log"
	"maps"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"syscall"

	"github.com/bjackman/falba/internal/db"
	"github.com/spf13/cobra"

	_ "github.com/marcboeker/go-duckdb"
)

var (
	// At least one letter, followed by alphanumerics and underscores.
	sqlColumnRE = regexp.MustCompile(`[A-Za-z]+[A-Za-z0-9_]*`)

	flagDuckdbCli string

	duckDBPath string = "falba.duckdb"
)

// TODO: Instead of this janky SQL codegen we should just give the Falba DB
// logic to generate an Arrow table or something
// (https://duckdb.org/docs/stable/guides/python/sql_on_arrow.html) and then
// have DuckDB import from that.
func createResultsTable(sqlDB *sql.DB, falbaDB *db.DB) error {
	// AFAICS there's no way to dynamically create column or STRUCT schemata
	// without being vulnerable to SQL injection. There's no real security issue
	// here but to avoid really confusing things happening, just require all the
	// fact names to obviously be valid SQL identifiers. Probably we can be more
	// relaxed about this but I CBA to research it.
	var structFields []string
	for name, falbaType := range falbaDB.FactTypes {
		if !sqlColumnRE.MatchString(name) {
			return fmt.Errorf("column name %q doesn't match %v, can't use as SQL column name",
				name, sqlColumnRE)
		}
		structFields = append(structFields, fmt.Sprintf("%s %s", name, falbaType.SQL()))
	}
	query := fmt.Sprintf(`CREATE OR REPLACE TABLE results (test_name STRING, id STRING, facts STRUCT(%s))`,
		strings.Join(structFields, ", "))
	if _, err := sqlDB.Exec(query); err != nil {
		return fmt.Errorf("could not create table users: %s", err.Error())
	}
	return nil
}

func insertResults(sqlDB *sql.DB, falbaDB *db.DB) error {
	// We have to do sketchy codegen anyway, but it's still worth trying to do
	// as much as possible with a prepared statement since that at least deals
	// with proper quoting for you.
	var b strings.Builder
	b.WriteString(`INSERT INTO results(test_name, id, facts) VALUES(?, ?, struct_pack(`)
	factNames := slices.Sorted(maps.Keys(falbaDB.FactTypes))
	for i, name := range factNames {
		b.WriteString(fmt.Sprintf("%s := ?", name))
		if i < len(factNames)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString(`))`)
	insertStmt, err := sqlDB.Prepare(b.String())
	if err != nil {
		return fmt.Errorf("preparing insert statement: %v", err)
	}

	for _, result := range falbaDB.Results {
		args := []any{result.TestName, result.ResultID}
		for _, factName := range factNames {
			// Explicitly check for fact presence to ensure we can set it to
			// NULL in the SQL, instead of the Go zero value, which would be
			// confusing.
			val, ok := result.Facts[factName]
			if ok {
				args = append(args, val.SQLValue())
			} else {
				args = append(args, falbaDB.FactTypes[factName].SQLNull())
			}
		}
		if _, err := insertStmt.Exec(args...); err != nil {
			return fmt.Errorf("failed to create row: %v", err)
		}
	}
	return nil
}

func setupSQL() error {
	falbaDB, err := db.ReadDB(flagResultDB)
	if err != nil {
		return fmt.Errorf("opening Falba DB: %v", err)
	}

	sqlDB, err := sql.Open("duckdb", duckDBPath)
	if err != nil {
		return fmt.Errorf("couldn't open DuckDB: %v", err)
	}

	if err := createResultsTable(sqlDB, falbaDB); err != nil {
		return fmt.Errorf("creating results SQL table: %w", err)
	}

	if err := insertResults(sqlDB, falbaDB); err != nil {
		return fmt.Errorf("inserting results int SQL table: %w", err)
	}

	return nil
}

// Here we don't use proper error handling because we are going to exec the
// DuckDB CLI so defer etc won't work.
func cmdSQL(cmd *cobra.Command, args []string) {
	if err := setupSQL(); err != nil {
		log.Fatalf("Setting up SQL DB: %v", err)
	}

	// Apparently yhe 'exec' package doesn't actually support exec-ing lol.
	// I got this from https://gobyexample.com/execing-processes
	cliPath, err := exec.LookPath(flagDuckdbCli)
	if err != nil {
		log.Fatalf("Searching $PATH for DuckDB CLI (%q, from --duckdb-cli): %v", flagDuckdbCli, err)
	}
	err = syscall.Exec(cliPath, []string{cliPath, duckDBPath}, os.Environ())
	if err != nil {
		log.Fatalf("exec()ing DuckDB CLI: %v", err)
	}
	// wat
	log.Fatalf("Unexpectedly returned from exec()ing DuckDB CLI")
}

// sqlCmd represents the sql command
var sqlCmd = &cobra.Command{
	Use:   "sql",
	Short: "Drop into a DuckDB SQL prompt.",
	Long: `Creates a DuckDB database and then uses the DuckDB CLI
(https://duckdb.org/docs/stable/clients/cli/overview.html) to drop you into
a SQL REPL where you can explore the Falba data.`,
	Run: cmdSQL,
}

func init() {
	sqlCmd.Flags().StringVar(&flagDuckdbCli, "duckdb-cli", "duckdb",
		"DuckDB CLI executable. Looked up in $PATH")
	rootCmd.AddCommand(sqlCmd)
}
