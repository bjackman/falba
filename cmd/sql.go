package cmd

import (
	"database/sql"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/bjackman/falba/internal/db"
	"github.com/spf13/cobra"

	_ "github.com/marcboeker/go-duckdb"
)

var (
	// At least one letter, followed by alphanumerics and underscores.
	sqlColumnRE = regexp.MustCompile(`[A-Za-z]+[A-Za-z0-9_]*`)
)

// GEMINI FLASH 2.5 WROTE THIS FUNCTION.
//
// I haven't read it. Do not copy from it. It doesn't even print things properly.
func dumpRows(rows *sql.Rows) error {
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get column names: %v", err)
	}

	// Print header
	for i, colName := range columns {
		fmt.Printf("%-20s", colName)
		if i < len(columns)-1 {
			fmt.Print("| ")
		}
	}
	fmt.Println()
	fmt.Println(strings.Repeat("-", 20*len(columns)+2*(len(columns)-1)))

	// Prepare slices for scanning values
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Iterate and print rows
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		for i, val := range values {
			if val == nil {
				fmt.Printf("%-20s", "NULL")
			} else {
				// Handle []byte for string types, others print as is.
				switch v := val.(type) {
				case []byte:
					fmt.Printf("%-20s", string(v))
				case map[string]interface{}: // This case will now likely not be hit for 'facts' itself,
					// as we are flattening the struct in the SELECT query.
					// However, it's kept here in case other columns might unmarshal to a map.
					var factStrings []string
					// Iterate through the map to format the struct fields
					for key, factVal := range v {
						// Convert factVal to string, handling []byte specifically
						var formattedFactVal string
						if byteVal, ok := factVal.([]byte); ok {
							formattedFactVal = string(byteVal)
						} else {
							formattedFactVal = fmt.Sprintf("%v", factVal)
						}
						factStrings = append(factStrings, fmt.Sprintf("%s:%s", key, formattedFactVal))
					}
					// Join the fact strings and print, ensuring it fits the column width
					fmt.Printf("%-20s", strings.Join(factStrings, ", "))
				default:
					fmt.Printf("%-20v", v)
				}
			}
			if i < len(columns)-1 {
				fmt.Print("| ")
			}
		}
		fmt.Println()
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("error during row iteration: %v", err)
	}

	return nil
}

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
	query := fmt.Sprintf(`CREATE TABLE results (test_name STRING, id STRING, facts STRUCT(%s))`,
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

func cmdSQL(cmd *cobra.Command, args []string) error {
	falbaDB, err := db.ReadDB(flagResultDB)
	if err != nil {
		return fmt.Errorf("opening Falba DB: %v", err)
	}

	sqlDB, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("couldn't open DuckDB: %v", err)
	}

	if err := createResultsTable(sqlDB, falbaDB); err != nil {
		return fmt.Errorf("creating results SQL table: %w", err)
	}

	if err := insertResults(sqlDB, falbaDB); err != nil {
		return fmt.Errorf("inserting results int SQL table: %w", err)
	}

	rows, err := sqlDB.Query("SELECT * FROM results")
	if err != nil {
		return fmt.Errorf("failed to query results: %v", err)
	}
	defer rows.Close()
	if err := dumpRows(rows); err != nil {
		return err
	}

	return nil
}

// sqlCmd represents the sql command
var sqlCmd = &cobra.Command{
	Use:   "sql",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	RunE: cmdSQL,
}

func init() {
	rootCmd.AddCommand(sqlCmd)
}
