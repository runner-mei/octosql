package main

import (
	"fmt"
	"log"
	"os"
	"reflect"

	"github.com/cube2222/octosql/graph"
	"github.com/cube2222/octosql/parser"
	"github.com/cube2222/octosql/parser/sqlparser"
	"github.com/spf13/cobra"
)

var configPath string
var outputFormat string
var describe bool

var logicalViz = &cobra.Command{
	Use:   "logical <query>",
	Short: ".",
	Long:  `.`,
	Args:  cobra.ExactValidArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		query := args[0]

		// Parse query
		stmt, err := sqlparser.Parse(query)
		if err != nil {
			log.Fatal("couldn't parse query: ", err)
		}
		typed, ok := stmt.(sqlparser.SelectStatement)
		if !ok {
			log.Fatalf("invalid statement type, wanted sqlparser.SelectStatement got %v", reflect.TypeOf(stmt))
		}
		plan, err := parser.ParseNode(typed)
		if err != nil {
			log.Fatal("couldn't parse query: ", err)
		}

		fmt.Println(graph.Show(plan.Visualize()))
	},
}

func main() {
	if err := logicalViz.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
