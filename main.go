package main

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"sort"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

var (
	skipTablesFlag = flag.String("skip-tables", "", "comma-separated list of tables to skip")
)

var dsn = "develop:develop@/horaro"

func main() {
	flag.Parse()

	log.Printf("Connecting to %s ...", dsn)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	dumper, err := NewDumper(db)
	if err != nil {
		log.Fatal(err)
	}

	tables, err := dumper.Tables("horaro")
	if err != nil {
		log.Fatal(err)
	}

	tables = stripIgnoredTables(tables, *skipTablesFlag)

	err = dumper.DumpDatabase("horaro", tables, os.Stdout)
	if err != nil {
		log.Fatal(err)
	}
}

func stripIgnoredTables(tables []string, skipString string) []string {
	output := make([]string, 0)
	skips := splitList(skipString)

	for _, table := range tables {
		skip := false

		for _, s := range skips {
			if s == table {
				skip = true
				break
			}
		}

		if !skip {
			output = append(output, table)
		}
	}

	return output
}

func splitList(list string) []string {
	output := make([]string, 0)
	parts := strings.Split(list, ",")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if len(part) == 0 {
			continue
		}

		exists := false
		for _, out := range output {
			if out == part {
				exists = true
				break
			}
		}

		if !exists {
			output = append(output, part)
		}
	}

	sort.Strings(output)

	return output
}
