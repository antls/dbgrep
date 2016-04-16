package main

import (
	"flag"

	"fmt"

	"os"

	"database/sql"

	"strings"

	"github.com/antls/dbgrep/finder"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	var user, password, host, database, pattern string
	var help bool

	flag.StringVar(&user, "user", "", "Database user")
	flag.StringVar(&host, "host", "", "Database host")
	flag.BoolVar(&help, "help", false, "Print help")
	flag.Parse()
	if help || len(flag.Args()) < 2 {
		printHelp()
		return
	}
	password = os.Getenv("PASSWORD")
	database = flag.Arg(1)
	pattern = flag.Arg(0)
	dsn := fmt.Sprintf("%s:%s@%s/%s", user, password, host, database)

	err := run(dsn, pattern)

	if err != nil {
		fmt.Fprint(os.Stderr, err)
		os.Exit(1)
	}
}

func run(dsn string, pattern string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	results := finder.Find(db, "antls")
	for _, result := range results {
		if result.Err != nil {
			fmt.Fprint(os.Stderr, result.Err)
			continue
		}
		fmt.Println(result.Table)
		for _, row := range result.Rows {
			fmt.Println(strings.Join(row, "\t"))
		}
	}
	return nil
}

func printHelp() {
	fmt.Fprintln(os.Stderr, "dbgrep [-login=<login>] [-host=<host>] pattern database")
	flag.PrintDefaults()
}
