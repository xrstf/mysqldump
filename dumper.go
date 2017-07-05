package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type Dumper struct {
	db      *sql.DB
	newline string
	writer  *bufio.Writer
}

func NewDumper(db *sql.DB) (*Dumper, error) {
	return &Dumper{db, newline(), nil}, nil
}

func (d *Dumper) DumpDatabase(database string, tables []string, output io.Writer) error {
	d.writer = bufio.NewWriter(output)

	d.writeln("SET @OLD_CHARACTER_SET_CLIENT = @@CHARACTER_SET_CLIENT;")
	d.writeln("SET @OLD_CHARACTER_SET_RESULTS = @@CHARACTER_SET_RESULTS;")
	d.writeln("SET @OLD_COLLATION_CONNECTION = @@COLLATION_CONNECTION;")
	d.writeln("SET NAMES utf8;")
	d.writeln("SET @OLD_TIME_ZONE = @@TIME_ZONE;")
	d.writeln("SET TIME_ZONE = \"+00:00\";")
	d.writeln("SET @OLD_UNIQUE_CHECKS = @@UNIQUE_CHECKS, UNIQUE_CHECKS = 0;")
	d.writeln("SET @OLD_FOREIGN_KEY_CHECKS = @@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS = 0;")
	d.writeln("SET @OLD_SQL_MODE = @@SQL_MODE, SQL_MODE = \"NO_AUTO_VALUE_ON_ZERO\";")
	d.writeln("SET @OLD_SQL_NOTES = @@SQL_NOTES, SQL_NOTES = 0;")
	d.writeln("")

	for _, table := range tables {
		d.writeTableDDL(table)
		d.writeTableDML(table)
	}

	d.writeln("SET TIME_ZONE = @OLD_TIME_ZONE;")
	d.writeln("SET SQL_MODE = @OLD_SQL_MODE;")
	d.writeln("SET FOREIGN_KEY_CHECKS = @OLD_FOREIGN_KEY_CHECKS;")
	d.writeln("SET UNIQUE_CHECKS = @OLD_UNIQUE_CHECKS;")
	d.writeln("SET CHARACTER_SET_CLIENT = @OLD_CHARACTER_SET_CLIENT;")
	d.writeln("SET CHARACTER_SET_RESULTS = @OLD_CHARACTER_SET_RESULTS;")
	d.writeln("SET COLLATION_CONNECTION = @OLD_COLLATION_CONNECTION;")
	d.writeln("SET SQL_NOTES = @OLD_SQL_NOTES;")
	d.writeln("")
	d.writeln("-- dump completed @ [TODO] UTC")

	d.writer.Flush()
	d.writer = nil

	return nil
}

func (d *Dumper) writeTableDDL(table string) error {
	var tableName sql.NullString
	var ddl sql.NullString

	query := fmt.Sprintf("SHOW CREATE TABLE %s", quoteIdentifier(table))
	err := d.db.QueryRow(query).Scan(&tableName, &ddl)
	if err != nil {
		return err
	}

	d.writeln("--")
	d.writefln("-- dumping table `%s`", table)
	d.writeln("--")
	d.writeln("")
	d.writeln(ddl.String + ";")
	d.writeln("")

	return nil
}

func (d *Dumper) writeTableDML(table string) error {
	var tableName sql.NullString
	var ddl sql.NullString

	query := fmt.Sprintf("SHOW CREATE TABLE %s", quoteIdentifier(table))
	err := d.db.QueryRow(query).Scan(&tableName, &ddl)
	if err != nil {
		return err
	}

	d.writeln("-- imagine data here...")
	d.writeln("")

	return nil
}

func (d *Dumper) write(str string) (err error) {
	_, err = d.writer.WriteString(str)
	return
}

func (d *Dumper) writeln(line string) (err error) {
	return d.write(strings.TrimSpace(line) + d.newline)
}

func (d *Dumper) writef(str string, parameters ...interface{}) error {
	return d.write(fmt.Sprintf(str, parameters...))
}

func (d *Dumper) writefln(line string, parameters ...interface{}) error {
	return d.writef(strings.TrimSpace(line)+d.newline, parameters...)
}

func (d *Dumper) Tables(database string) ([]string, error) {
	tables := make([]string, 0)

	rows, err := d.db.Query("SHOW TABLES")
	if err != nil {
		return tables, err
	}
	defer rows.Close()

	for rows.Next() {
		var table sql.NullString

		if err := rows.Scan(&table); err != nil {
			return tables, err
		}

		tables = append(tables, table.String)
	}

	sort.Strings(tables)

	return tables, rows.Err()
}

func quoteIdentifier(identifier string) string {
	return fmt.Sprintf("`%s`", identifier)
}

func newline() string {
	switch runtime.GOOS {
	case "windows":
		return "\r\n"
	default:
		return "\n"
	}
}
