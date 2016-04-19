package schema

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"reflect"
	"testing"

	"sort"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB

const DBNAME = "dbgrep"

const FAIL = 1

func TestMain(m *testing.M) {
	flag.Parse()
	code, err := run(m)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
	}
	os.Exit(code)
}

func TestTables(t *testing.T) {
	expectedNames := []string{"foo", "bar", "baz"}
	err := createTables(expectedNames...)
	if err != nil {
		t.Fatal(err)
	}
	s := NewMysql(db)
	actualNames, err := s.Tables()
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(expectedNames)
	sort.Strings(actualNames)
	if !reflect.DeepEqual(expectedNames, actualNames) {
		t.Fatalf("%v != %v", expectedNames, actualNames)
	}
}

func TestIdColumns(t *testing.T) {
	tableName := "customers"
	expectedName := "id"
	err := createTable(tableName)
	if err != nil {
		t.Fatal(err)
	}
	s := NewMysql(db)
	actualColumns, err := s.IDColumns(tableName)
	if err != nil {
		t.Fatal(err)
	}
	if len(actualColumns) != 1 {
		t.Fatalf("length of %v != 1", actualColumns)
	}
	if actualColumns[0] != expectedName {
		t.Fatalf("name of id column '%v' != '%v'", actualColumns[0], expectedName)
	}
}

func TestMultipleIdColumns(t *testing.T) {
	tableName := "users"
	expectedColumns := []string{"user", "host"}
	_, err := db.Exec("CREATE TABLE " + tableName +
		" (user VARCHAR(255), host VARCHAR(255), data TEXT, PRIMARY KEY (user, host))")
	if err != nil {
		t.Fatal(err)
	}
	s := NewMysql(db)
	actualColumns, err := s.IDColumns(tableName)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(expectedColumns)
	sort.Strings(actualColumns)
	if !reflect.DeepEqual(expectedColumns, actualColumns) {
		t.Fatalf("%v != %v", expectedColumns, actualColumns)
	}
}

func TestTextColumns(t *testing.T) {
	tableName := "datatypes"
	expectedColumns := []string{"chartype", "varchartype", "binarytype", "varbinarytype",
		"tinyblobtype", "blobtype", "mediumblobtype", "longblobtype",
		"tinytexttype", "texttype", "mediumtexttype", "longtexttype",
		"enumtype", "settype"}
	sql := "CREATE TABLE " + tableName + " (" +
		"id int PRIMARY KEY AUTO_INCREMENT, " +
		"chartype CHAR(10), " +
		"varchartype VARCHAR(20), " +
		"binarytype BINARY(10), " +
		"varbinarytype VARBINARY(20), " +
		"tinyblobtype TINYBLOB, " +
		"blobtype BLOB, " +
		"mediumblobtype MEDIUMBLOB, " +
		"longblobtype LONGBLOB, " +
		"tinytexttype TINYTEXT, " +
		"texttype TEXT, " +
		"mediumtexttype MEDIUMTEXT, " +
		"longtexttype LONGTEXT, " +
		"enumtype ENUM('x-small', 'small', 'medium', 'large', 'x-large'), " +
		"settype SET('a', 'b', 'c', 'd'), " +
		"inttype INT, " +
		"floattype FLOAT " +
		")"
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatal(err)
	}
	s := NewMysql(db)
	actualColumns, err := s.TextColumns(tableName)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(expectedColumns)
	sort.Strings(actualColumns)
	if !reflect.DeepEqual(expectedColumns, actualColumns) {
		t.Fatalf("%v != %v", expectedColumns, actualColumns)
	}
}

func run(m *testing.M) (int, error) {
	dsn := getDsn()
	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return FAIL, fmt.Errorf("Unable to open connection to database server: %s", err.Error())
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		return FAIL, fmt.Errorf("Unable to ping database server: %s", err.Error())
	}
	_, err = db.Exec("CREATE DATABASE " + DBNAME)
	if err != nil {
		return FAIL, fmt.Errorf("Unable to create database %s: %s", DBNAME, err.Error())
	}
	defer db.Exec("DROP DATABASE dbgrep")
	_, err = db.Exec("use dbgrep")
	if err != nil {
		return FAIL, fmt.Errorf("Unable to select database %s: %s", DBNAME, err.Error())
	}
	return m.Run(), nil
}

func getDsn() string {
	if dsn := os.Getenv("MYSQL_TEST_DSN"); dsn != "" {
		return dsn
	}
	return "root@/"
}

func createTables(names ...string) error {
	for _, name := range names {
		err := createTable(name)
		if err != nil {
			return err
		}
	}
	return nil
}

func createTable(name string) error {
	_, err := db.Exec("CREATE TABLE " + name + " (id int PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))")
	return err
}
