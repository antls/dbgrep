package schema

import "database/sql"

// Schema allows to get database meta data
type Schema interface {
	Tables() ([]string, error)
	IDColumns(table string) ([]string, error)
	TextColumns(table string) ([]string, error)
}

type mysql struct {
	db *sql.DB
}

// NewMysql creates schema for mysql database
func NewMysql(db *sql.DB) Schema {
	return &mysql{db}
}

// Tables returns all tables in database
func (s *mysql) Tables() (tables []string, err error) {
	rows, err := s.db.Query("SHOW TABLES")
	if err != nil {
		return
	}
	defer rows.Close()
	tables = make([]string, 0)
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return
		}
		tables = append(tables, table)
	}
	err = rows.Err()
	return
}

// IDColumns returns primary key columns in the table
func (s *mysql) IDColumns(table string) (columns []string, err error) {
	return s.columns(table, "`Key` = 'PRI'")
}

// TextColumns returns text columns in the table
func (s *mysql) TextColumns(table string) ([]string, error) {
	cond := "Type LIKE '%char%' OR " +
		"Type LIKE '%binary%' OR " +
		"Type LIKE '%blob%' OR " +
		"Type LIKE '%text%' OR " +
		"Type LIKE 'enum%' OR " +
		"Type LIKE 'set%'"
	return s.columns(table, cond)
}

func (s *mysql) columns(table string, condition string) (columns []string, err error) {
	rows, err := s.db.Query("SHOW COLUMNS FROM " + table + " WHERE " + condition)
	if err != nil {
		return
	}
	defer rows.Close()
	columns = make([]string, 0)
	for rows.Next() {
		var column, colType, null, key, extra string
		var colDefault sql.NullString
		err = rows.Scan(&column, &colType, &null, &key, &colDefault, &extra)
		if err != nil {
			return
		}
		columns = append(columns, column)
	}
	err = rows.Err()
	return
}
