package schema

import "database/sql"

type Schema interface {
	Tables() ([]string, error)
	IdColumns(table string) ([]string, error)
	TextColumns(table string) ([]string, error)
}

type Mysql struct {
	db *sql.DB
}

func NewMysql(db *sql.DB) Schema {
	return &Mysql{db}
}

func (s *Mysql) Tables() (tables []string, err error) {
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

func (s *Mysql) IdColumns(table string) (columns []string, err error) {
	return s.columns(table, "`Key` = 'PRI'")
}

func (s *Mysql) TextColumns(table string) ([]string, error) {
	cond := "Type LIKE '%char%' OR " +
		"Type LIKE '%binary%' OR " +
		"Type LIKE '%blob%' OR " +
		"Type LIKE '%text%' OR " +
		"Type LIKE 'enum%' OR " +
		"Type LIKE 'set%'"
	return s.columns(table, cond)
}

func (s *Mysql) columns(table string, condition string) (columns []string, err error) {
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
