package finder

import (
	"database/sql"

	"github.com/antls/dbgrep/schema"
)

// Result is a result of search in specified table
type Result struct {
	Table   string
	Columns []string
	Rows    []Row
	Err     error
}

// Row is a db row which contains search pattern
type Row []string

// Find performs search of pattern in database
func Find(db *sql.DB, pattern string) []Result {
	s := schema.NewMysql(db)
	var results []Result
	tables, err := s.Tables()
	if err != nil {
		results = append(results, Result{Err: err})
		return results
	}
	for _, table := range tables {
		var resultRows []Row
		columns, err := s.TextColumns(table)
		if err != nil {
			results = append(results, Result{Table: table, Err: err})
			continue
		}
		for _, column := range columns {
			rr, err := searchInColumn(db, table, column, pattern)
			if err != nil {
				results = append(results, Result{Table: table, Err: err})
				continue
			}
			resultRows = append(resultRows, rr...)
		}
		if len(resultRows) > 0 {
			results = append(results, Result{table, columns, resultRows, nil})
		}

	}
	return results
}

func searchInColumn(db *sql.DB, table, column, pattern string) ([]Row, error) {
	sql := "SELECT * FROM `" + table + "` WHERE `" + column + "` LIKE ?"
	rows, err := db.Query(sql, "%"+pattern+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	resultColumns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var resultRows []Row
	for rows.Next() {
		row, err := loadRow(rows, len(resultColumns))
		if err != nil {
			return nil, err
		}
		resultRows = append(resultRows, row)
	}
	return resultRows, nil
}

func loadRow(rows *sql.Rows, numColumns int) (Row, error) {
	byteRow := make([][]byte, numColumns)
	scanArgs := make([]interface{}, len(byteRow))
	for i := range byteRow {
		scanArgs[i] = &byteRow[i]
	}
	err := rows.Scan(scanArgs...)
	if err != nil {
		return Row{}, err
	}
	row := make([]string, len(byteRow))
	for i := range row {
		row[i] = string(byteRow[i])
	}
	return row, nil
}
