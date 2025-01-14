package service

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// CustomStore implements a SQLite store with foreign keys enabled
type CustomStore struct {
	*sql.DB
}

// NewCustomStore creates a new SQLite store with foreign keys enabled
func NewCustomStore(path string) (*CustomStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// Enable foreign keys
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable foreign keys: %v", err)
	}

	// Enable WAL mode
	if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable WAL mode: %v", err)
	}

	// Set busy timeout
	if _, err := db.Exec("PRAGMA busy_timeout = 10000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set busy timeout: %v", err)
	}

	return &CustomStore{DB: db}, nil
}

// QueryRow implements the sqlstore.Queryer interface
func (s *CustomStore) QueryRow(query string, args ...interface{}) *sql.Row {
	return s.DB.QueryRow(query, args...)
}

// Query implements the sqlstore.Queryer interface
func (s *CustomStore) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return s.DB.Query(query, args...)
}

// Exec implements the sqlstore.Queryer interface
func (s *CustomStore) Exec(query string, args ...interface{}) (sql.Result, error) {
	return s.DB.Exec(query, args...)
}

// Begin implements the sqlstore.Queryer interface
func (s *CustomStore) Begin() (*sql.Tx, error) {
	return s.DB.Begin()
}

// Close implements the sqlstore.Queryer interface
func (s *CustomStore) Close() error {
	return s.DB.Close()
}
