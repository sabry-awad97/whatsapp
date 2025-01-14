package service

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
	_ "modernc.org/sqlite"
)

var (
	storeOnce sync.Once
	store     *sqlstore.Container
	storeMu   sync.Mutex
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

	// Set pragmas for better concurrency
	if _, err := db.Exec("PRAGMA synchronous=NORMAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set synchronous mode: %v", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
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

// InitStore initializes the WhatsApp store
func InitStore(dataDir string) (*sqlstore.Container, error) {
	var err error
	storeOnce.Do(func() {
		// Ensure data directory exists
		if err = os.MkdirAll(dataDir, 0755); err != nil {
			return
		}

		dbPath := filepath.Join(dataDir, "whatsapp.db")
		
		// Open SQLite database with WAL mode and busy timeout
		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			return
		}

		// Set pragmas for better concurrency
		if _, err = db.Exec("PRAGMA journal_mode=WAL"); err != nil {
			return
		}
		if _, err = db.Exec("PRAGMA synchronous=NORMAL"); err != nil {
			return
		}
		if _, err = db.Exec("PRAGMA busy_timeout=5000"); err != nil {
			return
		}

		// Create container
		store = sqlstore.NewWithDB(db, "whatsapp", waLog.Stdout("database", "DEBUG", true))
		
		// Initialize tables
		if err = store.Upgrade(); err != nil {
			return
		}
	})

	return store, err
}

// GetStore returns the WhatsApp store instance
func GetStore() *sqlstore.Container {
	storeMu.Lock()
	defer storeMu.Unlock()
	return store
}
