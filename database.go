package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

// connectDB establishes a connection to the database
func connectDB(config *Config) (*sql.DB, error) {
	log.Printf("[Database] Connecting to database - Engine: %s, Host: %s, Port: %s, DB: %s", 
		config.DBEngine, config.DBHost, config.DBPort, config.DBName)
	
	var dsn string
	var driverName string

	switch config.DBEngine {
	case "postgresql", "postgres":
		driverName = "postgres" // lib/pq uses "postgres" as driver name
		dsn = fmt.Sprintf(
			"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
			config.DBHost,
			config.DBPort,
			config.DBUser,
			config.DBPass,
			config.DBName,
			config.DBSSLMode,
		)
	case "mysql", "mariadb":
		driverName = "mysql"
		dsn = fmt.Sprintf(
			"%s:%s@tcp(%s:%s)/%s?parseTime=true",
			config.DBUser,
			config.DBPass,
			config.DBHost,
			config.DBPort,
			config.DBName,
		)
	case "sqlite", "sqlite3":
		driverName = "sqlite3"
		dsn = config.DBPath
	default:
		return nil, fmt.Errorf("unsupported database engine: %s", config.DBEngine)
	}

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	return db, nil
}

// initCustomViewsTable creates the custom_views table if it doesn't exist
func (s *Service) initCustomViewsTable() error {
	log.Printf("[Database] Initializing custom_views table for engine: %s", s.config.DBEngine)
	var createTableQuery string

	switch s.config.DBEngine {
	case "postgresql", "postgres":
		createTableQuery = `
			CREATE TABLE IF NOT EXISTS custom_views (
				id SERIAL PRIMARY KEY,
				name VARCHAR(255) NOT NULL,
				description TEXT,
				column_order JSONB NOT NULL DEFAULT '[]'::jsonb,
				column_sizing JSONB NOT NULL DEFAULT '{}'::jsonb,
				column_visibility JSONB NOT NULL DEFAULT '{}'::jsonb,
				column_display_types JSONB NOT NULL DEFAULT '{}'::jsonb,
				filter_rules JSONB DEFAULT '[]'::jsonb,
				filter_visibility JSONB DEFAULT '{}'::jsonb,
				subrow_enabled BOOLEAN DEFAULT FALSE,
				subrow_content VARCHAR(50),
				column_spanning JSONB DEFAULT '{}'::jsonb,
				sort_field VARCHAR(255),
				sort_reverse BOOLEAN DEFAULT FALSE,
				is_global BOOLEAN DEFAULT FALSE,
				owner_id INTEGER,
				username VARCHAR(255),
				created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				deleted_at TIMESTAMP
			);
			CREATE INDEX IF NOT EXISTS idx_custom_views_owner ON custom_views(owner_id);
			CREATE INDEX IF NOT EXISTS idx_custom_views_global ON custom_views(is_global);
			CREATE INDEX IF NOT EXISTS idx_custom_views_deleted ON custom_views(deleted_at);
		`
	case "mysql", "mariadb":
		createTableQuery = `
			CREATE TABLE IF NOT EXISTS custom_views (
				id INT AUTO_INCREMENT PRIMARY KEY,
				name VARCHAR(255) NOT NULL,
				description TEXT,
				column_order JSON NOT NULL DEFAULT '[]',
				column_sizing JSON NOT NULL DEFAULT '{}',
				column_visibility JSON NOT NULL DEFAULT '{}',
				column_display_types JSON NOT NULL DEFAULT '{}',
				filter_rules JSON DEFAULT '[]',
				filter_visibility JSON DEFAULT '{}',
				subrow_enabled BOOLEAN DEFAULT FALSE,
				subrow_content VARCHAR(50),
				column_spanning JSON DEFAULT '{}',
				sort_field VARCHAR(255),
				sort_reverse BOOLEAN DEFAULT FALSE,
				is_global BOOLEAN DEFAULT FALSE,
				owner_id INT,
				username VARCHAR(255),
				created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				deleted_at TIMESTAMP NULL,
				INDEX idx_owner (owner_id),
				INDEX idx_global (is_global),
				INDEX idx_deleted (deleted_at)
			);
		`
	case "sqlite", "sqlite3":
		createTableQuery = `
			CREATE TABLE IF NOT EXISTS custom_views (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name TEXT NOT NULL,
				description TEXT,
				column_order TEXT NOT NULL DEFAULT '[]',
				column_sizing TEXT NOT NULL DEFAULT '{}',
				column_visibility TEXT NOT NULL DEFAULT '{}',
				column_display_types TEXT NOT NULL DEFAULT '{}',
				filter_rules TEXT DEFAULT '[]',
				filter_visibility TEXT DEFAULT '{}',
				subrow_enabled INTEGER DEFAULT 0,
				subrow_content TEXT,
				column_spanning TEXT DEFAULT '{}',
				sort_field TEXT,
				sort_reverse INTEGER DEFAULT 0,
				is_global INTEGER DEFAULT 0,
				owner_id INTEGER,
				username TEXT,
				created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				deleted_at TIMESTAMP
			);
			CREATE INDEX IF NOT EXISTS idx_custom_views_owner ON custom_views(owner_id);
			CREATE INDEX IF NOT EXISTS idx_custom_views_global ON custom_views(is_global);
			CREATE INDEX IF NOT EXISTS idx_custom_views_deleted ON custom_views(deleted_at);
		`
	default:
		log.Printf("[Database] Unsupported database engine: %s", s.config.DBEngine)
		return fmt.Errorf("unsupported database engine: %s", s.config.DBEngine)
	}

	log.Printf("[Database] Executing CREATE TABLE statement for custom_views")
	if _, err := s.db.Exec(createTableQuery); err != nil {
		log.Printf("[Database] Error creating custom_views table: %v", err)
		return fmt.Errorf("failed to create custom_views table: %w", err)
	}

	// Migrate existing tables to add new columns if they don't exist
	log.Printf("[Database] Migrating custom_views table to add new columns")
	migrationQueries := []string{}
	switch s.config.DBEngine {
	case "postgresql", "postgres":
		migrationQueries = []string{
			"ALTER TABLE custom_views ADD COLUMN IF NOT EXISTS subrow_enabled BOOLEAN DEFAULT FALSE",
			"ALTER TABLE custom_views ADD COLUMN IF NOT EXISTS subrow_content VARCHAR(50)",
			"ALTER TABLE custom_views ADD COLUMN IF NOT EXISTS column_spanning JSONB DEFAULT '{}'::jsonb",
		}
	case "mysql", "mariadb":
		migrationQueries = []string{
			"ALTER TABLE custom_views ADD COLUMN IF NOT EXISTS subrow_enabled BOOLEAN DEFAULT FALSE",
			"ALTER TABLE custom_views ADD COLUMN IF NOT EXISTS subrow_content VARCHAR(50)",
			"ALTER TABLE custom_views ADD COLUMN IF NOT EXISTS column_spanning JSON DEFAULT '{}'",
		}
	case "sqlite", "sqlite3":
		// SQLite doesn't support IF NOT EXISTS for ALTER TABLE ADD COLUMN
		// We'll check if columns exist first
		var count int
		checkQuery := "SELECT COUNT(*) FROM pragma_table_info('custom_views') WHERE name IN ('subrow_enabled', 'subrow_content', 'column_spanning')"
		err := s.db.QueryRow(checkQuery).Scan(&count)
		if err == nil && count < 3 {
			migrationQueries = []string{
				"ALTER TABLE custom_views ADD COLUMN subrow_enabled INTEGER DEFAULT 0",
				"ALTER TABLE custom_views ADD COLUMN subrow_content TEXT",
				"ALTER TABLE custom_views ADD COLUMN column_spanning TEXT DEFAULT '{}'",
			}
		}
	}

	for _, migrationQuery := range migrationQueries {
		if migrationQuery != "" {
			if _, err := s.db.Exec(migrationQuery); err != nil {
				// Log but don't fail - column might already exist
				log.Printf("[Database] Migration query may have failed (column might already exist): %v", err)
			}
		}
	}

	log.Printf("[Database] Successfully created/verified custom_views table")
	return nil
}

