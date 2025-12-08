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
			"ALTER TABLE custom_views ADD COLUMN IF NOT EXISTS filter_types JSONB DEFAULT '{}'::jsonb",
			"ALTER TABLE custom_views ADD COLUMN IF NOT EXISTS edit_mode_settings JSONB DEFAULT '{}'::jsonb",
		}
	case "mysql", "mariadb":
		migrationQueries = []string{
			"ALTER TABLE custom_views ADD COLUMN IF NOT EXISTS subrow_enabled BOOLEAN DEFAULT FALSE",
			"ALTER TABLE custom_views ADD COLUMN IF NOT EXISTS subrow_content VARCHAR(50)",
			"ALTER TABLE custom_views ADD COLUMN IF NOT EXISTS column_spanning JSON DEFAULT '{}'",
			"ALTER TABLE custom_views ADD COLUMN IF NOT EXISTS filter_types JSON DEFAULT '{}'",
			"ALTER TABLE custom_views ADD COLUMN IF NOT EXISTS edit_mode_settings JSON DEFAULT '{}'",
		}
	case "sqlite", "sqlite3":
		// SQLite doesn't support IF NOT EXISTS for ALTER TABLE ADD COLUMN
		// We'll check if columns exist first
		var count int
		checkQuery := "SELECT COUNT(*) FROM pragma_table_info('custom_views') WHERE name IN ('subrow_enabled', 'subrow_content', 'column_spanning', 'filter_types', 'edit_mode_settings')"
		err := s.db.QueryRow(checkQuery).Scan(&count)
		if err == nil && count < 5 {
			migrationQueries = []string{
				"ALTER TABLE custom_views ADD COLUMN subrow_enabled INTEGER DEFAULT 0",
				"ALTER TABLE custom_views ADD COLUMN subrow_content TEXT",
				"ALTER TABLE custom_views ADD COLUMN column_spanning TEXT DEFAULT '{}'",
				"ALTER TABLE custom_views ADD COLUMN filter_types TEXT DEFAULT '{}'",
				"ALTER TABLE custom_views ADD COLUMN edit_mode_settings TEXT DEFAULT '{}'",
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

// initTagGroupsTables creates the tag_groups and tag_group_memberships tables if they don't exist
// Also creates tag_descriptions table for storing descriptions for tags
func (s *Service) initTagGroupsTables() error {
	log.Printf("[Database] Initializing tag groups tables for engine: %s", s.config.DBEngine)

	// Create tag_groups table
	var createTagGroupsQuery string
	switch s.config.DBEngine {
	case "postgresql", "postgres":
		createTagGroupsQuery = `
			CREATE TABLE IF NOT EXISTS tag_groups (
				id SERIAL PRIMARY KEY,
				name VARCHAR(255) NOT NULL UNIQUE,
				description TEXT,
				created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);
			CREATE INDEX IF NOT EXISTS idx_tag_groups_name ON tag_groups(name);
		`
	case "mysql", "mariadb":
		createTagGroupsQuery = `
			CREATE TABLE IF NOT EXISTS tag_groups (
				id INT AUTO_INCREMENT PRIMARY KEY,
				name VARCHAR(255) NOT NULL UNIQUE,
				description TEXT,
				created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				INDEX idx_name (name)
			);
		`
	case "sqlite", "sqlite3":
		createTagGroupsQuery = `
			CREATE TABLE IF NOT EXISTS tag_groups (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name TEXT NOT NULL UNIQUE,
				description TEXT,
				created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);
			CREATE INDEX IF NOT EXISTS idx_tag_groups_name ON tag_groups(name);
		`
	default:
		return fmt.Errorf("unsupported database engine: %s", s.config.DBEngine)
	}

	log.Printf("[Database] Executing CREATE TABLE statement for tag_groups")
	if _, err := s.db.Exec(createTagGroupsQuery); err != nil {
		log.Printf("[Database] Error creating tag_groups table: %v", err)
		return fmt.Errorf("failed to create tag_groups table: %w", err)
	}

	// Create tag_group_memberships table (many-to-many relationship)
	var createMembershipsQuery string
	switch s.config.DBEngine {
	case "postgresql", "postgres":
		createMembershipsQuery = `
			CREATE TABLE IF NOT EXISTS tag_group_memberships (
				id SERIAL PRIMARY KEY,
				tag_group_id INTEGER NOT NULL REFERENCES tag_groups(id) ON DELETE CASCADE,
				tag_id INTEGER NOT NULL,
				created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				UNIQUE(tag_group_id, tag_id)
			);
			CREATE INDEX IF NOT EXISTS idx_memberships_group ON tag_group_memberships(tag_group_id);
			CREATE INDEX IF NOT EXISTS idx_memberships_tag ON tag_group_memberships(tag_id);
		`
	case "mysql", "mariadb":
		createMembershipsQuery = `
			CREATE TABLE IF NOT EXISTS tag_group_memberships (
				id INT AUTO_INCREMENT PRIMARY KEY,
				tag_group_id INT NOT NULL,
				tag_id INT NOT NULL,
				created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				UNIQUE KEY unique_membership (tag_group_id, tag_id),
				INDEX idx_group (tag_group_id),
				INDEX idx_tag (tag_id),
				FOREIGN KEY (tag_group_id) REFERENCES tag_groups(id) ON DELETE CASCADE
			);
		`
	case "sqlite", "sqlite3":
		createMembershipsQuery = `
			CREATE TABLE IF NOT EXISTS tag_group_memberships (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				tag_group_id INTEGER NOT NULL,
				tag_id INTEGER NOT NULL,
				created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				UNIQUE(tag_group_id, tag_id),
				FOREIGN KEY (tag_group_id) REFERENCES tag_groups(id) ON DELETE CASCADE
			);
			CREATE INDEX IF NOT EXISTS idx_memberships_group ON tag_group_memberships(tag_group_id);
			CREATE INDEX IF NOT EXISTS idx_memberships_tag ON tag_group_memberships(tag_id);
		`
	}

	log.Printf("[Database] Executing CREATE TABLE statement for tag_group_memberships")
	if _, err := s.db.Exec(createMembershipsQuery); err != nil {
		log.Printf("[Database] Error creating tag_group_memberships table: %v", err)
		return fmt.Errorf("failed to create tag_group_memberships table: %w", err)
	}

	// Create tag_descriptions table for storing descriptions for individual tags
	var createDescriptionsQuery string
	switch s.config.DBEngine {
	case "postgresql", "postgres":
		createDescriptionsQuery = `
			CREATE TABLE IF NOT EXISTS tag_descriptions (
				id SERIAL PRIMARY KEY,
				tag_id INTEGER NOT NULL UNIQUE,
				description TEXT,
				created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);
			CREATE INDEX IF NOT EXISTS idx_tag_descriptions_tag ON tag_descriptions(tag_id);
		`
	case "mysql", "mariadb":
		createDescriptionsQuery = `
			CREATE TABLE IF NOT EXISTS tag_descriptions (
				id INT AUTO_INCREMENT PRIMARY KEY,
				tag_id INT NOT NULL UNIQUE,
				description TEXT,
				created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				INDEX idx_tag (tag_id)
			);
		`
	case "sqlite", "sqlite3":
		createDescriptionsQuery = `
			CREATE TABLE IF NOT EXISTS tag_descriptions (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				tag_id INTEGER NOT NULL UNIQUE,
				description TEXT,
				created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);
			CREATE INDEX IF NOT EXISTS idx_tag_descriptions_tag ON tag_descriptions(tag_id);
		`
	}

	log.Printf("[Database] Executing CREATE TABLE statement for tag_descriptions")
	if _, err := s.db.Exec(createDescriptionsQuery); err != nil {
		log.Printf("[Database] Error creating tag_descriptions table: %v", err)
		return fmt.Errorf("failed to create tag_descriptions table: %w", err)
	}

	log.Printf("[Database] Successfully created/verified tag groups tables")
	return nil
}
