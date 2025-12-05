package main

import (
	"database/sql"
	"fmt"
)

// Service represents the application service with database connection
type Service struct {
	db     *sql.DB
	config *Config
}

// NewService creates a new service instance with database connection
func NewService(config *Config) (*Service, error) {
	db, err := connectDB(config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	service := &Service{
		db:     db,
		config: config,
	}

	// Initialize custom views table
	if err := service.initCustomViewsTable(); err != nil {
		return nil, fmt.Errorf("failed to initialize custom views table: %w", err)
	}

	return service, nil
}

