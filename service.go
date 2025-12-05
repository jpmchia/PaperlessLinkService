package main

import (
	"database/sql"
	"fmt"
	"log"
)

// Service represents the application service with database connection
type Service struct {
	db     *sql.DB
	config *Config
}

// NewService creates a new service instance with database connection
func NewService(config *Config) (*Service, error) {
	log.Printf("[Service] Initializing service with DB engine: %s", config.DBEngine)
	
	db, err := connectDB(config)
	if err != nil {
		log.Printf("[Service] Failed to connect to database: %v", err)
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	log.Printf("[Service] Database connection established")

	// Test connection
	if err := db.Ping(); err != nil {
		log.Printf("[Service] Failed to ping database: %v", err)
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	log.Printf("[Service] Database ping successful")

	service := &Service{
		db:     db,
		config: config,
	}

	// Initialize custom views table
	log.Printf("[Service] Initializing custom views table")
	if err := service.initCustomViewsTable(); err != nil {
		log.Printf("[Service] Failed to initialize custom views table: %v", err)
		return nil, fmt.Errorf("failed to initialize custom views table: %w", err)
	}
	log.Printf("[Service] Custom views table initialized successfully")

	return service, nil
}

