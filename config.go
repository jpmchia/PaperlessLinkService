package main

import (
	"os"
	"time"

	"github.com/joho/godotenv"
)

// Config holds the application configuration
type Config struct {
	Port         string
	DBHost       string
	DBPort       string
	DBName       string
	DBUser       string
	DBPass       string
	DBEngine     string // "postgresql", "mysql", "sqlite"
	DBPath       string // For SQLite
	DBSSLMode    string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// loadConfig loads configuration from environment variables
func loadConfig() *Config {
	// Load .env file if it exists
	_ = godotenv.Load()

	config := &Config{
		Port:         getEnv("PORT", "8080"),
		DBHost:       getEnv("DB_HOST", "localhost"),
		DBPort:       getEnv("DB_PORT", "5432"),
		DBName:       getEnv("DB_NAME", "paperless"),
		DBUser:       getEnv("DB_USER", "paperless"),
		DBPass:       getEnv("DB_PASS", "paperless"),
		DBEngine:     getEnv("DB_ENGINE", "postgresql"),
		DBPath:       getEnv("DB_PATH", ""),
		DBSSLMode:    getEnv("DB_SSL_MODE", "prefer"),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	return config
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

