package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/joho/godotenv"
)

// Configuration
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

// Response types
type CustomFieldValueOption struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Count int    `json:"count"`
}

type CustomFieldValuesResponse struct {
	FieldID        int                     `json:"field_id"`
	FieldName      string                  `json:"field_name"`
	Values         []CustomFieldValueOption `json:"values"`
	TotalDocuments int                     `json:"total_documents"`
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// Service
type Service struct {
	db     *sql.DB
	config *Config
}

func NewService(config *Config) (*Service, error) {
	db, err := connectDB(config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Service{
		db:     db,
		config: config,
	}, nil
}

func connectDB(config *Config) (*sql.DB, error) {
	var dsn string

	switch config.DBEngine {
	case "postgresql", "postgres":
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
		dsn = fmt.Sprintf(
			"%s:%s@tcp(%s:%s)/%s?parseTime=true",
			config.DBUser,
			config.DBPass,
			config.DBHost,
			config.DBPort,
			config.DBName,
		)
	case "sqlite", "sqlite3":
		dsn = config.DBPath
	default:
		return nil, fmt.Errorf("unsupported database engine: %s", config.DBEngine)
	}

	db, err := sql.Open(config.DBEngine, dsn)
	if err != nil {
		return nil, err
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	return db, nil
}

func (s *Service) GetFieldValues(fieldID int) (*CustomFieldValuesResponse, error) {
	// First, get the field name
	var fieldName string
	var queryFieldName string
	var argsFieldName []interface{}
	
	switch s.config.DBEngine {
	case "postgresql", "postgres":
		queryFieldName = "SELECT name FROM documents_customfield WHERE id = $1"
		argsFieldName = []interface{}{fieldID}
	case "mysql", "mariadb", "sqlite", "sqlite3":
		queryFieldName = "SELECT name FROM documents_customfield WHERE id = ?"
		argsFieldName = []interface{}{fieldID}
	default:
		return nil, fmt.Errorf("unsupported database engine: %s", s.config.DBEngine)
	}
	
	err := s.db.QueryRow(queryFieldName, argsFieldName...).Scan(&fieldName)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("custom field with id %d not found", fieldID)
		}
		return nil, fmt.Errorf("failed to get field name: %w", err)
	}

	// Get the field data type to determine which value column to query
	var dataType string
	var queryDataType string
	var argsDataType []interface{}
	
	switch s.config.DBEngine {
	case "postgresql", "postgres":
		queryDataType = "SELECT data_type FROM documents_customfield WHERE id = $1"
		argsDataType = []interface{}{fieldID}
	case "mysql", "mariadb", "sqlite", "sqlite3":
		queryDataType = "SELECT data_type FROM documents_customfield WHERE id = ?"
		argsDataType = []interface{}{fieldID}
	default:
		return nil, fmt.Errorf("unsupported database engine: %s", s.config.DBEngine)
	}
	
	err = s.db.QueryRow(queryDataType, argsDataType...).Scan(&dataType)
	if err != nil {
		return nil, fmt.Errorf("failed to get field data type: %w", err)
	}

	// Determine the value column name based on data type
	valueColumn := getValueColumnName(dataType)

	// Query to aggregate unique values and their counts
	// This handles different data types and also parses comma/colon separated values
	// Note: PostgreSQL uses $1, MySQL uses ?, SQLite uses ?
	var query string
	var args []interface{}
	
	switch s.config.DBEngine {
	case "postgresql", "postgres":
		query = fmt.Sprintf(`
			SELECT 
				%s as value,
				COUNT(DISTINCT document_id) as count
			FROM documents_customfieldinstance
			WHERE field_id = $1 
				AND deleted_at IS NULL
				AND %s IS NOT NULL
				AND %s != ''
			GROUP BY %s
			ORDER BY count DESC, %s ASC
		`, valueColumn, valueColumn, valueColumn, valueColumn, valueColumn)
		args = []interface{}{fieldID}
	case "mysql", "mariadb", "sqlite", "sqlite3":
		query = fmt.Sprintf(`
			SELECT 
				%s as value,
				COUNT(DISTINCT document_id) as count
			FROM documents_customfieldinstance
			WHERE field_id = ? 
				AND deleted_at IS NULL
				AND %s IS NOT NULL
				AND %s != ''
			GROUP BY %s
			ORDER BY count DESC, %s ASC
		`, valueColumn, valueColumn, valueColumn, valueColumn, valueColumn)
		args = []interface{}{fieldID}
	default:
		return nil, fmt.Errorf("unsupported database engine: %s", s.config.DBEngine)
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query field values: %w", err)
	}
	defer rows.Close()

	values := []CustomFieldValueOption{}
	valueMap := make(map[string]int) // To aggregate parsed values

	for rows.Next() {
		var value string
		var count int
		if err := rows.Scan(&value, &count); err != nil {
			continue
		}

		// Parse comma or colon separated values
		parts := parseValueList(value)
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part != "" {
				valueMap[part] += count
			}
		}
	}

	// Convert map to slice
	for value, count := range valueMap {
		values = append(values, CustomFieldValueOption{
			ID:    generateID(value),
			Label: value,
			Count: count,
		})
	}

	// Sort by count descending, then by label ascending
	for i := 0; i < len(values)-1; i++ {
		for j := i + 1; j < len(values); j++ {
			if values[i].Count < values[j].Count ||
				(values[i].Count == values[j].Count && values[i].Label > values[j].Label) {
				values[i], values[j] = values[j], values[i]
			}
		}
	}

	// Get total document count
	var totalDocuments int
	var queryTotalDocs string
	
	switch s.config.DBEngine {
	case "postgresql", "postgres", "mysql", "mariadb", "sqlite", "sqlite3":
		queryTotalDocs = "SELECT COUNT(DISTINCT id) FROM documents_document WHERE deleted_at IS NULL"
	default:
		return nil, fmt.Errorf("unsupported database engine: %s", s.config.DBEngine)
	}
	
	err = s.db.QueryRow(queryTotalDocs).Scan(&totalDocuments)
	if err != nil {
		totalDocuments = 0
	}

	return &CustomFieldValuesResponse{
		FieldID:        fieldID,
		FieldName:      fieldName,
		Values:         values,
		TotalDocuments: totalDocuments,
	}, nil
}

func (s *Service) SearchFieldValues(fieldID int, query string) ([]CustomFieldValueOption, error) {
	// Get all values first
	response, err := s.GetFieldValues(fieldID)
	if err != nil {
		return nil, err
	}

	// Filter by query string (case-insensitive)
	queryLower := strings.ToLower(query)
	filtered := []CustomFieldValueOption{}

	for _, value := range response.Values {
		if strings.Contains(strings.ToLower(value.Label), queryLower) {
			filtered = append(filtered, value)
		}
	}

	return filtered, nil
}

func (s *Service) GetValueCounts(fieldID int, filterRulesJSON string) ([]CustomFieldValueOption, error) {
	// For now, return all values with counts
	// In the future, we can apply filter rules to count only matching documents
	response, err := s.GetFieldValues(fieldID)
	if err != nil {
		return nil, err
	}

	return response.Values, nil
}

// Helper functions
func getValueColumnName(dataType string) string {
	switch dataType {
	case "string":
		return "value_text"
	case "url":
		return "value_url"
	case "date":
		return "value_date"
	case "boolean":
		return "value_bool"
	case "integer":
		return "value_int"
	case "float":
		return "value_float"
	case "monetary":
		return "value_monetary"
	case "documentlink":
		return "value_document_ids"
	case "select":
		return "value_select"
	case "longtext":
		return "value_long_text"
	default:
		return "value_text"
	}
}

func parseValueList(value string) []string {
	// Split by comma or colon
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ':'
	})
	return parts
}

func generateID(value string) string {
	// Simple hash-based ID generation
	hash := 0
	for _, char := range value {
		hash = hash*31 + int(char)
	}
	return fmt.Sprintf("val-%d", hash)
}

// HTTP Handlers
func (s *Service) handleGetFieldValues(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fieldIDStr := vars["fieldId"]

	fieldID, err := strconv.Atoi(fieldIDStr)
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid field ID")
		return
	}

	response, err := s.GetFieldValues(fieldID)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, response)
}

func (s *Service) handleSearchFieldValues(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fieldIDStr := vars["fieldId"]

	fieldID, err := strconv.Atoi(fieldIDStr)
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid field ID")
		return
	}

	query := r.URL.Query().Get("q")
	if query == "" {
		respondError(w, http.StatusBadRequest, "Query parameter 'q' is required")
		return
	}

	values, err := s.SearchFieldValues(fieldID, query)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, values)
}

func (s *Service) handleGetValueCounts(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fieldIDStr := vars["fieldId"]

	fieldID, err := strconv.Atoi(fieldIDStr)
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid field ID")
		return
	}

	// Parse filter rules from request body if present
	var filterRulesJSON string
	if r.Body != nil {
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			if rules, ok := body["filter_rules"].([]interface{}); ok {
				rulesBytes, _ := json.Marshal(rules)
				filterRulesJSON = string(rulesBytes)
			}
		}
	}

	values, err := s.GetValueCounts(fieldID, filterRulesJSON)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, values)
}

func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, ErrorResponse{
		Error:   http.StatusText(status),
		Message: message,
	})
}

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

func main() {
	config := loadConfig()
	log.Printf("Starting Paperless Link Service on port %s", config.Port)

	service, err := NewService(config)
	if err != nil {
		log.Fatalf("Failed to initialize service: %v", err)
	}
	defer service.db.Close()

	// Setup router
	router := mux.NewRouter()

	// API routes
	api := router.PathPrefix("/api/custom-field-values").Subrouter()
	api.HandleFunc("/{fieldId:[0-9]+}/", service.handleGetFieldValues).Methods("GET")
	api.HandleFunc("/{fieldId:[0-9]+}/search/", service.handleSearchFieldValues).Methods("GET")
	api.HandleFunc("/{fieldId:[0-9]+}/counts/", service.handleGetValueCounts).Methods("POST")

	// Health check
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if err := service.db.Ping(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}).Methods("GET")

	// CORS middleware
	corsHandler := handlers.CORS(
		handlers.AllowedOrigins([]string{"*"}),
		handlers.AllowedMethods([]string{"GET", "POST", "OPTIONS"}),
		handlers.AllowedHeaders([]string{"Content-Type"}),
	)(router)

	// Setup server
	srv := &http.Server{
		Addr:         ":" + config.Port,
		Handler:      corsHandler,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	}

	// Start server in goroutine
	go func() {
		log.Printf("Server listening on :%s", config.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
}

