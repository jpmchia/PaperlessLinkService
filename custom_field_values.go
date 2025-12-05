package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

// GetFieldValues retrieves all unique values for a specific custom field
func (s *Service) GetFieldValues(fieldID int, sortBy string, sortOrder string, ignoreCase bool) (*CustomFieldValuesResponse, error) {
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
	var query string
	var args []interface{}

	// Query to get all values with their document IDs
	// We need document_id to properly count unique documents per individual value
	switch s.config.DBEngine {
	case "postgresql", "postgres":
		query = fmt.Sprintf(`
			SELECT 
				%s as value,
				document_id
			FROM documents_customfieldinstance
			WHERE field_id = $1 
				AND deleted_at IS NULL
				AND %s IS NOT NULL
				AND %s != ''
		`, valueColumn, valueColumn, valueColumn)
		args = []interface{}{fieldID}
	case "mysql", "mariadb", "sqlite", "sqlite3":
		query = fmt.Sprintf(`
			SELECT 
				%s as value,
				document_id
			FROM documents_customfieldinstance
			WHERE field_id = ? 
				AND deleted_at IS NULL
				AND %s IS NOT NULL
				AND %s != ''
		`, valueColumn, valueColumn, valueColumn)
		args = []interface{}{fieldID}
	default:
		return nil, fmt.Errorf("unsupported database engine: %s", s.config.DBEngine)
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query field values: %w", err)
	}
	defer rows.Close()

	// Map to aggregate individual values and their document counts
	// Key: individual value (e.g., "Dawson Davies")
	// Value: set of document IDs that contain this value
	valueDocumentMap := make(map[string]map[int]bool)

	for rows.Next() {
		var value string
		var documentID int
		if err := rows.Scan(&value, &documentID); err != nil {
			continue
		}

		// Parse comma or colon separated values
		parts := parseValueList(value)
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part != "" {
				// Initialize map for this value if it doesn't exist
				if valueDocumentMap[part] == nil {
					valueDocumentMap[part] = make(map[int]bool)
				}
				// Add this document ID to the set for this value
				valueDocumentMap[part][documentID] = true
			}
		}
	}

	// Convert map to slice, counting unique documents per value
	values := []CustomFieldValueOption{}
	for value, documentSet := range valueDocumentMap {
		values = append(values, CustomFieldValueOption{
			ID:    generateID(value),
			Label: value,
			Count: len(documentSet), // Count of unique documents containing this value
		})
	}

	// Sort values based on sortBy and sortOrder parameters
	values = sortValues(values, sortBy, sortOrder, ignoreCase)

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

// SearchFieldValues searches for values matching a query string
func (s *Service) SearchFieldValues(fieldID int, query string, sortBy string, sortOrder string, ignoreCase bool) ([]CustomFieldValueOption, error) {
	// Get all values first
	response, err := s.GetFieldValues(fieldID, sortBy, sortOrder, ignoreCase)
	if err != nil {
		return nil, err
	}

	// Filter by query string
	filtered := []CustomFieldValueOption{}
	queryLower := strings.ToLower(query)

	for _, value := range response.Values {
		valueLabel := value.Label
		queryStr := query
		if ignoreCase {
			valueLabel = strings.ToLower(valueLabel)
			queryStr = queryLower
		}
		if strings.Contains(valueLabel, queryStr) {
			filtered = append(filtered, value)
		}
	}

	// Re-sort filtered results
	filtered = sortValues(filtered, sortBy, sortOrder, ignoreCase)

	return filtered, nil
}

// GetValueCounts retrieves value counts with optional filter rules applied
func (s *Service) GetValueCounts(fieldID int, filterRulesJSON string, sortBy string, sortOrder string, ignoreCase bool) ([]CustomFieldValueOption, error) {
	// For now, return all values with counts
	// In the future, we can apply filter rules to count only matching documents
	response, err := s.GetFieldValues(fieldID, sortBy, sortOrder, ignoreCase)
	if err != nil {
		return nil, err
	}

	return response.Values, nil
}

// HTTP Handlers for Custom Field Values
func (s *Service) handleGetFieldValues(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fieldIDStr := vars["fieldId"]

	fieldID, err := strconv.Atoi(fieldIDStr)
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid field ID")
		return
	}

	// Parse query parameters
	sortBy := r.URL.Query().Get("sort_by")
	sortOrder := r.URL.Query().Get("sort_order")
	ignoreCaseStr := r.URL.Query().Get("ignore_case")
	ignoreCase := ignoreCaseStr == "true" || ignoreCaseStr == "1"

	response, err := s.GetFieldValues(fieldID, sortBy, sortOrder, ignoreCase)
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

	// Parse query parameters
	sortBy := r.URL.Query().Get("sort_by")
	sortOrder := r.URL.Query().Get("sort_order")
	ignoreCaseStr := r.URL.Query().Get("ignore_case")
	ignoreCase := ignoreCaseStr == "true" || ignoreCaseStr == "1"

	values, err := s.SearchFieldValues(fieldID, query, sortBy, sortOrder, ignoreCase)
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

	// Parse query parameters
	sortBy := r.URL.Query().Get("sort_by")
	sortOrder := r.URL.Query().Get("sort_order")
	ignoreCaseStr := r.URL.Query().Get("ignore_case")
	ignoreCase := ignoreCaseStr == "true" || ignoreCaseStr == "1"

	values, err := s.GetValueCounts(fieldID, filterRulesJSON, sortBy, sortOrder, ignoreCase)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, values)
}

