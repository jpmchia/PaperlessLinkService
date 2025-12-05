package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// respondJSON sends a JSON response
func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// respondError sends an error response
func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, ErrorResponse{
		Error:   http.StatusText(status),
		Message: message,
	})
}

// getUserIDFromRequest extracts user ID from request headers
// In production, this should validate JWT tokens or session cookies
func getUserIDFromRequest(r *http.Request) (*int, error) {
	// For now, extract from X-User-ID header if present
	// In production, implement proper authentication
	userIDStr := r.Header.Get("X-User-ID")
	if userIDStr != "" {
		userID, err := strconv.Atoi(userIDStr)
		if err == nil {
			return &userID, nil
		}
	}
	// Default to user ID 1 for development (you may want to return error in production)
	userID := 1
	return &userID, nil
}

// getUsernameFromRequest extracts username from request headers
func getUsernameFromRequest(r *http.Request) *string {
	username := r.Header.Get("X-Username")
	if username != "" {
		return &username
	}
	// Default username for development
	defaultUsername := "admin"
	return &defaultUsername
}

// getValueColumnName returns the column name for a given data type
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

// parseValueList splits a value string by comma, colon, or semicolon
func parseValueList(value string) []string {
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ':' || r == ';'
	})
	return parts
}

// generateID generates a simple hash-based ID for a value
func generateID(value string) string {
	hash := 0
	for _, char := range value {
		hash = hash*31 + int(char)
	}
	return fmt.Sprintf("val-%d", hash)
}

// compareLabels compares two labels, optionally ignoring case
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
func compareLabels(a, b string, ignoreCase bool) int {
	if ignoreCase {
		a = strings.ToLower(a)
		b = strings.ToLower(b)
	}
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// sortValues sorts the values based on sortBy, sortOrder, and ignoreCase parameters
// sortBy: "count" or "label" (default: "count")
// sortOrder: "asc" or "desc" (default: "desc" for count, "asc" for label)
// ignoreCase: if true, case-insensitive comparison for label sorting
func sortValues(values []CustomFieldValueOption, sortBy string, sortOrder string, ignoreCase bool) []CustomFieldValueOption {
	// Default values
	if sortBy == "" {
		sortBy = "count"
	}
	if sortOrder == "" {
		if sortBy == "count" {
			sortOrder = "desc"
		} else {
			sortOrder = "asc"
		}
	}

	// Normalize sortBy and sortOrder
	sortBy = strings.ToLower(sortBy)
	sortOrder = strings.ToLower(sortOrder)

	// Create a copy to avoid modifying the original slice
	sorted := make([]CustomFieldValueOption, len(values))
	copy(sorted, values)

	// Sort based on sortBy
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			var shouldSwap bool

			if sortBy == "count" {
				// Sort by count
				if sortOrder == "asc" {
					shouldSwap = sorted[i].Count > sorted[j].Count ||
						(sorted[i].Count == sorted[j].Count && compareLabels(sorted[i].Label, sorted[j].Label, ignoreCase) > 0)
				} else { // desc
					shouldSwap = sorted[i].Count < sorted[j].Count ||
						(sorted[i].Count == sorted[j].Count && compareLabels(sorted[i].Label, sorted[j].Label, ignoreCase) > 0)
				}
			} else { // sortBy == "label"
				// Sort by label
				labelComparison := compareLabels(sorted[i].Label, sorted[j].Label, ignoreCase)
				if sortOrder == "asc" {
					shouldSwap = labelComparison > 0 ||
						(labelComparison == 0 && sorted[i].Count < sorted[j].Count)
				} else { // desc
					shouldSwap = labelComparison < 0 ||
						(labelComparison == 0 && sorted[i].Count < sorted[j].Count)
				}
			}

			if shouldSwap {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	return sorted
}

