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

	// Get extra_data for SELECT fields to map option IDs to labels
	var extraDataJSON []byte
	var queryExtraData string
	var argsExtraData []interface{}

	switch s.config.DBEngine {
	case "postgresql", "postgres":
		queryExtraData = "SELECT extra_data FROM documents_customfield WHERE id = $1"
		argsExtraData = []interface{}{fieldID}
	case "mysql", "mariadb", "sqlite", "sqlite3":
		queryExtraData = "SELECT extra_data FROM documents_customfield WHERE id = ?"
		argsExtraData = []interface{}{fieldID}
	default:
		return nil, fmt.Errorf("unsupported database engine: %s", s.config.DBEngine)
	}

	err = s.db.QueryRow(queryExtraData, argsExtraData...).Scan(&extraDataJSON)
	if err != nil && err != sql.ErrNoRows {
		// Log but don't fail - extra_data might not exist for all fields
		fmt.Printf("Warning: Could not fetch extra_data for field %d: %v\n", fieldID, err)
	}

	// Parse select_options if this is a SELECT field
	selectOptionMap := make(map[string]string)
	if dataType == "select" && len(extraDataJSON) > 0 {
		var extraData map[string]interface{}
		if err := json.Unmarshal(extraDataJSON, &extraData); err == nil {
			if selectOptions, ok := extraData["select_options"].([]interface{}); ok {
				for _, opt := range selectOptions {
					if optMap, ok := opt.(map[string]interface{}); ok {
						if optID, ok := optMap["id"].(string); ok {
							if optLabel, ok := optMap["label"].(string); ok {
								selectOptionMap[optID] = optLabel
							}
						}
					}
				}
			}
		}
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
		// For SELECT fields, use the value (option ID) as the ID and look up the label
		// For other field types, use the value as both ID and label
		var optionID string
		var label string

		if dataType == "select" {
			// For SELECT fields, the value is the option ID
			optionID = value
			// Look up the label from selectOptionMap
			if mappedLabel, exists := selectOptionMap[value]; exists {
				label = mappedLabel
			} else {
				// Fallback to value if label not found (shouldn't happen, but handle gracefully)
				label = value
			}
		} else {
			// For non-SELECT fields, generate an ID and use value as label
			optionID = generateID(value)
			label = value
		}

		values = append(values, CustomFieldValueOption{
			ID:    optionID,
			Label: label,
			Count: len(documentSet), // Count of unique documents containing this value
		})
	}

	// Count documents where the field is blank/null
	var blankCountQuery string
	var blankCountArgs []interface{}

	switch s.config.DBEngine {
	case "postgresql", "postgres":
		blankCountQuery = fmt.Sprintf(`
			SELECT COUNT(DISTINCT d.id)
			FROM documents_document d
			WHERE d.deleted_at IS NULL
			AND NOT EXISTS (
				SELECT 1 FROM documents_customfieldinstance cfi3
				WHERE cfi3.document_id = d.id
				AND cfi3.field_id = $1
				AND cfi3.deleted_at IS NULL
				AND cfi3.%s IS NOT NULL
				AND cfi3.%s != ''
			)
		`, valueColumn, valueColumn)
		blankCountArgs = []interface{}{fieldID}
	case "mysql", "mariadb", "sqlite", "sqlite3":
		blankCountQuery = fmt.Sprintf(`
			SELECT COUNT(DISTINCT d.id)
			FROM documents_document d
			WHERE d.deleted_at IS NULL
			AND NOT EXISTS (
				SELECT 1 FROM documents_customfieldinstance cfi3
				WHERE cfi3.document_id = d.id
				AND cfi3.field_id = ?
				AND cfi3.deleted_at IS NULL
				AND cfi3.%s IS NOT NULL
				AND cfi3.%s != ''
			)
		`, valueColumn, valueColumn)
		blankCountArgs = []interface{}{fieldID}
	default:
		return nil, fmt.Errorf("unsupported database engine: %s", s.config.DBEngine)
	}

	var blankCount int
	if err := s.db.QueryRow(blankCountQuery, blankCountArgs...).Scan(&blankCount); err == nil {
		if blankCount > 0 {
			// Add blank/null option
			values = append(values, CustomFieldValueOption{
				ID:    "__blank__",
				Label: "(Blank)",
				Count: blankCount,
			})
		}
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

// buildDocumentFilterQuery builds a WHERE clause to filter documents based on filter rules
// Returns the WHERE clause and arguments, excluding filters for the specified fieldID
func (s *Service) buildDocumentFilterQuery(filterRulesJSON string, excludeFieldID int) (string, []interface{}, error) {
	if filterRulesJSON == "" {
		return "", nil, nil
	}

	// Parse filter rules JSON
	var filterRules []map[string]interface{}
	if err := json.Unmarshal([]byte(filterRulesJSON), &filterRules); err != nil {
		return "", nil, fmt.Errorf("failed to parse filter rules: %w", err)
	}

	if len(filterRules) == 0 {
		return "", nil, nil
	}

	var conditions []string
	var args []interface{}
	argIndex := 1
	usePostgres := s.config.DBEngine == "postgresql" || s.config.DBEngine == "postgres"

	// Filter rule type constants (matching frontend)
	const (
		FILTER_CORRESPONDENT       = 1
		FILTER_DOCUMENT_TYPE       = 2
		FILTER_HAS_TAGS_ANY        = 3
		FILTER_STORAGE_PATH        = 4
		FILTER_OWNER_ANY           = 5
		FILTER_CREATED_AFTER       = 6
		FILTER_CREATED_BEFORE      = 7
		FILTER_ASN                 = 8
		FILTER_IS_IN_INBOX         = 9
		FILTER_CUSTOM_FIELDS_QUERY = 42
	)

	for _, rule := range filterRules {
		ruleType, ok := rule["rule_type"].(float64)
		if !ok {
			continue
		}
		value, ok := rule["value"].(string)
		if !ok {
			continue
		}

		switch int(ruleType) {
		case FILTER_CORRESPONDENT:
			// Filter by correspondent ID
			if usePostgres {
				conditions = append(conditions, fmt.Sprintf("d.correspondent_id = $%d", argIndex))
			} else {
				conditions = append(conditions, "d.correspondent_id = ?")
			}
			args = append(args, value)
			argIndex++

		case FILTER_DOCUMENT_TYPE:
			// Filter by document type ID (column is document_type_id, not category_id)
			if usePostgres {
				conditions = append(conditions, fmt.Sprintf("d.document_type_id = $%d", argIndex))
			} else {
				conditions = append(conditions, "d.document_type_id = ?")
			}
			args = append(args, value)
			argIndex++

		case FILTER_HAS_TAGS_ANY:
			// Filter by tag ID (using many-to-many relationship)
			if usePostgres {
				conditions = append(conditions, fmt.Sprintf("EXISTS (SELECT 1 FROM documents_document_tags dt WHERE dt.document_id = d.id AND dt.tag_id = $%d)", argIndex))
			} else {
				conditions = append(conditions, "EXISTS (SELECT 1 FROM documents_document_tags dt WHERE dt.document_id = d.id AND dt.tag_id = ?)")
			}
			args = append(args, value)
			argIndex++

		case FILTER_STORAGE_PATH:
			// Filter by storage path ID
			if usePostgres {
				conditions = append(conditions, fmt.Sprintf("d.storage_path_id = $%d", argIndex))
			} else {
				conditions = append(conditions, "d.storage_path_id = ?")
			}
			args = append(args, value)
			argIndex++

		case FILTER_OWNER_ANY:
			// Filter by owner ID (owner is a ForeignKey to User)
			if usePostgres {
				conditions = append(conditions, fmt.Sprintf("d.owner_id = $%d", argIndex))
			} else {
				conditions = append(conditions, "d.owner_id = ?")
			}
			args = append(args, value)
			argIndex++

		case FILTER_CREATED_AFTER:
			// Filter by created date >= value
			if usePostgres {
				conditions = append(conditions, fmt.Sprintf("d.created >= $%d::date", argIndex))
			} else {
				conditions = append(conditions, "d.created >= ?")
			}
			args = append(args, value)
			argIndex++

		case FILTER_CREATED_BEFORE:
			// Filter by created date <= value
			if usePostgres {
				conditions = append(conditions, fmt.Sprintf("d.created <= $%d::date", argIndex))
			} else {
				conditions = append(conditions, "d.created <= ?")
			}
			args = append(args, value)
			argIndex++

		case FILTER_ASN:
			// Filter by ASN
			if usePostgres {
				conditions = append(conditions, fmt.Sprintf("d.archive_serial_number = $%d", argIndex))
			} else {
				conditions = append(conditions, "d.archive_serial_number = ?")
			}
			args = append(args, value)
			argIndex++

		case FILTER_IS_IN_INBOX:
			// Filter by inbox status (is_in_inbox = true)
			if usePostgres {
				conditions = append(conditions, "d.is_in_inbox = true")
			} else {
				conditions = append(conditions, "d.is_in_inbox = 1")
			}

		case FILTER_CUSTOM_FIELDS_QUERY:
			// Parse custom field query JSON
			// Format: ["fieldId", "operator", value] or ["AND", [query1, query2]]
			var customFieldQuery interface{}
			if err := json.Unmarshal([]byte(value), &customFieldQuery); err == nil {
				// Build conditions for custom field filters, excluding the current field
				customConditions, customArgs, customArgIndex := s.buildCustomFieldConditions(customFieldQuery, excludeFieldID, argIndex, usePostgres)
				if len(customConditions) > 0 {
					conditions = append(conditions, customConditions...)
					args = append(args, customArgs...)
					argIndex = customArgIndex
				}
			}
		}
	}

	if len(conditions) == 0 {
		return "", nil, nil
	}

	whereClause := "WHERE " + strings.Join(conditions, " AND ")
	return whereClause, args, nil
}

// buildCustomFieldConditions builds SQL conditions for custom field filters
// Excludes filters for the specified excludeFieldID
func (s *Service) buildCustomFieldConditions(query interface{}, excludeFieldID int, startArgIndex int, usePostgres bool) ([]string, []interface{}, int) {
	var conditions []string
	var args []interface{}
	argIndex := startArgIndex

	queryArray, ok := query.([]interface{})
	if !ok {
		return conditions, args, argIndex
	}

	// Check if it's an AND or OR operator
	if len(queryArray) > 0 {
		if operator, ok := queryArray[0].(string); ok {
			if operator == "AND" {
				// Process all sub-queries with AND
				if subQueries, ok := queryArray[1].([]interface{}); ok {
					for _, subQuery := range subQueries {
						subConditions, subArgs, newArgIndex := s.buildCustomFieldConditions(subQuery, excludeFieldID, argIndex, usePostgres)
						conditions = append(conditions, subConditions...)
						args = append(args, subArgs...)
						argIndex = newArgIndex
					}
				}
				return conditions, args, argIndex
			} else if operator == "OR" {
				// Process all sub-queries with OR
				if subQueries, ok := queryArray[1].([]interface{}); ok {
					var orConditions []string
					for _, subQuery := range subQueries {
						subConditions, subArgs, newArgIndex := s.buildCustomFieldConditions(subQuery, excludeFieldID, argIndex, usePostgres)
						if len(subConditions) > 0 {
							// Wrap each condition in parentheses and join with OR
							for _, cond := range subConditions {
								orConditions = append(orConditions, fmt.Sprintf("(%s)", cond))
							}
							args = append(args, subArgs...)
							argIndex = newArgIndex
						}
					}
					if len(orConditions) > 0 {
						// Combine OR conditions into a single condition, wrapped in parentheses
						// This ensures proper operator precedence when combined with AND
						combinedOrCondition := strings.Join(orConditions, " OR ")
						conditions = append(conditions, fmt.Sprintf("(%s)", combinedOrCondition))
					}
				}
				return conditions, args, argIndex
			}
		}
	}

	// Single query: [fieldId, "operator", value]
	if len(queryArray) >= 3 {
		fieldIDFloat, ok := queryArray[0].(float64)
		if !ok {
			return conditions, args, argIndex
		}
		fieldID := int(fieldIDFloat)

		// Skip if this is the field we're querying
		if fieldID == excludeFieldID {
			return conditions, args, argIndex
		}

		operator, ok := queryArray[1].(string)
		if !ok {
			return conditions, args, argIndex
		}

		// Build condition based on operator
		switch operator {
		case "exists":
			// Field exists (is not null)
			if usePostgres {
				conditions = append(conditions, fmt.Sprintf("EXISTS (SELECT 1 FROM documents_customfieldinstance cfi2 WHERE cfi2.document_id = d.id AND cfi2.field_id = %d AND cfi2.deleted_at IS NULL)", fieldID))
			} else {
				conditions = append(conditions, fmt.Sprintf("EXISTS (SELECT 1 FROM documents_customfieldinstance cfi2 WHERE cfi2.document_id = d.id AND cfi2.field_id = %d AND cfi2.deleted_at IS NULL)", fieldID))
			}

		case "isnull":
			// Field is null or empty - check both missing instances and instances with NULL/empty values
			// First, get the field's data type to determine which value column to check
			var dataType string
			switch s.config.DBEngine {
			case "postgresql", "postgres":
				if err := s.db.QueryRow("SELECT data_type FROM documents_customfield WHERE id = $1", fieldID).Scan(&dataType); err != nil {
					fmt.Printf("[buildCustomFieldConditions] Warning: Could not fetch data_type for field %d: %v\n", fieldID, err)
					dataType = "string" // Default fallback
				}
			case "mysql", "mariadb", "sqlite", "sqlite3":
				if err := s.db.QueryRow("SELECT data_type FROM documents_customfield WHERE id = ?", fieldID).Scan(&dataType); err != nil {
					fmt.Printf("[buildCustomFieldConditions] Warning: Could not fetch data_type for field %d: %v\n", fieldID, err)
					dataType = "string" // Default fallback
				}
			}

			valueColumn := getValueColumnName(dataType)

			// Check for documents that either:
			// 1. Don't have a custom field instance for this field, OR
			// 2. Have an instance but the value column is NULL or empty
			if usePostgres {
				conditions = append(conditions, fmt.Sprintf("NOT EXISTS (SELECT 1 FROM documents_customfieldinstance cfi2 WHERE cfi2.document_id = d.id AND cfi2.field_id = %d AND cfi2.deleted_at IS NULL AND cfi2.%s IS NOT NULL AND cfi2.%s != '')", fieldID, valueColumn, valueColumn))
			} else {
				conditions = append(conditions, fmt.Sprintf("NOT EXISTS (SELECT 1 FROM documents_customfieldinstance cfi2 WHERE cfi2.document_id = d.id AND cfi2.field_id = %d AND cfi2.deleted_at IS NULL AND cfi2.%s IS NOT NULL AND cfi2.%s != '')", fieldID, valueColumn, valueColumn))
			}

		case "in":
			// Field value in list
			if values, ok := queryArray[2].([]interface{}); ok && len(values) > 0 {
				// Check if this is a select field and map labels to option IDs
				var dataType string
				var extraDataJSON []byte

				switch s.config.DBEngine {
				case "postgresql", "postgres":
					if err := s.db.QueryRow("SELECT data_type, extra_data FROM documents_customfield WHERE id = $1", fieldID).Scan(&dataType, &extraDataJSON); err != nil {
						// If we can't fetch field metadata, proceed without label mapping
						fmt.Printf("[buildCustomFieldConditions] Warning: Could not fetch field metadata for field %d: %v\n", fieldID, err)
						dataType = ""
					}
				case "mysql", "mariadb", "sqlite", "sqlite3":
					if err := s.db.QueryRow("SELECT data_type, extra_data FROM documents_customfield WHERE id = ?", fieldID).Scan(&dataType, &extraDataJSON); err != nil {
						// If we can't fetch field metadata, proceed without label mapping
						fmt.Printf("[buildCustomFieldConditions] Warning: Could not fetch field metadata for field %d: %v\n", fieldID, err)
						dataType = ""
					}
				}

				// Build label -> option ID map for select fields
				labelToOptionIDMap := make(map[string]string)
				if dataType == "select" && len(extraDataJSON) > 0 {
					var extraData map[string]interface{}
					if err := json.Unmarshal(extraDataJSON, &extraData); err == nil {
						if selectOptions, ok := extraData["select_options"].([]interface{}); ok {
							for _, opt := range selectOptions {
								if optMap, ok := opt.(map[string]interface{}); ok {
									if optID, ok := optMap["id"].(string); ok {
										if optLabel, ok := optMap["label"].(string); ok {
											labelToOptionIDMap[optLabel] = optID
										}
									}
								}
							}
						}
					}
				}

				// Determine the correct value column based on data type
				valueColumn := getValueColumnName(dataType)
				fmt.Printf("[buildCustomFieldConditions] Field %d: dataType=%s, valueColumn=%s, originalValues=%v\n", fieldID, dataType, valueColumn, values)

				placeholders := []string{}
				for _, val := range values {
					valStr := fmt.Sprintf("%v", val)
					originalValStr := valStr

					// For select fields, map label to option ID
					if dataType == "select" {
						if optionID, found := labelToOptionIDMap[valStr]; found {
							valStr = optionID
							fmt.Printf("[buildCustomFieldConditions] Field %d: Mapped label '%s' to option ID '%s'\n", fieldID, originalValStr, valStr)
						} else {
							fmt.Printf("[buildCustomFieldConditions] Field %d: Label '%s' not found in map, using as-is (might already be an ID)\n", fieldID, valStr)
						}
					}

					if usePostgres {
						placeholders = append(placeholders, fmt.Sprintf("$%d", argIndex))
					} else {
						placeholders = append(placeholders, "?")
					}
					args = append(args, valStr)
					argIndex++
				}
				placeholderStr := strings.Join(placeholders, ", ")
				conditions = append(conditions, fmt.Sprintf("EXISTS (SELECT 1 FROM documents_customfieldinstance cfi2 WHERE cfi2.document_id = d.id AND cfi2.field_id = %d AND cfi2.%s IN (%s) AND cfi2.deleted_at IS NULL)", fieldID, valueColumn, placeholderStr))
				fmt.Printf("[buildCustomFieldConditions] Field %d: Built condition with valueColumn=%s, args=%v\n", fieldID, valueColumn, args)
			}

		case "range":
			// Date range
			if dateRange, ok := queryArray[2].([]interface{}); ok && len(dateRange) >= 2 {
				startDate := fmt.Sprintf("%v", dateRange[0])
				endDate := fmt.Sprintf("%v", dateRange[1])
				if usePostgres {
					conditions = append(conditions, fmt.Sprintf("EXISTS (SELECT 1 FROM documents_customfieldinstance cfi2 WHERE cfi2.document_id = d.id AND cfi2.field_id = %d AND cfi2.value_date >= '%s'::date AND cfi2.value_date <= '%s'::date AND cfi2.deleted_at IS NULL)", fieldID, startDate, endDate))
				} else {
					conditions = append(conditions, fmt.Sprintf("EXISTS (SELECT 1 FROM documents_customfieldinstance cfi2 WHERE cfi2.document_id = d.id AND cfi2.field_id = %d AND cfi2.value_date >= '%s' AND cfi2.value_date <= '%s' AND cfi2.deleted_at IS NULL)", fieldID, startDate, endDate))
				}
			}

		case "gte":
			// Greater than or equal
			val := fmt.Sprintf("%v", queryArray[2])
			if usePostgres {
				conditions = append(conditions, fmt.Sprintf("EXISTS (SELECT 1 FROM documents_customfieldinstance cfi2 WHERE cfi2.document_id = d.id AND cfi2.field_id = %d AND cfi2.value_date >= '%s'::date AND cfi2.deleted_at IS NULL)", fieldID, val))
			} else {
				conditions = append(conditions, fmt.Sprintf("EXISTS (SELECT 1 FROM documents_customfieldinstance cfi2 WHERE cfi2.document_id = d.id AND cfi2.field_id = %d AND cfi2.value_date >= '%s' AND cfi2.deleted_at IS NULL)", fieldID, val))
			}

		case "lte":
			// Less than or equal
			val := fmt.Sprintf("%v", queryArray[2])
			if usePostgres {
				conditions = append(conditions, fmt.Sprintf("EXISTS (SELECT 1 FROM documents_customfieldinstance cfi2 WHERE cfi2.document_id = d.id AND cfi2.field_id = %d AND cfi2.value_date <= '%s'::date AND cfi2.deleted_at IS NULL)", fieldID, val))
			} else {
				conditions = append(conditions, fmt.Sprintf("EXISTS (SELECT 1 FROM documents_customfieldinstance cfi2 WHERE cfi2.document_id = d.id AND cfi2.field_id = %d AND cfi2.value_date <= '%s' AND cfi2.deleted_at IS NULL)", fieldID, val))
			}
		}
	}

	return conditions, args, argIndex
}

// GetValueCounts retrieves value counts with optional filter rules applied
func (s *Service) GetValueCounts(fieldID int, filterRulesJSON string, sortBy string, sortOrder string, ignoreCase bool) ([]CustomFieldValueOption, error) {
	// Get field metadata (same as GetFieldValues)
	var fieldName string
	var dataType string
	var extraDataJSON []byte

	switch s.config.DBEngine {
	case "postgresql", "postgres":
		err := s.db.QueryRow("SELECT name, data_type, extra_data FROM documents_customfield WHERE id = $1", fieldID).Scan(&fieldName, &dataType, &extraDataJSON)
		if err != nil {
			return nil, fmt.Errorf("failed to get field info: %w", err)
		}
	case "mysql", "mariadb", "sqlite", "sqlite3":
		err := s.db.QueryRow("SELECT name, data_type, extra_data FROM documents_customfield WHERE id = ?", fieldID).Scan(&fieldName, &dataType, &extraDataJSON)
		if err != nil {
			return nil, fmt.Errorf("failed to get field info: %w", err)
		}
	}

	// Parse select_options for SELECT fields
	selectOptionMap := make(map[string]string)
	if dataType == "select" && len(extraDataJSON) > 0 {
		var extraData map[string]interface{}
		if err := json.Unmarshal(extraDataJSON, &extraData); err == nil {
			if selectOptions, ok := extraData["select_options"].([]interface{}); ok {
				for _, opt := range selectOptions {
					if optMap, ok := opt.(map[string]interface{}); ok {
						if optID, ok := optMap["id"].(string); ok {
							if optLabel, ok := optMap["label"].(string); ok {
								selectOptionMap[optID] = optLabel
							}
						}
					}
				}
			}
		}
	}

	valueColumn := getValueColumnName(dataType)

	// Build document filter query (excluding current field)
	docFilterWhere, docFilterArgs, err := s.buildDocumentFilterQuery(filterRulesJSON, fieldID)
	if err != nil {
		// If filter parsing fails, fall back to unfiltered query
		fmt.Printf("[GetValueCounts] Error building document filter query for field %d: %v\n", fieldID, err)
		docFilterWhere = ""
		docFilterArgs = nil
	} else {
		fmt.Printf("[GetValueCounts] Field %d: docFilterWhere=%s, docFilterArgs=%v\n", fieldID, docFilterWhere, docFilterArgs)
	}

	// Build the main query with optional document filtering
	var query string
	var args []interface{}

	if docFilterWhere != "" {
		// Join with documents_document to apply filters
		switch s.config.DBEngine {
		case "postgresql", "postgres":
			query = fmt.Sprintf(`
				SELECT 
					cfi.%s as value,
					cfi.document_id
				FROM documents_customfieldinstance cfi
				INNER JOIN documents_document d ON cfi.document_id = d.id
				%s
				AND cfi.field_id = $%d
				AND cfi.deleted_at IS NULL
				AND cfi.%s IS NOT NULL
				AND cfi.%s != ''
				AND d.deleted_at IS NULL
			`, valueColumn, docFilterWhere, len(docFilterArgs)+1, valueColumn, valueColumn)
			args = append(docFilterArgs, fieldID)
		case "mysql", "mariadb", "sqlite", "sqlite3":
			query = fmt.Sprintf(`
				SELECT 
					cfi.%s as value,
					cfi.document_id
				FROM documents_customfieldinstance cfi
				INNER JOIN documents_document d ON cfi.document_id = d.id
				%s
				AND cfi.field_id = ?
				AND cfi.deleted_at IS NULL
				AND cfi.%s IS NOT NULL
				AND cfi.%s != ''
				AND d.deleted_at IS NULL
			`, valueColumn, docFilterWhere, valueColumn, valueColumn)
			args = append(docFilterArgs, fieldID)
		}
	} else {
		// No filters, use simple query
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
		}
	}

	fmt.Printf("[GetValueCounts] Field %d: Executing query: %s\n", fieldID, query)
	fmt.Printf("[GetValueCounts] Field %d: Query args: %v (len=%d)\n", fieldID, args, len(args))

	// Debug: Test if the filter is actually matching any documents
	if docFilterWhere != "" {
		testQuery := fmt.Sprintf("SELECT COUNT(*) FROM documents_document d %s", docFilterWhere)
		var testCount int
		if err := s.db.QueryRow(testQuery, docFilterArgs...).Scan(&testCount); err == nil {
			fmt.Printf("[GetValueCounts] Field %d: Filter matches %d documents\n", fieldID, testCount)
		} else {
			fmt.Printf("[GetValueCounts] Field %d: Error testing filter: %v\n", fieldID, err)
		}
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		fmt.Printf("[GetValueCounts] Field %d: Query error: %v\n", fieldID, err)
		return nil, fmt.Errorf("failed to query field values: %w", err)
	}
	defer rows.Close()

	// Aggregate values and counts
	valueDocumentMap := make(map[string]map[int]bool)
	rowCount := 0
	for rows.Next() {
		rowCount++
		var value string
		var documentID int
		if err := rows.Scan(&value, &documentID); err != nil {
			continue
		}

		parts := parseValueList(value)
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part != "" {
				if valueDocumentMap[part] == nil {
					valueDocumentMap[part] = make(map[int]bool)
				}
				valueDocumentMap[part][documentID] = true
			}
		}
	}

	// Convert to slice
	values := []CustomFieldValueOption{}
	for value, documentSet := range valueDocumentMap {
		var optionID string
		var label string

		if dataType == "select" {
			optionID = value
			if mappedLabel, exists := selectOptionMap[value]; exists {
				label = mappedLabel
			} else {
				label = value
			}
		} else {
			optionID = generateID(value)
			label = value
		}

		values = append(values, CustomFieldValueOption{
			ID:    optionID,
			Label: label,
			Count: len(documentSet),
		})
	}

	fmt.Printf("[GetValueCounts] Field %d: Processed %d rows, found %d unique values\n", fieldID, rowCount, len(values))

	// Count documents where the field is blank/null
	// This includes documents that either:
	// 1. Don't have a custom field instance for this field
	// 2. Have a custom field instance but the value column is NULL or empty
	var blankCountQuery string
	var blankCountArgs []interface{}

	if docFilterWhere != "" {
		// With filters: count documents matching filters that don't have this field or have it blank
		switch s.config.DBEngine {
		case "postgresql", "postgres":
			blankCountQuery = fmt.Sprintf(`
				SELECT COUNT(DISTINCT d.id)
				FROM documents_document d
				%s
				AND d.deleted_at IS NULL
				AND NOT EXISTS (
					SELECT 1 FROM documents_customfieldinstance cfi3
					WHERE cfi3.document_id = d.id
					AND cfi3.field_id = $%d
					AND cfi3.deleted_at IS NULL
					AND cfi3.%s IS NOT NULL
					AND cfi3.%s != ''
				)
			`, docFilterWhere, len(docFilterArgs)+1, valueColumn, valueColumn)
			blankCountArgs = append(docFilterArgs, fieldID)
		case "mysql", "mariadb", "sqlite", "sqlite3":
			blankCountQuery = fmt.Sprintf(`
				SELECT COUNT(DISTINCT d.id)
				FROM documents_document d
				%s
				AND d.deleted_at IS NULL
				AND NOT EXISTS (
					SELECT 1 FROM documents_customfieldinstance cfi3
					WHERE cfi3.document_id = d.id
					AND cfi3.field_id = ?
					AND cfi3.deleted_at IS NULL
					AND cfi3.%s IS NOT NULL
					AND cfi3.%s != ''
				)
			`, docFilterWhere, valueColumn, valueColumn)
			blankCountArgs = append(docFilterArgs, fieldID)
		}
	} else {
		// Without filters: count all documents that don't have this field or have it blank
		switch s.config.DBEngine {
		case "postgresql", "postgres":
			blankCountQuery = fmt.Sprintf(`
				SELECT COUNT(DISTINCT d.id)
				FROM documents_document d
				WHERE d.deleted_at IS NULL
				AND NOT EXISTS (
					SELECT 1 FROM documents_customfieldinstance cfi3
					WHERE cfi3.document_id = d.id
					AND cfi3.field_id = $1
					AND cfi3.deleted_at IS NULL
					AND cfi3.%s IS NOT NULL
					AND cfi3.%s != ''
				)
			`, valueColumn, valueColumn)
			blankCountArgs = []interface{}{fieldID}
		case "mysql", "mariadb", "sqlite", "sqlite3":
			blankCountQuery = fmt.Sprintf(`
				SELECT COUNT(DISTINCT d.id)
				FROM documents_document d
				WHERE d.deleted_at IS NULL
				AND NOT EXISTS (
					SELECT 1 FROM documents_customfieldinstance cfi3
					WHERE cfi3.document_id = d.id
					AND cfi3.field_id = ?
					AND cfi3.deleted_at IS NULL
					AND cfi3.%s IS NOT NULL
					AND cfi3.%s != ''
				)
			`, valueColumn, valueColumn)
			blankCountArgs = []interface{}{fieldID}
		}
	}

	var blankCount int
	if err := s.db.QueryRow(blankCountQuery, blankCountArgs...).Scan(&blankCount); err == nil {
		if blankCount > 0 {
			// Add blank/null option
			values = append(values, CustomFieldValueOption{
				ID:    "__blank__",
				Label: "(Blank)",
				Count: blankCount,
			})
			fmt.Printf("[GetValueCounts] Field %d: Found %d documents with blank/null values\n", fieldID, blankCount)
		}
	} else {
		fmt.Printf("[GetValueCounts] Field %d: Error counting blank values: %v\n", fieldID, err)
	}

	// Sort values (default to count desc for context-aware filtering)
	if sortBy == "" {
		sortBy = "count"
	}
	if sortOrder == "" {
		sortOrder = "desc"
	}
	values = sortValues(values, sortBy, sortOrder, ignoreCase)

	fmt.Printf("[GetValueCounts] Field %d: Returning %d sorted values (including blank)\n", fieldID, len(values))

	return values, nil
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
