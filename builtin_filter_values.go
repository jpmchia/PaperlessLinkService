package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

// BuiltinFilterValueOption represents a filter option with count
type BuiltinFilterValueOption struct {
	ID    interface{} `json:"id"` // Can be int (for IDs) or string (for ASN, owner)
	Label string      `json:"label"`
	Count int         `json:"count"`
}

// GetBuiltinFilterValues retrieves filter values with counts for built-in fields
// filterType: "correspondent", "document_type", "tag", "storage_path", "owner", "asn"
func (s *Service) GetBuiltinFilterValues(filterType string, filterRulesJSON string) ([]BuiltinFilterValueOption, error) {
	// Map filter type to rule type for exclusion
	const (
		FILTER_CORRESPONDENT = 1
		FILTER_DOCUMENT_TYPE = 2
		FILTER_HAS_TAGS_ANY  = 3
		FILTER_STORAGE_PATH  = 4
		FILTER_OWNER_ANY     = 5
		FILTER_ASN           = 8
	)

	var excludeRuleType int
	switch filterType {
	case "correspondent":
		excludeRuleType = FILTER_CORRESPONDENT
	case "document_type":
		excludeRuleType = FILTER_DOCUMENT_TYPE
	case "tag":
		excludeRuleType = FILTER_HAS_TAGS_ANY
	case "storage_path":
		excludeRuleType = FILTER_STORAGE_PATH
	case "owner":
		excludeRuleType = FILTER_OWNER_ANY
	case "asn":
		excludeRuleType = FILTER_ASN
	default:
		excludeRuleType = 0
	}

	// Build document filter query, excluding the current filter type
	docFilterWhere, docFilterArgs, err := s.buildDocumentFilterQuery(filterRulesJSON, 0, excludeRuleType)
	if err != nil {
		return nil, fmt.Errorf("failed to build filter query: %w", err)
	}

	var query string
	var args []interface{}
	usePostgres := s.config.DBEngine == "postgresql" || s.config.DBEngine == "postgres"

	switch filterType {
	case "correspondent":
		// Query correspondents with document counts
		if docFilterWhere != "" {
			query = fmt.Sprintf(`
				SELECT c.id, c.name, COUNT(DISTINCT d.id) as doc_count
				FROM documents_correspondent c
				INNER JOIN documents_document d ON d.correspondent_id = c.id AND d.deleted_at IS NULL
				WHERE %s
				GROUP BY c.id, c.name
				ORDER BY doc_count DESC, c.name ASC
			`, strings.Replace(docFilterWhere, "WHERE ", "", 1))
			args = docFilterArgs
		} else {
			if usePostgres {
				query = `
					SELECT c.id, c.name, COUNT(DISTINCT d.id) as doc_count
					FROM documents_correspondent c
					LEFT JOIN documents_document d ON d.correspondent_id = c.id AND d.deleted_at IS NULL
					GROUP BY c.id, c.name
					HAVING COUNT(DISTINCT d.id) > 0
					ORDER BY doc_count DESC, c.name ASC
				`
			} else {
				query = `
					SELECT c.id, c.name, COUNT(DISTINCT d.id) as doc_count
					FROM documents_correspondent c
					LEFT JOIN documents_document d ON d.correspondent_id = c.id AND d.deleted_at IS NULL
					GROUP BY c.id, c.name
					HAVING COUNT(DISTINCT d.id) > 0
					ORDER BY doc_count DESC, c.name ASC
				`
			}
			args = []interface{}{}
		}

	case "document_type":
		// Query document types with document counts
		if docFilterWhere != "" {
			query = fmt.Sprintf(`
				SELECT dt.id, dt.name, COUNT(DISTINCT d.id) as doc_count
				FROM documents_documenttype dt
				INNER JOIN documents_document d ON d.document_type_id = dt.id AND d.deleted_at IS NULL
				WHERE %s
				GROUP BY dt.id, dt.name
				ORDER BY doc_count DESC, dt.name ASC
			`, strings.Replace(docFilterWhere, "WHERE ", "", 1))
			args = docFilterArgs
		} else {
			if usePostgres {
				query = `
					SELECT dt.id, dt.name, COUNT(DISTINCT d.id) as doc_count
					FROM documents_documenttype dt
					LEFT JOIN documents_document d ON d.document_type_id = dt.id AND d.deleted_at IS NULL
					GROUP BY dt.id, dt.name
					HAVING COUNT(DISTINCT d.id) > 0
					ORDER BY doc_count DESC, dt.name ASC
				`
			} else {
				query = `
					SELECT dt.id, dt.name, COUNT(DISTINCT d.id) as doc_count
					FROM documents_documenttype dt
					LEFT JOIN documents_document d ON d.document_type_id = dt.id AND d.deleted_at IS NULL
					GROUP BY dt.id, dt.name
					HAVING COUNT(DISTINCT d.id) > 0
					ORDER BY doc_count DESC, dt.name ASC
				`
			}
			args = []interface{}{}
		}

	case "tag":
		// Query tags with document counts
		if docFilterWhere != "" {
			query = fmt.Sprintf(`
				SELECT t.id, t.name, COUNT(DISTINCT d.id) as doc_count
				FROM documents_tag t
				INNER JOIN documents_document_tags dt ON dt.tag_id = t.id
				INNER JOIN documents_document d ON d.id = dt.document_id AND d.deleted_at IS NULL
				WHERE %s
				GROUP BY t.id, t.name
				ORDER BY doc_count DESC, t.name ASC
			`, strings.Replace(docFilterWhere, "WHERE ", "", 1))
			args = docFilterArgs
		} else {
			if usePostgres {
				query = `
					SELECT t.id, t.name, COUNT(DISTINCT d.id) as doc_count
					FROM documents_tag t
					INNER JOIN documents_document_tags dt ON dt.tag_id = t.id
					INNER JOIN documents_document d ON d.id = dt.document_id AND d.deleted_at IS NULL
					GROUP BY t.id, t.name
					ORDER BY doc_count DESC, t.name ASC
				`
			} else {
				query = `
					SELECT t.id, t.name, COUNT(DISTINCT d.id) as doc_count
					FROM documents_tag t
					INNER JOIN documents_document_tags dt ON dt.tag_id = t.id
					INNER JOIN documents_document d ON d.id = dt.document_id AND d.deleted_at IS NULL
					GROUP BY t.id, t.name
					ORDER BY doc_count DESC, t.name ASC
				`
			}
			args = []interface{}{}
		}

	case "storage_path":
		// Query storage paths with document counts
		if docFilterWhere != "" {
			query = fmt.Sprintf(`
				SELECT sp.id, sp.name, COUNT(DISTINCT d.id) as doc_count
				FROM documents_storagepath sp
				INNER JOIN documents_document d ON d.storage_path_id = sp.id AND d.deleted_at IS NULL
				WHERE %s
				GROUP BY sp.id, sp.name
				ORDER BY doc_count DESC, sp.name ASC
			`, strings.Replace(docFilterWhere, "WHERE ", "", 1))
			args = docFilterArgs
		} else {
			if usePostgres {
				query = `
					SELECT sp.id, sp.name, COUNT(DISTINCT d.id) as doc_count
					FROM documents_storagepath sp
					LEFT JOIN documents_document d ON d.storage_path_id = sp.id AND d.deleted_at IS NULL
					GROUP BY sp.id, sp.name
					HAVING COUNT(DISTINCT d.id) > 0
					ORDER BY doc_count DESC, sp.name ASC
				`
			} else {
				query = `
					SELECT sp.id, sp.name, COUNT(DISTINCT d.id) as doc_count
					FROM documents_storagepath sp
					LEFT JOIN documents_document d ON d.storage_path_id = sp.id AND d.deleted_at IS NULL
					GROUP BY sp.id, sp.name
					HAVING COUNT(DISTINCT d.id) > 0
					ORDER BY doc_count DESC, sp.name ASC
				`
			}
			args = []interface{}{}
		}

	case "owner":
		// Query owners (usernames) with document counts
		if docFilterWhere != "" {
			query = fmt.Sprintf(`
				SELECT d.owner_id as username, COUNT(DISTINCT d.id) as doc_count
				FROM documents_document d
				WHERE d.deleted_at IS NULL AND d.owner_id IS NOT NULL AND d.owner_id != '' AND %s
				GROUP BY d.owner_id
				ORDER BY doc_count DESC, d.owner_id ASC
			`, strings.Replace(docFilterWhere, "WHERE ", "", 1))
			args = docFilterArgs
		} else {
			if usePostgres {
				query = `
					SELECT d.owner_id as username, COUNT(DISTINCT d.id) as doc_count
					FROM documents_document d
					WHERE d.deleted_at IS NULL AND d.owner_id IS NOT NULL AND d.owner_id != ''
					GROUP BY d.owner_id
					ORDER BY doc_count DESC, d.owner_id ASC
				`
			} else {
				query = `
					SELECT d.owner_id as username, COUNT(DISTINCT d.id) as doc_count
					FROM documents_document d
					WHERE d.deleted_at IS NULL AND d.owner_id IS NOT NULL AND d.owner_id != ''
					GROUP BY d.owner_id
					ORDER BY doc_count DESC, d.owner_id ASC
				`
			}
			args = []interface{}{}
		}

	case "asn":
		// Query ASN values with document counts
		if docFilterWhere != "" {
			query = fmt.Sprintf(`
				SELECT d.archive_serial_number as asn, COUNT(DISTINCT d.id) as doc_count
				FROM documents_document d
				WHERE d.deleted_at IS NULL AND d.archive_serial_number IS NOT NULL AND %s
				GROUP BY d.archive_serial_number
				ORDER BY doc_count DESC, d.archive_serial_number ASC
			`, strings.Replace(docFilterWhere, "WHERE ", "", 1))
			args = docFilterArgs
		} else {
			if usePostgres {
				query = `
					SELECT d.archive_serial_number as asn, COUNT(DISTINCT d.id) as doc_count
					FROM documents_document d
					WHERE d.deleted_at IS NULL AND d.archive_serial_number IS NOT NULL
					GROUP BY d.archive_serial_number
					ORDER BY doc_count DESC, d.archive_serial_number ASC
				`
			} else {
				query = `
					SELECT d.archive_serial_number as asn, COUNT(DISTINCT d.id) as doc_count
					FROM documents_document d
					WHERE d.deleted_at IS NULL AND d.archive_serial_number IS NOT NULL
					GROUP BY d.archive_serial_number
					ORDER BY doc_count DESC, d.archive_serial_number ASC
				`
			}
			args = []interface{}{}
		}

	default:
		return nil, fmt.Errorf("unsupported filter type: %s", filterType)
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query %s values: %w", filterType, err)
	}
	defer rows.Close()

	var values []BuiltinFilterValueOption
	for rows.Next() {
		var id interface{}
		var label string
		var count int

		if err := rows.Scan(&id, &label, &count); err != nil {
			continue
		}

		values = append(values, BuiltinFilterValueOption{
			ID:    id,
			Label: label,
			Count: count,
		})
	}

	return values, nil
}

func (s *Service) handleGetBuiltinFilterValues(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filterType := vars["filterType"]

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

	values, err := s.GetBuiltinFilterValues(filterType, filterRulesJSON)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, values)
}
