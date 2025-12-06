package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

// ListCustomViews retrieves a list of custom views for a user
func (s *Service) ListCustomViews(userID *int, includeGlobal bool) ([]CustomView, error) {
	log.Printf("[CustomViews] ListCustomViews - UserID: %v, IncludeGlobal: %v", userID, includeGlobal)
	var query string
	var args []interface{}

	// Build query based on user and global flag
	if userID != nil {
		if includeGlobal {
			// Get user's views OR global views
			switch s.config.DBEngine {
			case "postgresql", "postgres":
				query = `
					SELECT id, name, description, column_order, column_sizing, column_visibility,
						column_display_types, filter_rules, filter_visibility, sort_field, sort_reverse,
						is_global, owner_id, username, created, modified, deleted_at
					FROM custom_views
					WHERE deleted_at IS NULL
						AND (owner_id = $1 OR is_global = true)
					ORDER BY created DESC
				`
				args = []interface{}{*userID}
			case "mysql", "mariadb", "sqlite", "sqlite3":
				query = `
					SELECT id, name, description, column_order, column_sizing, column_visibility,
						column_display_types, filter_rules, filter_visibility, sort_field, sort_reverse,
						is_global, owner_id, username, created, modified, deleted_at
					FROM custom_views
					WHERE deleted_at IS NULL
						AND (owner_id = ? OR is_global = 1)
					ORDER BY created DESC
				`
				args = []interface{}{*userID}
			}
		} else {
			// Only user's views
			switch s.config.DBEngine {
			case "postgresql", "postgres":
				query = `
					SELECT id, name, description, column_order, column_sizing, column_visibility,
						column_display_types, filter_rules, filter_visibility, sort_field, sort_reverse,
						is_global, owner_id, username, created, modified, deleted_at
					FROM custom_views
					WHERE deleted_at IS NULL AND owner_id = $1
					ORDER BY created DESC
				`
				args = []interface{}{*userID}
			case "mysql", "mariadb", "sqlite", "sqlite3":
				query = `
					SELECT id, name, description, column_order, column_sizing, column_visibility,
						column_display_types, filter_rules, filter_visibility, sort_field, sort_reverse,
						is_global, owner_id, username, created, modified, deleted_at
					FROM custom_views
					WHERE deleted_at IS NULL AND owner_id = ?
					ORDER BY created DESC
				`
				args = []interface{}{*userID}
			}
		}
	} else {
		// No user ID - return global views only
		switch s.config.DBEngine {
		case "postgresql", "postgres":
			query = `
				SELECT id, name, description, column_order, column_sizing, column_visibility,
					column_display_types, filter_rules, filter_visibility, sort_field, sort_reverse,
					is_global, owner_id, username, created, modified, deleted_at
				FROM custom_views
				WHERE deleted_at IS NULL AND is_global = true
				ORDER BY created DESC
			`
		case "mysql", "mariadb", "sqlite", "sqlite3":
			query = `
				SELECT id, name, description, column_order, column_sizing, column_visibility,
					column_display_types, filter_rules, filter_visibility, sort_field, sort_reverse,
					is_global, owner_id, username, created, modified, deleted_at
				FROM custom_views
				WHERE deleted_at IS NULL AND is_global = 1
				ORDER BY created DESC
			`
		}
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query custom views: %w", err)
	}
	defer rows.Close()

	views := []CustomView{}
	for rows.Next() {
		view, err := s.scanCustomView(rows)
		if err != nil {
			continue
		}
		views = append(views, view)
	}

	return views, nil
}

// GetCustomView retrieves a specific custom view by ID
func (s *Service) GetCustomView(id int) (*CustomView, error) {
	var query string

	switch s.config.DBEngine {
	case "postgresql", "postgres":
		query = `
			SELECT id, name, description, column_order, column_sizing, column_visibility,
				column_display_types, filter_rules, filter_visibility, sort_field, sort_reverse,
				is_global, owner_id, username, created, modified, deleted_at
			FROM custom_views
			WHERE id = $1 AND deleted_at IS NULL
		`
	case "mysql", "mariadb", "sqlite", "sqlite3":
		query = `
			SELECT id, name, description, column_order, column_sizing, column_visibility,
				column_display_types, filter_rules, filter_visibility, sort_field, sort_reverse,
				is_global, owner_id, username, created, modified, deleted_at
			FROM custom_views
			WHERE id = ? AND deleted_at IS NULL
		`
	}

	row := s.db.QueryRow(query, id)
	view, err := s.scanCustomView(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("custom view with id %d not found", id)
		}
		return nil, err
	}

	return &view, nil
}

// CreateCustomView creates a new custom view
func (s *Service) CreateCustomView(view CustomView, userID int, username string) (*CustomView, error) {
	log.Printf("[CustomViews] CreateCustomView - Name: %s, UserID: %d, Username: %s", view.Name, userID, username)
	// Marshal JSON fields
	columnOrderJSON, _ := json.Marshal(view.ColumnOrder)
	columnSizingJSON, _ := json.Marshal(view.ColumnSizing)
	columnVisibilityJSON, _ := json.Marshal(view.ColumnVisibility)
	columnDisplayTypesJSON, _ := json.Marshal(view.ColumnDisplayTypes)
	filterRulesJSON, _ := json.Marshal(view.FilterRules)
	filterVisibilityJSON, _ := json.Marshal(view.FilterVisibility)

	var insertQuery string
	var args []interface{}

	// Set defaults
	isGlobal := false
	if view.IsGlobal != nil {
		isGlobal = *view.IsGlobal
	}
	sortReverse := false
	if view.SortReverse != nil {
		sortReverse = *view.SortReverse
	}

	switch s.config.DBEngine {
	case "postgresql", "postgres":
		insertQuery = `
			INSERT INTO custom_views (name, description, column_order, column_sizing, column_visibility,
				column_display_types, filter_rules, filter_visibility, sort_field, sort_reverse,
				is_global, owner_id, username)
			VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb, $9, $10, $11, $12, $13)
			RETURNING id, created, modified
		`
		args = []interface{}{
			view.Name, view.Description, string(columnOrderJSON), string(columnSizingJSON),
			string(columnVisibilityJSON), string(columnDisplayTypesJSON), string(filterRulesJSON),
			string(filterVisibilityJSON), view.SortField, sortReverse, isGlobal, userID, username,
		}
	case "mysql", "mariadb":
		insertQuery = `
			INSERT INTO custom_views (name, description, column_order, column_sizing, column_visibility,
				column_display_types, filter_rules, filter_visibility, sort_field, sort_reverse,
				is_global, owner_id, username)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`
		args = []interface{}{
			view.Name, view.Description, string(columnOrderJSON), string(columnSizingJSON),
			string(columnVisibilityJSON), string(columnDisplayTypesJSON), string(filterRulesJSON),
			string(filterVisibilityJSON), view.SortField, sortReverse, isGlobal, userID, username,
		}
	case "sqlite", "sqlite3":
		insertQuery = `
			INSERT INTO custom_views (name, description, column_order, column_sizing, column_visibility,
				column_display_types, filter_rules, filter_visibility, sort_field, sort_reverse,
				is_global, owner_id, username)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`
		args = []interface{}{
			view.Name, view.Description, string(columnOrderJSON), string(columnSizingJSON),
			string(columnVisibilityJSON), string(columnDisplayTypesJSON), string(filterRulesJSON),
			string(filterVisibilityJSON), view.SortField, sortReverse, isGlobal, userID, username,
		}
	}

	var newID int
	var created, modified string

	if s.config.DBEngine == "postgresql" || s.config.DBEngine == "postgres" {
		err := s.db.QueryRow(insertQuery, args...).Scan(&newID, &created, &modified)
		if err != nil {
			return nil, fmt.Errorf("failed to create custom view: %w", err)
		}
	} else {
		result, err := s.db.Exec(insertQuery, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to create custom view: %w", err)
		}

		lastID, err := result.LastInsertId()
		if err != nil {
			return nil, fmt.Errorf("failed to get last insert ID: %w", err)
		}
		newID = int(lastID)

		// Fetch created/modified timestamps
		getTimeQuery := "SELECT created, modified FROM custom_views WHERE id = ?"
		s.db.QueryRow(getTimeQuery, newID).Scan(&created, &modified)
	}

	view.ID = &newID
	view.OwnerID = &userID
	view.Username = &username
	view.Created = &created
	view.Modified = &modified

	return &view, nil
}

// UpdateCustomView updates an existing custom view
func (s *Service) UpdateCustomView(id int, updates CustomView, userID int) (*CustomView, error) {
	log.Printf("[CustomViews] UpdateCustomView - ID: %d, UserID: %d", id, userID)
	// Get existing view
	existing, err := s.GetCustomView(id)
	if err != nil {
		return nil, err
	}

	// Check ownership (unless it's global and user is updating global)
	if existing.OwnerID != nil && *existing.OwnerID != userID {
		isGlobal := existing.IsGlobal != nil && *existing.IsGlobal
		if !isGlobal {
			return nil, fmt.Errorf("permission denied: view belongs to another user")
		}
	}

	// Build update query dynamically based on provided fields
	setParts := []string{}
	args := []interface{}{}
	usePostgres := s.config.DBEngine == "postgresql" || s.config.DBEngine == "postgres"
	argIndex := 1

	if updates.Name != "" {
		if usePostgres {
			setParts = append(setParts, fmt.Sprintf("name = $%d", argIndex))
		} else {
			setParts = append(setParts, "name = ?")
		}
		args = append(args, updates.Name)
		argIndex++
		existing.Name = updates.Name
	}
	if updates.Description != nil {
		if usePostgres {
			setParts = append(setParts, fmt.Sprintf("description = $%d", argIndex))
		} else {
			setParts = append(setParts, "description = ?")
		}
		args = append(args, updates.Description)
		argIndex++
		existing.Description = updates.Description
	}
	if updates.ColumnOrder != nil {
		columnOrderJSON, _ := json.Marshal(updates.ColumnOrder)
		if usePostgres {
			setParts = append(setParts, fmt.Sprintf("column_order = $%d::jsonb", argIndex))
		} else {
			setParts = append(setParts, "column_order = ?")
		}
		args = append(args, string(columnOrderJSON))
		argIndex++
		existing.ColumnOrder = updates.ColumnOrder
	}
	if updates.ColumnSizing != nil {
		columnSizingJSON, _ := json.Marshal(updates.ColumnSizing)
		if usePostgres {
			setParts = append(setParts, fmt.Sprintf("column_sizing = $%d::jsonb", argIndex))
		} else {
			setParts = append(setParts, "column_sizing = ?")
		}
		args = append(args, string(columnSizingJSON))
		argIndex++
		existing.ColumnSizing = updates.ColumnSizing
	}
	if updates.ColumnVisibility != nil {
		columnVisibilityJSON, _ := json.Marshal(updates.ColumnVisibility)
		if usePostgres {
			setParts = append(setParts, fmt.Sprintf("column_visibility = $%d::jsonb", argIndex))
		} else {
			setParts = append(setParts, "column_visibility = ?")
		}
		args = append(args, string(columnVisibilityJSON))
		argIndex++
		existing.ColumnVisibility = updates.ColumnVisibility
	}
	if updates.ColumnDisplayTypes != nil {
		columnDisplayTypesJSON, _ := json.Marshal(updates.ColumnDisplayTypes)
		if usePostgres {
			setParts = append(setParts, fmt.Sprintf("column_display_types = $%d::jsonb", argIndex))
		} else {
			setParts = append(setParts, "column_display_types = ?")
		}
		args = append(args, string(columnDisplayTypesJSON))
		argIndex++
		existing.ColumnDisplayTypes = updates.ColumnDisplayTypes
	}
	if updates.FilterRules != nil {
		filterRulesJSON, _ := json.Marshal(updates.FilterRules)
		if usePostgres {
			setParts = append(setParts, fmt.Sprintf("filter_rules = $%d::jsonb", argIndex))
		} else {
			setParts = append(setParts, "filter_rules = ?")
		}
		args = append(args, string(filterRulesJSON))
		argIndex++
		existing.FilterRules = updates.FilterRules
	}
	if updates.FilterVisibility != nil {
		filterVisibilityJSON, _ := json.Marshal(updates.FilterVisibility)
		if usePostgres {
			setParts = append(setParts, fmt.Sprintf("filter_visibility = $%d::jsonb", argIndex))
		} else {
			setParts = append(setParts, "filter_visibility = ?")
		}
		args = append(args, string(filterVisibilityJSON))
		argIndex++
		existing.FilterVisibility = updates.FilterVisibility
	}
	if updates.SortField != nil {
		if usePostgres {
			setParts = append(setParts, fmt.Sprintf("sort_field = $%d", argIndex))
		} else {
			setParts = append(setParts, "sort_field = ?")
		}
		args = append(args, updates.SortField)
		argIndex++
		existing.SortField = updates.SortField
	}
	if updates.SortReverse != nil {
		if usePostgres {
			setParts = append(setParts, fmt.Sprintf("sort_reverse = $%d", argIndex))
		} else {
			setParts = append(setParts, "sort_reverse = ?")
		}
		args = append(args, *updates.SortReverse)
		argIndex++
		existing.SortReverse = updates.SortReverse
	}
	if updates.IsGlobal != nil {
		if usePostgres {
			setParts = append(setParts, fmt.Sprintf("is_global = $%d", argIndex))
		} else {
			setParts = append(setParts, "is_global = ?")
		}
		args = append(args, *updates.IsGlobal)
		argIndex++
		existing.IsGlobal = updates.IsGlobal
	}

	if len(setParts) == 0 {
		return existing, nil // No updates
	}

	// Update modified timestamp
	// For all databases, use CURRENT_TIMESTAMP directly in SQL (not as a parameter)
	if s.config.DBEngine == "mysql" || s.config.DBEngine == "mariadb" {
		setParts = append(setParts, "modified = CURRENT_TIMESTAMP")
	} else if usePostgres {
		// PostgreSQL: Use CURRENT_TIMESTAMP directly in SQL, not as a parameter
		setParts = append(setParts, "modified = CURRENT_TIMESTAMP")
	} else {
		// SQLite
		setParts = append(setParts, "modified = CURRENT_TIMESTAMP")
	}

	var updateQuery string
	if usePostgres {
		updateQuery = fmt.Sprintf("UPDATE custom_views SET %s WHERE id = $%d", strings.Join(setParts, ", "), argIndex)
		args = append(args, id)
	} else {
		updateQuery = fmt.Sprintf("UPDATE custom_views SET %s WHERE id = ?", strings.Join(setParts, ", "))
		args = append(args, id)
	}

	_, err = s.db.Exec(updateQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to update custom view: %w", err)
	}

	// Fetch updated view
	return s.GetCustomView(id)
}

// DeleteCustomView soft-deletes a custom view
func (s *Service) DeleteCustomView(id int, userID int) error {
	log.Printf("[CustomViews] DeleteCustomView - ID: %d, UserID: %d", id, userID)
	// Get existing view to check ownership
	existing, err := s.GetCustomView(id)
	if err != nil {
		return err
	}

	// Check ownership
	if existing.OwnerID != nil && *existing.OwnerID != userID {
		return fmt.Errorf("permission denied: view belongs to another user")
	}

	// Soft delete
	var deleteQuery string
	switch s.config.DBEngine {
	case "postgresql", "postgres":
		deleteQuery = "UPDATE custom_views SET deleted_at = CURRENT_TIMESTAMP WHERE id = $1"
	case "mysql", "mariadb", "sqlite", "sqlite3":
		deleteQuery = "UPDATE custom_views SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?"
	}

	_, err = s.db.Exec(deleteQuery, id)
	if err != nil {
		return fmt.Errorf("failed to delete custom view: %w", err)
	}

	return nil
}

// scanCustomView scans a CustomView from a database row or rows
func (s *Service) scanCustomView(scanner interface{}) (CustomView, error) {
	var view CustomView
	var id sql.NullInt64
	var description, sortField, username, created, modified, deletedAt sql.NullString
	var columnOrderJSON, columnSizingJSON, columnVisibilityJSON, columnDisplayTypesJSON sql.NullString
	var filterRulesJSON, filterVisibilityJSON sql.NullString
	var isGlobal, sortReverse sql.NullBool

	var scanErr error

	switch scanner.(type) {
	case *sql.Row:
		row := scanner.(*sql.Row)
		scanErr = row.Scan(
			&id, &view.Name, &description, &columnOrderJSON, &columnSizingJSON,
			&columnVisibilityJSON, &columnDisplayTypesJSON, &filterRulesJSON,
			&filterVisibilityJSON, &sortField, &sortReverse, &isGlobal,
			&view.OwnerID, &username, &created, &modified, &deletedAt,
		)
	case *sql.Rows:
		rows := scanner.(*sql.Rows)
		scanErr = rows.Scan(
			&id, &view.Name, &description, &columnOrderJSON, &columnSizingJSON,
			&columnVisibilityJSON, &columnDisplayTypesJSON, &filterRulesJSON,
			&filterVisibilityJSON, &sortField, &sortReverse, &isGlobal,
			&view.OwnerID, &username, &created, &modified, &deletedAt,
		)
	default:
		return view, fmt.Errorf("unsupported scanner type")
	}

	if scanErr != nil {
		return view, scanErr
	}

	if id.Valid {
		idInt := int(id.Int64)
		view.ID = &idInt
	}
	if description.Valid {
		view.Description = &description.String
	}
	if sortField.Valid {
		view.SortField = &sortField.String
	}
	if username.Valid {
		view.Username = &username.String
	}
	if created.Valid {
		view.Created = &created.String
	}
	if modified.Valid {
		view.Modified = &modified.String
	}
	if deletedAt.Valid {
		view.DeletedAt = &deletedAt.String
	}
	if isGlobal.Valid {
		view.IsGlobal = &isGlobal.Bool
	}
	if sortReverse.Valid {
		view.SortReverse = &sortReverse.Bool
	}

	// Parse JSON fields
	if columnOrderJSON.Valid {
		json.Unmarshal([]byte(columnOrderJSON.String), &view.ColumnOrder)
	}
	if columnSizingJSON.Valid {
		json.Unmarshal([]byte(columnSizingJSON.String), &view.ColumnSizing)
	}
	if columnVisibilityJSON.Valid {
		json.Unmarshal([]byte(columnVisibilityJSON.String), &view.ColumnVisibility)
	}
	if columnDisplayTypesJSON.Valid {
		json.Unmarshal([]byte(columnDisplayTypesJSON.String), &view.ColumnDisplayTypes)
	}
	if filterRulesJSON.Valid {
		json.Unmarshal([]byte(filterRulesJSON.String), &view.FilterRules)
	}
	if filterVisibilityJSON.Valid {
		json.Unmarshal([]byte(filterVisibilityJSON.String), &view.FilterVisibility)
	}

	return view, nil
}

// HTTP Handlers for Custom Views
func (s *Service) handleListCustomViews(w http.ResponseWriter, r *http.Request) {
	log.Printf("[CustomViews] GET /api/custom_views/ - Request from %s", r.RemoteAddr)
	
	userID, err := getUserIDFromRequest(r)
	if err != nil {
		log.Printf("[CustomViews] Error getting user ID: %v", err)
		respondError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	log.Printf("[CustomViews] User ID: %d", *userID)

	// Include global views by default
	includeGlobal := r.URL.Query().Get("global_only") != "true"
	log.Printf("[CustomViews] Include global views: %v", includeGlobal)

	views, err := s.ListCustomViews(userID, includeGlobal)
	if err != nil {
		log.Printf("[CustomViews] Error listing views: %v", err)
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	log.Printf("[CustomViews] Found %d views", len(views))
	response := CustomViewListResponse{
		Count:   len(views),
		Results: views,
	}

	respondJSON(w, http.StatusOK, response)
	log.Printf("[CustomViews] Successfully returned %d views", len(views))
}

func (s *Service) handleGetCustomView(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	idStr := vars["id"]
	log.Printf("[CustomViews] GET /api/custom_views/%s/ - Request from %s", idStr, r.RemoteAddr)

	id, err := strconv.Atoi(idStr)
	if err != nil {
		log.Printf("[CustomViews] Invalid view ID: %s", idStr)
		respondError(w, http.StatusBadRequest, "Invalid view ID")
		return
	}

	log.Printf("[CustomViews] Fetching view ID: %d", id)
	view, err := s.GetCustomView(id)
	if err != nil {
		log.Printf("[CustomViews] Error getting view %d: %v", id, err)
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	log.Printf("[CustomViews] Successfully retrieved view %d: %s", id, view.Name)
	respondJSON(w, http.StatusOK, view)
}

func (s *Service) handleCreateCustomView(w http.ResponseWriter, r *http.Request) {
	log.Printf("[CustomViews] POST /api/custom_views/ - Request from %s", r.RemoteAddr)
	
	var view CustomView
	if err := json.NewDecoder(r.Body).Decode(&view); err != nil {
		log.Printf("[CustomViews] Error decoding request body: %v", err)
		respondError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %v", err))
		return
	}

	log.Printf("[CustomViews] Creating view: Name=%s, IsGlobal=%v", view.Name, view.IsGlobal)

	if view.Name == "" {
		log.Printf("[CustomViews] Validation error: Name is required")
		respondError(w, http.StatusBadRequest, "Name is required")
		return
	}

	// Set defaults
	if view.ColumnOrder == nil {
		view.ColumnOrder = []interface{}{}
	}
	if view.ColumnSizing == nil {
		view.ColumnSizing = make(map[string]int)
	}
	if view.ColumnVisibility == nil {
		view.ColumnVisibility = make(map[string]bool)
	}
	if view.ColumnDisplayTypes == nil {
		view.ColumnDisplayTypes = make(map[string]string)
	}

	userID, err := getUserIDFromRequest(r)
	if err != nil {
		log.Printf("[CustomViews] Error getting user ID: %v", err)
		respondError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	username := getUsernameFromRequest(r)
	log.Printf("[CustomViews] User ID: %d, Username: %s", *userID, *username)

	created, err := s.CreateCustomView(view, *userID, *username)
	if err != nil {
		log.Printf("[CustomViews] Error creating view: %v", err)
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	log.Printf("[CustomViews] Successfully created view ID: %d, Name: %s", created.ID, created.Name)
	respondJSON(w, http.StatusCreated, created)
}

func (s *Service) handleUpdateCustomView(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	idStr := vars["id"]
	method := r.Method
	log.Printf("[CustomViews] %s /api/custom_views/%s/ - Request from %s", method, idStr, r.RemoteAddr)

	id, err := strconv.Atoi(idStr)
	if err != nil {
		log.Printf("[CustomViews] Invalid view ID: %s", idStr)
		respondError(w, http.StatusBadRequest, "Invalid view ID")
		return
	}

	var updates CustomView
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		log.Printf("[CustomViews] Error decoding request body for view %d: %v", id, err)
		respondError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %v", err))
		return
	}

	log.Printf("[CustomViews] Updating view ID: %d", id)
	userID, err := getUserIDFromRequest(r)
	if err != nil {
		log.Printf("[CustomViews] Error getting user ID: %v", err)
		respondError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	log.Printf("[CustomViews] User ID: %d", *userID)

	updated, err := s.UpdateCustomView(id, updates, *userID)
	if err != nil {
		log.Printf("[CustomViews] Error updating view %d: %v", id, err)
		if strings.Contains(err.Error(), "permission denied") {
			respondError(w, http.StatusForbidden, err.Error())
			return
		}
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	log.Printf("[CustomViews] Successfully updated view ID: %d", id)
	respondJSON(w, http.StatusOK, updated)
}

func (s *Service) handleDeleteCustomView(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	idStr := vars["id"]
	log.Printf("[CustomViews] DELETE /api/custom_views/%s/ - Request from %s", idStr, r.RemoteAddr)

	id, err := strconv.Atoi(idStr)
	if err != nil {
		log.Printf("[CustomViews] Invalid view ID: %s", idStr)
		respondError(w, http.StatusBadRequest, "Invalid view ID")
		return
	}

	userID, err := getUserIDFromRequest(r)
	if err != nil {
		log.Printf("[CustomViews] Error getting user ID: %v", err)
		respondError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	log.Printf("[CustomViews] Deleting view ID: %d, User ID: %d", id, *userID)

	if err := s.DeleteCustomView(id, *userID); err != nil {
		log.Printf("[CustomViews] Error deleting view %d: %v", id, err)
		if strings.Contains(err.Error(), "permission denied") {
			respondError(w, http.StatusForbidden, err.Error())
			return
		}
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	log.Printf("[CustomViews] Successfully deleted view ID: %d", id)
	w.WriteHeader(http.StatusNoContent)
}

