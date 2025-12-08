package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

// ListTagGroups retrieves all tag groups
func (s *Service) ListTagGroups() ([]TagGroup, error) {
	log.Printf("[TagGroups] ListTagGroups")
	var query string

	switch s.config.DBEngine {
	case "postgresql", "postgres":
		query = `
			SELECT id, name, description, created, modified
			FROM tag_groups
			ORDER BY name ASC
		`
	case "mysql", "mariadb", "sqlite", "sqlite3":
		query = `
			SELECT id, name, description, created, modified
			FROM tag_groups
			ORDER BY name ASC
		`
	}

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tag groups: %w", err)
	}
	defer rows.Close()

	groups := []TagGroup{}
	for rows.Next() {
		group, err := s.scanTagGroup(rows)
		if err != nil {
			continue
		}
		// Load tag IDs for this group
		tagIDs, err := s.getTagGroupMemberships(group.ID)
		if err == nil {
			group.TagIDs = tagIDs
		}
		groups = append(groups, group)
	}

	return groups, nil
}

// GetTagGroup retrieves a specific tag group by ID
func (s *Service) GetTagGroup(id int) (*TagGroup, error) {
	var query string

	switch s.config.DBEngine {
	case "postgresql", "postgres":
		query = `
			SELECT id, name, description, created, modified
			FROM tag_groups
			WHERE id = $1
		`
	case "mysql", "mariadb", "sqlite", "sqlite3":
		query = `
			SELECT id, name, description, created, modified
			FROM tag_groups
			WHERE id = ?
		`
	}

	row := s.db.QueryRow(query, id)
	group, err := s.scanTagGroup(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("tag group with id %d not found", id)
		}
		return nil, err
	}

	// Load tag IDs for this group
	tagIDs, err := s.getTagGroupMemberships(&id)
	if err == nil {
		group.TagIDs = tagIDs
	}

	return &group, nil
}

// CreateTagGroup creates a new tag group
func (s *Service) CreateTagGroup(group TagGroup) (*TagGroup, error) {
	log.Printf("[TagGroups] CreateTagGroup - Name: %s", group.Name)

	if group.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	var query string
	var result sql.Result
	var err error

	switch s.config.DBEngine {
	case "postgresql", "postgres":
		query = `
			INSERT INTO tag_groups (name, description)
			VALUES ($1, $2)
			RETURNING id, created, modified
		`
		var id int
		var created, modified time.Time
		err = s.db.QueryRow(query, group.Name, group.Description).Scan(&id, &created, &modified)
		if err == nil {
			group.ID = &id
			createdStr := created.Format(time.RFC3339)
			modifiedStr := modified.Format(time.RFC3339)
			group.Created = &createdStr
			group.Modified = &modifiedStr
		}
	case "mysql", "mariadb":
		query = `
			INSERT INTO tag_groups (name, description)
			VALUES (?, ?)
		`
		result, err = s.db.Exec(query, group.Name, group.Description)
		if err == nil {
			id, _ := result.LastInsertId()
			idInt := int(id)
			group.ID = &idInt
			now := time.Now().Format(time.RFC3339)
			group.Created = &now
			group.Modified = &now
		}
	case "sqlite", "sqlite3":
		query = `
			INSERT INTO tag_groups (name, description)
			VALUES (?, ?)
		`
		result, err = s.db.Exec(query, group.Name, group.Description)
		if err == nil {
			id, _ := result.LastInsertId()
			idInt := int(id)
			group.ID = &idInt
			now := time.Now().Format(time.RFC3339)
			group.Created = &now
			group.Modified = &now
		}
	}

	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint") || strings.Contains(err.Error(), "duplicate key") {
			return nil, fmt.Errorf("tag group with name '%s' already exists", group.Name)
		}
		return nil, fmt.Errorf("failed to create tag group: %w", err)
	}

	// Add tag memberships if provided
	if len(group.TagIDs) > 0 {
		if err := s.updateTagGroupMemberships(group.ID, group.TagIDs); err != nil {
			log.Printf("[TagGroups] Warning: Failed to add tag memberships: %v", err)
		}
	}

	return &group, nil
}

// UpdateTagGroup updates an existing tag group
func (s *Service) UpdateTagGroup(id int, updates TagGroup) (*TagGroup, error) {
	log.Printf("[TagGroups] UpdateTagGroup - ID: %d", id)

	// Get existing group
	existing, err := s.GetTagGroup(id)
	if err != nil {
		return nil, err
	}

	// Update fields
	if updates.Name != "" {
		existing.Name = updates.Name
	}
	if updates.Description != nil {
		existing.Description = updates.Description
	}

	var query string
	switch s.config.DBEngine {
	case "postgresql", "postgres":
		query = `
			UPDATE tag_groups
			SET name = $1, description = $2, modified = CURRENT_TIMESTAMP
			WHERE id = $3
			RETURNING modified
		`
		var modified time.Time
		err = s.db.QueryRow(query, existing.Name, existing.Description, id).Scan(&modified)
		if err == nil {
			modifiedStr := modified.Format(time.RFC3339)
			existing.Modified = &modifiedStr
		}
	case "mysql", "mariadb":
		query = `
			UPDATE tag_groups
			SET name = ?, description = ?, modified = CURRENT_TIMESTAMP
			WHERE id = ?
		`
		_, err = s.db.Exec(query, existing.Name, existing.Description, id)
		if err == nil {
			now := time.Now().Format(time.RFC3339)
			existing.Modified = &now
		}
	case "sqlite", "sqlite3":
		query = `
			UPDATE tag_groups
			SET name = ?, description = ?, modified = CURRENT_TIMESTAMP
			WHERE id = ?
		`
		_, err = s.db.Exec(query, existing.Name, existing.Description, id)
		if err == nil {
			now := time.Now().Format(time.RFC3339)
			existing.Modified = &now
		}
	}

	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint") || strings.Contains(err.Error(), "duplicate key") {
			return nil, fmt.Errorf("tag group with name '%s' already exists", existing.Name)
		}
		return nil, fmt.Errorf("failed to update tag group: %w", err)
	}

	// Update tag memberships if provided
	if updates.TagIDs != nil {
		if err := s.updateTagGroupMemberships(&id, updates.TagIDs); err != nil {
			log.Printf("[TagGroups] Warning: Failed to update tag memberships: %v", err)
		}
		existing.TagIDs = updates.TagIDs
	}

	return existing, nil
}

// DeleteTagGroup deletes a tag group
func (s *Service) DeleteTagGroup(id int) error {
	log.Printf("[TagGroups] DeleteTagGroup - ID: %d")

	var query string
	switch s.config.DBEngine {
	case "postgresql", "postgres":
		query = `DELETE FROM tag_groups WHERE id = $1`
	case "mysql", "mariadb", "sqlite", "sqlite3":
		query = `DELETE FROM tag_groups WHERE id = ?`
	}

	result, err := s.db.Exec(query, id)
	if err != nil {
		return fmt.Errorf("failed to delete tag group: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("tag group with id %d not found", id)
	}

	return nil
}

// getTagGroupMemberships retrieves tag IDs for a tag group
func (s *Service) getTagGroupMemberships(groupID *int) ([]int, error) {
	if groupID == nil {
		return []int{}, nil
	}

	var query string
	switch s.config.DBEngine {
	case "postgresql", "postgres":
		query = `SELECT tag_id FROM tag_group_memberships WHERE tag_group_id = $1 ORDER BY tag_id ASC`
	case "mysql", "mariadb", "sqlite", "sqlite3":
		query = `SELECT tag_id FROM tag_group_memberships WHERE tag_group_id = ? ORDER BY tag_id ASC`
	}

	rows, err := s.db.Query(query, *groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tagIDs := []int{}
	for rows.Next() {
		var tagID int
		if err := rows.Scan(&tagID); err == nil {
			tagIDs = append(tagIDs, tagID)
		}
	}

	return tagIDs, nil
}

// updateTagGroupMemberships updates the tag memberships for a group
func (s *Service) updateTagGroupMemberships(groupID *int, tagIDs []int) error {
	if groupID == nil {
		return fmt.Errorf("group ID is required")
	}

	// Delete existing memberships
	var deleteQuery string
	switch s.config.DBEngine {
	case "postgresql", "postgres":
		deleteQuery = `DELETE FROM tag_group_memberships WHERE tag_group_id = $1`
	case "mysql", "mariadb", "sqlite", "sqlite3":
		deleteQuery = `DELETE FROM tag_group_memberships WHERE tag_group_id = ?`
	}

	_, err := s.db.Exec(deleteQuery, *groupID)
	if err != nil {
		return fmt.Errorf("failed to delete existing memberships: %w", err)
	}

	// Insert new memberships
	if len(tagIDs) == 0 {
		return nil
	}

	var insertQuery string
	switch s.config.DBEngine {
	case "postgresql", "postgres":
		insertQuery = `INSERT INTO tag_group_memberships (tag_group_id, tag_id) VALUES ($1, $2)`
	case "mysql", "mariadb", "sqlite", "sqlite3":
		insertQuery = `INSERT INTO tag_group_memberships (tag_group_id, tag_id) VALUES (?, ?)`
	}

	for _, tagID := range tagIDs {
		_, err := s.db.Exec(insertQuery, *groupID, tagID)
		if err != nil {
			log.Printf("[TagGroups] Warning: Failed to add membership for tag %d: %v", tagID, err)
		}
	}

	return nil
}

// scanTagGroup scans a TagGroup from a database row
func (s *Service) scanTagGroup(scanner interface{}) (TagGroup, error) {
	var group TagGroup
	var id sql.NullInt64
	var description, created, modified sql.NullString

	switch sc := scanner.(type) {
	case *sql.Row:
		err := sc.Scan(&id, &group.Name, &description, &created, &modified)
		if err != nil {
			return group, err
		}
	case *sql.Rows:
		err := sc.Scan(&id, &group.Name, &description, &created, &modified)
		if err != nil {
			return group, err
		}
	default:
		return group, fmt.Errorf("unsupported scanner type")
	}

	if id.Valid {
		idInt := int(id.Int64)
		group.ID = &idInt
	}
	if description.Valid {
		group.Description = &description.String
	}
	if created.Valid {
		group.Created = &created.String
	}
	if modified.Valid {
		group.Modified = &modified.String
	}

	return group, nil
}

// HTTP Handlers

func (s *Service) handleListTagGroups(w http.ResponseWriter, r *http.Request) {
	log.Printf("[TagGroups] GET /api/tag-groups/ - Request from %s", r.RemoteAddr)

	groups, err := s.ListTagGroups()
	if err != nil {
		log.Printf("[TagGroups] Error listing groups: %v", err)
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	log.Printf("[TagGroups] Found %d groups", len(groups))
	response := TagGroupListResponse{
		Count:   len(groups),
		Results: groups,
	}

	respondJSON(w, http.StatusOK, response)
}

func (s *Service) handleGetTagGroup(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	idStr := vars["id"]
	log.Printf("[TagGroups] GET /api/tag-groups/%s/ - Request from %s", idStr, r.RemoteAddr)

	id, err := strconv.Atoi(idStr)
	if err != nil {
		log.Printf("[TagGroups] Invalid group ID: %s", idStr)
		respondError(w, http.StatusBadRequest, "Invalid group ID")
		return
	}

	group, err := s.GetTagGroup(id)
	if err != nil {
		log.Printf("[TagGroups] Error getting group %d: %v", id, err)
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	log.Printf("[TagGroups] Successfully retrieved group %d: %s", id, group.Name)
	respondJSON(w, http.StatusOK, group)
}

func (s *Service) handleCreateTagGroup(w http.ResponseWriter, r *http.Request) {
	log.Printf("[TagGroups] POST /api/tag-groups/ - Request from %s", r.RemoteAddr)

	var group TagGroup
	if err := json.NewDecoder(r.Body).Decode(&group); err != nil {
		log.Printf("[TagGroups] Error decoding request body: %v", err)
		respondError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %v", err))
		return
	}

	log.Printf("[TagGroups] Creating group: Name=%s", group.Name)

	if group.Name == "" {
		log.Printf("[TagGroups] Validation error: Name is required")
		respondError(w, http.StatusBadRequest, "Name is required")
		return
	}

	created, err := s.CreateTagGroup(group)
	if err != nil {
		log.Printf("[TagGroups] Error creating group: %v", err)
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	log.Printf("[TagGroups] Successfully created group ID: %d, Name: %s", *created.ID, created.Name)
	respondJSON(w, http.StatusCreated, created)
}

func (s *Service) handleUpdateTagGroup(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	idStr := vars["id"]
	method := r.Method
	log.Printf("[TagGroups] %s /api/tag-groups/%s/ - Request from %s", method, idStr, r.RemoteAddr)

	id, err := strconv.Atoi(idStr)
	if err != nil {
		log.Printf("[TagGroups] Invalid group ID: %s", idStr)
		respondError(w, http.StatusBadRequest, "Invalid group ID")
		return
	}

	var updates TagGroup
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		log.Printf("[TagGroups] Error decoding request body for group %d: %v", id, err)
		respondError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %v", err))
		return
	}

	log.Printf("[TagGroups] Updating group ID: %d", id)

	updated, err := s.UpdateTagGroup(id, updates)
	if err != nil {
		log.Printf("[TagGroups] Error updating group %d: %v", id, err)
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	log.Printf("[TagGroups] Successfully updated group ID: %d", id)
	respondJSON(w, http.StatusOK, updated)
}

func (s *Service) handleDeleteTagGroup(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	idStr := vars["id"]
	log.Printf("[TagGroups] DELETE /api/tag-groups/%s/ - Request from %s", idStr, r.RemoteAddr)

	id, err := strconv.Atoi(idStr)
	if err != nil {
		log.Printf("[TagGroups] Invalid group ID: %s", idStr)
		respondError(w, http.StatusBadRequest, "Invalid group ID")
		return
	}

	log.Printf("[TagGroups] Deleting group ID: %d", id)

	if err := s.DeleteTagGroup(id); err != nil {
		log.Printf("[TagGroups] Error deleting group %d: %v", id, err)
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	log.Printf("[TagGroups] Successfully deleted group ID: %d", id)
	w.WriteHeader(http.StatusNoContent)
}

// Tag Description Functions

// GetTagDescription retrieves a description for a tag
func (s *Service) GetTagDescription(tagID int) (*TagDescription, error) {
	var query string

	switch s.config.DBEngine {
	case "postgresql", "postgres":
		query = `
			SELECT id, tag_id, description, created, modified
			FROM tag_descriptions
			WHERE tag_id = $1
		`
	case "mysql", "mariadb", "sqlite", "sqlite3":
		query = `
			SELECT id, tag_id, description, created, modified
			FROM tag_descriptions
			WHERE tag_id = ?
		`
	}

	row := s.db.QueryRow(query, tagID)
	desc, err := s.scanTagDescription(row)
	if err != nil {
		if err == sql.ErrNoRows {
			// Return empty description if not found
			return &TagDescription{TagID: tagID}, nil
		}
		return nil, err
	}

	return &desc, nil
}

// SetTagDescription creates or updates a description for a tag
func (s *Service) SetTagDescription(desc TagDescription) (*TagDescription, error) {
	log.Printf("[TagDescriptions] SetTagDescription - TagID: %d", desc.TagID)

	// Check if description exists
	existing, err := s.GetTagDescription(desc.TagID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to check existing description: %w", err)
	}

	var query string
	var result sql.Result

	if existing != nil && existing.ID != nil {
		// Update existing
		switch s.config.DBEngine {
		case "postgresql", "postgres":
			query = `
				UPDATE tag_descriptions
				SET description = $1, modified = CURRENT_TIMESTAMP
				WHERE tag_id = $2
				RETURNING modified
			`
			var modified time.Time
			err = s.db.QueryRow(query, desc.Description, desc.TagID).Scan(&modified)
			if err == nil {
				modifiedStr := modified.Format(time.RFC3339)
				desc.Modified = &modifiedStr
				desc.ID = existing.ID
				desc.Created = existing.Created
			}
		case "mysql", "mariadb":
			query = `
				UPDATE tag_descriptions
				SET description = ?, modified = CURRENT_TIMESTAMP
				WHERE tag_id = ?
			`
			result, err = s.db.Exec(query, desc.Description, desc.TagID)
			if err == nil {
				desc.ID = existing.ID
				desc.Created = existing.Created
				now := time.Now().Format(time.RFC3339)
				desc.Modified = &now
			}
		case "sqlite", "sqlite3":
			query = `
				UPDATE tag_descriptions
				SET description = ?, modified = CURRENT_TIMESTAMP
				WHERE tag_id = ?
			`
			result, err = s.db.Exec(query, desc.Description, desc.TagID)
			if err == nil {
				desc.ID = existing.ID
				desc.Created = existing.Created
				now := time.Now().Format(time.RFC3339)
				desc.Modified = &now
			}
		}
	} else {
		// Create new
		switch s.config.DBEngine {
		case "postgresql", "postgres":
			query = `
				INSERT INTO tag_descriptions (tag_id, description)
				VALUES ($1, $2)
				RETURNING id, created, modified
			`
			var id int
			var created, modified time.Time
			err = s.db.QueryRow(query, desc.TagID, desc.Description).Scan(&id, &created, &modified)
			if err == nil {
				desc.ID = &id
				createdStr := created.Format(time.RFC3339)
				modifiedStr := modified.Format(time.RFC3339)
				desc.Created = &createdStr
				desc.Modified = &modifiedStr
			}
		case "mysql", "mariadb":
			query = `
				INSERT INTO tag_descriptions (tag_id, description)
				VALUES (?, ?)
			`
			result, err = s.db.Exec(query, desc.TagID, desc.Description)
			if err == nil {
				id, _ := result.LastInsertId()
				idInt := int(id)
				desc.ID = &idInt
				now := time.Now().Format(time.RFC3339)
				desc.Created = &now
				desc.Modified = &now
			}
		case "sqlite", "sqlite3":
			query = `
				INSERT INTO tag_descriptions (tag_id, description)
				VALUES (?, ?)
			`
			result, err = s.db.Exec(query, desc.TagID, desc.Description)
			if err == nil {
				id, _ := result.LastInsertId()
				idInt := int(id)
				desc.ID = &idInt
				now := time.Now().Format(time.RFC3339)
				desc.Created = &now
				desc.Modified = &now
			}
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to save tag description: %w", err)
	}

	return &desc, nil
}

// DeleteTagDescription deletes a description for a tag
func (s *Service) DeleteTagDescription(tagID int) error {
	log.Printf("[TagDescriptions] DeleteTagDescription - TagID: %d")

	var query string
	switch s.config.DBEngine {
	case "postgresql", "postgres":
		query = `DELETE FROM tag_descriptions WHERE tag_id = $1`
	case "mysql", "mariadb", "sqlite", "sqlite3":
		query = `DELETE FROM tag_descriptions WHERE tag_id = ?`
	}

	_, err := s.db.Exec(query, tagID)
	if err != nil {
		return fmt.Errorf("failed to delete tag description: %w", err)
	}

	return nil
}

// scanTagDescription scans a TagDescription from a database row
func (s *Service) scanTagDescription(row *sql.Row) (TagDescription, error) {
	var desc TagDescription
	var id sql.NullInt64
	var description, created, modified sql.NullString

	err := row.Scan(&id, &desc.TagID, &description, &created, &modified)
	if err != nil {
		return desc, err
	}

	if id.Valid {
		idInt := int(id.Int64)
		desc.ID = &idInt
	}
	if description.Valid {
		desc.Description = &description.String
	}
	if created.Valid {
		desc.Created = &created.String
	}
	if modified.Valid {
		desc.Modified = &modified.String
	}

	return desc, nil
}

// HTTP Handlers for Tag Descriptions

func (s *Service) handleGetTagDescription(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tagIDStr := vars["tagId"]
	log.Printf("[TagDescriptions] GET /api/tag-descriptions/%s/ - Request from %s", tagIDStr, r.RemoteAddr)

	tagID, err := strconv.Atoi(tagIDStr)
	if err != nil {
		log.Printf("[TagDescriptions] Invalid tag ID: %s", tagIDStr)
		respondError(w, http.StatusBadRequest, "Invalid tag ID")
		return
	}

	desc, err := s.GetTagDescription(tagID)
	if err != nil {
		log.Printf("[TagDescriptions] Error getting description for tag %d: %v", tagID, err)
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, desc)
}

func (s *Service) handleSetTagDescription(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tagIDStr := vars["tagId"]
	log.Printf("[TagDescriptions] PUT /api/tag-descriptions/%s/ - Request from %s", tagIDStr, r.RemoteAddr)

	tagID, err := strconv.Atoi(tagIDStr)
	if err != nil {
		log.Printf("[TagDescriptions] Invalid tag ID: %s", tagIDStr)
		respondError(w, http.StatusBadRequest, "Invalid tag ID")
		return
	}

	var desc TagDescription
	if err := json.NewDecoder(r.Body).Decode(&desc); err != nil {
		log.Printf("[TagDescriptions] Error decoding request body: %v", err)
		respondError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %v", err))
		return
	}

	desc.TagID = tagID
	saved, err := s.SetTagDescription(desc)
	if err != nil {
		log.Printf("[TagDescriptions] Error saving description for tag %d: %v", tagID, err)
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	log.Printf("[TagDescriptions] Successfully saved description for tag %d", tagID)
	respondJSON(w, http.StatusOK, saved)
}

func (s *Service) handleDeleteTagDescription(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tagIDStr := vars["tagId"]
	log.Printf("[TagDescriptions] DELETE /api/tag-descriptions/%s/ - Request from %s", tagIDStr, r.RemoteAddr)

	tagID, err := strconv.Atoi(tagIDStr)
	if err != nil {
		log.Printf("[TagDescriptions] Invalid tag ID: %s", tagIDStr)
		respondError(w, http.StatusBadRequest, "Invalid tag ID")
		return
	}

	if err := s.DeleteTagDescription(tagID); err != nil {
		log.Printf("[TagDescriptions] Error deleting description for tag %d: %v", tagID, err)
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	log.Printf("[TagDescriptions] Successfully deleted description for tag %d", tagID)
	w.WriteHeader(http.StatusNoContent)
}

