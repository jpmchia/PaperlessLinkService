package main

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// CustomFieldValueOption represents a single custom field value option
type CustomFieldValueOption struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Count int    `json:"count"`
}

// CustomFieldValuesResponse represents the response for custom field values
type CustomFieldValuesResponse struct {
	FieldID        int                      `json:"field_id"`
	FieldName      string                   `json:"field_name"`
	Values         []CustomFieldValueOption `json:"values"`
	TotalDocuments int                      `json:"total_documents"`
}

// CustomView represents a custom document list view configuration
type CustomView struct {
	ID                 *int                     `json:"id,omitempty"`
	Name               string                   `json:"name"`
	Description        *string                  `json:"description,omitempty"`
	ColumnOrder        []interface{}            `json:"column_order"` // []string or []number
	ColumnSizing       map[string]int           `json:"column_sizing"`
	ColumnVisibility   map[string]bool          `json:"column_visibility"`
	ColumnDisplayTypes map[string]string        `json:"column_display_types"`
	FilterRules        []map[string]interface{} `json:"filter_rules,omitempty"`
	FilterVisibility   map[string]bool          `json:"filter_visibility,omitempty"`
	FilterTypes        map[string]string        `json:"filter_types,omitempty"`
	EditModeSettings   map[string]interface{}   `json:"edit_mode_settings,omitempty"` // map[fieldId]{enabled: bool, entry_type: string}
	ColumnStyles       map[string]string        `json:"column_styles,omitempty"`      // map[fieldId]cssString
	SubrowEnabled      *bool                    `json:"subrow_enabled,omitempty"`
	SubrowContent      *string                  `json:"subrow_content,omitempty"` // 'summary', 'tags', or 'none'
	ColumnSpanning     map[string]bool          `json:"column_spanning,omitempty"`
	SortField          *string                  `json:"sort_field,omitempty"`
	SortReverse        *bool                    `json:"sort_reverse,omitempty"`
	IsGlobal           *bool                    `json:"is_global,omitempty"`
	Created            *string                  `json:"created,omitempty"`
	Modified           *string                  `json:"modified,omitempty"`
	DeletedAt          *string                  `json:"deleted_at,omitempty"`
	Username           *string                  `json:"username,omitempty"`
	OwnerID            *int                     `json:"owner_id,omitempty"` // Internal: user ID
}

// CustomViewListResponse represents a paginated list of custom views
type CustomViewListResponse struct {
	Count    int          `json:"count"`
	Next     *string      `json:"next,omitempty"`
	Previous *string      `json:"previous,omitempty"`
	Results  []CustomView `json:"results"`
}

// TagGroup represents a group of tags
type TagGroup struct {
	ID          *int    `json:"id,omitempty"`
	Name        string  `json:"name"`
	Description *string `json:"description,omitempty"`
	TagIDs      []int   `json:"tag_ids,omitempty"` // Tags in this group
	Created     *string `json:"created,omitempty"`
	Modified    *string `json:"modified,omitempty"`
}

// TagGroupListResponse represents a list of tag groups
type TagGroupListResponse struct {
	Count   int        `json:"count"`
	Results []TagGroup `json:"results"`
}

// TagDescription represents a description for a tag
type TagDescription struct {
	ID          *int    `json:"id,omitempty"`
	TagID       int     `json:"tag_id"`
	Description *string `json:"description,omitempty"`
	Created     *string `json:"created,omitempty"`
	Modified    *string `json:"modified,omitempty"`
}
