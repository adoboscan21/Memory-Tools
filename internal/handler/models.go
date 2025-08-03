package handler

// UserInfo structure
type UserInfo struct {
	Username     string            `json:"username"`
	PasswordHash string            `json:"password_hash"`
	IsRoot       bool              `json:"is_root,omitempty"`
	Permissions  map[string]string `json:"permissions,omitempty"` // Key: collection name, Value: "read" or "write". "*" for all collections.
}

// Query defines the structure for a collection query command,
// encompassing filtering, ordering, limiting, and aggregation.
type Query struct {
	Filter       map[string]any         `json:"filter,omitempty"`       // WHERE clause equivalents (AND, OR, NOT, LIKE, BETWEEN, IN, IS NULL)
	OrderBy      []OrderByClause        `json:"order_by,omitempty"`     // ORDER BY clause
	Limit        *int                   `json:"limit,omitempty"`        // LIMIT clause
	Offset       int                    `json:"offset,omitempty"`       // OFFSET clause
	Count        bool                   `json:"count,omitempty"`        // COUNT(*) equivalent
	Aggregations map[string]Aggregation `json:"aggregations,omitempty"` // SUM, AVG, MIN, MAX
	GroupBy      []string               `json:"group_by,omitempty"`     // GROUP BY clause
	Having       map[string]any         `json:"having,omitempty"`       // HAVING clause (filters aggregated results)
	Distinct     string                 `json:"distinct,omitempty"`     // DISTINCT field
}

// OrderByClause defines a single ordering criterion.
type OrderByClause struct {
	Field     string `json:"field"`
	Direction string `json:"direction"` // "asc" or "desc"
}

// Aggregation defines an aggregation function.
type Aggregation struct {
	Func  string `json:"func"`  // "sum", "avg", "min", "max", "count"
	Field string `json:"field"` // Field to aggregate on, "*" for count
}
