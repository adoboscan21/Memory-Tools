package handler

import "sync"

type LookupClause struct {
	FromCollection string `json:"from"`         // The collection to join with
	LocalField     string `json:"localField"`   // Field from the input documents
	ForeignField   string `json:"foreignField"` // Field from the documents of the "from" collection
	As             string `json:"as"`           // The new array field to add to the input documents
}

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
	Projection   []string               `json:"projection,omitempty"`
	Lookups      []LookupClause         `json:"lookups,omitempty"`
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

//Reset limpia la estructura Query para su reutilización.
func (q *Query) Reset() {
	q.Filter = nil
	q.OrderBy = nil
	q.Limit = nil
	q.Offset = 0
	q.Count = false
	q.Aggregations = nil
	q.GroupBy = nil
	q.Having = nil
	q.Distinct = ""
	q.Projection = nil
	q.Lookups = nil
}

// NUEVO POOL: Pool para objetos Query.
var queryPool = sync.Pool{
	New: func() any {
		return new(Query)
	},
}
