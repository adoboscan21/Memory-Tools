package globalconst

// This package centralizes all constants and "magic strings" used throughout the application
// to improve maintainability and reduce errors from typos.

const (
	// =========================================================================
	// Document Fields
	// =========================================================================

	// ID is the field for the document's unique identifier.
	ID = "_id"
	// CREATED_AT is the field for the document's creation timestamp.
	CREATED_AT = "created_at"
	// UPDATED_AT is the field for the last update timestamp.
	UPDATED_AT = "updated_at"
	// DELETED_FLAG is the boolean field that acts as a tombstone for soft deletes.
	DELETED_FLAG = "_deleted"

	// =========================================================================
	// System Identifiers
	// =========================================================================

	// SystemCollectionName is the name of the reserved collection for system data.
	SystemCollectionName = "_system"
	// UserPrefix is the prefix used for user document keys in the system collection.
	UserPrefix = "user:"

	// =========================================================================
	// Permission Levels
	// =========================================================================

	// PermissionRead defines the read-only permission level.
	PermissionRead = "read"
	// PermissionWrite defines the read and write permission level.
	PermissionWrite = "write"

	// =========================================================================
	// Query Keywords
	// =========================================================================

	// --- Comparison Operators ---
	OpEqual              = "="
	OpNotEqual           = "!="
	OpGreaterThan        = ">"
	OpGreaterThanOrEqual = ">="
	OpLessThan           = "<"
	OpLessThanOrEqual    = "<="
	OpLike               = "like"
	OpIn                 = "in"
	OpBetween            = "between"
	OpIsNull             = "is null"
	OpIsNotNull          = "is not null"

	// --- Logical Operators ---
	OpAnd = "and"
	OpOr  = "or"
	OpNot = "not"

	// --- Aggregation Functions ---
	AggCount = "count"
	AggSum   = "sum"
	AggAvg   = "avg"
	AggMin   = "min"
	AggMax   = "max"

	// --- Sort Directions ---
	SortDesc = "desc"
	SortAsc  = "asc"

	// =========================================================================
	// Persistence Keywords
	// =========================================================================

	// BackupsDirName is the root directory name for backups.
	BackupsDirName = "backups"
	// CollectionsDirName is the root directory name for collection data.
	CollectionsDirName = "collections"
	// DBFileExtension is the file extension for database data files.
	DBFileExtension = ".mtdb"
	// TempFileSuffix is the suffix added to temporary files during writes.
	TempFileSuffix = ".tmp"
)
