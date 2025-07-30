package handler

import (
	"fmt"
	"io"
	"log"
	"memory-tools/internal/protocol"
	"memory-tools/internal/store"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/crypto/bcrypt"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const SystemCollectionName = "_system"
const UserPrefix = "user:"

// UserInfo structure
type UserInfo struct {
	Username     string `json:"username"`
	PasswordHash string `json:"password_hash"`
	IsRoot       bool   `json:"is_root,omitempty"`
}

// ActivityUpdater is an interface for updating activity timestamps.
type ActivityUpdater interface {
	UpdateActivity()
}

// ConnectionHandler holds the dependencies needed to handle client connections.
type ConnectionHandler struct {
	MainStore         store.DataStore
	CollectionManager *store.CollectionManager
	ActivityUpdater   ActivityUpdater // Dependency for updating activity
	IsAuthenticated   bool            // Tracks authentication status for this connection
	AuthenticatedUser string          // Stores the authenticated username
	IsLocalhostConn   bool            // True if connection is from localhost
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

// NewConnectionHandler creates a new instance of ConnectionHandler.
func NewConnectionHandler(mainStore store.DataStore, colManager *store.CollectionManager, updater ActivityUpdater, conn net.Conn) *ConnectionHandler {
	isLocal := false
	if host, _, err := net.SplitHostPort(conn.RemoteAddr().String()); err == nil {
		if host == "127.0.0.1" || host == "::1" || host == "localhost" {
			isLocal = true
		}
	}

	return &ConnectionHandler{
		MainStore:         mainStore,
		CollectionManager: colManager,
		ActivityUpdater:   updater,
		IsAuthenticated:   false, // Initially not authenticated
		AuthenticatedUser: "",
		IsLocalhostConn:   isLocal, // Set based on connection origin
	}
}

// HandleConnection processes incoming commands from a TCP client.
func (h *ConnectionHandler) HandleConnection(conn net.Conn) {
	defer conn.Close()
	log.Printf("New client connected: %s (Is Localhost: %t)", conn.RemoteAddr(), h.IsLocalhostConn)

	for {
		cmdType, err := protocol.ReadCommandType(conn)
		if err != nil {
			if err == io.EOF {
				log.Printf("Client disconnected: %s", conn.RemoteAddr())
			} else {
				log.Printf("Error reading command type from %s: %v", conn.RemoteAddr(), err)
			}
			return // Exit goroutine on read error
		}

		h.ActivityUpdater.UpdateActivity()

		// --- MAIN COMMAND DISPATCH ---
		// Handle commands based on type. Authentication is checked *within* relevant cases.
		switch cmdType {
		case protocol.CmdAuthenticate:
			h.handleAuthenticate(conn)
			// After authentication attempt, continue the loop to process next command.
			continue

		case protocol.CmdChangeUserPassword:
			// This command has its own specific authorization logic (requires 'root' from localhost),
			// which is checked inside its handler.
			h.handleChangeUserPassword(conn)
			continue

		// --- ALL OTHER COMMANDS require prior authentication ---
		default:
			if !h.IsAuthenticated {
				log.Printf("Unauthorized access attempt from %s for command %d. Connection not authenticated.", conn.RemoteAddr(), cmdType)
				protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Please authenticate first.", nil)
				continue // Go to the next loop iteration, expecting an AUTH command
			}

			// If we reach here, the client is authenticated. Now, dispatch the command.
			switch cmdType {
			// Main Store Commands.
			case protocol.CmdSet:
				key, value, ttl, err := protocol.ReadSetCommand(conn)
				if err != nil {
					log.Printf("Error reading SET command from %s: %v", conn.RemoteAddr(), err)
					protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid SET command format", nil)
					continue
				}
				h.MainStore.Set(key, value, ttl)
				if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' set in main store", key), nil); err != nil {
					log.Printf("Error writing SET response to %s: %v", conn.RemoteAddr(), err)
				}

			case protocol.CmdGet:
				key, err := protocol.ReadGetCommand(conn)
				if err != nil {
					log.Printf("Error reading GET command from %s: %v", conn.RemoteAddr(), err)
					protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid GET command format", nil)
					continue
				}
				value, found := h.MainStore.Get(key)
				if found {
					if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' retrieved from main store", key), value); err != nil {
						log.Printf("Error writing GET success response to %s: %v", conn.RemoteAddr(), err)
					}
				} else {
					if err := protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' not found or expired in main store", key), nil); err != nil {
						log.Printf("Error writing GET not found response to %s: %v", conn.RemoteAddr(), err)
					}
				}

			// Collection Management Commands.
			case protocol.CmdCollectionCreate:
				collectionName, err := protocol.ReadCollectionCreateCommand(conn)
				if err != nil {
					log.Printf("Error reading CREATE_COLLECTION command from %s: %v", conn.RemoteAddr(), err)
					protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid CREATE_COLLECTION command format", nil)
					continue
				}
				if collectionName == "" {
					protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
					continue
				}
				// Specific authorization check for _system collection (even if authenticated)
				if collectionName == SystemCollectionName && !(h.AuthenticatedUser == "root" && h.IsLocalhostConn) {
					protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: Only 'root' from localhost can create collection '%s'", SystemCollectionName), nil)
					continue
				}

				colStore := h.CollectionManager.GetCollection(collectionName)
				if err := h.CollectionManager.SaveCollectionToDisk(collectionName, colStore); err != nil {
					log.Printf("Error saving new/ensured collection '%s' to disk: %v", collectionName, err)
					protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("Failed to ensure collection '%s' persistence", collectionName), nil)
				} else {
					protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Collection '%s' ensured and persisted", collectionName), nil)
				}

			case protocol.CmdCollectionDelete:
				collectionName, err := protocol.ReadCollectionDeleteCommand(conn)
				if err != nil {
					log.Printf("Error reading DELETE_COLLECTION command from %s: %v", conn.RemoteAddr(), err)
					protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid DELETE_COLLECTION command format", nil)
					continue
				}
				if collectionName == "" {
					protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
					continue
				}
				if !h.CollectionManager.CollectionExists(collectionName) {
					protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for deletion", collectionName), nil)
					continue
				}
				// Specific authorization check for _system collection (even if authenticated)
				if collectionName == SystemCollectionName && !(h.AuthenticatedUser == "root" && h.IsLocalhostConn) {
					protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: Only 'root' from localhost can delete collection '%s'", SystemCollectionName), nil)
					continue
				}

				h.CollectionManager.DeleteCollection(collectionName)
				if err := h.CollectionManager.DeleteCollectionFromDisk(collectionName); err != nil {
					log.Printf("Error deleting collection file for '%s': %v", collectionName, err)
					protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("Failed to delete collection '%s' from disk", collectionName), nil)
				} else {
					protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Collection '%s' deleted", collectionName), nil)
				}

			case protocol.CmdCollectionList:
				collectionNames := h.CollectionManager.ListCollections()
				jsonNames, err := json.Marshal(collectionNames)
				if err != nil {
					log.Printf("Error marshalling collection names to JSON: %v", err)
					protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal collection names", nil)
					continue
				}
				if err := protocol.WriteResponse(conn, protocol.StatusOk, "OK: Collections listed", jsonNames); err != nil {
					log.Printf("Error writing collection list response to %s: %v", conn.RemoteAddr(), err)
				}

			// Collection Item Commands.
			case protocol.CmdCollectionItemSet:
				collectionName, key, value, ttl, err := protocol.ReadCollectionItemSetCommand(conn)
				if err != nil {
					log.Printf("Error reading COLLECTION_ITEM_SET command from %s: %v", conn.RemoteAddr(), err)
					protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_SET command format", nil)
					continue
				}
				if collectionName == "" || len(value) == 0 { // Key can be empty for UUID generation
					protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or value cannot be empty", nil)
					continue
				}
				if key == "" { // The client is expected to send a UUID here, but as a fallback.
					key = uuid.New().String()
					log.Printf("Warning: Empty key received for COLLECTION_ITEM_SET. Generated UUID '%s'. Ensure client sends UUIDs.", key)
				}

				// Specific authorization check for _system collection (even if authenticated)
				if collectionName == SystemCollectionName && !(h.AuthenticatedUser == "root" && h.IsLocalhostConn) {
					protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: Only 'root' from localhost can modify collection '%s'", SystemCollectionName), nil)
					continue
				}

				// Ensure _id field exists in the JSON value
				updatedValue, err := ensureIDField(value, key)
				if err != nil {
					log.Printf("Error ensuring _id field for key '%s' in collection '%s': %v", key, collectionName, err)
					protocol.WriteResponse(conn, protocol.StatusError, "Failed to process value for _id field", nil)
					continue
				}

				colStore := h.CollectionManager.GetCollection(collectionName)
				colStore.Set(key, updatedValue, ttl)
				if err := h.CollectionManager.SaveCollectionToDisk(collectionName, colStore); err != nil {
					log.Printf("Error saving collection '%s' to disk after SET operation: %v", collectionName, err)
					protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' set in collection '%s' (persistence error logged)", key, collectionName), nil)
				} else {
					protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' set in collection '%s'", key, collectionName), nil)
				}

			case protocol.CmdCollectionItemGet:
				collectionName, key, err := protocol.ReadCollectionItemGetCommand(conn)
				if err != nil {
					log.Printf("Error reading COLLECTION_ITEM_GET command from %s: %v", conn.RemoteAddr(), err)
					protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_GET command format", nil)
					continue
				}
				if collectionName == "" || key == "" {
					protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or key cannot be empty", nil)
					continue
				}
				// Specific authorization check for _system collection (even if authenticated)
				if collectionName == SystemCollectionName && !(h.AuthenticatedUser == "root" && h.IsLocalhostConn) {
					log.Printf("Unauthorized attempt to GET item '%s' from _system collection by user '%s' from %s.", key, h.AuthenticatedUser, conn.RemoteAddr())
					protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: Only 'root' from localhost can get items from collection '%s'", SystemCollectionName), nil)
					continue
				}

				colStore := h.CollectionManager.GetCollection(collectionName)
				value, found := colStore.Get(key)
				if found {
					// Special handling for reading user data (do not send raw password hash)
					if collectionName == SystemCollectionName && strings.HasPrefix(key, UserPrefix) {
						var userInfo UserInfo
						if err := json.Unmarshal(value, &userInfo); err == nil {
							sanitizedInfo := map[string]string{"username": userInfo.Username, "is_root": fmt.Sprintf("%t", userInfo.IsRoot)}
							sanitizedBytes, _ := json.Marshal(sanitizedInfo)
							protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' retrieved from collection '%s' (sanitized)", key, collectionName), sanitizedBytes)
							continue
						}
					}
					if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' retrieved from collection '%s'", key, collectionName), value); err != nil {
						log.Printf("Error writing COLLECTION_ITEM_GET success response to %s: %v", conn.RemoteAddr(), err)
					}
				} else {
					if err := protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' not found or expired in collection '%s'", key, collectionName), nil); err != nil {
						log.Printf("Error writing COLLECTION_ITEM_GET not found response to %s: %v", conn.RemoteAddr(), err)
					}
				}

			case protocol.CmdCollectionItemDelete:
				collectionName, key, err := protocol.ReadCollectionItemDeleteCommand(conn)
				if err != nil {
					log.Printf("Error reading COLLECTION_ITEM_DELETE command from %s: %v", conn.RemoteAddr(), err)
					protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_DELETE command format", nil)
					continue
				}
				if collectionName == "" || key == "" {
					protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or key cannot be empty", nil)
					continue
				}
				if !h.CollectionManager.CollectionExists(collectionName) {
					protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for deletion", collectionName), nil)
					continue
				}
				// Specific authorization check for _system collection (even if authenticated)
				if collectionName == SystemCollectionName && !(h.AuthenticatedUser == "root" && h.IsLocalhostConn) {
					protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: Only 'root' from localhost can delete from collection '%s'", SystemCollectionName), nil)
					continue
				}
				colStore := h.CollectionManager.GetCollection(collectionName)
				colStore.Delete(key)
				if err := h.CollectionManager.SaveCollectionToDisk(collectionName, colStore); err != nil {
					log.Printf("Error saving collection '%s' to disk after DELETE operation: %v", collectionName, err)
					protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' deleted from collection '%s' (persistence error logged)", key, collectionName), nil)
				} else {
					protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' deleted from collection '%s'", key, collectionName), nil)
				}

			case protocol.CmdCollectionItemList:
				collectionName, err := protocol.ReadCollectionItemListCommand(conn)
				if err != nil {
					log.Printf("Error reading COLLECTION_ITEM_LIST command from %s: %v", conn.RemoteAddr(), err)
					protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_LIST command format", nil)
					continue
				}
				if collectionName == "" {
					protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
					continue
				}
				if !h.CollectionManager.CollectionExists(collectionName) {
					protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for listing items", collectionName), nil)
					continue
				}
				// Specific authorization check for _system collection (even if authenticated)
				if collectionName == SystemCollectionName && !(h.AuthenticatedUser == "root" && h.IsLocalhostConn) {
					log.Printf("Unauthorized attempt to LIST items from _system collection by user '%s' from %s.", h.AuthenticatedUser, conn.RemoteAddr())
					protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: Only 'root' from localhost can list items from collection '%s'", SystemCollectionName), nil)
					continue
				}

				colStore := h.CollectionManager.GetCollection(collectionName)
				allData := colStore.GetAll()

				// Special handling for user data in _system collection: do not expose password hashes
				if collectionName == SystemCollectionName {
					sanitizedData := make(map[string]map[string]string)
					for key, val := range allData {
						if strings.HasPrefix(key, UserPrefix) {
							var userInfo UserInfo
							if err := json.Unmarshal(val, &userInfo); err == nil {
								// Only expose username and IsRoot flag, not password hash
								sanitizedData[key] = map[string]string{
									"username": userInfo.Username,
									"is_root":  fmt.Sprintf("%t", userInfo.IsRoot),
								}
							} else {
								log.Printf("Warning: Failed to unmarshal user info for key '%s': %v", key, err)
								sanitizedData[key] = map[string]string{"username": "UNKNOWN", "status": "corrupted"}
							}
						} else {
							// For non-user system data, just indicate it's non-user for security reasons.
							sanitizedData[key] = map[string]string{"data": "non-user system data (omitted for security)"}
						}
					}
					jsonResponseData, err := json.Marshal(sanitizedData)
					if err != nil {
						log.Printf("Error marshalling sanitized system collection items to JSON for '%s': %v", collectionName, err)
						protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal sanitized collection items", nil)
						continue
					}
					if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Sanitized items from collection '%s' retrieved", collectionName), jsonResponseData); err != nil {
						log.Printf("Error writing COLLECTION_ITEM_LIST response to %s: %v", conn.RemoteAddr(), err)
					}
				} else {
					// For normal collections, marshal as before
					jsonResponseData, err := json.Marshal(allData)
					if err != nil {
						log.Printf("Error marshalling collection items to JSON for '%s': %v", collectionName, err)
						protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal collection items", nil)
						continue
					}
					if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Items from collection '%s' retrieved", collectionName), jsonResponseData); err != nil {
						log.Printf("Error writing COLLECTION_ITEM_LIST response to %s: %v", conn.RemoteAddr(), err)
					}
				}

			// NEW: Collection Query Command
			case protocol.CmdCollectionQuery:
				collectionName, queryJSONBytes, err := protocol.ReadCollectionQueryCommand(conn)
				if err != nil {
					log.Printf("Error reading COLLECTION_QUERY command from %s: %v", conn.RemoteAddr(), err)
					protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_QUERY command format", nil)
					continue
				}
				if collectionName == "" {
					protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
					continue
				}
				// Specific authorization check for _system collection (even if authenticated)
				if collectionName == SystemCollectionName && !(h.AuthenticatedUser == "root" && h.IsLocalhostConn) {
					log.Printf("Unauthorized attempt to QUERY _system collection by user '%s' from %s.", h.AuthenticatedUser, conn.RemoteAddr())
					protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: Only 'root' from localhost can query collection '%s'", SystemCollectionName), nil)
					continue
				}
				if !h.CollectionManager.CollectionExists(collectionName) {
					protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for query", collectionName), nil)
					continue
				}

				var query Query
				if err := json.Unmarshal(queryJSONBytes, &query); err != nil {
					log.Printf("Error unmarshalling query JSON for collection '%s': %v", collectionName, err)
					protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid query JSON format", nil)
					continue
				}

				// Process the query
				results, err := h.processCollectionQuery(collectionName, query)
				if err != nil {
					log.Printf("Error processing query for collection '%s': %v", collectionName, err)
					protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("Failed to execute query: %v", err), nil)
					continue
				}

				responseBytes, err := json.Marshal(results)
				if err != nil {
					log.Printf("Error marshalling query results for collection '%s': %v", collectionName, err)
					protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal query results", nil)
					continue
				}

				if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Query executed on collection '%s'", collectionName), responseBytes); err != nil {
					log.Printf("Error writing COLLECTION_QUERY response to %s: %v", conn.RemoteAddr(), err)
				}

			default:
				// This 'default' case handles any command type that is explicitly listed as requiring
				// authentication but is not handled in the specific cases above (e.g., a typo in cmdType).
				log.Printf("Received unhandled or unknown authenticated command type %d from client %s.", cmdType, conn.RemoteAddr())
				if err := protocol.WriteResponse(conn, protocol.StatusBadCommand, fmt.Sprintf("BAD COMMAND: Unhandled or unknown command type %d", cmdType), nil); err != nil {
					log.Printf("Error writing bad command response to %s: %v", conn.RemoteAddr(), err)
				}
			}
		}
	}
}

// handleAuthenticate processes the CmdAuthenticate command.
func (h *ConnectionHandler) handleAuthenticate(conn net.Conn) {
	username, password, err := protocol.ReadAuthenticateCommand(conn)
	if err != nil {
		log.Printf("Error reading AUTH command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid AUTH command format", nil)
		return
	}

	if username == "" || password == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Username and password cannot be empty.", nil)
		return
	}

	sysCol := h.CollectionManager.GetCollection(SystemCollectionName)
	userKey := UserPrefix + username

	userDataBytes, found := sysCol.Get(userKey)
	if !found {
		log.Printf("Authentication failed for user '%s' from %s: User not found.", username, conn.RemoteAddr())
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "Authentication failed: Invalid username or password.", nil)
		return
	}

	var storedUserInfo UserInfo
	if err := json.Unmarshal(userDataBytes, &storedUserInfo); err != nil {
		log.Printf("Error unmarshalling user info for '%s' from %s: %v", username, conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusError, "Authentication failed: Internal server error.", nil)
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(storedUserInfo.PasswordHash), []byte(password)); err != nil {
		log.Printf("Authentication failed for user '%s' from %s: Invalid password.", username, conn.RemoteAddr())
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "Authentication failed: Invalid username or password.", nil)
		return
	}

	// Root user specific check: if IsRoot but not localhost, deny.
	// This check applies ONLY to users marked as IsRoot in the system collection.
	if storedUserInfo.IsRoot && !h.IsLocalhostConn {
		log.Printf("Authentication failed for root user '%s' from %s: Not a localhost connection.", username, conn.RemoteAddr())
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "Authentication failed: Root access only from localhost.", nil)
		return
	}

	// Authentication successful!
	h.IsAuthenticated = true
	h.AuthenticatedUser = username
	log.Printf("User '%s' authenticated successfully from %s.", username, conn.RemoteAddr())
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Authenticated as '%s'.", username), nil)
}

// handleChangeUserPassword processes the CmdChangeUserPassword command.
// This command is now exclusively for the root user from localhost.
func (h *ConnectionHandler) handleChangeUserPassword(conn net.Conn) {
	targetUsername, newPassword, err := protocol.ReadChangeUserPasswordCommand(conn)
	if err != nil {
		log.Printf("Error reading CHANGE_USER_PASSWORD command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid CHANGE_USER_PASSWORD command format", nil)
		return
	}

	if targetUsername == "" || newPassword == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Target username and new password cannot be empty.", nil)
		return
	}

	// Security Check: Only 'root' authenticated from localhost can execute this command.
	if !(h.IsAuthenticated && h.AuthenticatedUser == "root" && h.IsLocalhostConn) {
		log.Printf("Unauthorized attempt to change password for '%s' by user '%s' from %s (IsLocalhost: %t).",
			targetUsername, h.AuthenticatedUser, conn.RemoteAddr(), h.IsLocalhostConn)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: This command can only be executed by 'root' from localhost.", nil)
		return
	}

	sysCol := h.CollectionManager.GetCollection(SystemCollectionName)
	targetUserKey := UserPrefix + targetUsername

	userDataBytes, found := sysCol.Get(targetUserKey)
	if !found {
		log.Printf("Failed to change password for '%s': Target user not found.", targetUsername)
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: User '%s' does not exist.", targetUsername), nil)
		return
	}

	var storedUserInfo UserInfo
	if err := json.Unmarshal(userDataBytes, &storedUserInfo); err != nil {
		log.Printf("Error unmarshalling user info for '%s' during password change: %v", targetUsername, err)
		protocol.WriteResponse(conn, protocol.StatusError, "Internal server error: Invalid user data.", nil)
		return
	}

	newHashedPassword, hashErr := HashPassword(newPassword)
	if hashErr != nil {
		log.Printf("Error hashing new password for user '%s': %v", targetUsername, hashErr)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to hash new password.", nil)
		return
	}

	storedUserInfo.PasswordHash = newHashedPassword
	updatedUserInfoBytes, marshalErr := json.Marshal(storedUserInfo)
	if marshalErr != nil {
		log.Printf("Error marshalling updated user info for '%s': %v", targetUsername, marshalErr)
		protocol.WriteResponse(conn, protocol.StatusError, "Internal server error: Failed to marshal updated user data.", nil)
		return
	}

	sysCol.Set(targetUserKey, updatedUserInfoBytes, 0)
	if err := h.CollectionManager.SaveCollectionToDisk(SystemCollectionName, sysCol); err != nil {
		log.Printf("Error saving system collection after password change for '%s': %v", targetUsername, err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to persist password change.", nil)
	} else {
		log.Printf("Password for user '%s' changed successfully by 'root' from %s.", targetUsername, conn.RemoteAddr())
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Password for user '%s' updated successfully.", targetUsername), nil)
	}
}

// HashPassword hashes a password using bcrypt.
func HashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	return string(bytes), err
}

// CheckPasswordHash compares a password with its bcrypt hash.
func CheckPasswordHash(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

// processCollectionQuery executes a complex query on a collection.
// It applies filters, performs aggregations, orders, limits, and offsets.
func (h *ConnectionHandler) processCollectionQuery(collectionName string, query Query) (any, error) {
	colStore := h.CollectionManager.GetCollection(collectionName)
	allData := colStore.GetAll() // Get all non-expired data from the collection

	// Convert map[string][]byte to map[string]map[string]any for easier processing
	// We also need to store the original key to return it if needed
	var itemsWithKeys []struct {
		Key string
		Val map[string]any
	}
	for k, vBytes := range allData {
		var val map[string]any
		if err := json.Unmarshal(vBytes, &val); err != nil {
			log.Printf("Warning: Failed to unmarshal JSON for key '%s' in collection '%s': %v", k, collectionName, err)
			continue // Skip corrupted items
		}
		itemsWithKeys = append(itemsWithKeys, struct {
			Key string
			Val map[string]any
		}{Key: k, Val: val})
	}

	// 1. Filtering (WHERE clause)
	filteredItems := []struct {
		Key string
		Val map[string]any
	}{}
	for _, item := range itemsWithKeys {
		if h.matchFilter(item.Val, query.Filter) {
			filteredItems = append(filteredItems, item)
		}
	}

	// Handle DISTINCT early if requested
	if query.Distinct != "" {
		distinctValues := make(map[any]bool)
		var resultList []any
		for _, item := range filteredItems {
			if val, ok := item.Val[query.Distinct]; ok && val != nil {
				if _, seen := distinctValues[val]; !seen {
					distinctValues[val] = true
					resultList = append(resultList, val)
				}
			}
		}
		return resultList, nil // Distinct is a terminal operation
	}

	// NEW: Handle top-level Count if no other aggregations or group by are specified
	if query.Count && len(query.Aggregations) == 0 && len(query.GroupBy) == 0 {
		return map[string]int{"count": len(filteredItems)}, nil
	}

	// 2. Aggregations & Group By
	if len(query.Aggregations) > 0 || len(query.GroupBy) > 0 {
		return h.performAggregations(filteredItems, query)
	}

	// For non-aggregated queries, continue with sorting and pagination
	results := make([]map[string]any, 0, len(filteredItems))
	for _, item := range filteredItems {
		results = append(results, item.Val)
	}

	// 3. Ordering (ORDER BY clause)
	if len(query.OrderBy) > 0 {
		sort.Slice(results, func(i, j int) bool {
			for _, ob := range query.OrderBy {
				valA, okA := results[i][ob.Field]
				valB, okB := results[j][ob.Field]

				// Handle missing fields (nulls first/last depends on DB, here we'll put missing first for consistency)
				if !okA && !okB {
					continue // Both missing, continue to next order by
				}
				if !okA {
					return true // A is missing, A comes first
				}
				if !okB {
					return false // B is missing, B comes first (A does not come first)
				}

				cmp := compare(valA, valB)
				if cmp != 0 {
					if ob.Direction == "desc" {
						return cmp > 0
					}
					return cmp < 0
				}
			}
			return false // Items are equal based on all order by criteria
		})
	}

	// 4. Pagination (OFFSET and LIMIT)
	offset := query.Offset
	if offset < 0 {
		offset = 0
	}
	if offset > len(results) {
		offset = len(results)
	}
	results = results[offset:]

	if query.Limit != nil && *query.Limit >= 0 {
		limit := *query.Limit
		if limit == 0 { // Explicitly limit 0 means empty result set
			return []map[string]any{}, nil
		}
		if limit > len(results) {
			limit = len(results)
		}
		results = results[:limit]
	}

	return results, nil
}

// matchFilter evaluates an item against a filter condition (recursive for AND/OR/NOT).
// Filter structure example:
// {"and": [{"field": "age", "op": ">", "value": 30}, {"field": "city", "op": "=", "value": "New York"}]}
// {"or": [...]}
// {"not": {"field": "status", "op": "=", "value": "inactive"}}
// {"field": "name", "op": "like", "value": "J%"}
func (h *ConnectionHandler) matchFilter(item map[string]any, filter map[string]any) bool {
	if len(filter) == 0 {
		return true // No filter, matches all
	}

	// AND condition
	if andConditions, ok := filter["and"].([]any); ok {
		for _, cond := range andConditions {
			if condMap, isMap := cond.(map[string]any); isMap {
				if !h.matchFilter(item, condMap) {
					return false
				}
			} else {
				log.Printf("Warning: Invalid 'and' condition format: %+v", cond)
				return false // Treat malformed filter as no match
			}
		}
		return true
	}

	// OR condition
	if orConditions, ok := filter["or"].([]any); ok {
		for _, cond := range orConditions {
			if condMap, isMap := cond.(map[string]any); isMap {
				if h.matchFilter(item, condMap) {
					return true
				}
			} else {
				log.Printf("Warning: Invalid 'or' condition format: %+v", cond)
				return false // Treat malformed filter as no match
			}
		}
		return false
	}

	// NOT condition
	if notCondition, ok := filter["not"].(map[string]any); ok {
		return !h.matchFilter(item, notCondition)
	}

	// Single field condition
	field, fieldOk := filter["field"].(string)
	op, opOk := filter["op"].(string)
	value := filter["value"] // Value can be nil, array, string, number, bool

	if !fieldOk || !opOk {
		log.Printf("Warning: Invalid filter condition (missing field/op): %+v", filter)
		return false
	}

	itemValue, itemValueExists := item[field]

	switch op {
	case "=":
		if !itemValueExists {
			return false
		}
		return compare(itemValue, value) == 0
	case "!=":
		if !itemValueExists {
			return true
		} // If item value doesn't exist, it's not equal to anything
		return compare(itemValue, value) != 0
	case ">":
		if !itemValueExists {
			return false
		}
		return compare(itemValue, value) > 0
	case ">=":
		if !itemValueExists {
			return false
		}
		return compare(itemValue, value) >= 0
	case "<":
		if !itemValueExists {
			return false
		}
		return compare(itemValue, value) < 0
	case "<=":
		if !itemValueExists {
			return false
		}
		return compare(itemValue, value) <= 0
	case "like": // Case-insensitive, % as wildcard
		if !itemValueExists {
			return false
		}
		if sVal, isStr := itemValue.(string); isStr {
			if pattern, isStrPattern := value.(string); isStrPattern {
				pattern = strings.ReplaceAll(regexp.QuoteMeta(pattern), "%", ".*")
				matched, err := regexp.MatchString("(?i)^"+pattern+"$", sVal) // (?i) for case-insensitive
				if err != nil {
					log.Printf("Error in LIKE regex for pattern '%s': %v", pattern, err)
					return false
				}
				return matched
			}
		}
		return false
	case "between":
		if !itemValueExists {
			return false
		}
		if values, ok := value.([]any); ok && len(values) == 2 {
			return compare(itemValue, values[0]) >= 0 && compare(itemValue, values[1]) <= 0
		}
		return false
	case "in":
		if !itemValueExists {
			return false
		}
		if values, ok := value.([]any); ok {
			for _, v := range values {
				if compare(itemValue, v) == 0 {
					return true
				}
			}
		}
		return false
	case "is null":
		return !itemValueExists || itemValue == nil
	case "is not null":
		return itemValueExists && itemValue != nil
	default:
		log.Printf("Warning: Unsupported filter operator '%s'", op)
		return false
	}
}

// compare two any values (numbers, strings, bools). Returns -1 if a<b, 0 if a==b, 1 if a>b.
// Handles different numeric types by converting to float64 for comparison.
func compare(a, b any) int {
	// Try numeric comparison first
	if numA, okA := toFloat64(a); okA {
		if numB, okB := toFloat64(b); okB {
			if numA < numB {
				return -1
			}
			if numA > numB {
				return 1
			}
			return 0
		}
	}

	// Fallback to string comparison
	strA := fmt.Sprintf("%v", a)
	strB := fmt.Sprintf("%v", b)
	return strings.Compare(strA, strB)
}

// toFloat64 attempts to convert an any to float64, returns false if not a number.
func toFloat64(val any) (float64, bool) {
	switch v := val.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case jsoniter.Number: // jsoniter's numeric type
		f, err := v.Float64()
		return f, err == nil
	case string: // Try parsing string to float
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// performAggregations handles GROUP BY and aggregation functions.
func (h *ConnectionHandler) performAggregations(items []struct {
	Key string
	Val map[string]any
}, query Query) (any, error) {
	// Map to store grouped results: groupKey -> []items_in_group
	groupedData := make(map[string][]map[string]any)

	// If no GROUP BY, all items go into a single "default" group
	if len(query.GroupBy) == 0 {
		groupKey := "_no_group_"
		groupedData[groupKey] = make([]map[string]any, 0, len(items))
		for _, item := range items {
			groupedData[groupKey] = append(groupedData[groupKey], item.Val)
		}
	} else {
		for _, item := range items {
			groupKeyParts := make([]string, len(query.GroupBy))
			for i, field := range query.GroupBy {
				if val, ok := item.Val[field]; ok && val != nil {
					groupKeyParts[i] = fmt.Sprintf("%v", val)
				} else {
					groupKeyParts[i] = "NULL" // Consistent representation for missing/null group keys
				}
			}
			groupKey := strings.Join(groupKeyParts, "|") // Composite key for grouping
			groupedData[groupKey] = append(groupedData[groupKey], item.Val)
		}
	}

	// Calculate aggregations for each group
	var aggregatedResults []map[string]any
	for groupKey, groupItems := range groupedData {
		resultRow := make(map[string]any)

		// Add GroupBy fields to the result row
		if len(query.GroupBy) > 0 {
			if groupKey == "_no_group_" {
				// This case should ideally not happen if len(query.GroupBy) > 0
				// but as a fallback, ensure the group by fields are not added if no actual grouping occurred.
			} else {
				groupKeyValues := strings.Split(groupKey, "|")
				for i, field := range query.GroupBy {
					if i < len(groupKeyValues) {
						// Attempt to convert back to original type if possible (e.g., number, bool)
						// For now, keep as string as we don't know original type
						resultRow[field] = groupKeyValues[i]
						// More robust parsing could go here based on expected field types
					}
				}
			}
		}

		// Process aggregations
		for aggName, agg := range query.Aggregations {
			var aggValue any
			var err error

			switch agg.Func {
			case "count":
				// Count is special; depends on whether it's count(*) or count(field)
				if agg.Field == "*" {
					aggValue = len(groupItems)
				} else {
					count := 0
					for _, item := range groupItems {
						if _, ok := item[agg.Field]; ok {
							count++
						}
					}
					aggValue = count
				}
			case "sum", "avg", "min", "max":
				numbers := []float64{}
				for _, item := range groupItems {
					if val, ok := item[agg.Field]; ok {
						if num, convertedOk := toFloat64(val); convertedOk {
							numbers = append(numbers, num)
						}
					}
				}

				if len(numbers) == 0 {
					aggValue = nil // No numeric data to aggregate
					continue
				}

				switch agg.Func {
				case "sum":
					sum := 0.0
					for _, n := range numbers {
						sum += n
					}
					aggValue = sum
				case "avg":
					sum := 0.0
					for _, n := range numbers {
						sum += n
					}
					aggValue = sum / float64(len(numbers))
				case "min":
					min := numbers[0]
					for _, n := range numbers {
						if n < min {
							min = n
						}
					}
					aggValue = min
				case "max":
					max := numbers[0]
					for _, n := range numbers {
						if n > max {
							max = n
						}
					}
					aggValue = max
				default:
					err = fmt.Errorf("unsupported aggregation function: %s", agg.Func)
				}
			default:
				err = fmt.Errorf("unsupported aggregation function: %s", agg.Func)
			}

			if err != nil {
				return nil, err
			}
			resultRow[aggName] = aggValue
		}

		// 3. Having clause (filters aggregated results)
		if h.matchFilter(resultRow, query.Having) {
			aggregatedResults = append(aggregatedResults, resultRow)
		}
	}

	return aggregatedResults, nil
}

// ensureIDField unmarshals the value, ensures it's a JSON object (map),
// sets the "_id" field with the provided key, and marshals it back to bytes.
func ensureIDField(value []byte, key string) ([]byte, error) {
	var data map[string]any
	// Attempt to unmarshal as a map. If it's not a map, we can't inject _id.
	if err := json.Unmarshal(value, &data); err != nil {
		// If it's not a JSON object, or is invalid JSON, return original value and an error.
		// Or, if you want to be more strict, return an error.
		return value, fmt.Errorf("value is not a JSON object, cannot inject _id field: %w", err)
	}

	// Set or override the _id field.
	data["_id"] = key

	updatedValue, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON after injecting _id: %w", err)
	}
	return updatedValue, nil
}
