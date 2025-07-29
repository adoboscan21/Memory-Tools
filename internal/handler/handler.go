package handler

import (
	"fmt"
	"io"
	"log"
	"memory-tools/internal/protocol"
	"memory-tools/internal/store"
	"net"
	"strings"

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
			return
		}

		h.ActivityUpdater.UpdateActivity()

		// Commands that do not require prior authentication.
		switch cmdType {
		case protocol.CmdAuthenticate:
			h.handleAuthenticate(conn)
			continue
		case protocol.CmdChangeUserPassword:
			h.handleChangeUserPassword(conn)
			continue
		}

		// All other commands require authentication.
		if !h.IsAuthenticated {
			log.Printf("Unauthorized access attempt from %s (command %d). Connection not authenticated.", conn.RemoteAddr(), cmdType)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Please authenticate first.", nil)
			continue
		}

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
			if collectionName == "" || key == "" || len(value) == 0 {
				protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name, key, or value cannot be empty", nil)
				continue
			}
			// Prevent normal users from modifying the system collection directly
			if collectionName == SystemCollectionName && !(h.AuthenticatedUser == "root" && h.IsLocalhostConn) {
				protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: Only 'root' from localhost can modify collection '%s'", SystemCollectionName), nil)
				continue
			}

			colStore := h.CollectionManager.GetCollection(collectionName)
			colStore.Set(key, value, ttl)
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
			// Prevent normal users from deleting from the system collection
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

		default:
			log.Printf("Received unknown command type %d from %s", cmdType, conn.RemoteAddr())
			if err := protocol.WriteResponse(conn, protocol.StatusBadCommand, fmt.Sprintf("BAD COMMAND: Unknown command type %d", cmdType), nil); err != nil {
				log.Printf("Error writing unknown command response to %s: %v", conn.RemoteAddr(), err)
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

	// Root user specific check: if is_root but not localhost, deny.
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

	// Security Check: Only root from localhost can execute this command.
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
