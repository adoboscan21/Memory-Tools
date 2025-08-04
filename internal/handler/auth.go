package handler

import (
	"fmt"
	"log"
	"memory-tools/internal/protocol"
	"net"

	"golang.org/x/crypto/bcrypt"
)

// hasPermission checks if the authenticated user has the required permission level for a collection.
func (h *ConnectionHandler) hasPermission(collectionName string, requiredLevel string) bool {
	// Root user bypasses all permission checks.
	if h.IsRoot {
		return true
	}

	// Get the specific permission for the collection.
	level, specificFound := h.Permissions[collectionName]

	// If not found, check for wildcard permission.
	if !specificFound {
		level, specificFound = h.Permissions["*"]
	}

	// If still no permission is found, access is denied.
	if !specificFound {
		return false
	}

	// "write" permission implies "read" permission.
	if requiredLevel == "read" && level == "write" {
		return true
	}

	// Direct match.
	return level == requiredLevel
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

	if storedUserInfo.IsRoot && !h.IsLocalhostConn {
		log.Printf("Authentication failed for root user '%s' from %s: Not a localhost connection.", username, conn.RemoteAddr())
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "Authentication failed: Root access only from localhost.", nil)
		return
	}

	// Authentication successful!
	h.IsAuthenticated = true
	h.AuthenticatedUser = username
	h.IsRoot = storedUserInfo.IsRoot           // Cache IsRoot status
	h.Permissions = storedUserInfo.Permissions // Cache permissions

	log.Printf("User '%s' authenticated successfully from %s.", username, conn.RemoteAddr())
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Authenticated as '%s'.", username), nil)
}

// handleChangeUserPassword processes the CmdChangeUserPassword command.
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

	// Authorization: Only the root user can change passwords.
	if !h.IsRoot {
		log.Printf("Unauthorized password change attempt by non-root user '%s' from %s.", h.AuthenticatedUser, conn.RemoteAddr())
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Only root can change passwords.", nil)
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
	h.CollectionManager.EnqueueSaveTask(SystemCollectionName, sysCol)

	log.Printf("Password for user '%s' changed successfully by '%s' from %s.", targetUsername, h.AuthenticatedUser, conn.RemoteAddr())
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Password for user '%s' updated successfully.", targetUsername), nil)

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
