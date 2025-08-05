package handler

import (
	"fmt"
	"log/slog"
	"memory-tools/internal/protocol"
	"net"
)

// handleUserCreate processes the CmdUserCreate command.
func (h *ConnectionHandler) handleUserCreate(conn net.Conn) {
	// Authorization check: Must have write permission on the system collection.
	if !h.hasPermission(SystemCollectionName, "write") {
		slog.Warn("Unauthorized user creation attempt",
			"user", h.AuthenticatedUser,
			"remote_addr", conn.RemoteAddr().String(),
		)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: You do not have permission to create users.", nil)
		return
	}

	username, password, permissionsJSON, err := protocol.ReadUserCreateCommand(conn)
	if err != nil {
		slog.Error("Failed to read USER_CREATE command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid USER_CREATE command format", nil)
		return
	}
	if username == "" || password == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Username and password cannot be empty", nil)
		return
	}

	var permissions map[string]string
	if err := json.Unmarshal(permissionsJSON, &permissions); err != nil {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid permissions JSON format", nil)
		return
	}

	sysCol := h.CollectionManager.GetCollection(SystemCollectionName)
	userKey := UserPrefix + username

	if _, found := sysCol.Get(userKey); found {
		slog.Warn("User creation failed: user already exists", "username", username, "admin_user", h.AuthenticatedUser)
		protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("User '%s' already exists", username), nil)
		return
	}

	hashedPassword, err := HashPassword(password)
	if err != nil {
		slog.Error("Failed to hash password during user creation", "username", username, "error", err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to hash password", nil)
		return
	}

	newUser := UserInfo{
		Username:     username,
		PasswordHash: hashedPassword,
		IsRoot:       false, // New users created via command are never root.
		Permissions:  permissions,
	}

	userBytes, err := json.Marshal(newUser)
	if err != nil {
		slog.Error("Failed to serialize new user data", "username", username, "error", err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to serialize user data", nil)
		return
	}

	sysCol.Set(userKey, userBytes, 0)
	h.CollectionManager.EnqueueSaveTask(SystemCollectionName, sysCol)

	slog.Info("User created successfully", "admin_user", h.AuthenticatedUser, "new_user", username)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("User '%s' created successfully", username), nil)
}

// handleUserUpdate processes the CmdUserUpdate command.
func (h *ConnectionHandler) handleUserUpdate(conn net.Conn) {
	// Authorization check
	if !h.hasPermission(SystemCollectionName, "write") {
		slog.Warn("Unauthorized user update attempt",
			"user", h.AuthenticatedUser,
			"remote_addr", conn.RemoteAddr().String(),
		)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: You do not have permission to update users.", nil)
		return
	}

	username, permissionsJSON, err := protocol.ReadUserUpdateCommand(conn)
	if err != nil {
		slog.Error("Failed to read USER_UPDATE command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid USER_UPDATE command format", nil)
		return
	}

	var newPermissions map[string]string
	if err := json.Unmarshal(permissionsJSON, &newPermissions); err != nil {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid permissions JSON format", nil)
		return
	}

	sysCol := h.CollectionManager.GetCollection(SystemCollectionName)
	userKey := UserPrefix + username

	userData, found := sysCol.Get(userKey)
	if !found {
		slog.Warn("User update failed: user not found", "target_user", username, "admin_user", h.AuthenticatedUser)
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("User '%s' not found", username), nil)
		return
	}

	var userInfo UserInfo
	json.Unmarshal(userData, &userInfo)

	// Root status cannot be changed via this command.
	if userInfo.IsRoot {
		slog.Warn("User update failed: attempt to modify root user", "target_user", username, "admin_user", h.AuthenticatedUser)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "Cannot modify root user's permissions via this command.", nil)
		return
	}

	userInfo.Permissions = newPermissions
	userBytes, _ := json.Marshal(userInfo)

	sysCol.Set(userKey, userBytes, 0)
	h.CollectionManager.EnqueueSaveTask(SystemCollectionName, sysCol)

	slog.Info("User permissions updated successfully", "admin_user", h.AuthenticatedUser, "target_user", username)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("Permissions for user '%s' updated successfully", username), nil)
}

// handleUserDelete processes the CmdUserDelete command.
func (h *ConnectionHandler) handleUserDelete(conn net.Conn) {
	// Authorization check
	if !h.hasPermission(SystemCollectionName, "write") {
		slog.Warn("Unauthorized user delete attempt",
			"user", h.AuthenticatedUser,
			"remote_addr", conn.RemoteAddr().String(),
		)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: You do not have permission to delete users.", nil)
		return
	}

	username, err := protocol.ReadUserDeleteCommand(conn)
	if err != nil {
		slog.Error("Failed to read USER_DELETE command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid USER_DELETE command format", nil)
		return
	}

	sysCol := h.CollectionManager.GetCollection(SystemCollectionName)
	userKey := UserPrefix + username

	userData, found := sysCol.Get(userKey)
	if !found {
		slog.Warn("User delete failed: user not found", "target_user", username, "admin_user", h.AuthenticatedUser)
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("User '%s' not found", username), nil)
		return
	}

	var userInfo UserInfo
	json.Unmarshal(userData, &userInfo)

	if userInfo.IsRoot {
		slog.Warn("User delete failed: attempt to delete root user", "target_user", username, "admin_user", h.AuthenticatedUser)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "Cannot delete root user.", nil)
		return
	}

	sysCol.Delete(userKey)
	h.CollectionManager.EnqueueSaveTask(SystemCollectionName, sysCol)

	slog.Info("User deleted successfully", "admin_user", h.AuthenticatedUser, "deleted_user", username)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("User '%s' deleted successfully", username), nil)
}
