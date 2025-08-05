package handler

import (
	"fmt"
	"io"
	"log/slog"
	"memory-tools/internal/persistence"
	"memory-tools/internal/protocol"
	"memory-tools/internal/store"
	"net"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const SystemCollectionName = "_system"
const UserPrefix = "user:"

// ActivityUpdater is an interface for updating activity timestamps.
type ActivityUpdater interface {
	UpdateActivity()
}

// ConnectionHandler holds the dependencies needed to handle client connections.
type ConnectionHandler struct {
	MainStore         store.DataStore
	CollectionManager *store.CollectionManager
	BackupManager     *persistence.BackupManager
	ActivityUpdater   ActivityUpdater   // Dependency for updating activity
	IsAuthenticated   bool              // Tracks authentication status for this connection
	AuthenticatedUser string            // Stores the authenticated username
	IsLocalhostConn   bool              // True if connection is from localhost
	IsRoot            bool              // Cache if the user is root
	Permissions       map[string]string // Cache user permissions for quick lookups
}

// NewConnectionHandler creates a new instance of ConnectionHandler.
func NewConnectionHandler(mainStore store.DataStore, colManager *store.CollectionManager, backupManager *persistence.BackupManager, updater ActivityUpdater, conn net.Conn) *ConnectionHandler {
	isLocal := false
	if host, _, err := net.SplitHostPort(conn.RemoteAddr().String()); err == nil {
		if host == "127.0.0.1" || host == "::1" || host == "localhost" {
			isLocal = true
		}
	}

	return &ConnectionHandler{
		MainStore:         mainStore,
		CollectionManager: colManager,
		BackupManager:     backupManager,
		ActivityUpdater:   updater,
		IsAuthenticated:   false,
		AuthenticatedUser: "",
		IsLocalhostConn:   isLocal,
		Permissions:       make(map[string]string),
	}
}

// HandleConnection processes incoming commands from a TCP client.
func (h *ConnectionHandler) HandleConnection(conn net.Conn) {
	defer conn.Close()
	slog.Info("New client connected", "remote_addr", conn.RemoteAddr().String(), "is_localhost", h.IsLocalhostConn)

	for {
		cmdType, err := protocol.ReadCommandType(conn)
		if err != nil {
			if err == io.EOF {
				slog.Info("Client disconnected", "remote_addr", conn.RemoteAddr().String())
			} else {
				slog.Error("Failed to read command type", "remote_addr", conn.RemoteAddr().String(), "error", err)
			}
			return // Exit goroutine on read error
		}

		h.ActivityUpdater.UpdateActivity()

		// Authentication command can be run by anyone.
		if cmdType == protocol.CmdAuthenticate {
			h.handleAuthenticate(conn)
			continue
		}

		// All other commands require authentication.
		if !h.IsAuthenticated {
			slog.Warn("Unauthorized access attempt",
				"remote_addr", conn.RemoteAddr().String(),
				"command_type", cmdType,
			)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Please authenticate first.", nil)
			continue
		}

		// If we reach here, the client is authenticated. Now, dispatch the command.
		switch cmdType {
		// Main Store Commands.
		case protocol.CmdSet:
			h.handleMainStoreSet(conn)
		case protocol.CmdGet:
			h.handleMainStoreGet(conn)

		// Collection Management Commands.
		case protocol.CmdCollectionCreate:
			h.handleCollectionCreate(conn)
		case protocol.CmdCollectionDelete:
			h.handleCollectionDelete(conn)
		case protocol.CmdCollectionList:
			h.handleCollectionList(conn)
		case protocol.CmdCollectionIndexCreate:
			h.handleCollectionIndexCreate(conn)
		case protocol.CmdCollectionIndexDelete:
			h.handleCollectionIndexDelete(conn)
		case protocol.CmdCollectionIndexList:
			h.handleCollectionIndexList(conn)

		// Collection Item Commands.
		case protocol.CmdCollectionItemSet:
			h.handleCollectionItemSet(conn)
		case protocol.CmdCollectionItemSetMany:
			h.handleCollectionItemSetMany(conn)
		case protocol.CmdCollectionItemDeleteMany:
			h.handleCollectionItemDeleteMany(conn)
		case protocol.CmdCollectionItemGet:
			h.handleCollectionItemGet(conn)
		case protocol.CmdCollectionItemDelete:
			h.handleCollectionItemDelete(conn)
		case protocol.CmdCollectionItemList:
			h.handleCollectionItemList(conn)
		case protocol.CmdCollectionItemUpdate:
			h.handleCollectionItemUpdate(conn)
		case protocol.CmdCollectionItemUpdateMany:
			h.handleCollectionItemUpdateMany(conn)

		// Collection Query Command
		case protocol.CmdCollectionQuery:
			h.handleCollectionQuery(conn)

		// User Management Commands
		case protocol.CmdChangeUserPassword:
			h.handleChangeUserPassword(conn)
		case protocol.CmdUserCreate:
			h.handleUserCreate(conn)
		case protocol.CmdUserUpdate:
			h.handleUserUpdate(conn)
		case protocol.CmdUserDelete:
			h.handleUserDelete(conn)

		case protocol.CmdBackup:
			h.handleBackup(conn)
		case protocol.CmdRestore:
			h.handleRestore(conn)

		default:
			slog.Warn("Received unhandled command type",
				"command_type", cmdType,
				"remote_addr", conn.RemoteAddr().String(),
			)
			if err := protocol.WriteResponse(conn, protocol.StatusBadCommand, fmt.Sprintf("BAD COMMAND: Unhandled or unknown command type %d", cmdType), nil); err != nil {
				slog.Error("Failed to write bad command response", "remote_addr", conn.RemoteAddr().String(), "error", err)
			}
		}
	}
}
