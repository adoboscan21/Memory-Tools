package handler

import (
	"fmt"
	"io"
	"log"
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
			return // Exit goroutine on read error
		}

		h.ActivityUpdater.UpdateActivity()

		// --- MAIN COMMAND DISPATCH ---
		switch cmdType {
		case protocol.CmdAuthenticate:
			h.handleAuthenticate(conn)
			continue

		case protocol.CmdChangeUserPassword:
			h.handleChangeUserPassword(conn)
			continue

		default:
			if !h.IsAuthenticated {
				log.Printf("Unauthorized access attempt from %s for command %d. Connection not authenticated.", conn.RemoteAddr(), cmdType)
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

			// Collection Query Command
			case protocol.CmdCollectionQuery:
				h.handleCollectionQuery(conn)

			default:
				log.Printf("Received unhandled or unknown authenticated command type %d from client %s.", cmdType, conn.RemoteAddr())
				if err := protocol.WriteResponse(conn, protocol.StatusBadCommand, fmt.Sprintf("BAD COMMAND: Unhandled or unknown command type %d", cmdType), nil); err != nil {
					log.Printf("Error writing bad command response to %s: %v", conn.RemoteAddr(), err)
				}
			}
		}
	}
}
