package handler

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"memory-tools/internal/persistence"
	"memory-tools/internal/protocol"
	"memory-tools/internal/store"
	"memory-tools/internal/wal"
	"net"
	"sync"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type ActivityUpdater interface {
	UpdateActivity()
}

type ConnectionHandler struct {
	Wal                  *wal.WAL
	MainStore            store.DataStore
	CollectionManager    *store.CollectionManager
	BackupManager        *persistence.BackupManager
	ActivityUpdater      ActivityUpdater
	IsAuthenticated      bool
	AuthenticatedUser    string
	IsLocalhostConn      bool
	IsRoot               bool
	Permissions          map[string]string
	TransactionManager   *store.TransactionManager
	CurrentTransactionID string
}

var connectionHandlerPool = sync.Pool{
	New: func() any {
		return &ConnectionHandler{
			Permissions: make(map[string]string),
		}
	},
}

func (h *ConnectionHandler) Reset() {
	h.Wal = nil
	h.MainStore = nil
	h.CollectionManager = nil
	h.BackupManager = nil
	h.ActivityUpdater = nil
	h.IsAuthenticated = false
	h.AuthenticatedUser = ""
	h.IsLocalhostConn = false
	h.IsRoot = false
	clear(h.Permissions)
	h.TransactionManager = nil
	h.CurrentTransactionID = ""
}

func GetConnectionHandlerFromPool(
	wal *wal.WAL,
	mainStore store.DataStore,
	colManager *store.CollectionManager,
	backupManager *persistence.BackupManager,
	txManager *store.TransactionManager,
	updater ActivityUpdater,
	conn net.Conn,
) *ConnectionHandler {
	h := connectionHandlerPool.Get().(*ConnectionHandler)

	isLocal := false
	if conn != nil {
		if host, _, err := net.SplitHostPort(conn.RemoteAddr().String()); err == nil {
			if host == "127.0.0.1" || host == "::1" || host == "localhost" {
				isLocal = true
			}
		}
	}

	h.Wal = wal
	h.MainStore = mainStore
	h.CollectionManager = colManager
	h.BackupManager = backupManager
	h.TransactionManager = txManager
	h.ActivityUpdater = updater
	h.IsLocalhostConn = isLocal

	return h
}

func PutConnectionHandlerToPool(h *ConnectionHandler) {
	if h.CurrentTransactionID != "" {
		slog.Warn("Connection closed mid-transaction, rolling back.", "txID", h.CurrentTransactionID)
		h.TransactionManager.Rollback(h.CurrentTransactionID)
	}
	h.Reset()
	connectionHandlerPool.Put(h)
}

func isWriteCommand(cmdType protocol.CommandType) bool {
	switch cmdType {
	case
		protocol.CmdSet,
		protocol.CmdCollectionCreate,
		protocol.CmdCollectionDelete,
		protocol.CmdCollectionIndexCreate,
		protocol.CmdCollectionIndexDelete,
		protocol.CmdCollectionItemSet,
		protocol.CmdCollectionItemSetMany,
		protocol.CmdCollectionItemDelete,
		protocol.CmdCollectionItemDeleteMany,
		protocol.CmdCollectionItemUpdate,
		protocol.CmdCollectionItemUpdateMany,
		protocol.CmdChangeUserPassword,
		protocol.CmdUserCreate,
		protocol.CmdUserUpdate,
		protocol.CmdUserDelete,
		protocol.CmdCommit,
		protocol.CmdRestore:
		return true
	default:
		return false
	}
}

func (h *ConnectionHandler) HandleConnection(conn net.Conn) {
	defer conn.Close()
	slog.Info("New client connected", "remote_addr", conn.RemoteAddr().String(), "is_localhost", h.IsLocalhostConn)

	for {
		cmdType, err := protocol.ReadCommandType(conn)
		if err != nil {
			if err != io.EOF {
				slog.Error("Failed to read command type", "remote_addr", conn.RemoteAddr().String(), "error", err)
			} else {
				slog.Info("Client disconnected", "remote_addr", conn.RemoteAddr().String())
			}
			return
		}

		h.ActivityUpdater.UpdateActivity()

		var reader io.Reader = conn

		if h.Wal != nil && isWriteCommand(cmdType) {
			payload, err := protocol.ReadCommandPayload(conn, cmdType)
			if err != nil {
				slog.Error("Failed to read command payload for WAL", "error", err, "command_type", cmdType)
				protocol.WriteResponse(conn, protocol.StatusError, "Internal server error reading command", nil)
				continue
			}

			entry := wal.WalEntry{
				CommandType: cmdType,
				Payload:     payload,
			}

			if err := h.Wal.Write(entry); err != nil {
				slog.Error("CRITICAL: Failed to write to WAL", "error", err)
				protocol.WriteResponse(conn, protocol.StatusError, "Internal server error: could not persist command", nil)
				continue
			}
			reader = bytes.NewReader(payload)
		}

		if cmdType == protocol.CmdAuthenticate {
			h.handleAuthenticate(reader, conn)
			continue
		}

		if !h.IsAuthenticated {
			slog.Warn("Unauthorized access attempt", "remote_addr", conn.RemoteAddr().String(), "command_type", cmdType)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Please authenticate first.", nil)
			io.Copy(io.Discard, reader)
			continue
		}

		switch cmdType {
		case protocol.CmdBegin:
			h.handleBegin(reader, conn)
		case protocol.CmdCommit:
			h.HandleCommit(reader, conn)
		case protocol.CmdRollback:
			h.handleRollback(reader, conn)
		case protocol.CmdSet:
			h.HandleMainStoreSet(reader, conn)
		case protocol.CmdGet:
			h.handleMainStoreGet(reader, conn)
		case protocol.CmdCollectionCreate:
			h.HandleCollectionCreate(reader, conn)
		case protocol.CmdCollectionDelete:
			h.HandleCollectionDelete(reader, conn)
		case protocol.CmdCollectionList:
			h.handleCollectionList(reader, conn)
		case protocol.CmdCollectionIndexCreate:
			h.HandleCollectionIndexCreate(reader, conn)
		case protocol.CmdCollectionIndexDelete:
			h.HandleCollectionIndexDelete(reader, conn)
		case protocol.CmdCollectionIndexList:
			h.handleCollectionIndexList(reader, conn)
		case protocol.CmdCollectionItemSet:
			h.HandleCollectionItemSet(reader, conn)
		case protocol.CmdCollectionItemSetMany:
			h.HandleCollectionItemSetMany(reader, conn)
		case protocol.CmdCollectionItemDeleteMany:
			h.HandleCollectionItemDeleteMany(reader, conn)
		case protocol.CmdCollectionItemGet:
			h.handleCollectionItemGet(reader, conn)
		case protocol.CmdCollectionItemDelete:
			h.HandleCollectionItemDelete(reader, conn)
		case protocol.CmdCollectionItemList:
			h.handleCollectionItemList(reader, conn)
		case protocol.CmdCollectionItemUpdate:
			h.HandleCollectionItemUpdate(reader, conn)
		case protocol.CmdCollectionItemUpdateMany:
			h.HandleCollectionItemUpdateMany(reader, conn)
		case protocol.CmdCollectionQuery:
			h.handleCollectionQuery(reader, conn)
		case protocol.CmdChangeUserPassword:
			h.HandleChangeUserPassword(reader, conn)
		case protocol.CmdUserCreate:
			h.HandleUserCreate(reader, conn)
		case protocol.CmdUserUpdate:
			h.HandleUserUpdate(reader, conn)
		case protocol.CmdUserDelete:
			h.HandleUserDelete(reader, conn)
		case protocol.CmdBackup:
			h.handleBackup(reader, conn)
		case protocol.CmdRestore:
			h.HandleRestore(reader, conn)
		default:
			slog.Warn("Received unhandled command type", "command_type", cmdType, "remote_addr", conn.RemoteAddr().String())
			protocol.WriteResponse(conn, protocol.StatusBadCommand, fmt.Sprintf("BAD COMMAND: Unhandled or unknown command type %d", cmdType), nil)
			io.Copy(io.Discard, reader)
		}
	}
}
