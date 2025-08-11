// ./internal/handler/handler.go

package handler

import (
	"fmt"
	"io"
	"log/slog"
	"memory-tools/internal/persistence"
	"memory-tools/internal/protocol"
	"memory-tools/internal/store"
	"net"
	"sync"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// ActivityUpdater es una interfaz para actualizar los timestamps de actividad.
type ActivityUpdater interface {
	UpdateActivity()
}

// ConnectionHandler contiene las dependencias necesarias para manejar las conexiones de los clientes.
type ConnectionHandler struct {
	MainStore         store.DataStore
	CollectionManager *store.CollectionManager
	BackupManager     *persistence.BackupManager
	ActivityUpdater   ActivityUpdater
	IsAuthenticated   bool
	AuthenticatedUser string
	IsLocalhostConn   bool
	IsRoot            bool
	Permissions       map[string]string
	// --- NUEVOS CAMPOS PARA TRANSACCIONES ---
	TransactionManager   *store.TransactionManager
	CurrentTransactionID string
}

// connectionHandlerPool aloja objetos ConnectionHandler para su reutilización.
var connectionHandlerPool = sync.Pool{
	New: func() any {
		return &ConnectionHandler{
			Permissions: make(map[string]string),
		}
	},
}

// Reset prepara el ConnectionHandler para ser reutilizado por una nueva conexión.
func (h *ConnectionHandler) Reset() {
	h.MainStore = nil
	h.CollectionManager = nil
	h.BackupManager = nil
	h.ActivityUpdater = nil
	h.IsAuthenticated = false
	h.AuthenticatedUser = ""
	h.IsLocalhostConn = false
	h.IsRoot = false
	clear(h.Permissions)
	// --- Limpiar campos de la transacción ---
	h.TransactionManager = nil
	h.CurrentTransactionID = ""
}

// GetConnectionHandlerFromPool obtiene un handler del pool y lo inicializa con los datos de la nueva conexión.
// NOTA: La firma ha cambiado para aceptar el TransactionManager. ¡Asegúrate de actualizar la llamada en main.go!
func GetConnectionHandlerFromPool(
	mainStore store.DataStore,
	colManager *store.CollectionManager,
	backupManager *persistence.BackupManager,
	txManager *store.TransactionManager, // Nuevo parámetro
	updater ActivityUpdater,
	conn net.Conn,
) *ConnectionHandler {
	h := connectionHandlerPool.Get().(*ConnectionHandler)

	isLocal := false
	if host, _, err := net.SplitHostPort(conn.RemoteAddr().String()); err == nil {
		if host == "127.0.0.1" || host == "::1" || host == "localhost" {
			isLocal = true
		}
	}

	h.MainStore = mainStore
	h.CollectionManager = colManager
	h.BackupManager = backupManager
	h.TransactionManager = txManager // Inyectar el gestor de transacciones
	h.ActivityUpdater = updater
	h.IsLocalhostConn = isLocal

	return h
}

// PutConnectionHandlerToPool resetea y devuelve un handler al pool para que pueda ser reutilizado.
func PutConnectionHandlerToPool(h *ConnectionHandler) {
	// Si una conexión se cierra a mitad de una transacción, asegúrate de que se aborte.
	if h.CurrentTransactionID != "" {
		slog.Warn("Connection closed mid-transaction, rolling back.", "txID", h.CurrentTransactionID)
		h.TransactionManager.Rollback(h.CurrentTransactionID)
	}
	h.Reset()
	connectionHandlerPool.Put(h)
}

// HandleConnection procesa los comandos entrantes de un cliente TCP.
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
			return
		}

		h.ActivityUpdater.UpdateActivity()

		if cmdType == protocol.CmdAuthenticate {
			h.handleAuthenticate(conn)
			continue
		}

		if !h.IsAuthenticated {
			slog.Warn("Unauthorized access attempt", "remote_addr", conn.RemoteAddr().String(), "command_type", cmdType)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Please authenticate first.", nil)
			continue
		}

		switch cmdType {
		// --- NUEVOS COMANDOS DE TRANSACCIÓN ---
		case protocol.CmdBegin:
			h.handleBegin(conn)
		case protocol.CmdCommit:
			h.handleCommit(conn)
		case protocol.CmdRollback:
			h.handleRollback(conn)

		// Comandos del Almacén Principal
		case protocol.CmdSet:
			h.handleMainStoreSet(conn)
		case protocol.CmdGet:
			h.handleMainStoreGet(conn)

		// Comandos de Gestión de Colecciones
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

		// Comandos de Items de Colección
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

		// Comando de Consulta de Colección
		case protocol.CmdCollectionQuery:
			h.handleCollectionQuery(conn)

		// Comandos de Gestión de Usuarios
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
			slog.Warn("Received unhandled command type", "command_type", cmdType, "remote_addr", conn.RemoteAddr().String())
			protocol.WriteResponse(conn, protocol.StatusBadCommand, fmt.Sprintf("BAD COMMAND: Unhandled or unknown command type %d", cmdType), nil)
		}
	}
}
