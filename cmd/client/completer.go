// cmd/client/completer.go

package main

import (
	"bytes"
	"fmt"
	"memory-tools/internal/protocol"
	"os"
	"strings"

	"github.com/chzyer/readline"
)

// --- MODIFICADO ---
// getCompleter ahora es mucho más simple.
func (c *cli) getCompleter() readline.AutoCompleter {
	// Si no está autenticado, devuelve el completador básico.
	if !c.isAuthenticated {
		return readline.NewPrefixCompleter(
			readline.PcItem("login"),
			readline.PcItem("help"),
			readline.PcItem("exit"),
			readline.PcItem("clear"),
		)
	}

	// --- ELIMINADO ---
	// La lógica del completador contextual para 'currentCollection' ha sido eliminada.

	// Completador por defecto cuando está autenticado.
	// Ahora contiene todos los comandos disponibles.
	return readline.NewPrefixCompleter(
		readline.PcItem("user",
			readline.PcItem("create"),
			readline.PcItem("update"),
			readline.PcItem("delete"),
		),
		readline.PcItem("update", readline.PcItem("password")),
		readline.PcItem("backup"),
		readline.PcItem("restore"),
		readline.PcItem("set"),
		readline.PcItem("get"),
		readline.PcItem("collection",
			readline.PcItem("create"),
			readline.PcItem("delete", readline.PcItemDynamic(c.fetchCollectionNames)),
			readline.PcItem("list"),
			readline.PcItem("index",
				readline.PcItem("create", readline.PcItemDynamic(c.fetchCollectionNames)),
				readline.PcItem("delete", readline.PcItemDynamic(c.fetchCollectionNames)),
				readline.PcItem("list", readline.PcItemDynamic(c.fetchCollectionNames)),
			),
			readline.PcItem("item",
				readline.PcItem("get", readline.PcItemDynamic(c.fetchCollectionNames)),
				readline.PcItem("set", readline.PcItemDynamic(c.fetchCollectionNames)),
				readline.PcItem("delete", readline.PcItemDynamic(c.fetchCollectionNames)),
				readline.PcItem("update", readline.PcItemDynamic(c.fetchCollectionNames)),
				readline.PcItem("list", readline.PcItemDynamic(c.fetchCollectionNames)),
				readline.PcItem("set many", readline.PcItemDynamic(c.fetchCollectionNames)),
				readline.PcItem("delete many", readline.PcItemDynamic(c.fetchCollectionNames)),
				readline.PcItem("update many", readline.PcItemDynamic(c.fetchCollectionNames)),
			),
			readline.PcItem("query", readline.PcItemDynamic(c.fetchCollectionNames)),
		),
		readline.PcItem("clear"),
		readline.PcItem("help"),
		readline.PcItem("exit"),
	)
}

// La función fetchCollectionNames no necesita cambios en su lógica interna.
func (c *cli) fetchCollectionNames(line string) []string {
	// ... (sin cambios)
	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	const maxRetries = 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		var cmdBuf bytes.Buffer
		if err := protocol.WriteCollectionListCommand(&cmdBuf); err != nil {
			continue
		}
		if _, err := c.conn.Write(cmdBuf.Bytes()); err != nil {
			continue
		}

		statusByte := make([]byte, 1)
		if _, err := c.conn.Read(statusByte); err != nil {
			continue
		}
		status := protocol.ResponseStatus(statusByte[0])

		if _, err := protocol.ReadString(c.conn); err != nil {
			continue
		}

		dataBytes, err := protocol.ReadBytes(c.conn)
		if err != nil || status != protocol.StatusOk {
			continue
		}

		var collections []string
		if json.Unmarshal(dataBytes, &collections) != nil {
			continue
		}

		parts := strings.Fields(line)
		prefix := ""
		if len(parts) > 0 {
			if len(parts) > 1 {
				prefix = parts[len(parts)-1]
			}
		}

		var suggestions []string
		for _, collection := range collections {
			if strings.HasPrefix(collection, prefix) {
				suggestions = append(suggestions, collection)
			}
		}
		return suggestions
	}
	fmt.Fprintln(os.Stderr, colorErr("Warning: Could not fetch collection names for autocompletion after %d attempts.", maxRetries))
	return nil
}
