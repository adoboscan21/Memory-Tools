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

func (c *cli) getCompleter() readline.AutoCompleter {
	// If not authenticated, return the basic completer.
	if !c.isAuthenticated {
		return readline.NewPrefixCompleter(
			readline.PcItem("login"),
			readline.PcItem("help"),
			readline.PcItem("exit"),
			readline.PcItem("clear"),
		)
	}

	// ---- START OF MODIFICATION ----
	// If inside a collection, provide a CLEAN list of contextual commands.
	if c.currentCollection != "" {
		return readline.NewPrefixCompleter(
			// === Contextual Aliases ===
			readline.PcItem("list"),
			readline.PcItem("query"),
			readline.PcItem("get"),
			readline.PcItem("set"),
			readline.PcItem("delete"),
			readline.PcItem("update"),
			readline.PcItem("index",
				readline.PcItem("create"),
				readline.PcItem("delete"),
				readline.PcItem("list"),
			),

			// === Universal Commands ===
			// 'use' is kept to allow switching or exiting the context.
			readline.PcItem("use",
				readline.PcItem("exit"),
				readline.PcItemDynamic(c.fetchCollectionNames),
			),
			readline.PcItem("help"),
			readline.PcItem("exit"),
			readline.PcItem("clear"),
		)
	}
	// ---- END OF MODIFICATION ----

	// Default completer when NOT in a collection context.
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
		readline.PcItem("use",
			readline.PcItem("exit"),
			readline.PcItemDynamic(c.fetchCollectionNames),
		),
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
			),
			readline.PcItem("query", readline.PcItemDynamic(c.fetchCollectionNames)),
		),
		readline.PcItem("clear"),
		readline.PcItem("help"),
		readline.PcItem("exit"),
	)
}

func (c *cli) fetchCollectionNames(line string) []string {
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
		// Use the existing 'json' package from utils.go if it were in the same package,
		// otherwise, import a json library here.
		if json.Unmarshal(dataBytes, &collections) != nil {
			continue
		}

		parts := strings.Fields(line)
		prefix := ""
		if len(parts) > 0 {
			// This logic might need adjustment depending on the exact completer behavior,
			// but for single-word suggestions, it's often fine.
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
		// Add an empty suggestion for 'use' to allow exiting context.
		// Note: The library might handle this differently. This is one way.
		if strings.HasPrefix("", prefix) {
			suggestions = append(suggestions, "")
		}
		return suggestions
	}
	fmt.Fprintln(os.Stderr, colorErr("Warning: Could not fetch collection names for autocompletion after %d attempts.", maxRetries))
	return nil
}
