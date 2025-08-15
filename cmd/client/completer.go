package main

import (
	"bytes"
	"fmt"
	"memory-tools/internal/protocol"
	"os"
	"strings"

	"github.com/chzyer/readline"
)

// getCompleter returns the readline.AutoCompleter based on the user's authentication status.
func (c *cli) getCompleter() readline.AutoCompleter {
	if !c.isAuthenticated {
		return readline.NewPrefixCompleter(
			readline.PcItem("login"),
			readline.PcItem("help"),
			readline.PcItem("exit"),
			readline.PcItem("clear"),
		)
	}

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
				readline.PcItem("set many", readline.PcItemDynamic(c.fetchCollectionNames, readline.PcItemDynamic(c.fetchJSONFileNames))),
				readline.PcItem("delete many", readline.PcItemDynamic(c.fetchCollectionNames, readline.PcItemDynamic(c.fetchJSONFileNames))),
				readline.PcItem("update many", readline.PcItemDynamic(c.fetchCollectionNames, readline.PcItemDynamic(c.fetchJSONFileNames))),
			),
			readline.PcItem("query", readline.PcItemDynamic(c.fetchCollectionNames, readline.PcItemDynamic(c.fetchJSONFileNames))),
		),
		readline.PcItem("begin"),
		readline.PcItem("commit"),
		readline.PcItem("rollback"),
		readline.PcItem("clear"),
		readline.PcItem("help"),
		readline.PcItem("exit"),
	)
}

// fetchCollectionNames dynamically fetches a list of collection names from the server for autocompletion.
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
		if json.Unmarshal(dataBytes, &collections) != nil {
			continue
		}

		var prefix string
		if strings.HasSuffix(line, " ") {
			prefix = ""
		} else {
			parts := strings.Fields(line)
			if len(parts) > 0 {
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

// fetchJSONFileNames dynamically fetches a list of JSON file names from the 'json' directory.
func (c *cli) fetchJSONFileNames(line string) []string {
	const jsonDir = "json"
	files, err := os.ReadDir(jsonDir)

	if err != nil {
		return nil
	}

	var suggestions []string
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".json") {
			suggestions = append(suggestions, file.Name())
		}
	}
	return suggestions
}
