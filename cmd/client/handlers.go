package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"memory-tools/internal/protocol"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/olekukonko/tablewriter"
)

func (c *cli) getCommands() map[string]command {
	return map[string]command{
		"login":                       {help: "login <username> <password> - Authenticate to the server", handler: (*cli).handleLogin},
		"help":                        {help: "help - Shows this help message", handler: (*cli).handleHelp},
		"exit":                        {help: "exit - Exits the client", handler: (*cli).handleExit},
		"clear":                       {help: "clear - Clears the screen", handler: (*cli).handleClear},
		"use":                         {help: "use [collection_name] - Set or clear the active collection", handler: (*cli).handleUse},
		"user create":                 {help: "user create <user> <pass> <perms_json|-|file:path> - Create a new user", handler: (*cli).handleUserCreate},
		"user update":                 {help: "user update <user> <perms_json|-|file:path> - Update a user's permissions", handler: (*cli).handleUserUpdate},
		"user delete":                 {help: "user delete <username> - Delete a user", handler: (*cli).handleUserDelete},
		"update password":             {help: "update password <user> <new_pass> - Change a user's password", handler: (*cli).handleChangePassword},
		"backup":                      {help: "backup - Triggers a manual server backup (root only)", handler: (*cli).handleBackup},
		"restore":                     {help: "restore <backup_name> - Restores from a backup (root only)", handler: (*cli).handleRestore},
		"set":                         {help: "set <key> <value_json> [ttl] - Set a key in the main store (root only)", handler: (*cli).handleMainSet},
		"get":                         {help: "get <key> - Get a key from the main store (root only)", handler: (*cli).handleMainGet},
		"collection create":           {help: "collection create <name> - Creates a new collection", handler: (*cli).handleCollectionCreate},
		"collection delete":           {help: "collection delete [name] - Deletes a collection", handler: (*cli).handleCollectionDelete},
		"collection list":             {help: "collection list - Lists all available collections", handler: (*cli).handleCollectionList},
		"collection index create":     {help: "collection index create [coll] <field> - Creates an index on a field", handler: (*cli).handleIndexCreate},
		"collection index delete":     {help: "collection index delete [coll] <field> - Deletes an index", handler: (*cli).handleIndexDelete},
		"collection index list":       {help: "collection index list [coll] - Lists indexes on a collection", handler: (*cli).handleIndexList},
		"collection item set":         {help: "collection item set [coll] [<key>] <value_json|-|file:path> [ttl] - Sets an item in a collection", handler: (*cli).handleItemSet},
		"collection item get":         {help: "collection item get [coll] <key> - Gets an item from a collection", handler: (*cli).handleItemGet},
		"collection item delete":      {help: "collection item delete [coll] <key> - Deletes an item from a collection", handler: (*cli).handleItemDelete},
		"collection item update":      {help: "collection item update [coll] <key> <patch_json|-|file:path> - Updates an item", handler: (*cli).handleItemUpdate},
		"collection item list":        {help: "collection item list [coll] - Lists all items in a collection (root only)", handler: (*cli).handleItemList},
		"collection query":            {help: "collection query [coll] <query_json|-|file:path> - Performs a complex query", handler: (*cli).handleQuery},
		"collection item set many":    {help: "collection item set many [coll] <json_array|-|file:path> - Sets multiple items", handler: (*cli).handleItemSetMany},
		"collection item update many": {help: "collection item update many [coll] <patch_json_array|-|file:path> - Updates multiple items", handler: (*cli).handleItemUpdateMany},
		"collection item delete many": {help: "collection item delete many [coll] <keys_json_array|-|file:path> - Deletes multiple items", handler: (*cli).handleItemDeleteMany},
	}
}

func (c *cli) handleLogin(args string) error {
	if c.isAuthenticated {
		return errors.New("you are already logged in")
	}
	parts := strings.Fields(args)
	if len(parts) != 2 {
		return errors.New("usage: login <username> <password>")
	}
	username, password := parts[0], parts[1]

	// Prepare and send the command
	var cmdBuf bytes.Buffer
	protocol.WriteAuthenticateCommand(&cmdBuf, username, password)

	if _, err := c.conn.Write(cmdBuf.Bytes()); err != nil {
		return fmt.Errorf("could not send login command: %w", err)
	}

	// Read the raw response from the server
	status, msg, _, err := c.readRawResponse()
	if err != nil {
		return err // Returns I/O errors, etc.
	}

	// Display the response consistently with other commands
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Status", "Message"})
	table.Append([]string{getStatusString(status), msg})
	table.Render()
	fmt.Println("---")

	// Handle logic based on the status
	if status == protocol.StatusOk {
		c.isAuthenticated = true
		c.currentUser = username

		// === FIX 1: Update the completer without the incorrect error check ===
		c.rlConfig.AutoComplete = c.getCompleter()
		c.rl.SetConfig(c.rlConfig) // No error check needed here

		// === FIX 2: Use Printf for correct formatting ===
		fmt.Printf(colorOK("√ Login successful. Welcome, %s!\n"), c.currentUser)
		return nil // Success
	}

	// If status was not OK, return an error to the main loop
	return errors.New("authentication failed")
}

func (c *cli) handleHelp(args string) error {
	fmt.Println(colorInfo("\nMemory Tools CLI Help"))
	fmt.Println("---------------------")
	fmt.Println("Commands are grouped by category. Use 'use <collection_name>' for contextual collections.")
	fmt.Println("JSON arguments can be provided directly, via '-' (editor), or 'file:/path/to/file.json'.")
	fmt.Println("---------------------")

	// Definir categorías y sus comandos asociados
	categories := map[string][]string{
		"Authentication":        {"login", "help", "exit", "clear"},
		"User Management":       {"user create", "user update", "user delete", "update password"},
		"Server Operations":     {"backup", "restore", "set", "get"},
		"Collection Management": {"collection create", "collection delete", "collection list", "use"},
		"Index Management":      {"collection index create", "collection index delete", "collection index list"},
		"Item Operations": {
			"collection item set", "collection item get", "collection item delete",
			"collection item update", "collection item list", "collection item set many",
			"collection item update many", "collection item delete many",
		},
		"Query": {"collection query"},
	}

	for category, cmds := range categories {
		fmt.Printf("\n%s%s%s\n", colorOK("== "), colorOK(category), colorOK(" =="))
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Command", "Description"})
		table.SetAutoWrapText(false)
		for _, cmd := range cmds {
			details, exists := c.commands[cmd]
			if exists {
				table.Append([]string{cmd, details.help})
			}
		}
		table.Render()
	}
	fmt.Println("---------------------")
	return nil
}

func (c *cli) handleExit(args string) error {
	return io.EOF
}

func (c *cli) handleClear(args string) error {
	clearScreen()
	return nil
}

// cmd/client/handlers.go

func (c *cli) handleUse(args string) error {
	parts := strings.Fields(args)

	// Case 1: No arguments provided. This is an error.
	if len(parts) == 0 {
		return errors.New("you must specify a collection name, or use 'exit' to leave the current collection")
	}

	targetCollection := parts[0]

	// Case 2: The argument is 'exit'.
	if targetCollection == "exit" {
		// And we ARE in a collection, so we exit.
		if c.currentCollection != "" {
			fmt.Println(colorInfo("Exited collection mode."))
			c.currentCollection = ""
		} else {
			// And we are NOT in a collection, so we inform the user and do nothing else.
			fmt.Println(colorInfo("You are not currently in any collection."))
			return nil // Return to avoid unnecessarily updating the completer.
		}
	} else {
		// === START OF VALIDATION ===
		// Case 3: A collection name is provided. It must be validated.

		// We reuse the function from the completer to get the existing collections.
		collections := c.fetchCollectionNames("")
		if collections == nil {
			// This can happen if there's a connection issue while fetching the list.
			return errors.New("could not retrieve collection list from server")
		}

		found := false
		for _, collection := range collections {
			if collection == targetCollection {
				found = true
				break
			}
		}

		// If the collection was found, we switch the context.
		if found {
			c.currentCollection = targetCollection
			fmt.Println(colorInfo("Now using collection: ", c.currentCollection))
		} else {
			// If not, we return an error and DO NOT change the context.
			return fmt.Errorf("collection '%s' not found", targetCollection)
		}
		// === END OF VALIDATION ===
	}

	// Update the completer to reflect the change of context.
	c.rlConfig.AutoComplete = c.getCompleter()
	c.rl.SetConfig(c.rlConfig)

	return nil
}

func (c *cli) handleUserCreate(args string) error {
	parts := strings.SplitN(args, " ", 3)
	if len(parts) < 3 {
		return errors.New("usage: user create <username> <password> <permissions_json|-|file:path>")
	}
	username, password, jsonArg := parts[0], parts[1], parts[2]

	jsonPayload, err := c.getJSONPayload(jsonArg)
	if err != nil {
		return err
	}
	if !json.Valid(jsonPayload) {
		return errors.New("invalid permissions JSON format")
	}

	var cmdBuf bytes.Buffer
	protocol.WriteUserCreateCommand(&cmdBuf, username, password, jsonPayload)
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("user create")
}

func (c *cli) handleUserUpdate(args string) error {
	parts := strings.SplitN(args, " ", 2)
	if len(parts) < 2 {
		return errors.New("usage: user update <username> <permissions_json|-|file:path>")
	}
	username, jsonArg := parts[0], parts[1]

	jsonPayload, err := c.getJSONPayload(jsonArg)
	if err != nil {
		return err
	}
	if !json.Valid(jsonPayload) {
		return errors.New("invalid permissions JSON format")
	}

	var cmdBuf bytes.Buffer
	protocol.WriteUserUpdateCommand(&cmdBuf, username, jsonPayload)
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("user update")
}

func (c *cli) handleUserDelete(args string) error {
	parts := strings.Fields(args)
	if len(parts) != 1 {
		return errors.New("usage: user delete <username>")
	}
	var cmdBuf bytes.Buffer
	protocol.WriteUserDeleteCommand(&cmdBuf, parts[0])
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("user delete")
}

func (c *cli) handleChangePassword(args string) error {
	parts := strings.Fields(args)
	if len(parts) != 2 {
		return errors.New("usage: update password <target_username> <new_password>")
	}
	var cmdBuf bytes.Buffer
	protocol.WriteChangeUserPasswordCommand(&cmdBuf, parts[0], parts[1])
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("update password")
}

func (c *cli) handleBackup(args string) error {
	var cmdBuf bytes.Buffer
	protocol.WriteBackupCommand(&cmdBuf)
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("backup")
}

func (c *cli) handleRestore(args string) error {
	parts := strings.Fields(args)
	if len(parts) != 1 {
		return errors.New("usage: restore <backup_name>")
	}
	var cmdBuf bytes.Buffer
	protocol.WriteRestoreCommand(&cmdBuf, parts[0])
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("restore")
}

func (c *cli) handleMainSet(args string) error {
	parts := strings.SplitN(args, " ", 2)
	if len(parts) < 2 {
		return errors.New("usage: set <key> <value_json> [ttl]")
	}

	var cmdBuf bytes.Buffer
	protocol.WriteSetCommand(&cmdBuf, parts[0], []byte(parts[1]), 0)
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("set")
}

func (c *cli) handleMainGet(args string) error {
	parts := strings.Fields(args)
	if len(parts) != 1 {
		return errors.New("usage: get <key>")
	}
	var cmdBuf bytes.Buffer
	protocol.WriteGetCommand(&cmdBuf, parts[0])
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("get")
}

func (c *cli) handleCollectionCreate(args string) error {
	parts := strings.Fields(args)
	if len(parts) != 1 {
		return errors.New("usage: collection create <name>")
	}
	var cmdBuf bytes.Buffer
	protocol.WriteCollectionCreateCommand(&cmdBuf, parts[0])
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection create")
}

func (c *cli) handleCollectionDelete(args string) error {
	collName, _, err := c.resolveCollectionName(args)
	if err != nil {
		return err
	}

	// Solicitar confirmación
	fmt.Printf(colorInfo("Are you sure you want to delete collection '%s'? (y/N): "), collName)
	input, err := c.rl.Readline()
	if err != nil {
		return err
	}
	if strings.ToLower(strings.TrimSpace(input)) != "y" {
		fmt.Println(colorInfo("Deletion cancelled."))
		return nil
	}

	var cmdBuf bytes.Buffer
	protocol.WriteCollectionDeleteCommand(&cmdBuf, collName)
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection delete")
}

func (c *cli) handleCollectionList(args string) error {
	var cmdBuf bytes.Buffer
	protocol.WriteCollectionListCommand(&cmdBuf)
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection list")
}

func (c *cli) handleIndexCreate(args string) error {
	collName, remainingArgs, err := c.resolveCollectionName(args)
	if err != nil {
		return err
	}
	parts := strings.Fields(remainingArgs)
	if len(parts) != 1 {
		return errors.New("usage: collection index create [collection] <field_name>")
	}
	var cmdBuf bytes.Buffer
	protocol.WriteCollectionIndexCreateCommand(&cmdBuf, collName, parts[0])
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection index create")
}

func (c *cli) handleIndexDelete(args string) error {
	collName, remainingArgs, err := c.resolveCollectionName(args)
	if err != nil {
		return err
	}
	parts := strings.Fields(remainingArgs)
	if len(parts) != 1 {
		return errors.New("usage: collection index delete [collection] <field_name>")
	}
	var cmdBuf bytes.Buffer
	protocol.WriteCollectionIndexDeleteCommand(&cmdBuf, collName, parts[0])
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection index delete")
}

func (c *cli) handleIndexList(args string) error {
	collName, _, err := c.resolveCollectionName(args)
	if err != nil {
		return err
	}
	var cmdBuf bytes.Buffer
	protocol.WriteCollectionIndexListCommand(&cmdBuf, collName)
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection index list")
}

func (c *cli) handleItemSet(args string) error {
	collName, remainingArgs, err := c.resolveCollectionName(args)
	if err != nil {
		return err
	}

	parts := strings.SplitN(remainingArgs, " ", 2)
	key, jsonArg := "", ""
	if len(parts) == 0 || parts[0] == "" {
		return errors.New("usage: collection item set [coll] [<key>] <value_json|-|file:path> [ttl]")
	}

	if len(parts) == 1 {
		key = uuid.New().String()
		jsonArg = parts[0]
	} else {
		if strings.HasPrefix(parts[1], "{") || strings.HasPrefix(parts[1], "[") || parts[1] == "-" || strings.HasPrefix(parts[1], "file:") {
			key = parts[0]
			jsonArg = parts[1]
		} else {
			key = uuid.New().String()
			jsonArg = remainingArgs
		}
	}

	jsonPayload, err := c.getJSONPayload(jsonArg)
	if err != nil {
		return fmt.Errorf("failed to parse JSON payload: %w", err)
	}
	if !json.Valid(jsonPayload) {
		return errors.New("invalid JSON format for value")
	}

	if len(strings.TrimSpace(string(jsonPayload))) == 0 {
		return errors.New("JSON payload cannot be empty")
	}

	var cmdBuf bytes.Buffer
	protocol.WriteCollectionItemSetCommand(&cmdBuf, collName, key, jsonPayload, 0*time.Second)
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection item set")
}

func (c *cli) handleItemGet(args string) error {
	collName, remainingArgs, err := c.resolveCollectionName(args)
	if err != nil {
		return err
	}
	parts := strings.Fields(remainingArgs)
	if len(parts) != 1 {
		return errors.New("usage: collection item get [collection] <key>")
	}
	var cmdBuf bytes.Buffer
	protocol.WriteCollectionItemGetCommand(&cmdBuf, collName, parts[0])
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection item get")
}

func (c *cli) handleItemDelete(args string) error {
	collName, remainingArgs, err := c.resolveCollectionName(args)
	if err != nil {
		return err
	}
	parts := strings.Fields(remainingArgs)
	if len(parts) != 1 {
		return errors.New("usage: collection item delete [collection] <key>")
	}
	var cmdBuf bytes.Buffer
	protocol.WriteCollectionItemDeleteCommand(&cmdBuf, collName, parts[0])
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection item delete")
}

func (c *cli) handleItemList(args string) error {
	collName, _, err := c.resolveCollectionName(args)
	if err != nil {
		return err
	}
	var cmdBuf bytes.Buffer
	protocol.WriteCollectionItemListCommand(&cmdBuf, collName)
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection item list")
}

func (c *cli) handleItemUpdate(args string) error {
	collName, remainingArgs, err := c.resolveCollectionName(args)
	if err != nil {
		return err
	}
	parts := strings.SplitN(remainingArgs, " ", 2)
	if len(parts) != 2 {
		return errors.New("usage: collection item update [coll] <key> <patch_json|-|file:path>")
	}
	key, jsonArg := parts[0], parts[1]

	jsonPayload, err := c.getJSONPayload(jsonArg)
	if err != nil {
		return err
	}

	var cmdBuf bytes.Buffer
	protocol.WriteCollectionItemUpdateCommand(&cmdBuf, collName, key, jsonPayload)
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection item update")
}

func (c *cli) handleQuery(args string) error {
	collName, remainingArgs, err := c.resolveCollectionName(args)
	if err != nil {
		return err
	}
	if remainingArgs == "" {
		return errors.New("usage: collection query [coll] <query_json|-|file:path>")
	}

	var jsonPayload []byte
	if remainingArgs == "interactive" {
		jsonPayload, err = c.getInteractiveJSONPayload()
	} else {
		jsonPayload, err = c.getJSONPayload(remainingArgs)
	}
	if err != nil {
		return err
	}

	var cmdBuf bytes.Buffer
	protocol.WriteCollectionQueryCommand(&cmdBuf, collName, jsonPayload)
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection query")
}

func (c *cli) handleItemSetMany(args string) error {
	collName, remainingArgs, err := c.resolveCollectionName(args)
	if err != nil {
		return err
	}
	if remainingArgs == "" {
		return errors.New("usage: collection item set many [coll] <json_array|-|file:path>")
	}

	jsonPayload, err := c.getJSONPayload(remainingArgs)
	if err != nil {
		return err
	}

	var cmdBuf bytes.Buffer
	protocol.WriteCollectionItemSetManyCommand(&cmdBuf, collName, jsonPayload)
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection item set many")
}

func (c *cli) handleItemUpdateMany(args string) error {
	collName, remainingArgs, err := c.resolveCollectionName(args)
	if err != nil {
		return err
	}
	if remainingArgs == "" {
		return errors.New("usage: collection item update many [coll] <patch_json_array|-|file:path>")
	}

	jsonPayload, err := c.getJSONPayload(remainingArgs)
	if err != nil {
		return err
	}

	var cmdBuf bytes.Buffer
	protocol.WriteCollectionItemUpdateManyCommand(&cmdBuf, collName, jsonPayload)
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection item update many")
}

func (c *cli) handleItemDeleteMany(args string) error {
	collName, remainingArgs, err := c.resolveCollectionName(args)
	if err != nil {
		return err
	}
	if remainingArgs == "" {
		return errors.New("usage: collection item delete many [coll] <keys_json_array|-|file:path>")
	}

	jsonPayload, err := c.getJSONPayload(remainingArgs)
	if err != nil {
		return err
	}

	var keysToDelete []string
	if err := json.Unmarshal(jsonPayload, &keysToDelete); err != nil {
		return fmt.Errorf("invalid keys JSON array: %w", err)
	}

	var cmdBuf bytes.Buffer
	protocol.WriteCollectionItemDeleteManyCommand(&cmdBuf, collName, keysToDelete)
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection item delete many")
}
