// cmd/client/handlers.go

package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"memory-tools/internal/protocol"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/olekukonko/tablewriter"
)

// getCommands defines all available commands, their help, handler, and category.
func (c *cli) getCommands() map[string]command {
	return map[string]command{
		// Authentication
		"login": {help: "login <username> <password> - Authenticate to the server", handler: (*cli).handleLogin, category: "Authentication"},
		"help":  {help: "help - Shows this help message", handler: (*cli).handleHelp, category: "Authentication"},
		"exit":  {help: "exit - Exits the client", handler: (*cli).handleExit, category: "Authentication"},
		"clear": {help: "clear - Clears the screen", handler: (*cli).handleClear, category: "Authentication"},

		// User Management
		"user create":     {help: "user create <user> <pass> <perms_json|path> - Create a new user", handler: (*cli).handleUserCreate, category: "User Management"},
		"user update":     {help: "user update <user> <perms_json|path> - Update a user's permissions", handler: (*cli).handleUserUpdate, category: "User Management"},
		"user delete":     {help: "user delete <username> - Delete a user", handler: (*cli).handleUserDelete, category: "User Management"},
		"update password": {help: "update password <user> <new_pass> - Change a user's password", handler: (*cli).handleChangePassword, category: "User Management"},

		// --- NUEVA CATEGORÍA: TRANSACCIONES ---
		"begin":    {help: "begin - Starts a new transaction", handler: (*cli).handleBegin, category: "Transactions"},
		"commit":   {help: "commit - Commits the current transaction", handler: (*cli).handleCommit, category: "Transactions"},
		"rollback": {help: "rollback - Rolls back the current transaction", handler: (*cli).handleRollback, category: "Transactions"},
		// ------------------------------------

		// Server Operations (Root only)
		"backup":  {help: "backup - Triggers a manual server backup (root only)", handler: (*cli).handleBackup, category: "Server Operations"},
		"restore": {help: "restore <backup_name> - Restores from a backup (root only)", handler: (*cli).handleRestore, category: "Server Operations"},
		"set":     {help: "set <key> <value_json> [ttl] - Set a key in the main store (root only)", handler: (*cli).handleMainSet, category: "Server Operations"},
		"get":     {help: "get <key> - Get a key from the main store (root only)", handler: (*cli).handleMainGet, category: "Server Operations"},

		// Collection Management
		"collection create": {help: "collection create <name> - Creates a new collection", handler: (*cli).handleCollectionCreate, category: "Collection Management"},
		"collection delete": {help: "collection delete <name> - Deletes a collection", handler: (*cli).handleCollectionDelete, category: "Collection Management"},
		"collection list":   {help: "collection list - Lists all available collections", handler: (*cli).handleCollectionList, category: "Collection Management"},

		// Index Management
		"collection index create": {help: "collection index create <coll> <field> - Creates an index on a field", handler: (*cli).handleIndexCreate, category: "Index Management"},
		"collection index delete": {help: "collection index delete <coll> <field> - Deletes an index", handler: (*cli).handleIndexDelete, category: "Index Management"},
		"collection index list":   {help: "collection index list <coll> - Lists indexes on a collection", handler: (*cli).handleIndexList, category: "Index Management"},

		// Item Operations
		"collection item set":         {help: "collection item set <coll> [<key>] <value_json|path> [ttl] - Sets an item", handler: (*cli).handleItemSet, category: "Item Operations"},
		"collection item get":         {help: "collection item get <coll> <key> - Gets an item from a collection", handler: (*cli).handleItemGet, category: "Item Operations"},
		"collection item delete":      {help: "collection item delete <coll> <key> - Deletes an item from a collection", handler: (*cli).handleItemDelete, category: "Item Operations"},
		"collection item update":      {help: "collection item update <coll> <key> <patch_json|path> - Updates an item", handler: (*cli).handleItemUpdate, category: "Item Operations"},
		"collection item list":        {help: "collection item list <coll> - Lists all items in a collection (root only)", handler: (*cli).handleItemList, category: "Item Operations"},
		"collection item set many":    {help: "collection item set many <coll> <json_array|path> - Sets multiple items", handler: (*cli).handleItemSetMany, category: "Item Operations"},
		"collection item update many": {help: "collection item update many <coll> <patch_json_array|path> - Updates multiple items", handler: (*cli).handleItemUpdateMany, category: "Item Operations"},
		"collection item delete many": {help: "collection item delete many <coll> <keys_json_array|path> - Deletes multiple items", handler: (*cli).handleItemDeleteMany, category: "Item Operations"},

		// Query
		"collection query": {help: "collection query <coll> <query_json|path> - Performs a complex query", handler: (*cli).handleQuery, category: "Query"},
	}
}

func (c *cli) handleBegin(args string) error {
	if c.inTransaction {
		return errors.New("a transaction is already in progress")
	}

	var cmdBuf bytes.Buffer
	if err := protocol.WriteBeginCommand(&cmdBuf); err != nil {
		return fmt.Errorf("could not build begin command: %w", err)
	}

	if _, err := c.conn.Write(cmdBuf.Bytes()); err != nil {
		return fmt.Errorf("could not send begin command: %w", err)
	}

	// Leemos la respuesta para confirmar que el servidor inició la transacción
	status, msg, _, err := c.readRawResponse()
	if err != nil {
		return err
	}

	// Renderizar la respuesta simple
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Status", "Message"})
	table.Append([]string{getStatusString(status), msg})
	table.Render()
	fmt.Println("---")

	if status == protocol.StatusOk {
		c.inTransaction = true
		fmt.Println(colorOK("√ Transaction started."))
	}

	return nil
}

func (c *cli) handleCommit(args string) error {
	if !c.inTransaction {
		return errors.New("no transaction is in progress to commit")
	}

	var cmdBuf bytes.Buffer
	if err := protocol.WriteCommitCommand(&cmdBuf); err != nil {
		return fmt.Errorf("could not build commit command: %w", err)
	}

	if _, err := c.conn.Write(cmdBuf.Bytes()); err != nil {
		return fmt.Errorf("could not send commit command: %w", err)
	}

	status, msg, _, err := c.readRawResponse()
	if err != nil {

		return err
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Status", "Message"})
	table.Append([]string{getStatusString(status), msg})
	table.Render()
	fmt.Println("---")

	if status == protocol.StatusOk {
		c.inTransaction = false
		fmt.Println(colorOK("√ Transaction committed successfully."))
	} else {
		c.inTransaction = false
		fmt.Println(colorErr("Transaction failed on the server and was rolled back."))
	}

	return nil
}

func (c *cli) handleRollback(args string) error {
	if !c.inTransaction {
		return errors.New("no transaction is in progress to roll back")
	}

	var cmdBuf bytes.Buffer
	if err := protocol.WriteRollbackCommand(&cmdBuf); err != nil {
		return fmt.Errorf("could not build rollback command: %w", err)
	}

	if _, err := c.conn.Write(cmdBuf.Bytes()); err != nil {
		return fmt.Errorf("could not send rollback command: %w", err)
	}

	status, msg, _, err := c.readRawResponse()
	if err != nil {
		return err
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Status", "Message"})
	table.Append([]string{getStatusString(status), msg})
	table.Render()
	fmt.Println("---")

	c.inTransaction = false

	if status == protocol.StatusOk {
		fmt.Println(colorInfo("√ Transaction rolled back."))
	}

	return nil
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

	var cmdBuf bytes.Buffer
	protocol.WriteAuthenticateCommand(&cmdBuf, username, password)

	if _, err := c.conn.Write(cmdBuf.Bytes()); err != nil {
		return fmt.Errorf("could not send login command: %w", err)
	}

	status, msg, _, err := c.readRawResponse()
	if err != nil {
		return err
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Status", "Message"})
	table.Append([]string{getStatusString(status), msg})
	table.Render()
	fmt.Println("---")

	if status == protocol.StatusOk {
		c.isAuthenticated = true
		c.currentUser = username
		c.rlConfig.AutoComplete = c.getCompleter()
		c.rl.SetConfig(c.rlConfig)
		fmt.Printf(colorOK("√ Login successful. Welcome, %s!\n"), c.currentUser)
		return nil
	}

	return errors.New("authentication failed")
}

func (c *cli) handleHelp(args string) error {
	fmt.Println(colorInfo("\nMemory Tools CLI Help"))
	fmt.Println("---------------------")
	fmt.Println("All commands require their full name. The collection must be specified as the first argument where required.")
	fmt.Println("---------------------")

	categories := make(map[string][]string)
	for cmdName, cmdDetails := range c.commands {
		if cmdDetails.category == "" {
			continue
		}
		if _, ok := categories[cmdDetails.category]; !ok {
			categories[cmdDetails.category] = []string{}
		}
		categories[cmdDetails.category] = append(categories[cmdDetails.category], cmdName)
	}

	categoryNames := make([]string, 0, len(categories))
	for name := range categories {
		categoryNames = append(categoryNames, name)
	}
	sort.Strings(categoryNames)

	for _, category := range categoryNames {
		fmt.Printf("\n%s%s%s\n", colorOK("== "), colorOK(category), colorOK(" =="))
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Command", "Description"})
		table.SetAutoWrapText(false)

		cmds := categories[category]
		sort.Strings(cmds)

		for _, cmd := range cmds {
			if details, exists := c.commands[cmd]; exists {
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

func (c *cli) handleUserCreate(args string) error {
	parts := strings.SplitN(args, " ", 3)
	if len(parts) < 3 {
		return errors.New("usage: user create <username> <password> <permissions_json|path>")
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
		return errors.New("usage: user update <username> <permissions_json|path>")
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
	collName, _, err := c.resolveCollectionName(args, "collection delete")
	if err != nil {
		return err
	}

	fmt.Println(colorInfo("Are you sure you want to delete collection? (y/N): "), collName)

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
	collName, remainingArgs, err := c.resolveCollectionName(args, "collection index create")
	if err != nil {
		return err
	}
	parts := strings.Fields(remainingArgs)
	if len(parts) != 1 {
		return errors.New("usage: collection index create <collection> <field_name>")
	}
	var cmdBuf bytes.Buffer
	protocol.WriteCollectionIndexCreateCommand(&cmdBuf, collName, parts[0])
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection index create")
}

func (c *cli) handleIndexDelete(args string) error {
	collName, remainingArgs, err := c.resolveCollectionName(args, "collection index delete")
	if err != nil {
		return err
	}
	parts := strings.Fields(remainingArgs)
	if len(parts) != 1 {
		return errors.New("usage: collection index delete <collection> <field_name>")
	}
	var cmdBuf bytes.Buffer
	protocol.WriteCollectionIndexDeleteCommand(&cmdBuf, collName, parts[0])
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection index delete")
}

func (c *cli) handleIndexList(args string) error {
	collName, _, err := c.resolveCollectionName(args, "collection index list")
	if err != nil {
		return err
	}
	var cmdBuf bytes.Buffer
	protocol.WriteCollectionIndexListCommand(&cmdBuf, collName)
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection index list")
}

func (c *cli) handleItemSet(args string) error {
	collName, remainingArgs, err := c.resolveCollectionName(args, "collection item set")
	if err != nil {
		return err
	}

	parts := strings.SplitN(remainingArgs, " ", 2)
	key, jsonArg := "", ""
	if len(parts) == 0 || parts[0] == "" {
		return errors.New("usage: collection item set <coll> [<key>] <value_json|path> [ttl]")
	}

	if len(parts) == 1 {
		key = uuid.New().String()
		jsonArg = parts[0]
	} else {
		if strings.HasPrefix(parts[1], "{") || strings.HasPrefix(parts[1], "[") {
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
	collName, remainingArgs, err := c.resolveCollectionName(args, "collection item get")
	if err != nil {
		return err
	}
	parts := strings.Fields(remainingArgs)
	if len(parts) != 1 {
		return errors.New("usage: collection item get <collection> <key>")
	}
	var cmdBuf bytes.Buffer
	protocol.WriteCollectionItemGetCommand(&cmdBuf, collName, parts[0])
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection item get")
}

func (c *cli) handleItemDelete(args string) error {
	collName, remainingArgs, err := c.resolveCollectionName(args, "collection item delete")
	if err != nil {
		return err
	}
	parts := strings.Fields(remainingArgs)
	if len(parts) != 1 {
		return errors.New("usage: collection item delete <collection> <key>")
	}
	var cmdBuf bytes.Buffer
	protocol.WriteCollectionItemDeleteCommand(&cmdBuf, collName, parts[0])
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection item delete")
}

func (c *cli) handleItemList(args string) error {
	collName, _, err := c.resolveCollectionName(args, "collection item list")
	if err != nil {
		return err
	}
	var cmdBuf bytes.Buffer
	protocol.WriteCollectionItemListCommand(&cmdBuf, collName)
	c.conn.Write(cmdBuf.Bytes())
	return c.readResponse("collection item list")
}

func (c *cli) handleItemUpdate(args string) error {
	collName, remainingArgs, err := c.resolveCollectionName(args, "collection item update")
	if err != nil {
		return err
	}
	parts := strings.SplitN(remainingArgs, " ", 2)
	if len(parts) != 2 {
		return errors.New("usage: collection item update <coll> <key> <patch_json|path>")
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
	collName, remainingArgs, err := c.resolveCollectionName(args, "collection query")
	if err != nil {
		return err
	}
	if remainingArgs == "" {
		return errors.New("usage: collection query <coll> <query_json|path>")
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
	collName, remainingArgs, err := c.resolveCollectionName(args, "collection item set many")
	if err != nil {
		return err
	}
	if remainingArgs == "" {
		return errors.New("usage: collection item set many <coll> <json_array|path>")
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
	collName, remainingArgs, err := c.resolveCollectionName(args, "collection item update many")
	if err != nil {
		return err
	}
	if remainingArgs == "" {
		return errors.New("usage: collection item update many <coll> <patch_json_array|path>")
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
	collName, remainingArgs, err := c.resolveCollectionName(args, "collection item delete many")
	if err != nil {
		return err
	}
	if remainingArgs == "" {
		return errors.New("usage: collection item delete many <coll> <keys_json_array|path>")
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
