package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	stdjson "encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"memory-tools/internal/protocol"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/chzyer/readline"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// --- NEW: Define two separate completers for pre and post-login states ---

var preLoginCompleter = readline.NewPrefixCompleter(
	readline.PcItem("login"),
	readline.PcItem("help"),
	readline.PcItem("exit"),
	readline.PcItem("clear"),
)

var postLoginCompleter = readline.NewPrefixCompleter(
	readline.PcItem("login"),
	readline.PcItem("update password"),
	readline.PcItem("set"),
	readline.PcItem("get"),
	readline.PcItem("collection",
		readline.PcItem("create"),
		readline.PcItem("delete"),
		readline.PcItem("list"),
		readline.PcItem("index",
			readline.PcItem("create"),
			readline.PcItem("delete"),
			readline.PcItem("list"),
		),
		readline.PcItem("item",
			readline.PcItem("set"),
			readline.PcItem("set many"),
			readline.PcItem("get"),
			readline.PcItem("delete"),
			readline.PcItem("delete many"),
			readline.PcItem("list"),
		),
		readline.PcItem("query"),
	),
	readline.PcItem("clear"),
	readline.PcItem("help"),
	readline.PcItem("exit"),
)

func main() {
	log.SetFlags(0)

	// Command-line arguments using flags.
	usernamePtr := flag.String("u", "", "Username for authentication")
	passwordPtr := flag.String("p", "", "Password for authentication")
	flag.Parse()

	addr := "localhost:5876" // Default address.
	if flag.NArg() > 0 {
		addr = flag.Arg(0) // Use positional argument as address if provided.
	}

	// TLS Configuration for Client.
	caCert, err := os.ReadFile("certificates/server.crt")
	if err != nil {
		log.Fatalf("Failed to read server certificate 'certificates/server.crt': %v", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		ServerName:         strings.Split(addr, ":")[0],
		InsecureSkipVerify: false,
	}

	// Connect using TLS.
	conn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		log.Fatalf("Failed to connect via TLS to %s: %v", addr, err)
	}
	defer conn.Close()

	fmt.Printf("Connected securely to Memory Tools server at %s.\n", addr)

	// Authentication state tracking
	var isAuthenticated bool

	// Initialize readline with the limited completer first.
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "> ",
		HistoryFile:     "/tmp/readline_history.tmp",
		AutoComplete:    preLoginCompleter, // MODIFIED
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
		VimMode:         false,
	})
	if err != nil {
		log.Fatalf("Failed to initialize readline: %v", err)
	}
	defer rl.Close()

	// Automatic authentication if credentials are provided.
	if *usernamePtr != "" && *passwordPtr != "" {
		log.Printf("Attempting automatic login for user '%s'...", *usernamePtr)
		var cmdBuf bytes.Buffer
		writeErr := protocol.WriteAuthenticateCommand(&cmdBuf, *usernamePtr, *passwordPtr)
		if writeErr != nil {
			fmt.Printf("Error encoding login command: %v\n", writeErr)
		} else {
			if _, err := conn.Write(cmdBuf.Bytes()); err != nil {
				log.Fatalf("Failed to send login command to server: %v", err)
			}
			status := readResponse(conn, "login")
			if status == protocol.StatusOk {
				isAuthenticated = true
				rl.Config.AutoComplete = postLoginCompleter // MODIFIED: Update completer on success
			}
		}
	}

	if !isAuthenticated {
		fmt.Println("Please login using: login <username> <password>")
	}

	for {
		input, err := rl.Readline()
		if err == readline.ErrInterrupt {
			if len(input) == 0 {
				fmt.Println("Exiting client.")
				return
			} else {
				continue
			}
		} else if err == io.EOF {
			fmt.Println("Exiting client.")
			return
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		// Utility commands that should always work.
		if input == "exit" {
			fmt.Println("Exiting client.")
			return
		}
		if input == "help" {
			printHelp()
			continue
		}
		if input == "clear" {
			clearScreen()
			continue
		}

		cmd, rawArgs := getCommandAndRawArgs(input)

		// Client-side authentication check
		if !isAuthenticated {
			if cmd != "login" {
				fmt.Println("Error: You must log in first. Use: login <username> <password>")
				continue // Skip to the next command input.
			}
		}

		var cmdBuf bytes.Buffer
		var writeErr error

		if cmd == "login" {
			argsList := strings.Fields(rawArgs)
			if len(argsList) != 2 {
				fmt.Println("Error: login command requires username and password.")
				fmt.Println("Usage: login <username> <password>")
				continue
			}
			username := argsList[0]
			password := argsList[1]
			writeErr = protocol.WriteAuthenticateCommand(&cmdBuf, username, password)
		} else if cmd == "update password" {
			argsList := strings.Fields(rawArgs)
			if len(argsList) != 2 {
				fmt.Println("Error: 'update password' command requires <target_username> and <new_password>.")
				fmt.Println("Usage: update password <target_username> <new_password>")
				fmt.Println("Note: This command can only be executed by the 'root' user from localhost.")
				continue
			}
			targetUsername := argsList[0]
			newPassword := argsList[1]
			writeErr = protocol.WriteChangeUserPasswordCommand(&cmdBuf, targetUsername, newPassword)
		} else if cmd == "collection item set many" {
			parts := strings.SplitN(rawArgs, " ", 2)
			if len(parts) < 2 {
				fmt.Println("Error: 'collection item set many' requires a collection name and a JSON array.")
				fmt.Println("Usage: collection item set many <collection_name> <json_array>")
				printHelp()
				continue
			}
			collectionName := parts[0]
			jsonArray := strings.TrimSpace(parts[1])

			if !json.Valid([]byte(jsonArray)) {
				fmt.Printf("Error: Invalid JSON array: '%s'\n", jsonArray)
				continue
			}
			writeErr = protocol.WriteCollectionItemSetManyCommand(&cmdBuf, collectionName, []byte(jsonArray))

		} else if cmd == "collection item delete many" {
			parts := strings.SplitN(rawArgs, " ", 2)
			if len(parts) < 2 {
				fmt.Println("Error: 'collection item delete many' requires a collection name and a JSON array of objects.")
				fmt.Println("Usage: collection item delete many <collection_name> <json_array_of_objects>")
				printHelp()
				continue
			}

			collectionName := parts[0]
			jsonArray := strings.TrimSpace(parts[1])

			if !json.Valid([]byte(jsonArray)) {
				fmt.Printf("Error: Invalid JSON array: '%s'\n", jsonArray)
				continue
			}

			var records []map[string]any
			if err := json.Unmarshal([]byte(jsonArray), &records); err != nil {
				fmt.Printf("Error parsing the JSON array: %v\n", err)
				continue
			}

			var keysToDelete []string
			for _, record := range records {
				if id, ok := record["_id"].(string); ok && id != "" {
					keysToDelete = append(keysToDelete, id)
				} else {
					fmt.Printf("Warning: Found an object without a valid '_id' field. Object omitted: %+v\n", record)
				}
			}

			if len(keysToDelete) == 0 {
				fmt.Println("Error: No valid keys ('_id') found in the JSON array to delete.")
				continue
			}

			writeErr = protocol.WriteCollectionItemDeleteManyCommand(&cmdBuf, collectionName, keysToDelete)
		} else {
			switch cmd {
			case "set":
				parsedArgs, jsonVal, ttlSeconds, parseErr := parseArgsForJSON(rawArgs, 1)
				if parseErr != nil {
					fmt.Printf("Error parsing 'set' command: %v\n", parseErr)
					printHelp()
					continue
				}
				key := parsedArgs[0]
				writeErr = protocol.WriteSetCommand(&cmdBuf, key, []byte(jsonVal), time.Duration(ttlSeconds)*time.Second)

			case "get":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 1 {
					fmt.Println("Error: get command requires a key.")
					printHelp()
					continue
				}
				key := argsList[0]
				writeErr = protocol.WriteGetCommand(&cmdBuf, key)

			case "collection create":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 1 {
					fmt.Println("Error: collection create command requires a collection name.")
					printHelp()
					continue
				}
				collectionName := argsList[0]
				writeErr = protocol.WriteCollectionCreateCommand(&cmdBuf, collectionName)
			case "collection delete":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 1 {
					fmt.Println("Error: collection delete command requires a collection name.")
					printHelp()
					continue
				}
				collectionName := argsList[0]
				writeErr = protocol.WriteCollectionDeleteCommand(&cmdBuf, collectionName)
			case "collection list":
				writeErr = protocol.WriteCollectionListCommand(&cmdBuf)

			case "collection index create":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 2 {
					fmt.Println("Error: collection index create requires a collection name and a field name.")
					printHelp()
					continue
				}
				collectionName := argsList[0]
				fieldName := argsList[1]
				writeErr = protocol.WriteCollectionIndexCreateCommand(&cmdBuf, collectionName, fieldName)

			case "collection index delete":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 2 {
					fmt.Println("Error: collection index delete requires a collection name and a field name.")
					printHelp()
					continue
				}
				collectionName := argsList[0]
				fieldName := argsList[1]
				writeErr = protocol.WriteCollectionIndexDeleteCommand(&cmdBuf, collectionName, fieldName)

			case "collection index list":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 1 {
					fmt.Println("Error: collection index list requires a collection name.")
					printHelp()
					continue
				}
				collectionName := argsList[0]
				writeErr = protocol.WriteCollectionIndexListCommand(&cmdBuf, collectionName)

			case "collection item set":
				parsedArgs, jsonVal, ttlSeconds, parseErr := parseArgsForJSON(rawArgs, 2)
				if parseErr != nil {
					fmt.Printf("Error parsing 'collection item set' command: %v\n", parseErr)
					printHelp()
					continue
				}
				collectionName := parsedArgs[0]
				key := parsedArgs[1]
				writeErr = protocol.WriteCollectionItemSetCommand(&cmdBuf, collectionName, key, []byte(jsonVal), time.Duration(ttlSeconds)*time.Second)

			case "collection item get":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 2 {
					fmt.Println("Error: collection item get requires a collection name and a key.")
					printHelp()
					continue
				}
				collectionName := argsList[0]
				key := argsList[1]
				writeErr = protocol.WriteCollectionItemGetCommand(&cmdBuf, collectionName, key)
			case "collection item delete":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 2 {
					fmt.Println("Error: collection item delete requires a collection name and a key.")
					printHelp()
					continue
				}
				collectionName := argsList[0]
				key := argsList[1]
				writeErr = protocol.WriteCollectionItemDeleteCommand(&cmdBuf, collectionName, key)
			case "collection item list":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 1 {
					fmt.Println("Error: collection item list requires a collection name.")
					printHelp()
					continue
				}
				collectionName := argsList[0]
				writeErr = protocol.WriteCollectionItemListCommand(&cmdBuf, collectionName)

			case "collection query":
				parts := strings.SplitN(rawArgs, " ", 2)
				if len(parts) < 2 {
					fmt.Println("Error: collection query command requires a collection name and a query JSON.")
					fmt.Println("Usage: collection query <collection_name> <query_json>")
					printHelp()
					continue
				}
				collectionName := parts[0]
				queryJSON := strings.TrimSpace(parts[1])

				if !json.Valid([]byte(queryJSON)) {
					fmt.Printf("Error: Invalid JSON query: '%s'\n", queryJSON)
					continue
				}
				writeErr = protocol.WriteCollectionQueryCommand(&cmdBuf, collectionName, []byte(queryJSON))

			default:
				fmt.Printf("Error: Unknown command '%s'. Type 'help' for commands.\n", cmd)
				continue
			}
		}

		if writeErr != nil {
			fmt.Printf("Error encoding command: %v\n", writeErr)
			continue
		}

		if _, err := conn.Write(cmdBuf.Bytes()); err != nil {
			log.Fatalf("Failed to send command to server: %v", err)
		}

		// Update authentication status on successful login.
		status := readResponse(conn, cmd)
		if cmd == "login" && status == protocol.StatusOk {
			isAuthenticated = true
			rl.Config.AutoComplete = postLoginCompleter // MODIFIED: Update completer on success
		}
	}
}

// getCommandAndRawArgs parses the input string into a command and its raw arguments.
func getCommandAndRawArgs(input string) (cmd string, rawArgs string) {
	multiWordCommands := []string{
		"collection item set many",
		"collection item delete many",
		"collection item set",
		"collection item get",
		"collection item delete",
		"collection item list",
		"collection index create",
		"collection index delete",
		"collection index list",
		"collection create",
		"collection delete",
		"collection list",
		"collection query",
		"update password",
	}

	for _, mwCmd := range multiWordCommands {
		if strings.HasPrefix(input, mwCmd) {
			remaining := strings.TrimSpace(input[len(mwCmd):])
			return mwCmd, remaining
		}
	}

	firstSpace := strings.Index(input, " ")
	if firstSpace == -1 {
		return input, ""
	}
	cmd = input[:firstSpace]
	rawArgs = strings.TrimSpace(input[firstSpace:])
	return cmd, rawArgs
}

// parseArgsForJSON parses arguments that include a JSON string and an optional TTL.
func parseArgsForJSON(rawArgs string, fixedArgCount int) (leadingArgs []string, jsonString string, ttlSeconds int64, err error) {
	parts := strings.Fields(rawArgs)

	isKeyOptional := (fixedArgCount == 2)

	if !isKeyOptional {
		if len(parts) < fixedArgCount+1 {
			return nil, "", 0, fmt.Errorf("not enough arguments provided (need %d leading args + JSON value)", fixedArgCount)
		}
	} else {
		if len(parts) < 1 {
			return nil, "", 0, fmt.Errorf("not enough arguments for collection item set (need collection name and JSON, or collection name, key, and JSON)")
		}
	}

	leadingArgs = make([]string, fixedArgCount)
	var actualJsonStart int
	var hasExplicitKey = true

	if isKeyOptional {
		collectionName := parts[0]
		leadingArgs[0] = collectionName

		if len(parts) >= fixedArgCount {
			potentialKeyCandidate := parts[fixedArgCount-1]
			if strings.HasPrefix(potentialKeyCandidate, "{") || strings.HasPrefix(potentialKeyCandidate, "[") {
				hasExplicitKey = false
				leadingArgs[1] = uuid.New().String()
				actualJsonStart = fixedArgCount - 1
			} else {
				leadingArgs[1] = potentialKeyCandidate
				actualJsonStart = fixedArgCount
			}
		} else {
			return nil, "", 0, fmt.Errorf("missing key and/or JSON value for collection item set")
		}
	} else {
		for i := 0; i < fixedArgCount; i++ {
			leadingArgs[i] = parts[i]
		}
		actualJsonStart = fixedArgCount
	}

	jsonPartStartIndex := actualJsonStart
	potentialTTLStr := parts[len(parts)-1]
	ttlSeconds = 0
	isLastPartTTL := false

	if len(parts) > jsonPartStartIndex {
		if val, err := strconv.ParseInt(potentialTTLStr, 10, 64); err == nil {
			isLastPartTTL = true
			ttlSeconds = val
		}
	}

	jsonPartEndIndex := len(parts)
	if isLastPartTTL {
		jsonPartEndIndex--
	}

	if jsonPartStartIndex >= jsonPartEndIndex {
		return nil, "", 0, fmt.Errorf("missing JSON value (e.g., use {} or [])")
	}

	jsonString = strings.Join(parts[jsonPartStartIndex:jsonPartEndIndex], " ")
	jsonString = strings.TrimSpace(jsonString)

	if len(jsonString) == 0 {
		return nil, "", 0, fmt.Errorf("JSON value cannot be empty (use {} or [])")
	}

	if isKeyOptional && !hasExplicitKey {
		var jsonData map[string]any
		if err := json.Unmarshal([]byte(jsonString), &jsonData); err != nil {
			return nil, "", 0, fmt.Errorf("invalid initial JSON for _id injection: %w", err)
		}
		jsonData["_id"] = leadingArgs[fixedArgCount-1]
		updatedJSONBytes, err := json.Marshal(jsonData)
		if err != nil {
			return nil, "", 0, fmt.Errorf("failed to marshal JSON after _id injection: %w", err)
		}
		jsonString = string(updatedJSONBytes)
		fmt.Printf("Note: Key not provided. Generated key: '%s' and injected '_id' into JSON.\n", leadingArgs[fixedArgCount-1])
	}

	if !json.Valid([]byte(jsonString)) {
		return nil, "", 0, fmt.Errorf("invalid JSON value: '%s'", jsonString)
	}

	return leadingArgs, jsonString, ttlSeconds, nil
}

// readResponse now returns the status code from the server.
func readResponse(conn net.Conn, lastCmd string) protocol.ResponseStatus {
	statusByte := make([]byte, 1)
	if _, err := io.ReadFull(conn, statusByte); err != nil {
		fmt.Printf("Error: Failed to read response status: %v\n", err)
		return protocol.StatusError
	}
	status := protocol.ResponseStatus(statusByte[0])

	msgLenBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, msgLenBytes); err != nil {
		fmt.Printf("Error: Failed to read message length: %v\n", err)
		return protocol.StatusError
	}
	msgLen := protocol.ByteOrder.Uint32(msgLenBytes)
	msgBytes := make([]byte, msgLen)
	if _, err := io.ReadFull(conn, msgBytes); err != nil {
		fmt.Printf("Error: Failed to read message: %v\n", err)
		return protocol.StatusError
	}
	message := string(msgBytes)

	dataLenBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, dataLenBytes); err != nil {
		fmt.Printf("Error: Failed to read data length: %v\n", err)
		return protocol.StatusError
	}
	dataLen := protocol.ByteOrder.Uint32(dataLenBytes)
	dataBytes := make([]byte, dataLen)
	if dataLen > 0 {
		if _, err := io.ReadFull(conn, dataBytes); err != nil {
			fmt.Printf("Error: Failed to read data: %v\n", err)
			return protocol.StatusError
		}
	}

	fmt.Printf("    Status: %s (%d)\n", getStatusString(status), status)
	fmt.Printf("    Message: %s\n", message)
	if dataLen > 0 {
		var decodedData []byte
		var decodeErr error

		isCollectionItemListSystemCmd := (lastCmd == "collection item list" && strings.Contains(message, "from collection '_system' retrieved"))

		if isCollectionItemListSystemCmd || lastCmd == "collection query" || lastCmd == "collection index list" {
			decodedData = dataBytes
		} else if lastCmd == "get" || lastCmd == "collection item get" {
			decodedData = dataBytes
		} else if lastCmd == "collection item list" {
			var rawMap map[string]string
			if err := json.Unmarshal(dataBytes, &rawMap); err != nil {
				decodeErr = fmt.Errorf("failed to unmarshal raw map for item list: %w", err)
			} else {
				decodedMap := make(map[string]any)
				for k, v := range rawMap {
					decodedVal, err := base64.StdEncoding.DecodeString(v)
					if err != nil {
						log.Printf("Warning: Failed to Base64 decode value for key '%s' in collection list: %v", k, err)
						decodedMap[k] = v
					} else {
						var jsonVal any
						if err := json.Unmarshal(decodedVal, &jsonVal); err != nil {
							log.Printf("Warning: Failed to unmarshal decoded JSON for key '%s' in collection list: %v", k, err)
							decodedMap[k] = string(decodedVal)
						} else {
							decodedMap[k] = jsonVal
						}
					}
				}
				dataBytes, decodeErr = json.Marshal(decodedMap)
				if decodeErr != nil {
					decodeErr = fmt.Errorf("failed to marshal decoded map for item list: %w", decodeErr)
				}
				decodedData = dataBytes
			}
		} else {
			decodedData = dataBytes
		}

		if decodeErr != nil || !json.Valid(decodedData) {
			if decodeErr != nil {
				fmt.Printf("    Warning: Failed to decode/process data: %v\n", decodeErr)
			}
			fmt.Printf("    Data (Raw):\n%s\n", string(dataBytes))
		} else {
			var prettyJSON bytes.Buffer
			if err := stdjson.Indent(&prettyJSON, decodedData, "    ", "    "); err == nil {
				fmt.Printf("    Data (JSON):\n%s\n", prettyJSON.String())
			} else {
				fmt.Printf("    Data (Raw - not valid JSON for pretty print):\n%s\n", string(decodedData))
			}
		}
	}
	fmt.Println("---")
	return status
}

// getStatusString returns a string representation of a ResponseStatus.
func getStatusString(s protocol.ResponseStatus) string {
	switch s {
	case protocol.StatusOk:
		return "OK"
	case protocol.StatusNotFound:
		return "NOT_FOUND"
	case protocol.StatusError:
		return "ERROR"
	case protocol.StatusBadCommand:
		return "BAD_COMMAND"
	case protocol.StatusUnauthorized:
		return "UNAUTHORIZED"
	case protocol.StatusBadRequest:
		return "BAD_REQUEST"
	default:
		return "UNKNOWN"
	}
}

// clearScreen clears the terminal screen based on the operating system.
func clearScreen() {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("cmd", "/c", "cls")
	case "linux", "darwin":
		cmd = exec.Command("clear")
	default:
		fmt.Println("Cannot clear screen: Unsupported operating system.")
		return
	}
	cmd.Stdout = os.Stdout
	cmd.Run()
}

// printHelp displays the available commands and their usage.
func printHelp() {
	fmt.Println("\nAvailable Commands:")
	fmt.Println("    login <username> <password>")
	fmt.Println("    update password <target_username> <new_password>")
	fmt.Println("    set <key> <value_json> [ttl_seconds]")
	fmt.Println("    get <key>")
	fmt.Println("    collection create <collection_name>")
	fmt.Println("    collection delete <collection_name>")
	fmt.Println("    collection list")
	fmt.Println("    collection index create <collection_name> <field_name>")
	fmt.Println("    collection index delete <collection_name> <field_name>")
	fmt.Println("    collection index list <collection_name>")
	fmt.Println("    collection item set <collection_name> [<key>] <value_json> [ttl_seconds] (Key is optional, UUID generated if omitted)")
	fmt.Println("    collection item set many <collection_name> <value_json_array>")
	fmt.Println("    collection item get <collection_name> <key>")
	fmt.Println("    collection item delete <collection_name> <key>")
	fmt.Println("    collection item delete many <collection_name> <value_json_array>")
	fmt.Println("    collection item list <collection_name>")
	fmt.Println("    collection query <collection_name> <query_json>")
	fmt.Println("    clear")
	fmt.Println("    exit")
	fmt.Println("---")
	fmt.Println("Query JSON Examples:")
	fmt.Println("    Filter (WHERE):")
	fmt.Println(`        {"filter": {"field": "status", "op": "=", "value": "active"}}`)
	fmt.Println(`        {"filter": {"and": [{"field": "age", "op": ">", "value": 30}, {"field": "city", "op": "like", "value": "New%"}]}}`)
	fmt.Println(`        {"filter": {"field": "tags", "op": "in", "value": ["A", "B"]}}`)
	fmt.Println(`        {"filter": {"field": "description", "op": "is not null"}}`)
	fmt.Println("    Order By:")
	fmt.Println(`        {"order_by": [{"field": "name", "direction": "asc"}, {"field": "age", "direction": "desc"}]}`)
	fmt.Println("    Limit/Offset:")
	fmt.Println(`        {"limit": 5, "offset": 10}`)
	fmt.Println("    Count:")
	fmt.Println(`        {"count": true, "filter": {"field": "active", "op": "=", "value": true}}`)
	fmt.Println("    Aggregations (SUM, AVG, MIN, MAX):")
	fmt.Println(`        {"aggregations": {"total_sales": {"func": "sum", "field": "sales"}}, "group_by": ["category"]}`)
	fmt.Println("    Distinct:")
	fmt.Println(`        {"distinct": "city"}`)
	fmt.Println("---")
}
