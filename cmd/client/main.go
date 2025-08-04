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

// --- Completers for different authentication states ---

// preLoginCompleter provides command suggestions before a user logs in.
var preLoginCompleter = readline.NewPrefixCompleter(
	readline.PcItem("login"),
	readline.PcItem("help"),
	readline.PcItem("exit"),
	readline.PcItem("clear"),
)

// postLoginCompleter provides command suggestions after a user logs in.
var postLoginCompleter = readline.NewPrefixCompleter(
	readline.PcItem("user",
		readline.PcItem("create"),
		readline.PcItem("update"),
		readline.PcItem("delete"),
	),
	readline.PcItem("update password"),
	readline.PcItem("backup"),
	readline.PcItem("restore"),
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
			readline.PcItem("update"),
			readline.PcItem("update many"),
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

	// Command-line arguments
	usernamePtr := flag.String("u", "", "Username for authentication")
	passwordPtr := flag.String("p", "", "Password for authentication")
	flag.Parse()

	addr := "localhost:5876" // Default address
	if flag.NArg() > 0 {
		addr = flag.Arg(0)
	}

	// TLS Configuration
	caCert, err := os.ReadFile("certificates/server.crt")
	if err != nil {
		log.Fatalf("Failed to read server certificate 'certificates/server.crt': %v", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		RootCAs:    caCertPool,
		ServerName: strings.Split(addr, ":")[0],
	}

	// Connect using TLS
	conn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		log.Fatalf("Failed to connect via TLS to %s: %v", addr, err)
	}
	defer conn.Close()

	fmt.Printf("Connected securely to Memory Tools server at %s.\n", addr)

	var isAuthenticated bool

	// Initialize readline
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "> ",
		HistoryFile:     "/tmp/readline_history.tmp",
		AutoComplete:    preLoginCompleter,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		log.Fatalf("Failed to initialize readline: %v", err)
	}
	defer rl.Close()

	// Automatic authentication if credentials are provided
	if *usernamePtr != "" && *passwordPtr != "" {
		log.Printf("Attempting automatic login for user '%s'...", *usernamePtr)
		var cmdBuf bytes.Buffer
		if err := protocol.WriteAuthenticateCommand(&cmdBuf, *usernamePtr, *passwordPtr); err == nil {
			if _, err := conn.Write(cmdBuf.Bytes()); err != nil {
				log.Fatalf("Failed to send login command to server: %v", err)
			}
			if status := readResponse(conn, "login"); status == protocol.StatusOk {
				isAuthenticated = true
				rl.Config.AutoComplete = postLoginCompleter
			} else {
				os.Exit(1) // Exit if auto-login fails
			}
		}
	}

	if !isAuthenticated {
		fmt.Println("Please login using: login <username> <password>")
	}

	// Main command loop
	for {
		input, err := rl.Readline()
		if err == readline.ErrInterrupt {
			if len(input) == 0 {
				break
			} else {
				continue
			}
		} else if err == io.EOF {
			break
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		// Client-side utility commands
		if input == "exit" {
			break
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

		if !isAuthenticated && cmd != "login" {
			fmt.Println("Error: You must log in first. Use: login <username> <password>")
			continue
		}

		var cmdBuf bytes.Buffer
		var writeErr error

		// --- Command Processing ---
		if cmd == "login" {
			argsList := strings.Fields(rawArgs)
			if len(argsList) != 2 {
				fmt.Println("Usage: login <username> <password>")
				continue
			}
			writeErr = protocol.WriteAuthenticateCommand(&cmdBuf, argsList[0], argsList[1])
		} else if cmd == "user create" {
			parts := strings.SplitN(rawArgs, " ", 3)
			if len(parts) < 3 {
				fmt.Println("Usage: user create <username> <password> <permissions_json>")
				continue
			}
			username, password, permissionsJSON := parts[0], parts[1], parts[2]
			if !json.Valid([]byte(permissionsJSON)) {
				fmt.Println("Error: Invalid permissions JSON format.")
				continue
			}
			writeErr = protocol.WriteUserCreateCommand(&cmdBuf, username, password, []byte(permissionsJSON))
		} else if cmd == "user update" {
			parts := strings.SplitN(rawArgs, " ", 2)
			if len(parts) < 2 {
				fmt.Println("Usage: user update <username> <permissions_json>")
				continue
			}
			username, permissionsJSON := parts[0], parts[1]
			if !json.Valid([]byte(permissionsJSON)) {
				fmt.Println("Error: Invalid permissions JSON format.")
				continue
			}
			writeErr = protocol.WriteUserUpdateCommand(&cmdBuf, username, []byte(permissionsJSON))
		} else if cmd == "user delete" {
			argsList := strings.Fields(rawArgs)
			if len(argsList) < 1 {
				fmt.Println("Usage: user delete <username>")
				continue
			}
			writeErr = protocol.WriteUserDeleteCommand(&cmdBuf, argsList[0])
		} else {
			// Fallback to the large switch for all other commands
			switch cmd {
			case "backup":
				writeErr = protocol.WriteBackupCommand(&cmdBuf)
			case "restore":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 1 {
					fmt.Println("Usage: restore <backup_directory_name>")
					continue
				}
				writeErr = protocol.WriteRestoreCommand(&cmdBuf, argsList[0])
			case "update password":
				argsList := strings.Fields(rawArgs)
				if len(argsList) != 2 {
					fmt.Println("Usage: update password <target_username> <new_password>")
					continue
				}
				writeErr = protocol.WriteChangeUserPasswordCommand(&cmdBuf, argsList[0], argsList[1])
			case "set":
				parsedArgs, jsonVal, ttlSeconds, parseErr := parseArgsForJSON(rawArgs, 1, false)
				if parseErr != nil {
					fmt.Printf("Error parsing 'set' command: %v\n", parseErr)
					continue
				}
				writeErr = protocol.WriteSetCommand(&cmdBuf, parsedArgs[0], []byte(jsonVal), time.Duration(ttlSeconds)*time.Second)

			case "get":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 1 {
					fmt.Println("Usage: get <key>")
					continue
				}
				writeErr = protocol.WriteGetCommand(&cmdBuf, argsList[0])

			case "collection create":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 1 {
					fmt.Println("Usage: collection create <collection_name>")
					continue
				}
				writeErr = protocol.WriteCollectionCreateCommand(&cmdBuf, argsList[0])
			case "collection delete":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 1 {
					fmt.Println("Usage: collection delete <collection_name>")
					continue
				}
				writeErr = protocol.WriteCollectionDeleteCommand(&cmdBuf, argsList[0])
			case "collection list":
				writeErr = protocol.WriteCollectionListCommand(&cmdBuf)

			case "collection index create":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 2 {
					fmt.Println("Usage: collection index create <collection_name> <field_name>")
					continue
				}
				writeErr = protocol.WriteCollectionIndexCreateCommand(&cmdBuf, argsList[0], argsList[1])

			case "collection index delete":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 2 {
					fmt.Println("Usage: collection index delete <collection_name> <field_name>")
					continue
				}
				writeErr = protocol.WriteCollectionIndexDeleteCommand(&cmdBuf, argsList[0], argsList[1])

			case "collection index list":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 1 {
					fmt.Println("Usage: collection index list <collection_name>")
					continue
				}
				writeErr = protocol.WriteCollectionIndexListCommand(&cmdBuf, argsList[0])

			case "collection item set":
				parsedArgs, jsonVal, ttlSeconds, parseErr := parseArgsForJSON(rawArgs, 2, true)
				if parseErr != nil {
					fmt.Printf("Error parsing 'collection item set' command: %v\n", parseErr)
					continue
				}
				writeErr = protocol.WriteCollectionItemSetCommand(&cmdBuf, parsedArgs[0], parsedArgs[1], []byte(jsonVal), time.Duration(ttlSeconds)*time.Second)

			case "collection item update":
				parsedArgs, jsonVal, _, parseErr := parseArgsForJSON(rawArgs, 2, false)
				if parseErr != nil {
					fmt.Printf("Error parsing 'collection item update' command: %v\n", parseErr)
					continue
				}
				writeErr = protocol.WriteCollectionItemUpdateCommand(&cmdBuf, parsedArgs[0], parsedArgs[1], []byte(jsonVal))
			case "collection item get":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 2 {
					fmt.Println("Usage: collection item get <collection_name> <key>")
					continue
				}
				writeErr = protocol.WriteCollectionItemGetCommand(&cmdBuf, argsList[0], argsList[1])
			case "collection item delete":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 2 {
					fmt.Println("Usage: collection item delete <collection_name> <key>")
					continue
				}
				writeErr = protocol.WriteCollectionItemDeleteCommand(&cmdBuf, argsList[0], argsList[1])
			case "collection item list":
				argsList := strings.Fields(rawArgs)
				if len(argsList) < 1 {
					fmt.Println("Usage: collection item list <collection_name>")
					continue
				}
				writeErr = protocol.WriteCollectionItemListCommand(&cmdBuf, argsList[0])
			case "collection item set many":
				parts := strings.SplitN(rawArgs, " ", 2)
				if len(parts) < 2 {
					fmt.Println("Usage: collection item set many <collection_name> <json_array>")
					continue
				}
				if !json.Valid([]byte(parts[1])) {
					fmt.Println("Error: Invalid JSON array format.")
					continue
				}
				writeErr = protocol.WriteCollectionItemSetManyCommand(&cmdBuf, parts[0], []byte(parts[1]))
			case "collection item update many":
				parts := strings.SplitN(rawArgs, " ", 2)
				if len(parts) < 2 {
					fmt.Println("Usage: collection item update many <collection_name> <patch_json_array>")
					continue
				}
				if !json.Valid([]byte(parts[1])) {
					fmt.Println("Error: Invalid patch JSON array format.")
					continue
				}
				writeErr = protocol.WriteCollectionItemUpdateManyCommand(&cmdBuf, parts[0], []byte(parts[1]))
			case "collection item delete many":
				parts := strings.SplitN(rawArgs, " ", 2)
				if len(parts) < 2 {
					fmt.Println("Usage: collection item delete many <collection_name> <keys_json_array>")
					continue
				}
				var keysToDelete []string
				if err := json.Unmarshal([]byte(parts[1]), &keysToDelete); err != nil {
					fmt.Printf("Error parsing keys JSON array: %v\n", err)
					continue
				}
				writeErr = protocol.WriteCollectionItemDeleteManyCommand(&cmdBuf, parts[0], keysToDelete)
			case "collection query":
				parts := strings.SplitN(rawArgs, " ", 2)
				if len(parts) < 2 {
					fmt.Println("Usage: collection query <collection_name> <query_json>")
					continue
				}
				if !json.Valid([]byte(parts[1])) {
					fmt.Println("Error: Invalid query JSON format.")
					continue
				}
				writeErr = protocol.WriteCollectionQueryCommand(&cmdBuf, parts[0], []byte(parts[1]))
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

		status := readResponse(conn, cmd)
		if cmd == "login" && status == protocol.StatusOk {
			isAuthenticated = true
			rl.Config.AutoComplete = postLoginCompleter
		}
	}
	fmt.Println("Exiting client.")
}

// getCommandAndRawArgs parses the input string into a command and its raw arguments.
func getCommandAndRawArgs(input string) (string, string) {
	multiWordCommands := []string{
		"user create",
		"user update",
		"user delete",
		"update password",
		"collection item update many",
		"collection item set many",
		"collection item delete many",
		"collection item update",
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
	}

	for _, mwCmd := range multiWordCommands {
		if strings.HasPrefix(input, mwCmd) {
			return mwCmd, strings.TrimSpace(input[len(mwCmd):])
		}
	}

	parts := strings.SplitN(input, " ", 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

// parseArgsForJSON parses arguments for commands that include a JSON string.
func parseArgsForJSON(rawArgs string, fixedArgCount int, isKeyOptionalInSet bool) (leadingArgs []string, jsonString string, ttlSeconds int64, err error) {
	parts := strings.Fields(rawArgs)
	if isKeyOptionalInSet {
		if len(parts) < 1 {
			return nil, "", 0, fmt.Errorf("not enough arguments, need at least collection name and JSON value")
		}
	} else {
		if len(parts) < fixedArgCount+1 {
			return nil, "", 0, fmt.Errorf("not enough arguments provided (need %d leading args + JSON value)", fixedArgCount)
		}
	}

	jsonPartStartIndex := 0
	leadingArgs = make([]string, fixedArgCount)

	if isKeyOptionalInSet {
		leadingArgs[0] = parts[0] // collection name
		if len(parts) > 1 && (strings.HasPrefix(parts[1], "{") || strings.HasPrefix(parts[1], "[")) {
			leadingArgs[1] = uuid.New().String() // Generate key
			jsonPartStartIndex = 1
			fmt.Printf("Note: Key not provided. Generated key: '%s'\n", leadingArgs[1])
		} else if len(parts) > 2 {
			leadingArgs[1] = parts[1] // key is provided
			jsonPartStartIndex = 2
		} else {
			return nil, "", 0, fmt.Errorf("missing JSON value for 'collection item set'")
		}
	} else {
		for i := 0; i < fixedArgCount; i++ {
			leadingArgs[i] = parts[i]
		}
		jsonPartStartIndex = fixedArgCount
	}

	potentialTTLStr := parts[len(parts)-1]
	isLastPartTTL := false
	if len(parts) > jsonPartStartIndex+1 {
		if val, err := strconv.ParseInt(potentialTTLStr, 10, 64); err == nil {
			ttlSeconds = val
			isLastPartTTL = true
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
	if !json.Valid([]byte(jsonString)) {
		return nil, "", 0, fmt.Errorf("invalid JSON value: '%s'", jsonString)
	}

	return leadingArgs, jsonString, ttlSeconds, nil
}

// readResponse reads and displays the structured response from the server.
func readResponse(conn net.Conn, lastCmd string) protocol.ResponseStatus {
	statusByte := make([]byte, 1)
	if _, err := io.ReadFull(conn, statusByte); err != nil {
		fmt.Printf("\nError: Failed to read response status from server: %v\n", err)
		return protocol.StatusError
	}
	status := protocol.ResponseStatus(statusByte[0])

	msg, err := protocol.ReadString(conn)
	if err != nil {
		fmt.Printf("\nError: Failed to read response message from server: %v\n", err)
		return protocol.StatusError
	}

	dataBytes, err := protocol.ReadBytes(conn)
	if err != nil {
		fmt.Printf("\nError: Failed to read response data from server: %v\n", err)
		return protocol.StatusError
	}

	fmt.Printf("    Status: %s (%d)\n", getStatusString(status), status)
	fmt.Printf("    Message: %s\n", msg)

	if len(dataBytes) > 0 {
		var finalDataForPrint []byte = dataBytes

		// Special handling for 'collection item list' to decode Base64 values
		if lastCmd == "collection item list" && status == protocol.StatusOk {
			var rawMap map[string]string
			if err := json.Unmarshal(dataBytes, &rawMap); err == nil {
				decodedMap := make(map[string]any)
				for key, b64Value := range rawMap {
					decodedValue, err := base64.StdEncoding.DecodeString(b64Value)
					if err != nil {
						decodedMap[key] = b64Value
						continue
					}
					var jsonValue any
					if err := json.Unmarshal(decodedValue, &jsonValue); err != nil {
						decodedMap[key] = string(decodedValue)
					} else {
						decodedMap[key] = jsonValue
					}
				}
				finalDataForPrint, _ = json.Marshal(decodedMap)
			}
		}

		var prettyJSON bytes.Buffer
		if err := stdjson.Indent(&prettyJSON, finalDataForPrint, "    ", "    "); err == nil {
			fmt.Printf("    Data (JSON):\n%s\n", prettyJSON.String())
		} else {
			fmt.Printf("    Data (Raw):\n%s\n", string(finalDataForPrint))
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

// clearScreen clears the terminal screen.
func clearScreen() {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("cmd", "/c", "cls")
	case "linux", "darwin":
		cmd = exec.Command("clear")
	default:
		return // Unsupported OS
	}
	cmd.Stdout = os.Stdout
	cmd.Run()
}

// printHelp displays the available commands and their usage.
func printHelp() {
	fmt.Println("\nAvailable Commands:")
	fmt.Println("--- Auth ---")
	fmt.Println("    login <username> <password>")
	fmt.Println("    update password <target_username> <new_password>")
	fmt.Println("\n--- Admin (Root Only) ---")
	fmt.Println("  backup")
	fmt.Println("  restore <backup_directory_name>")
	fmt.Println("\n--- User Management (Requires write access to _system) ---")
	fmt.Println(`    user create <username> <password> '{"<collection>":"<perm>", "*":"read"}'`)
	fmt.Println(`    user update <username> '{"<collection>":"<perm>"}'`)
	fmt.Println("    user delete <username>")
	fmt.Println("\n--- Main Store ---")
	fmt.Println("    set <key> <value_json> [ttl_seconds]")
	fmt.Println("    get <key>")
	fmt.Println("\n--- Collections ---")
	fmt.Println("    collection create <name>")
	fmt.Println("    collection delete <name>")
	fmt.Println("    collection list")
	fmt.Println("\n--- Collection Indexes ---")
	fmt.Println("    collection index create <coll_name> <field_name>")
	fmt.Println("    collection index delete <coll_name> <field_name>")
	fmt.Println("    collection index list <coll_name>")
	fmt.Println("\n--- Collection Items ---")
	fmt.Println("    collection item set <coll_name> [<key>] <value_json> [ttl]")
	fmt.Println("    collection item get <coll_name> <key>")
	fmt.Println("    collection item delete <coll_name> <key>")
	fmt.Println("    collection item update <coll_name> <key> <patch_json>")
	fmt.Println("    collection item list <coll_name>")
	fmt.Println("\n--- Batch & Query ---")
	fmt.Println("    collection item set many <coll_name> <json_array>")
	fmt.Println("    collection item update many <coll_name> <patch_json_array>")
	fmt.Println("    collection item delete many <coll_name> <keys_json_array_of_strings>")
	fmt.Println("    collection query <coll_name> <query_json>")
	fmt.Println("\n--- Client ---")
	fmt.Println("    clear, help, exit")
	fmt.Println("-----------------------------------------------------------------")
	fmt.Println("Permissions can be 'read' or 'write'. Use '*' as a wildcard for collections.")
}
