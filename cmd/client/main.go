package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	stdjson "encoding/json"
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

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func main() {
	log.SetFlags(0) // No timestamps for client logs.

	if len(os.Args) != 2 {
		fmt.Println("Usage: client <address>")
		fmt.Println("Example: client localhost:8080")
		return
	}

	addr := os.Args[1]

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

	fmt.Printf("Connected securely to Memory Tools server at %s. Type 'help' for commands, 'exit' to quit.\n", addr)
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "exit" {
			fmt.Println("Exiting client.")
			return
		}
		if input == "help" {
			printHelp()
			continue
		}
		if input == "clear" {
			clearScreen() // Handles terminal clearing based on OS.
			continue
		}
		if input == "" {
			continue
		}

		cmd, rawArgs := getCommandAndRawArgs(input)

		var cmdBuf bytes.Buffer
		var writeErr error

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
				fmt.Println("Error: get command requires key")
				printHelp()
				continue
			}
			key := argsList[0]
			writeErr = protocol.WriteGetCommand(&cmdBuf, key)

		case "collection create":
			argsList := strings.Fields(rawArgs)
			if len(argsList) < 1 {
				fmt.Println("Error: collection create command requires collection_name")
				printHelp()
				continue
			}
			collectionName := argsList[0]
			writeErr = protocol.WriteCollectionCreateCommand(&cmdBuf, collectionName)
		case "collection delete":
			argsList := strings.Fields(rawArgs)
			if len(argsList) < 1 {
				fmt.Println("Error: collection delete command requires collection_name")
				printHelp()
				continue
			}
			collectionName := argsList[0]
			writeErr = protocol.WriteCollectionDeleteCommand(&cmdBuf, collectionName)
		case "collection list":
			writeErr = protocol.WriteCollectionListCommand(&cmdBuf)

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
				fmt.Println("Error: collection item get command requires collection_name and key")
				printHelp()
				continue
			}
			collectionName := argsList[0]
			key := argsList[1]
			writeErr = protocol.WriteCollectionItemGetCommand(&cmdBuf, collectionName, key)
		case "collection item delete":
			argsList := strings.Fields(rawArgs)
			if len(argsList) < 2 {
				fmt.Println("Error: collection item delete command requires collection_name and key")
				printHelp()
				continue
			}
			collectionName := argsList[0]
			key := argsList[1]
			writeErr = protocol.WriteCollectionItemDeleteCommand(&cmdBuf, collectionName, key)
		case "collection item list":
			argsList := strings.Fields(rawArgs)
			if len(argsList) < 1 {
				fmt.Println("Error: collection item list command requires collection_name")
				printHelp()
				continue
			}
			collectionName := argsList[0]
			writeErr = protocol.WriteCollectionItemListCommand(&cmdBuf, collectionName)

		default:
			fmt.Printf("Error: Unknown command '%s'. Type 'help' for commands.\n", cmd)
			continue
		}

		if writeErr != nil {
			fmt.Printf("Error encoding command: %v\n", writeErr)
			continue
		}

		if _, err := conn.Write(cmdBuf.Bytes()); err != nil {
			log.Fatalf("Failed to send command to server: %v", err)
		}

		readResponse(conn, cmd)
	}
}

// getCommandAndRawArgs parses the input string into a command and its raw arguments.
func getCommandAndRawArgs(input string) (cmd string, rawArgs string) {
	multiWordCommands := []string{
		"collection item set",
		"collection item get",
		"collection item delete",
		"collection item list",
		"collection create",
		"collection delete",
		"collection list",
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

	if len(parts) < fixedArgCount+1 {
		return nil, "", 0, fmt.Errorf("not enough arguments provided (need %d leading args + JSON value)", fixedArgCount)
	}

	leadingArgs = make([]string, fixedArgCount)
	for i := range fixedArgCount {
		leadingArgs[i] = parts[i]
	}

	jsonPartStartIndex := fixedArgCount

	potentialTTLStr := parts[len(parts)-1]
	ttlSeconds = 0

	isLastPartTTL := false
	if len(parts) > fixedArgCount {
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

	if !json.Valid([]byte(jsonString)) {
		return nil, "", 0, fmt.Errorf("invalid JSON value: '%s'", jsonString)
	}

	return leadingArgs, jsonString, ttlSeconds, nil
}

// readResponse reads and processes the server's binary response.
func readResponse(conn net.Conn, lastCmd string) {
	statusByte := make([]byte, 1)
	if _, err := io.ReadFull(conn, statusByte); err != nil {
		fmt.Printf("Error: Failed to read response status: %v\n", err)
		return
	}
	status := protocol.ResponseStatus(statusByte[0])

	msgLenBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, msgLenBytes); err != nil {
		fmt.Printf("Error: Failed to read message length: %v\n", err)
		return
	}
	msgLen := protocol.ByteOrder.Uint32(msgLenBytes)
	msgBytes := make([]byte, msgLen)
	if _, err := io.ReadFull(conn, msgBytes); err != nil {
		fmt.Printf("Error: Failed to read message: %v\n", err)
		return
	}
	message := string(msgBytes)

	dataLenBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, dataLenBytes); err != nil {
		fmt.Printf("Error: Failed to read data length: %v\n", err)
		return
	}
	dataLen := protocol.ByteOrder.Uint32(dataLenBytes)
	dataBytes := make([]byte, dataLen)
	if dataLen > 0 {
		if _, err := io.ReadFull(conn, dataBytes); err != nil {
			fmt.Printf("Error: Failed to read data: %v\n", err)
			return
		}
	}

	fmt.Printf(" 	Status: %s (%d)\n", getStatusString(status), status)
	fmt.Printf(" 	Message: %s\n", message)
	if dataLen > 0 {
		var decodedData []byte
		var decodeErr error

		// Data handling based on the command type.
		isSingleValueJsonCommand := (lastCmd == "get" || lastCmd == "collection item get")
		isMapOfValuesJsonCommand := (lastCmd == "collection item list")
		isCollectionNamesListCommand := (lastCmd == "collection list")

		if isSingleValueJsonCommand {
			// Server sends raw JSON bytes; no Base64 decode needed.
			decodedData = dataBytes
		} else if isMapOfValuesJsonCommand {
			var rawMap map[string]string
			if err := json.Unmarshal(dataBytes, &rawMap); err != nil {
				decodeErr = fmt.Errorf("failed to unmarshal raw map for item list: %w", err)
			} else {
				decodedMap := make(map[string]any)
				for k, v := range rawMap {
					decodedVal, err := base64.StdEncoding.DecodeString(v)
					if err != nil {
						log.Printf("Warning: Failed to Base64 decode value for key '%s' in collection list: %v", k, err)
						decodedMap[k] = v // Keep raw Base64 if decoding fails.
					} else {
						var jsonVal any
						if err := json.Unmarshal(decodedVal, &jsonVal); err != nil {
							log.Printf("Warning: Failed to unmarshal decoded JSON for key '%s' in collection list: %v", k, err)
							decodedMap[k] = string(decodedVal) // Keep raw decoded string if JSON unmarshal fails.
						} else {
							decodedMap[k] = jsonVal
						}
					}
				}
				dataBytes, decodeErr = json.Marshal(decodedMap) // Remarshal the decoded map to JSON for pretty printing.
				if decodeErr != nil {
					decodeErr = fmt.Errorf("failed to marshal decoded map for item list: %w", decodeErr)
				}
				decodedData = dataBytes
			}
		} else if isCollectionNamesListCommand {
			decodedData = dataBytes // Already a plain JSON array of strings.
		} else {
			// Fallback for other commands that might return data.
			decodedData = dataBytes
		}

		// Attempt to pretty print decodedData.
		if decodeErr != nil || !json.Valid(decodedData) {
			if decodeErr != nil {
				fmt.Printf(" 	Warning: Failed to decode/process data: %v\n", decodeErr)
			}
			fmt.Printf(" 	Data (Raw):\n%s\n", string(dataBytes))
		} else {
			var prettyJSON bytes.Buffer
			if err := stdjson.Indent(&prettyJSON, decodedData, " 	", " 	"); err == nil {
				fmt.Printf(" 	Data (JSON):\n%s\n", prettyJSON.String())
			} else {
				fmt.Printf(" 	Data (Raw - not valid JSON for pretty print):\n%s\n", string(decodedData))
			}
		}
	}
	fmt.Println("---")
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
	case "linux", "darwin": // macOS is "darwin".
		cmd = exec.Command("clear")
	default:
		fmt.Println("Cannot clear screen: Unsupported operating system.")
		return
	}
	cmd.Stdout = os.Stdout
	cmd.Run() // Execute the command to clear the screen.
}

// printHelp displays the available commands and their usage.
func printHelp() {
	fmt.Println("\nAvailable Commands:")
	fmt.Println(" 	set <key> <value_json> [ttl_seconds]")
	fmt.Println(" 	get <key>")
	fmt.Println(" 	collection create <collection_name>")
	fmt.Println(" 	collection delete <collection_name>")
	fmt.Println(" 	collection list")
	fmt.Println(" 	collection item set <collection_name> <key> <value_json> [ttl_seconds]")
	fmt.Println(" 	collection item get <collection_name> <key>")
	fmt.Println(" 	collection item delete <collection_name> <key>")
	fmt.Println(" 	collection item list <collection_name>")
	fmt.Println(" 	clear")
	fmt.Println(" 	exit")
	fmt.Println("---")
}
