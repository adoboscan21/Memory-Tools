package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"

	"memory-tools/internal/protocol"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// CommandRequest struct to parse incoming JSON from HTTP client.
type CommandRequest struct {
	Command string `json:"command"`
	Args    string `json:"args,omitempty"`  // For commands like 'get', 'collection list' etc.
	Value   any    `json:"value,omitempty"` // For commands like 'set', 'collection item set'.
	TTL     int64  `json:"ttl,omitempty"`   // For TTL on 'set' commands.
}

// CommandResponse struct for HTTP client.
type CommandResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"` // Use any for dynamic JSON data.
	Error   string `json:"error,omitempty"`
}

var (
	memoryToolsAddr string
	tlsConfig       *tls.Config
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 3 {
		fmt.Println("Usage: api_server <api_listen_address> <memory_tools_server_address>")
		fmt.Println("Example: api_server :8081 localhost:8080")
		return
	}

	apiListenAddr := os.Args[1]
	memoryToolsAddr = os.Args[2]

	// Load CA certificate for TLS connection to Memory Tools Server.
	caCert, err := os.ReadFile("certificates/server.crt")
	if err != nil {
		log.Fatalf("Failed to read server certificate 'certificates/server.crt': %v", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Configure TLS for secure communication with the memory-tools server.
	tlsConfig = &tls.Config{
		RootCAs:            caCertPool,
		ServerName:         strings.Split(memoryToolsAddr, ":")[0],
		InsecureSkipVerify: false, // Ensure server certificate is verified.
	}

	http.HandleFunc("/command", commandHandler)

	fmt.Printf("API Server listening on %s, forwarding to Memory Tools server at %s\n", apiListenAddr, memoryToolsAddr)
	log.Fatal(http.ListenAndServe(apiListenAddr, nil))
}

// commandHandler processes incoming HTTP requests, translates them to internal protocol,
// communicates with the memory-tools server, and returns a JSON response.
func commandHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CommandRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON request: %v", err), http.StatusBadRequest)
		return
	}

	log.Printf("Received API request: Command='%s', Args='%s', Value set: %t", req.Command, req.Args, req.Value != nil)

	// Establish a TLS connection to the memory-tools server.
	conn, err := tls.Dial("tcp", memoryToolsAddr, tlsConfig)
	if err != nil {
		log.Printf("Failed to connect to memory-tools server: %v", err)
		writeJSONResponse(w, http.StatusInternalServerError, CommandResponse{
			Status:  "ERROR",
			Message: "Failed to connect to memory-tools server",
			Error:   err.Error(),
		})
		return
	}
	defer conn.Close()

	var cmdBuf bytes.Buffer
	var writeErr error
	var responseCmdType string // Used to guide parsing of the protocol response.

	// Handle different API commands and map them to the internal protocol.
	switch req.Command {
	case "set":
		if req.Args == "" {
			writeJSONResponse(w, http.StatusBadRequest, CommandResponse{Status: "BAD_REQUEST", Message: "Error: set command requires a key in 'args'"})
			return
		}
		if req.Value == nil {
			writeJSONResponse(w, http.StatusBadRequest, CommandResponse{Status: "BAD_REQUEST", Message: "Error: set command requires a 'value' field"})
			return
		}
		jsonBytes, marshalErr := json.Marshal(req.Value)
		if marshalErr != nil {
			writeJSONResponse(w, http.StatusBadRequest, CommandResponse{Status: "BAD_REQUEST", Message: "Invalid JSON value provided", Error: marshalErr.Error()})
			return
		}
		key := strings.TrimSpace(req.Args)
		writeErr = protocol.WriteSetCommand(&cmdBuf, key, jsonBytes, time.Duration(req.TTL)*time.Second)
		responseCmdType = "set"

	case "get":
		argsList := strings.Fields(req.Args)
		if len(argsList) < 1 {
			writeJSONResponse(w, http.StatusBadRequest, CommandResponse{Status: "BAD_REQUEST", Message: "Error: get command requires key"})
			return
		}
		key := argsList[0]
		writeErr = protocol.WriteGetCommand(&cmdBuf, key)
		responseCmdType = "get"

	case "collection create":
		argsList := strings.Fields(req.Args)
		if len(argsList) < 1 {
			writeJSONResponse(w, http.StatusBadRequest, CommandResponse{Status: "BAD_REQUEST", Message: "Error: collection create command requires collection_name"})
			return
		}
		collectionName := argsList[0]
		writeErr = protocol.WriteCollectionCreateCommand(&cmdBuf, collectionName)
		responseCmdType = "collection create"

	case "collection delete":
		argsList := strings.Fields(req.Args)
		if len(argsList) < 1 {
			writeJSONResponse(w, http.StatusBadRequest, CommandResponse{Status: "BAD_REQUEST", Message: "Error: collection delete command requires collection_name"})
			return
		}
		collectionName := argsList[0]
		writeErr = protocol.WriteCollectionDeleteCommand(&cmdBuf, collectionName)
		responseCmdType = "collection delete"

	case "collection list":
		writeErr = protocol.WriteCollectionListCommand(&cmdBuf)
		responseCmdType = "collection list"

	case "collection item set":
		argsParts := strings.Fields(req.Args)
		if len(argsParts) < 2 {
			writeJSONResponse(w, http.StatusBadRequest, CommandResponse{Status: "BAD_REQUEST", Message: "Error: collection item set command requires 'args' with collection_name and key"})
			return
		}
		if req.Value == nil {
			writeJSONResponse(w, http.StatusBadRequest, CommandResponse{Status: "BAD_REQUEST", Message: "Error: collection item set command requires a 'value' field"})
			return
		}
		jsonBytes, marshalErr := json.Marshal(req.Value)
		if marshalErr != nil {
			writeJSONResponse(w, http.StatusBadRequest, CommandResponse{Status: "BAD_REQUEST", Message: "Invalid JSON value provided", Error: marshalErr.Error()})
			return
		}
		collectionName := argsParts[0]
		key := argsParts[1]
		writeErr = protocol.WriteCollectionItemSetCommand(&cmdBuf, collectionName, key, jsonBytes, time.Duration(req.TTL)*time.Second)
		responseCmdType = "collection item set"

	case "collection item get":
		argsList := strings.Fields(req.Args)
		if len(argsList) < 2 {
			writeJSONResponse(w, http.StatusBadRequest, CommandResponse{Status: "BAD_REQUEST", Message: "Error: collection item get command requires collection_name and key"})
			return
		}
		collectionName := argsList[0]
		key := argsList[1]
		writeErr = protocol.WriteCollectionItemGetCommand(&cmdBuf, collectionName, key)
		responseCmdType = "collection item get"

	case "collection item delete":
		argsList := strings.Fields(req.Args)
		if len(argsList) < 2 {
			writeJSONResponse(w, http.StatusBadRequest, CommandResponse{Status: "BAD_REQUEST", Message: "Error: collection item delete command requires collection_name and key"})
			return
		}
		collectionName := argsList[0]
		key := argsList[1]
		writeErr = protocol.WriteCollectionItemDeleteCommand(&cmdBuf, collectionName, key)
		responseCmdType = "collection item delete"

	case "collection item list":
		argsList := strings.Fields(req.Args)
		if len(argsList) < 1 {
			writeJSONResponse(w, http.StatusBadRequest, CommandResponse{Status: "BAD_REQUEST", Message: "Error: collection item list command requires collection_name"})
			return
		}
		collectionName := argsList[0]
		writeErr = protocol.WriteCollectionItemListCommand(&cmdBuf, collectionName)
		responseCmdType = "collection item list"

	default:
		writeJSONResponse(w, http.StatusBadRequest, CommandResponse{Status: "BAD_COMMAND", Message: fmt.Sprintf("Unknown command: %s", req.Command)})
		return
	}

	if writeErr != nil {
		log.Printf("Error encoding command %s: %v", req.Command, writeErr)
		writeJSONResponse(w, http.StatusInternalServerError, CommandResponse{Status: "ERROR", Message: "Failed to encode command", Error: writeErr.Error()})
		return
	}

	// Send the encoded command to the memory-tools server.
	if _, err := conn.Write(cmdBuf.Bytes()); err != nil {
		log.Printf("Failed to send command to memory-tools server: %v", err)
		writeJSONResponse(w, http.StatusInternalServerError, CommandResponse{Status: "ERROR", Message: "Failed to send command to memory-tools server", Error: err.Error()})
		return
	}

	// Read and process the response from the memory-tools server.
	response := readProtocolResponse(conn, responseCmdType)
	writeJSONResponse(w, http.StatusOK, response)
}

// writeJSONResponse is a helper function to send JSON responses.
func writeJSONResponse(w http.ResponseWriter, statusCode int, data CommandResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Error writing JSON response: %v", err)
	}
}

// readProtocolResponse reads the raw protocol response and converts it to CommandResponse.
func readProtocolResponse(conn net.Conn, lastCmd string) CommandResponse {
	// Read status byte.
	statusByte := make([]byte, 1)
	if _, err := io.ReadFull(conn, statusByte); err != nil {
		return CommandResponse{Status: "ERROR", Message: "Failed to read response status", Error: err.Error()}
	}
	status := protocol.ResponseStatus(statusByte[0])

	// Read message length and message.
	msgLenBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, msgLenBytes); err != nil {
		return CommandResponse{Status: "ERROR", Message: "Failed to read message length", Error: err.Error()}
	}
	msgLen := protocol.ByteOrder.Uint32(msgLenBytes)
	msgBytes := make([]byte, msgLen)
	if _, err := io.ReadFull(conn, msgBytes); err != nil {
		return CommandResponse{Status: "ERROR", Message: "Failed to read message", Error: err.Error()}
	}
	message := string(msgBytes)

	// Read data length and data.
	dataLenBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, dataLenBytes); err != nil {
		return CommandResponse{Status: "ERROR", Message: "Failed to read data length", Error: err.Error()}
	}
	dataLen := protocol.ByteOrder.Uint32(dataLenBytes)
	dataBytes := make([]byte, dataLen)
	if dataLen > 0 {
		if _, err := io.ReadFull(conn, dataBytes); err != nil {
			return CommandResponse{Status: "ERROR", Message: "Failed to read data", Error: err.Error()}
		}
	}

	var decodedData any
	var decodeErr error

	// Process data based on command type.
	if dataLen > 0 {
		switch lastCmd {
		case "get", "collection item get", "collection list":
			// For single JSON values or plain JSON arrays.
			if err := json.Unmarshal(dataBytes, &decodedData); err != nil {
				decodeErr = fmt.Errorf("failed to unmarshal JSON data: %w", err)
				decodedData = string(dataBytes) // Fallback to raw string if unmarshal fails.
			}
		case "collection item list":
			// For map of Base64-encoded JSON values.
			var rawMap map[string]string
			if err := json.Unmarshal(dataBytes, &rawMap); err != nil {
				decodeErr = fmt.Errorf("failed to unmarshal raw map for item list: %w", err)
				decodedData = string(dataBytes) // Fallback to raw string.
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
				decodedData = decodedMap
			}
		default:
			// Default to raw string for other commands.
			decodedData = string(dataBytes)
		}
	}

	resp := CommandResponse{
		Status:  getStatusString(status),
		Message: message,
		Data:    decodedData,
	}
	if decodeErr != nil {
		resp.Error = decodeErr.Error()
	}
	return resp
}

// getStatusString converts a protocol.ResponseStatus to its string representation.
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
