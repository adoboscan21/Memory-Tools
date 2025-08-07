// cmd/client/utils.go

package main

import (
	"bytes"
	"encoding/base64"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"memory-tools/internal/protocol"
	"os"
	"os/exec"
	"runtime"
	"sort" // <-- IMPORT ADDED
	"strings"

	"github.com/chzyer/readline"
	"github.com/fatih/color"
	jsoniter "github.com/json-iterator/go"
	"github.com/olekukonko/tablewriter"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// Color definitions for the interface
var (
	colorOK     = color.New(color.FgGreen, color.Bold).SprintFunc()
	colorErr    = color.New(color.FgRed, color.Bold).SprintFunc()
	colorPrompt = color.New(color.FgMagenta).SprintFunc()
	colorInfo   = color.New(color.FgBlue).SprintFunc()
)

// getCommandAndRawArgs parses user input into a command and its arguments.
func getCommandAndRawArgs(input string) (string, string) {
	// List of multi-word commands, from longest to shortest.
	multiWordCommands := []string{
		"collection item update many", "collection item set many", "collection item delete many",
		"collection item update", "collection item set", "collection item get",
		"collection item delete", "collection item list", "collection index create",
		"collection index delete", "collection index list", "collection create",
		"collection delete", "collection list", "collection query", "user create",
		"user update", "user delete", "update password",
	}

	for _, mwCmd := range multiWordCommands {
		if strings.HasPrefix(input, mwCmd+" ") || input == mwCmd {
			return mwCmd, strings.TrimSpace(input[len(mwCmd):])
		}
	}

	parts := strings.SplitN(input, " ", 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

// clearScreen clears the terminal screen.
func clearScreen() {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("cmd", "/c", "cls")
	default:
		cmd = exec.Command("clear")
	}
	cmd.Stdout = os.Stdout
	_ = cmd.Run()
}

// getStatusString converts a ResponseStatus to a human-readable string.
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

func (c *cli) getJSONFromEditor() ([]byte, error) {
	editor := os.Getenv("EDITOR")
	if editor == "" {
		if runtime.GOOS == "windows" {
			editor = "notepad"
		} else {
			editor = "vim"
		}
	}

	tmpfile, err := os.CreateTemp("", "memory-tools-*.json")
	if err != nil {
		return nil, fmt.Errorf("could not create temp file: %w", err)
	}
	defer os.Remove(tmpfile.Name())

	// Close readline to give terminal control to the editor
	c.rl.Close()

	fmt.Println(colorInfo("Opening editor (%s) for JSON input. Save and close the file to continue...", editor))

	cmd := exec.Command(editor, tmpfile.Name())
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	runErr := cmd.Run()

	// IMPORTANT: Re-initialize readline after the editor is closed.
	c.rl, err = readline.NewEx(c.rlConfig)
	if err != nil {
		return nil, fmt.Errorf("fatal: could not re-initialize readline: %w", err)
	}

	if runErr != nil {
		return nil, fmt.Errorf("error running editor: %w", runErr)
	}

	return os.ReadFile(tmpfile.Name())
}

func (c *cli) getJSONPayload(payload string) ([]byte, error) {
	if payload == "-" {
		return c.getJSONFromEditor()
	}
	if strings.HasPrefix(payload, "file:") {
		filePath := strings.TrimPrefix(payload, "file:")
		return os.ReadFile(filePath)
	}
	return []byte(payload), nil
}

func (c *cli) resolveCollectionName(args string) (string, string, error) {
	parts := strings.Fields(args)
	if len(parts) > 0 &&
		!strings.HasPrefix(parts[0], "{") &&
		!strings.HasPrefix(parts[0], "[") &&
		!strings.HasPrefix(parts[0], "file:") &&
		parts[0] != "-" {
		return parts[0], strings.Join(parts[1:], " "), nil
	}

	if c.currentCollection != "" {

		return c.currentCollection, args, nil
	}
	return "", "", errors.New("no collection name provided and no collection is in use. Use 'use <collection_name>' or specify it in the command")
}

func (c *cli) readResponse(lastCmd string) error {
	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	statusByte := make([]byte, 1)
	if _, err := io.ReadFull(c.conn, statusByte); err != nil {
		return fmt.Errorf("failed to read response status from server: %w", err)
	}
	status := protocol.ResponseStatus(statusByte[0])

	msg, err := protocol.ReadString(c.conn)
	if err != nil {
		return fmt.Errorf("failed to read response message from server: %w", err)
	}

	dataBytes, err := protocol.ReadBytes(c.conn)
	if err != nil {
		return fmt.Errorf("failed to read response data from server: %w", err)
	}

	// Estandarizar la salida
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Status", "Message"})
	table.Append([]string{getStatusString(status), msg})
	table.Render()

	if len(dataBytes) == 0 {
		fmt.Println("---")
		return nil
	}

	switch lastCmd {
	case "collection list", "collection index list", "collection item list", "collection query":
		if err := printDynamicTable(dataBytes); err != nil {
			fmt.Println(colorErr("Could not render table, falling back to JSON view."))
			var prettyJSON bytes.Buffer
			if err := stdjson.Indent(&prettyJSON, dataBytes, "  ", "  "); err == nil {
				fmt.Printf("  %s\n%s\n", colorInfo("Data:"), prettyJSON.String())
			} else {
				fmt.Printf("  %s %s\n", colorInfo("Data (Raw):"), string(dataBytes))
			}
		}
	default:
		var prettyJSON bytes.Buffer
		if err := stdjson.Indent(&prettyJSON, dataBytes, "  ", "  "); err == nil {
			fmt.Printf("  %s\n%s\n", colorInfo("Data:"), prettyJSON.String())
		} else {
			fmt.Printf("  %s %s\n", colorInfo("Data (Raw):"), string(dataBytes))
		}
	}
	fmt.Println("---")
	return nil
}

// ---- NEW FUNCTION ADDED ----
// printDynamicTable attempts to render a slice of JSON objects as a formatted table.
func printDynamicTable(dataBytes []byte) error {
	// Attempt 1: Try to unmarshal as an array of objects (multi-column table).
	var objectArrayResults []map[string]any
	if err := json.Unmarshal(dataBytes, &objectArrayResults); err == nil {
		if len(objectArrayResults) == 0 {
			return nil
		}
		headerSet := make(map[string]bool)
		for _, doc := range objectArrayResults {
			for key := range doc {
				headerSet[key] = true
			}
		}
		headers := make([]string, 0, len(headerSet))
		for key := range headerSet {
			headers = append(headers, key)
		}
		sort.Strings(headers)
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader(headers)
		table.SetAutoWrapText(false)
		for _, doc := range objectArrayResults {
			row := make([]string, len(headers))
			for i, header := range headers {
				if val, ok := doc[header]; ok {
					var valStr string
					switch v := val.(type) {
					case map[string]any, []any:
						jsonVal, _ := json.MarshalIndent(v, "", "  ")
						valStr = string(jsonVal)
					case nil:
						valStr = "(nil)"
					default:
						valStr = fmt.Sprintf("%v", v)
					}
					row[i] = valStr
				} else {
					row[i] = "(n/a)"
				}
			}
			table.Append(row)
		}
		table.Render()
		return nil
	}

	// Attempt 2: If that failed, try as a single object (Key-Value table).
	var singleObjectResult map[string]any
	if err := json.Unmarshal(dataBytes, &singleObjectResult); err == nil {
		if len(singleObjectResult) == 0 {
			return nil
		}
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Key", "Value"})
		table.SetAutoWrapText(false)

		keys := make([]string, 0, len(singleObjectResult))
		for k := range singleObjectResult {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			val := singleObjectResult[k]
			var valStr string
			switch v := val.(type) {
			case map[string]any, []any:
				jsonVal, _ := json.MarshalIndent(v, "", "  ")
				valStr = string(jsonVal)
			case nil:
				valStr = "(nil)"
			default:
				// === START OF FIX ===
				// The value is likely a string. Check if it's Base64 encoded JSON.
				valStr = fmt.Sprintf("%v", v) // Default to the raw string
				if s, ok := v.(string); ok {
					// Attempt to decode from Base64
					if decodedBytes, err := base64.StdEncoding.DecodeString(s); err == nil {
						// If decoding succeeds, it might be JSON. Try to pretty-print it.
						var prettyJSON bytes.Buffer
						if stdjson.Indent(&prettyJSON, decodedBytes, "", "  ") == nil {
							valStr = prettyJSON.String()
						} else {
							// It was Base64 but not JSON, so show the decoded string.
							valStr = string(decodedBytes)
						}
					}
					// If Base64 decoding fails, we just keep the original string.
				}
				// === END OF FIX ===
			}
			table.Append([]string{k, valStr})
		}
		table.Render()
		return nil
	}

	// Attempt 3: If that also failed, try as an array of simple values (single-column table).
	var simpleArrayResults []any
	if err := json.Unmarshal(dataBytes, &simpleArrayResults); err == nil {
		if len(simpleArrayResults) == 0 {
			return nil
		}
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Value"})
		for _, item := range simpleArrayResults {
			table.Append([]string{fmt.Sprintf("%v", item)})
		}
		table.Render()
		return nil
	}

	// Fallback: If all attempts fail, return an error to trigger pretty JSON printing.
	var initialErr error
	_ = json.Unmarshal(dataBytes, &objectArrayResults)
	return initialErr
}

func (c *cli) getInteractiveJSONPayload() ([]byte, error) {
	fmt.Println(colorInfo("Enter JSON key-value pairs (e.g., key=value). Type 'done' to finish or 'cancel' to abort."))
	var pairs []string
	for {
		c.rl.SetPrompt(colorPrompt("JSON> "))
		input, err := c.rl.Readline()
		if err != nil {
			return nil, err
		}
		input = strings.TrimSpace(input)
		if input == "done" {
			break
		}
		if input == "cancel" {
			return nil, errors.New("JSON input cancelled")
		}
		if input != "" {
			pairs = append(pairs, input)
		}
	}

	if len(pairs) == 0 {
		return nil, errors.New("no JSON data provided")
	}

	jsonMap := make(map[string]interface{})
	for _, pair := range pairs {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid pair format: %s (use key=value)", pair)
		}
		key, value := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		// Intenta parsear el valor como JSON; si falla, trata como string
		var jsonValue interface{}
		if err := json.Unmarshal([]byte(value), &jsonValue); err != nil {
			jsonValue = value
		}
		jsonMap[key] = jsonValue
	}

	return json.Marshal(jsonMap)
}

// readRawResponse reads the raw components of a response from the server.
// This is a helper to avoid code duplication in readResponse and handleLogin.
func (c *cli) readRawResponse() (protocol.ResponseStatus, string, []byte, error) {
	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	statusByte := make([]byte, 1)
	if _, err := io.ReadFull(c.conn, statusByte); err != nil {
		return 0, "", nil, fmt.Errorf("failed to read response status from server: %w", err)
	}
	status := protocol.ResponseStatus(statusByte[0])

	msg, err := protocol.ReadString(c.conn)
	if err != nil {
		return status, "", nil, fmt.Errorf("failed to read response message from server: %w", err)
	}

	dataBytes, err := protocol.ReadBytes(c.conn)
	if err != nil {
		return status, msg, nil, fmt.Errorf("failed to read response data from server: %w", err)
	}

	return status, msg, dataBytes, nil
}
