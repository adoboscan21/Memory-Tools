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
	"sort"
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
// It is now a method of *cli to access the dynamic command list.
func (c *cli) getCommandAndRawArgs(input string) (string, string) {
	// Use the dynamically generated list of multi-word commands
	for _, mwCmd := range c.multiWordCommands {
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

	c.rl.Close()
	fmt.Println(colorInfo("Opening editor (%s) for JSON input. Save and close the file to continue...", editor))

	cmd := exec.Command(editor, tmpfile.Name())
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	runErr := cmd.Run()

	c.rl, err = readline.NewEx(c.rlConfig)
	if err != nil {
		return nil, fmt.Errorf("fatal: could not re-initialize readline: %w", err)
	}

	if runErr != nil {
		return nil, fmt.Errorf("error running editor: %w", runErr)
	}

	return os.ReadFile(tmpfile.Name())
}

// getJSONPayload is the method the compiler was missing.
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

// resolveCollectionName is the new simplified version that requires an explicit collection name.
func (c *cli) resolveCollectionName(args string, commandName string) (string, string, error) {
	parts := strings.Fields(args)
	if len(parts) == 0 {
		usage := fmt.Sprintf("usage: %s <collection_name> [other_args...]", commandName)
		return "", "", errors.New("no collection name provided. " + usage)
	}

	collectionName := parts[0]
	remainingArgs := strings.Join(parts[1:], " ")

	return collectionName, remainingArgs, nil
}

func (c *cli) readResponse(lastCmd string) error {
	status, msg, dataBytes, err := c.readRawResponse()
	if err != nil {
		return err
	}

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
			// Check for Base64 encoded data, common in 'get' commands for binary/JSON values
			if s, ok := tryDecodeBase64(dataBytes); ok {
				fmt.Printf("  %s %s\n", colorInfo("Data (Decoded):"), s)
			} else {
				fmt.Printf("  %s %s\n", colorInfo("Data (Raw):"), string(dataBytes))
			}
		}
	}
	fmt.Println("---")
	return nil
}

// tryDecodeBase64 is a helper for readResponse to handle potentially encoded data.
func tryDecodeBase64(data []byte) (string, bool) {
	decoded, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return "", false
	}
	// Attempt to pretty print if it's JSON
	var prettyJSON bytes.Buffer
	if stdjson.Indent(&prettyJSON, decoded, "  ", "  ") == nil {
		return prettyJSON.String(), true
	}
	// Otherwise return the decoded string
	return string(decoded), true
}

// printDynamicTable renders a slice of JSON objects as a formatted table.
func printDynamicTable(dataBytes []byte) error {
	var objectArrayResults []map[string]any
	if err := json.Unmarshal(dataBytes, &objectArrayResults); err == nil {
		if len(objectArrayResults) == 0 {
			fmt.Println("(No results)")
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
						jsonVal, _ := json.Marshal(v)
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

	var singleObjectResult map[string]any
	if err := json.Unmarshal(dataBytes, &singleObjectResult); err == nil {
		if len(singleObjectResult) == 0 {
			fmt.Println("(Empty object)")
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
				valStr = fmt.Sprintf("%v", v)
			}
			table.Append([]string{k, valStr})
		}
		table.Render()
		return nil
	}

	var simpleArrayResults []any
	if err := json.Unmarshal(dataBytes, &simpleArrayResults); err == nil {
		if len(simpleArrayResults) == 0 {
			fmt.Println("(Empty list)")
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
		var jsonValue interface{}
		if err := json.Unmarshal([]byte(value), &jsonValue); err != nil {
			jsonValue = value
		}
		jsonMap[key] = jsonValue
	}

	return json.Marshal(jsonMap)
}

// readRawResponse reads the raw components of a response from the server.
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
