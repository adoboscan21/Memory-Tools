package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/chzyer/readline"
)

// command represents an action the client can execute.
type command struct {
	help    string
	handler func(c *cli, args string) error
}

// cli holds the client's state and dependencies.
type cli struct {
	conn              net.Conn
	rl                *readline.Instance
	rlConfig          *readline.Config // Store the config to re-initialize
	isAuthenticated   bool
	currentUser       string
	currentCollection string
	commands          map[string]command
	connMutex         sync.Mutex // Mutex to protect the connection during autocompletion
}

// newCLI creates a new client instance.
func newCLI(conn net.Conn) *cli {
	c := &cli{
		conn: conn,
	}
	c.commands = c.getCommands()
	return c
}

// run initializes readline and the client's main loop.
func (c *cli) run(user, pass *string) error {
	c.rlConfig = &readline.Config{
		Prompt:          "> ",
		HistoryFile:     "/tmp/readline_history.tmp",
		AutoComplete:    c.getCompleter(),
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	}

	var err error
	c.rl, err = readline.NewEx(c.rlConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize readline: %w", err)
	}
	defer c.rl.Close()

	// Automatic authentication if flags are provided
	if *user != "" && *pass != "" {
		fmt.Println(colorInfo("Attempting automatic login for user '%s'...", *user))
		if err := c.handleLogin(fmt.Sprintf("%s %s", *user, *pass)); err != nil {
			fmt.Println(colorErr("Automatic login failed. Please login manually."))
			os.Exit(1)
		}
	}

	if !c.isAuthenticated {
		fmt.Println(colorInfo("Please login using: login <username> <password>"))
	}

	return c.mainLoop()
}

// mainLoop is the client's heart, reading and processing commands.
func (c *cli) mainLoop() error {
	// Defines the mapping from short, contextual aliases to their full command names.
	contextualAliases := map[string]string{
		"set":    "collection item set",
		"get":    "collection item get",
		"delete": "collection item delete",
		"update": "collection item update",
		"list":   "collection item list",
		"query":  "collection query",
	}

	for {
		var promptParts []string
		if c.isAuthenticated && c.currentUser != "" {
			promptParts = append(promptParts, c.currentUser)
		}
		if c.currentCollection != "" {
			promptParts = append(promptParts, c.currentCollection)
		}

		var finalPrompt string
		if len(promptParts) > 0 {
			finalPrompt = strings.Join(promptParts, "/") + "> "
		} else {
			finalPrompt = "> "
		}
		c.rl.SetPrompt(colorPrompt(finalPrompt))

		// Read user input.
		input, err := c.rl.Readline()
		if err != nil {
			if errors.Is(err, readline.ErrInterrupt) {
				if len(input) == 0 {
					break // Exit with Ctrl+C on an empty line.
				}
				continue
			} else if errors.Is(err, io.EOF) {
				break // Exit with Ctrl+D.
			}
			return err
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		cmd, args := getCommandAndRawArgs(input)

		// Prioritize resolving contextual aliases when a collection is in use.
		if c.currentCollection != "" {
			var resolvedCmd string // This will hold the full command name if an alias is found.
			isContextual := false

			if cmd == "index" {
				parts := strings.Fields(args)
				if len(parts) > 0 {
					// Attempt to construct the full command, e.g., "collection index create".
					potentialFullCmd := fmt.Sprintf("collection index %s", parts[0])
					if _, found := c.commands[potentialFullCmd]; found {
						resolvedCmd = potentialFullCmd
						args = strings.Join(parts[1:], " ") // Update args to exclude the subcommand.
						isContextual = true
					}
				}
			} else {
				// Check for other direct aliases like "get", "list", etc.
				if potentialFullCmd, isAlias := contextualAliases[cmd]; isAlias {
					if _, found := c.commands[potentialFullCmd]; found {
						resolvedCmd = potentialFullCmd
						// 'args' is already correct from the initial split.
						isContextual = true
					}
				}
			}

			// If a contextual alias was successfully resolved, overwrite the original command.
			if isContextual {
				cmd = resolvedCmd
			}
		}

		// Find the handler for the (potentially resolved) command.
		handler, found := c.commands[cmd]

		if !found {
			// This was corrected in a previous step, ensure it uses Printf
			fmt.Printf("%s", colorErr("Error: Unknown command. Type 'help' for commands.\n", cmd))
			continue
		}

		// Check for authentication before executing protected commands.
		if !c.isAuthenticated && cmd != "login" && cmd != "help" && cmd != "clear" && cmd != "exit" {
			fmt.Println(colorErr("Error: You must log in first. Use: login <username> <password>"))
			continue
		}

		// === START: TIMER IMPLEMENTATION ===

		// Start the timer right before executing the command.
		startTime := time.Now()

		// Execute the command handler.
		if err := handler.handler(c, args); err != nil {
			// Check if the error is the specific signal to exit the loop.
			if errors.Is(err, io.EOF) {
				break // Exit the loop gracefully.
			}
			// For all other errors, just print them.
			fmt.Println(colorErr("Command failed: ", err))
		}

		// Calculate the duration and print it.
		duration := time.Since(startTime)
		// Only show the timer for commands that are not purely local and instantaneous.
		if cmd != "clear" && cmd != "help" {
			fmt.Println(colorInfo("Request time: ", duration.Round(time.Millisecond)))
		}

		// === END: TIMER IMPLEMENTATION ===
	}
	fmt.Println(colorInfo("\nExiting client. Goodbye!"))
	return nil
}
