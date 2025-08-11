// cmd/client/cli.go

package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/chzyer/readline"
)

// The command struct now includes a category for dynamic help.
type command struct {
	help     string
	handler  func(c *cli, args string) error
	category string
}

// The cli struct no longer contains 'currentCollection'.
// 'multiWordCommands' is added for dynamic parsing.
type cli struct {
	conn              net.Conn
	rl                *readline.Instance
	rlConfig          *readline.Config
	isAuthenticated   bool
	currentUser       string
	commands          map[string]command
	multiWordCommands []string // Dynamically generated list
	connMutex         sync.Mutex
}

// newCLI now dynamically generates the list of multi-word commands.
func newCLI(conn net.Conn) *cli {
	c := &cli{
		conn: conn,
	}
	c.commands = c.getCommands()

	// Dynamically generate the list of multi-word commands
	var mwCmds []string
	for cmd := range c.commands {
		if strings.Contains(cmd, " ") {
			mwCmds = append(mwCmds, cmd)
		}
	}
	// Sort from longest to shortest for correct matching
	sort.Slice(mwCmds, func(i, j int) bool {
		return len(mwCmds[i]) > len(mwCmds[j])
	})
	c.multiWordCommands = mwCmds

	return c
}

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

	if *user != "" && *pass != "" {
		fmt.Println(colorInfo("Attempting automatic login for user ", *user))
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

// The main loop is now much simpler. No aliases or contextual logic.
func (c *cli) mainLoop() error {
	for {
		var prompt string
		if c.isAuthenticated && c.currentUser != "" {
			prompt = c.currentUser + "> "
		} else {
			prompt = "> "
		}
		c.rl.SetPrompt(colorPrompt(prompt))

		input, err := c.rl.Readline()
		if err != nil {
			if errors.Is(err, readline.ErrInterrupt) {
				if len(input) == 0 {
					break
				}
				continue
			} else if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		cmd, args := c.getCommandAndRawArgs(input)

		handler, found := c.commands[cmd]

		if !found {

			fmt.Println(colorErr("Error: Unknown command. Type 'help' for commands: ", cmd))
			continue
		}

		if !c.isAuthenticated && cmd != "login" && cmd != "help" && cmd != "clear" && cmd != "exit" {
			fmt.Println(colorErr("Error: You must log in first. Use: login <username> <password>"))
			continue
		}

		startTime := time.Now()
		if err := handler.handler(c, args); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Println(colorErr("Command failed: ", err))
		}
		duration := time.Since(startTime)
		if cmd != "clear" && cmd != "help" {
			fmt.Println(colorInfo("Request time: ", duration.Round(time.Millisecond)))
		}
	}
	fmt.Println(colorInfo("\nExiting client. Goodbye!"))
	return nil
}
