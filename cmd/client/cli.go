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

// --- MODIFICADO ---
// La struct de comando ahora incluye una categoría para la ayuda dinámica.
type command struct {
	help     string
	handler  func(c *cli, args string) error
	category string
}

// --- MODIFICADO ---
// El struct cli ya no contiene 'currentCollection'.
// Se añade 'multiWordCommands' para el parseo dinámico.
type cli struct {
	conn              net.Conn
	rl                *readline.Instance
	rlConfig          *readline.Config
	isAuthenticated   bool
	currentUser       string
	commands          map[string]command
	multiWordCommands []string // Lista generada dinámicamente
	connMutex         sync.Mutex
}

// --- MODIFICADO ---
// newCLI ahora genera la lista de comandos de varias palabras dinámicamente.
func newCLI(conn net.Conn) *cli {
	c := &cli{
		conn: conn,
	}
	c.commands = c.getCommands()

	// Generar dinámicamente la lista de comandos de varias palabras
	var mwCmds []string
	for cmd := range c.commands {
		if strings.Contains(cmd, " ") {
			mwCmds = append(mwCmds, cmd)
		}
	}
	// Ordenar de más largo a más corto para un matching correcto
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

// --- MODIFICADO ---
// El bucle principal es ahora mucho más simple. No hay alias ni lógica contextual.
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

		// Se llama al nuevo método 'getCommandAndRawArgs' que está en el struct
		cmd, args := c.getCommandAndRawArgs(input)

		// --- ELIMINADO ---
		// Toda la lógica de alias contextuales y de 'currentCollection' ha sido eliminada.

		handler, found := c.commands[cmd]

		if !found {
			// --- CORREGIDO ---
			// Bug de Printf corregido para mostrar el comando desconocido correctamente.
			fmt.Println(colorErr("Error: Unknown command '%s'. Type 'help' for commands.", cmd))
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
