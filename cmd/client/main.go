package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	log.SetFlags(0)

	usernamePtr := flag.String("u", "", "Username for authentication")
	passwordPtr := flag.String("p", "", "Password for authentication")
	flag.Parse()

	addr := "localhost:5876" // Default address
	if flag.NArg() > 0 {
		addr = flag.Arg(0)
	}

	// TLS Connection Configuration
	fmt.Println(colorInfo("Connecting to Memory Tools server at ", addr))
	caCert, err := os.ReadFile("certificates/server.crt")
	if err != nil {
		log.Fatal(colorErr("Failed to read server certificate 'certificates/server.crt': ", err))
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
		log.Fatal(colorErr("Failed to connect via TLS to %s: %v", addr, err))
	}
	defer conn.Close()

	fmt.Println(colorOK("√ Connected securely."))

	// Initialize and run the client
	client := newCLI(conn)
	if err := client.run(usernamePtr, passwordPtr); err != nil {
		log.Fatal(colorErr("Client error: %v", err))
	}
}
