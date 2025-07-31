# Memory Tools 🚀

Memory Tools is a high-performance, sharded in-memory key-value store designed for speed, security, and scalability. It provides a robust, encrypted backend for your applications, supporting flexible data organization and complex query operations.

---

## ✨ Features

- **Sharded In-Memory Storage:** Uses an efficient sharding design to distribute data and ensure fast, concurrent access.
- **Time-to-Live (TTL):** Assigns a time-to-live to keys so they expire automatically, ideal for cache data or sessions.
- **Data Persistence:** Data is saved to disk in an optimized binary format and reloaded on startup, ensuring nothing is lost.
- **Collections:** Organizes your data into named collections, acting as separate databases within the same engine.
- **Secure TLS Encryption:** All communications over the TCP interface are encrypted with TLS, guaranteeing the security of your data in transit.
- **Advanced Query Engine:** Performs complex queries on your collections with filters (`WHERE`), ordering (`ORDER BY`), pagination (`LIMIT/OFFSET`), and powerful aggregations (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) on JSON data.
- **Batch Operations:** Operates with multiple items at once, such as `SET_COLLECTION_ITEMS_MANY` and `DELETE_COLLECTION_ITEMS_MANY`, for greater efficiency.
- **Graceful Shutdown:** The engine handles a clean application shutdown, saving all data before exiting to prevent data corruption or loss.
- **Docker-Ready:** Easily deploy and manage the server with Docker and Docker Compose.

---

## ⚙️ Quick Start with Docker Compose

To get the Memory Tools server up and running quickly, follow these steps:

1.  **Build the Docker Image:**

    ```bash
    docker-compose build
    ```

2.  **Start the Services:**

    ```bash
    docker-compose up -d
    ```

    This starts the main database server on port `8080`.

---

## 🛠️ Manual Installation and Build

### Prerequisites

You need **Go version 1.24.4 or higher** to build and run this project.

### 1. Generate TLS Certificates 🔒

Memory Tools uses TLS for all its communications. You must generate a self-signed certificate pair and place it in the `./certificates/` directory.

1.  **Create the directory:**

    ```bash
    mkdir -p certificates
    ```

2.  **Run the following OpenSSL command to generate a certificate and key:**

    ```bash
    openssl req -x509 -newkey rsa:4096 -nodes -keyout certificates/server.key -out certificates/server.crt -days 36500 -subj "/CN=localhost" -addext "subjectAltName = DNS:localhost,IP:127.0.0.1"
    ```

### 2. Build and Run

- **Build the Database Server:**

  ```bash
  go build .
  ```

- **Build the CLI Client:**

  ```bash
  go build ./cmd/client/main.go
  ```

- **Run the Server Directly:**

  ```bash
  go run .
  ```

---

## 🖥️ CLI Client

You can use the interactive CLI client to connect and operate with the server.

- **To connect to the server running in Docker:**

  ```bash
  docker exec -it <container-id> memory-tools-client
  ```

- **For a direct and authenticated connection:**

  ```bash
  ./memory-tools-client -u admin -p adminpass localhost:8080
  ```

> **Important:** The default password for the `admin` user is `adminpass`, and for the `root` user (only accessible from localhost) is `rootpass`. Please change these immediately in a production environment.

For a full list of commands and detailed examples, see the [`docs/client.md`](./docs/client.md) file.

Once connected, type `help` for available commands.
