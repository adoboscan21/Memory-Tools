# Memory Tools: A High-Performance Key-Value Document API with Persistent Snapshots and TTL in Go

This project implements a basic REST API for storing and retrieving key-value pairs, where **values can be full JSON objects**. Designed for speed and efficiency, it keeps data in memory, supports individual item expiration (Time-To-Live), and automatically persists its state to a local binary file using configurable snapshots, ensuring data durability across application restarts.

---

## Features

- **High-Performance In-Memory Document Storage:** Data is stored in a **sharded map** (`[]*store.Shard` where each `Shard` contains a `map[string]store.Item` and its own `sync.RWMutex`) for rapid, concurrent access. This architecture significantly reduces mutex contention, especially for write-heavy workloads, allowing values to be arbitrary JSON documents.
- **Time-To-Live (TTL):** Individual key-value pairs can be set with an expiration time. A dedicated background cleaner automatically removes expired items from memory, efficiently managing resource usage.
- **Optimized Binary Data Persistence (`database.mtdb`):** Data is automatically saved to `database.mtdb` in a highly efficient binary format. This file is loaded on startup, restoring the previous state (only non-expired items are persisted and loaded).
- **Configurable Snapshots:** The application can be configured to take periodic snapshots of the in-memory data at specified intervals. Only non-expired items are included in these snapshots, ensuring clean data persistence.
- **Atomic Writes:** Snapshots are saved to a temporary file first, then atomically renamed, guaranteeing data integrity even if the system crashes during a save operation.
- **High-Performance JSON Handling:** Leverages the `json-iterator/go` library for faster JSON serialization and deserialization, minimizing CPU overhead and GC pressure per request.
- **REST API:** Provides simple HTTP endpoints for `SET` (save) and `GET` (retrieve) operations.
- **Advanced Concurrency Handling:** Utilizes `sync.RWMutex` per shard to ensure safe and highly concurrent data access from multiple requests, optimizing for parallel read/write operations.
- **Modular Go Design:** Structured with Go modules, promoting clear separation of concerns, maintainability, and testability.
- **Graceful Shutdown:** The application shuts down cleanly, ensuring the latest non-expired data is saved, scheduled snapshots and the TTL cleaner are stopped, and HTTP connections are closed safely.
- **Framework-less Core:** Built primarily with Go's standard library, offering full control and low overhead, with a minimal, high-performance external dependency for JSON processing.
- **Configurable via JSON:** Application settings (ports, timeouts, snapshot/TTL intervals) are loaded from an external JSON file, allowing flexible deployment without recompilation.

---

## Project Structure

```
memory-tools/
├── main.go                       # Main entry point and application orchestration.
├── api/                          # HTTP endpoint handlers, using json-iterator.
│   └── handlers.go
├── store/                        # Sharded in-memory data storage logic, including TTL management.
│   └── inmem.go
├── persistence/                  # Logic for persisting data to disk (binary file storage) and snapshot management.
│   └── binary_storage.go
├── config/                       # Application configuration definition and loading from JSON.
│   └── config.go
├── go.mod                        # Go module file.
├── go.sum                        # Dependency checksums.
├── .dockerignore                 # Docker ignore file
├── Dockerfile                    # Docker build file
├── docker-compose.yml            # Docker compose file.
└── docs/                         # Additional documentation.
├── running.md                # How to run, build, and deploy.
└── api.md                    # Detailed API documentation.
```

---

## Getting Started

To get started with Memory Tools, refer to the following documentation:

- **[Running the Application](docs/running.md)**: Instructions on how to build, run, and deploy the application (including Docker).
- **[API Documentation](docs/api.md)**: Detailed information about the available REST API endpoints and their usage.

---

### Technologies Used

- **Go (Golang):** The primary programming language.
- **Go Standard Library:** `net/http`, `sync`, `encoding/json` (aliased as `stdjson` for `RawMessage`), `encoding/binary`, `os`, `os/signal`, `syscall`, `log`, `context`, `time`, `maps`, `io`, `flag`, `hash/fnv`.
- **External Library:** `github.com/json-iterator/go` (for high-performance JSON processing).
