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
└── go.sum                        # Dependency checksums.
```

---

## How to Run

Make sure you have [Go installed (version go1.21 or higher)](https://go.dev/doc/install).

1. **Clone the repository:**
    
    ```bash
    git clone https://github.com/adoboscan21/Memory-Tools.git
    cd memory-tools
    ```
    
2. **Create a `config.json` file** in the project root with your desired settings. This file specifies how your application behaves (e.g., ports, timeouts, intervals). If omitted or not found, default values will be used.
    
    ### Example `config.json`

    
    ```json
    {
      "port": ":8080",
      "read_timeout": "5s",
      "write_timeout": "10s",
      "idle_timeout": "120s",
      "shutdown_timeout": "10s",
      "snapshot_interval": "5m",
      "enable_snapshots": true,
      "ttl_clean_interval": "1m"
    }
    ```
    
3. **Run the application:**
    
    ```bash
    go run .
    ```
    
    You'll see messages in the console indicating the server is listening on the configured port (default `:8080`), that scheduled snapshots are enabled, and the TTL cleaner is starting (if configured).
    
    You can also specify a custom config file path:
    
    ```bash
    go run . --config=./path/to/your_custom_config.json
    ```
    

---

## How to Compile

Make sure you have [Go installed (version go1.21 or higher)](https://go.dev/doc/install).

1. **Clone the repository** (as described in "How to Run").
    
2. Navigate to the **project root** in your terminal:
    
    ```bash
    go build .
    ```
    
    This will create an executable binary (e.g., `memory-tools` on Linux/macOS, `memory-tools.exe` on Windows) in your project root. You can then run it directly:
    
    ```bash
    ./memory-tools # or memory-tools.exe on Windows
    ```
    
    You can also pass the config flag to the compiled binary:
    
    ```bash
    ./memory-tools --config=./path/to/your_custom_config.json
    ```
    

---

## API Documentation

The API exposes two main endpoints for key-value interaction.

### Base URL

`http://localhost:8080` (or the port specified in `config.json`)

---

### 1. `POST /set`

Saves a key-value pair to the data store. If the key already exists, its value and TTL will be updated. The `value` field is expected to be a valid JSON document.

- **HTTP Method:** `POST`
    
- **Content-Type:** `application/json`
    

#### **Request Body Parameters (JSON):**

|Name|Type|Description|Required|Example|
|---|---|---|---|---|
|`key`|`string`|The unique key for the data.|Yes|`"user_profile"`|
|`value`|`JSON Object/Array` (represented as `stdjson.RawMessage`)|The value associated with the key, expected to be a valid JSON object or array.|Yes|`{"name": "Alice", "age": 30, "details": {"city": "Wonderland"}}`|
|`ttl_seconds`|`integer`|**(Optional)** Time-To-Live for the item in seconds. If `0` or omitted, the item never expires.|No|`600` (for 10 minutes), `3600` (for 1 hour), `5` (for 5 seconds, for testing expiration)|

#### **Example Request (using `curl`):**

**Example 1: Setting a key with a 10-minute TTL**

```bash
curl -X POST \
     -H "Content-Type: application/json" \
     -d '{
           "key": "user_session:abc123",
           "value": {
             "user_id": 101,
             "last_activity": "2025-07-25T09:30:00Z",
             "status": "active"
           },
           "ttl_seconds": 600
         }' \
     http://localhost:8080/set
```

**Example 2: Setting a key that never expires (omitting `ttl_seconds`)**

```bash
curl -X POST \
     -H "Content-Type: application/json" \
     -d '{
           "key": "product_categories",
           "value": [
             {"id": 1, "name": "Electronics"},
             {"id": 2, "name": "Books"}
           ]
         }' \
     http://localhost:8080/set
```

#### **Responses:**

- **`200 OK`**
    
    - **Description:** The key-value pair was successfully saved or updated.
        
    - **Response Body:** Plain text, e.g.: `Data saved: Key='user_session:abc123'`
        
- **`400 Bad Request`**
    
    - **Description:** Invalid JSON request body, `key` is empty, `value` is empty (not `{}` or `[]`), or `value` is not a valid JSON document (e.g., plain text).
        
    - **Example:** `Invalid JSON request body or unknown fields`, `Key cannot be empty`, `Value cannot be empty (e.g., use {} or [])`, or `'value' field must be a valid JSON document`.
        
- **`405 Method Not Allowed`**
    
    - **Description:** An HTTP method other than `POST` was attempted.
        

---

### 2. `GET /get`

Retrieves the value associated with a specific key. The returned value will be the stored JSON document. If the item has expired, it will be treated as not found.

- **HTTP Method:** `GET`
    

#### **URL Query Parameters:**

|Name|Type|Description|Required|Example|
|---|---|---|---|---|
|`key`|`string`|The key of the data to retrieve.|Yes|`user_session:abc123`|

#### **Example Request (using `curl`):**

```bash
curl "http://localhost:8080/get?key=user_session:abc123"
```

#### **Responses:**

- **`200 OK`**
    
    - **Description:** The key was found, and its JSON value is returned.
        
    - **Content-Type:** `application/json`
        
    - **Response Body (JSON):**
        
        ```json
        {
            "key": "user_session:abc123",
            "value": {
              "user_id": 101,
              "last_activity": "2025-07-25T09:30:00Z",
              "status": "active"
            }
        }
        ```
        
- **`400 Bad Request`**
    
    - **Description:** The `key` parameter is missing from the URL.
        
    - **Example:** `'key' query parameter is required`
        
- **`404 Not Found`**
    
    - **Description:** The requested key was not found in the data store **or it has expired**.
        
    - **Example:** `Key 'temp_cache_item' not found or expired`
        
- **`405 Method Not Allowed`**
    
    - **Description:** An HTTP method other than `GET` was attempted.
        

---

### Data Persistence and Snapshots

When the application starts, it attempts to load existing data from `database.mtdb`. If the file doesn't exist, it starts with an empty store. Only non-expired items are included in the loaded data.

During runtime, the application will periodically save the current in-memory data to the `database.mtdb` file. This process is managed by a **Snapshot Manager** and occurs at a configurable interval. This ensures that even if the application crashes unexpectedly, your data loss is limited to the period since the last snapshot. **Only non-expired items are saved during snapshots.**

Additionally, a **TTL Cleaner** runs in the background at a configurable interval. This cleaner physically removes expired items from the in-memory store, freeing up memory.

When the application receives a termination signal (e.g., `Ctrl+C` in the terminal or a `SIGTERM` signal in a server/container environment), both the Snapshot Manager and the TTL Cleaner are stopped, and a **final snapshot** of the current non-expired in-memory data is taken and saved to `database.mtdb` before the application exits. This guarantees the freshest possible data state upon shutdown.

You can configure the snapshot and TTL cleaner behavior using the `config.json` file.

---

### Technologies Used

- **Go (Golang):** The primary programming language.
    
- **Go Standard Library:** `net/http`, `sync`, `encoding/json` (aliased as `stdjson` for `RawMessage`), `encoding/binary`, `os`, `os/signal`, `syscall`, `log`, `context`, `time`, `maps`, `io`, `flag`, `hash/fnv`.
    
- **External Library:** `github.com/json-iterator/go` (for high-performance JSON processing).