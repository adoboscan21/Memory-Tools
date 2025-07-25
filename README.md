## **Simple Key-Value Document API with Persistent Snapshots and Time-To-Live (TTL) in Go-Lang**

This project implements a basic REST API for storing and retrieving key-value pairs, where **values can be full JSON objects**. Data is kept in memory for fast access, supports individual item expiration (TTL), and is automatically persisted to a local binary file (`database.mtdb`) using configurable snapshots, ensuring no data loss upon application restarts.

---

## Features

- **In-Memory Document Storage:** Data is stored in a `map[string]struct { Value []byte; CreatedAt time.Time; TTL time.Duration }` for rapid access, allowing values to be arbitrary JSON documents.
    
- **Time-To-Live (TTL):** Individual key-value pairs can be set with an expiration time. Expired items are automatically removed from memory, optimizing resource usage.
    
- **Binary Data Persistence (`.mtdb`):** Data is automatically saved to `database.mtdb` in a highly efficient binary format. This file is loaded on startup, restoring the previous state (non-expired items only).
    
- **Configurable Snapshots:** The application can be configured to take periodic snapshots of the in-memory data at specified intervals (e.g., every 5 minutes). Only non-expired items are included in snapshots.
    
- **Atomic Writes:** Snapshots are saved to a temporary file first, then atomically renamed, ensuring data integrity even if the system crashes during a save operation.
    
- **REST API:** Two endpoints for `SET` (save) and `GET` (retrieve) operations.
    
- **Concurrency Handling:** Uses `sync.RWMutex` to ensure safe data access from multiple concurrent requests.
    
- **Go Modules:** Modular project structure for better organization and maintainability.
    
- **Graceful Shutdown:** The application shuts down cleanly, saving the latest non-expired data, stopping scheduled snapshots and the TTL cleaner, and closing HTTP connections safely.
    
- **Framework-less:** Built purely with Go's standard library for full control and low overhead.
    

---

## Project Structure

```
memory-tools/
├── main.go                       # Main entry point and application orchestration.
├── api/                          # HTTP endpoint handlers.
│   └── handlers.go
├── store/                        # In-memory data storage logic, including TTL management.
│   └── inmem.go
├── persistence/                  # Logic for persisting data to disk (binary file storage) and snapshot management.
│   └── binary_storage.go
├── config/                       # Application configuration definition and loading.
│   └── config.go                 # Defines Config struct and functions to load from JSON.
└── go.mod                        # Go module file.
└── go.sum                        # Dependency checksums.
```

---

## How to Run

Make sure you have [Go installed (version go1.21 or higher)](https://go.dev/doc/install).

1. **Clone the repository** (or create the files and folders manually as described in the structure).
    
2. **Create a `config.json` file** in the project root with your desired settings (see example below).
    
3. Navigate to the **project root** in your terminal:
      
    ```bash
    go run .
    ```
    
    You'll see messages in the console indicating the server is listening on the configured port (default `:8080`), that scheduled snapshots are enabled, and the TTL cleaner is starting (if configured).
    
    You can also specify a custom config file path:
    
    ```bash
    go run . --config=my_custom_config.json
    ```
    
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

---

## How to Compile

Make sure you have [Go installed (version go1.21 or higher)](https://go.dev/doc/install).

1. **Clone the repository** (or create the files and folders manually as described in the structure).
    
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
    ./memory-tools --config=my_custom_config.json
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
|`value`|`JSON Object/Array` (represented as `json.RawMessage`)|The value associated with the key, expected to be a valid JSON object or array.|Yes|`{"name": "Alice", "age": 30, "details": {"city": "Wonderland"}}`|
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

During runtime, the application will periodically save the current in-memory data to the `database.mtdb` file. This process is managed by a **Snapshot Manager** and occurs at a configurable interval (e.g., every 5 minutes by default). This ensures that even if the application crashes unexpectedly, your data loss is limited to the period since the last snapshot. **Only non-expired items are saved during snapshots.**

Additionally, a **TTL Cleaner** runs in the background at a configurable interval (e.g., every 1 minute by default). This cleaner physically removes expired items from the in-memory store, freeing up memory.

When the application receives a termination signal (e.g., `Ctrl+C` in the terminal or a `SIGTERM` signal in a server/container environment), both the Snapshot Manager and the TTL Cleaner are stopped, and a **final snapshot** of the current non-expired in-memory data is taken and saved to `database.mtdb` before the application exits. This guarantees the freshest possible data state upon shutdown.

You can configure the snapshot and TTL cleaner behavior using the `config.json` file.

---
### Technologies Used

- **Go (Golang):** The primary programming language.
    
- **Go Standard Library:** `net/http`, `sync`, `encoding/json`, `encoding/binary`, `os`, `os/signal`, `syscall`, `log`, `context`, `time`, `maps`, `io`, `flag`.