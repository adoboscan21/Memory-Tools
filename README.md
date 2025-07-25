This project implements a basic REST API for storing and retrieving key-value pairs, where **values can be full JSON objects**. Data is kept in memory for fast access and is automatically persisted to a local binary file (`data.mtdb`) using configurable snapshots, ensuring no data loss upon application restarts.

---

## Features

- **In-Memory Document Storage:** Data is stored in a `map[string][]byte` for rapid access, allowing values to be arbitrary JSON documents.
    
- **Binary Data Persistence (.mtdb):** Data is automatically saved to `data.mtdb` in a highly efficient binary format. This file is loaded on startup, restoring the previous state.
    
- **Configurable Snapshots:** The application can be configured to take periodic snapshots of the in-memory data at specified intervals (e.g., every 5 minutes).
    
- **Atomic Writes:** Snapshots are saved to a temporary file first, then atomically renamed, ensuring data integrity even if the system crashes during a save operation.
    
- **REST API:** Two endpoints for `SET` (save) and `GET` (retrieve) operations.
    
- **Concurrency Handling:** Uses `sync.RWMutex` to ensure safe data access from multiple concurrent requests.
    
- **Go Modules:** Modular project structure for better organization and maintainability.
    
- **Graceful Shutdown:** The application shuts down cleanly, saving the latest data, stopping scheduled snapshots, and closing HTTP connections safely.
    
- **Framework-less:** Built purely with Go's standard library for full control and low overhead.
    

---

## Project Structure

```
my-rest-api/
├── main.go                       # Main entry point, server, and snapshot configuration.
├── api/                          # HTTP endpoint handlers.
│   └── handlers.go
├── store/                        # In-memory data storage logic.
│   └── inmem.go
├── persistence/                  # Logic for persisting data to disk (binary file storage) and snapshot management.
│   └── binary_storage.go
└── go.mod                        # Go module file.
└── go.sum                        # Dependency checksums.
```

---

## How to Run

Make sure you have [Go installed (version go1.21 or higher)](https://go.dev/doc/install).

1. **Clone the repository** (or create the files and folders manually as described in the structure).
    
2. Navigate to the **project root** in your terminal:
        
    ```bash
    go run .
    ```
    
    You'll see messages in the console indicating the server is listening on port 8080 and that scheduled snapshots are enabled (if configured).
    

---

## API Documentation

The API exposes two main endpoints for key-value interaction.

### Base URL

`http://localhost:8080`

---

### 1. `POST /set`

Saves a key-value pair to the data store. If the key already exists, its value will be updated. The `value` field is expected to be a valid JSON document.

- **HTTP Method:** `POST`
    
- **Content-Type:** `application/json`
    

#### **Request Body Parameters (JSON):**

|Name|Type|Description|Required|Example|
|---|---|---|---|---|
|`key`|`string`|The unique key for the data.|Yes|`"user_profile"`|
|`value`|`JSON Object/Array` (represented as `json.RawMessage`)|The value associated with the key, expected to be a valid JSON object or array.|Yes|`{"name": "Alice", "age": 30, "details": {"city": "Wonderland"}, "hobbies": ["reading", "coding"]}` or `["item1", "item2"]` or `{}` (empty object)|

Exportar a Hojas de cálculo

#### **Example Request (using `curl`):**


```bash
curl -X POST \
     -H "Content-Type: application/json" \
     -d '{
           "key": "user_settings_123",
           "value": {
             "theme": "dark",
             "notifications": {
               "email": true,
               "sms": false
             },
             "preferences": [
               "marketing",
               "analytics",
               {"feature_flags": ["new_ui", "beta_tester"]}
             ],
             "last_login": "2025-07-24T22:30:00Z"
           }
         }' \
     http://localhost:8080/set
```

#### **Responses:**

- **`200 OK`**
    
    - **Description:** The key-value pair was successfully saved or updated.
        
    - **Response Body:** Plain text, e.g.: `Data saved: Key='user_settings_123'`
        
- **`400 Bad Request`**
    
    - **Description:** Invalid JSON request body, `key` is empty, `value` is empty (not `{}` or `[]`), or `value` is not a valid JSON document (e.g., plain text).
        
    - **Example:** `Invalid JSON request body or unknown fields`, `Key cannot be empty`, `Value cannot be empty (e.g., use {} or [])`, or `'value' field must be a valid JSON document`.
        
- **`405 Method Not Allowed`**
    
    - **Description:** An HTTP method other than `POST` was attempted.
        

---

### 2. `GET /get`

Retrieves the value associated with a specific key. The returned value will be the stored JSON document.

- **HTTP Method:** `GET`
    

#### **URL Query Parameters:**

|Name|Type|Description|Required|Example|
|---|---|---|---|---|
|`key`|`string`|The key of the data to retrieve.|Yes|`user_settings_123`|

Exportar a Hojas de cálculo

#### **Example Request (using `curl`):**

```bash
curl "http://localhost:8080/get?key=user_settings_123"
```

#### **Responses:**

- **`200 OK`**
    
    - **Description:** The key was found, and its JSON value is returned.
        
    - **Content-Type:** `application/json`
        
    - **Response Body (JSON):**
                
        ```json
        {
            "key": "user_settings_123",
            "value": {
              "theme": "dark",
              "notifications": {
                "email": true,
                "sms": false
              },
              "preferences": [
                "marketing",
                "analytics",
                {"feature_flags": ["new_ui", "beta_tester"]}
              ],
              "last_login": "2025-07-24T22:30:00Z"
            }
        }
        ```
        
- **`400 Bad Request`**
    
    - **Description:** The `key` parameter is missing from the URL.
        
    - **Example:** `'key' query parameter is required`
        
- **`404 Not Found`**
    
    - **Description:** The requested key was not found in the data store.
        
    - **Example:** `Key 'non_existent_key' not found`
        
- **`405 Method Not Allowed`**
    
    - **Description:** An HTTP method other than `GET` was attempted.
        

---

### Data Persistence and Snapshots

When the application starts, it attempts to load existing data from `data.mtdb`. If the file doesn't exist, it starts with an empty store.

During runtime, the application will periodically save the current in-memory data to the `data.mtdb` file. This process is managed by a **Snapshot Manager** and occurs at a configurable interval (e.g., every 5 minutes by default). This ensures that even if the application crashes unexpectedly, your data loss is limited to the period since the last snapshot.

Additionally, when the application receives a termination signal (e.g., `Ctrl+C` in the terminal or a `SIGTERM` signal in a server/container environment), the Snapshot Manager is stopped, and a **final snapshot** of the current in-memory data is taken and saved to `data.mtdb` before the application exits. This guarantees the freshest possible data state upon shutdown.

You can configure the snapshot behavior in `main.go`:

Go

```go
// ... in main.go
cfg := Config{
    // ... other configurations
    SnapshotInterval: 5 * time.Minute, // Set the interval for automatic snapshots.
    EnableSnapshots:  true,            // Set to false to disable scheduled snapshots.
}
// ...
```

---

### Technologies Used

- **Go (Golang):** The primary programming language.
    
- **Go Standard Library:** `net/http`, `sync`, `encoding/json`, `encoding/binary`, `os`, `os/signal`, `syscall`, `log`, `context`, `time`, `maps`, `io`.