## **Simple Key-Value API with Persistence in Go-Lang**

This project implements a basic REST API for storing and retrieving key-value pairs. Data is kept in memory for fast access and is automatically persisted to a local file (`data.json`) to ensure it's not lost when the application restarts.

---
## Features

- **In-Memory Storage:** Data stored in a `map[string]string` for quick retrieval.
    
- **Data Persistence:** Data is automatically saved to `data.json` upon graceful application shutdown and loaded on startup.
    
- **REST API:** Two endpoints for `SET` (save) and `GET` (retrieve) operations.
    
- **Concurrency Handling:** Uses `sync.RWMutex` to ensure safe data access from multiple concurrent requests.
    
- **Go Modules:** Modular project structure for better organization and maintainability.
    
- **Graceful Shutdown:** The application shuts down cleanly, saving data and closing connections safely.
    
- **Framework-less:** Built purely with Go's standard library for full control and low overhead.
    

---

## Project Structure

```
my-rest-api/
├── main.go                       # Main entry point and server configuration.
├── api/                          # HTTP endpoint handlers.
│   └── handlers.go
├── store/                        # In-memory data storage logic.
│   └── inmem.go
├── persistence/                  # Logic for persisting data to disk (file storage).
│   └── file_storage.go
└── go.mod                        # Go module file.
└── go.sum                        # Dependency checksums.
```

---

## How to Run

Make sure you have [Go installed (version go1.24.4  or higher)](https://go.dev/doc/install).

1. **Clone the repository** (or create the files and folders manually as described in the structure).
    
2. Navigate to the **project root** in your terminal:
        
    ```bash
    go run .
    ```
    
    You'll see a message in the console indicating the server is listening on port 8080.
    

---

## API Documentation

The API exposes two main endpoints for key-value interaction.

### Base URL

`http://localhost:8080`

---

### 1. `POST /set`

Saves a key-value pair to the data store. If the key already exists, its value will be updated.

- **HTTP Method:** `POST`
    
- **Content-Type:** `application/json`
    

#### **Request Body Parameters (JSON):**

|Name|Type|Description|Required|Example|
|---|---|---|---|---|
|`key`|`string`|The unique key for the data.|Yes|`"username"`|
|`value`|`string`|The value associated with the key.|Yes|`"JohnDoe"`|

#### **Example Request (using `curl`):**

```bash
curl -X POST \
     -H "Content-Type: application/json" \
     -d '{"key": "product_id_123", "value": "Laptop_Pro_Max"}' \
     http://localhost:8080/set
```

#### **Responses:**

- **`200 OK`**
    
    - **Description:** The key-value pair was successfully saved or updated.
        
    - **Response Body:** Plain text, e.g.: `Data saved: Key='product_id_123', Value='Laptop_Pro_Max'`
        
- **`400 Bad Request`**
    
    - **Description:** Invalid JSON request body, or `key` or `value` are empty.
        
    - **Example:** `Invalid JSON request body or unknown fields` or `Key and value cannot be empty`
        
- **`405 Method Not Allowed`**
    
    - **Description:** An HTTP method other than `POST` was attempted.
        

---

### 2. `GET /get`

Retrieves the value associated with a specific key.

- **HTTP Method:** `GET`
    

#### **URL Query Parameters:**

| Name  | Type     | Description                      | Required | Example    |
| ----- | -------- | -------------------------------- | -------- | ---------- |
| `key` | `string` | The key of the data to retrieve. | Yes      | `username` |

Exportar a Hojas de cálculo

#### **Example Request (using `curl`):**

```bash
curl "http://localhost:8080/get?key=product_id_123"
```

#### **Responses:**

- **`200 OK`**
    
    - **Description:** The key was found, and its value is returned.
        
    - **Content-Type:** `application/json`
        
    - **Response Body (JSON):**
        
        
        ```json
        {
            "key": "product_id_123",
            "value": "Laptop_Pro_Max"
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

### Data Persistence

When the application receives a termination signal (e.g., `Ctrl+C` in the terminal or a `SIGTERM` signal in a server/container environment), the current in-memory data is automatically saved to the `data.json` file in the project root. Upon restarting the application, this data is automatically loaded, restoring the store's previous state.

---

### Technologies Used

- **Go (Golang):** The primary programming language.
    
- **Go Standard Library:** `net/http`, `sync`, `encoding/json`, `os`, `log`, `context`.