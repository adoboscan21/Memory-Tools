# Memory Tools API Documentation

This document describes the REST API endpoints provided by the Memory Tools application. All endpoints return **consistent JSON responses** for both success and error scenarios.

---

## Base URL

`http://localhost:8080` (or the port specified in your `config.json`)

---

## Standard Response Format

All API responses follow a consistent JSON structure:

```json
{
  "success": true, // Boolean indicating if the request was successful
  "message": "Optional message", // A human-readable message, often for errors or confirmations
  "data": {
    // Optional field for successful responses, contains the relevant data
    // ... actual response data ...
  }
}
```

# Memory Tools API Documentation

This document describes the REST API endpoints provided by the Memory Tools application. All endpoints return **consistent JSON responses** for both success and error scenarios.

---

## Base URL

`http://localhost:8080` (or the port specified in your `config.json`)

---

## Standard Response Format

All API responses follow a consistent JSON structure:

```json
{
  "success": true, // Boolean indicating if the request was successful
  "message": "Optional message", // A human-readable message, often for errors or confirmations
  "data": {
    // Optional field for successful responses, contains the relevant data
    // ... actual response data ...
  }
}
```

## Main Store API

These endpoints interact with the primary, unsharded in-memory key-value store.

### 1. `POST /set`

Saves a key-value pair to the main data store. If the key already exists, its value and TTL will be updated. The `value` field is expected to be a valid JSON document (object or array).

- **HTTP Method:** `POST`
- **Content-Type:** `application/json`

#### **Request Body Parameters (JSON):**

| Name          | Type                | Description                                                                                     | Required | Example                        |
| ------------- | ------------------- | ----------------------------------------------------------------------------------------------- | -------- | ------------------------------ |
| `key`         | `string`            | The unique key for the data.                                                                    | Yes      | `"user_profile"`               |
| `value`       | `JSON Object/Array` | The value associated with the key, expected to be a valid JSON object or array.                 | Yes      | `{"name": "Alice", "age": 30}` |
| `ttl_seconds` | `integer`           | **(Optional)** Time-To-Live for the item in seconds. If `0` or omitted, the item never expires. | No       | `600` (for 10 minutes)         |

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
  - **Response Body (JSON):**

    ```json
    {
      "success": true,
      "message": "Data saved for Key='user_session:abc123'"
    }
    ```

- **`400 Bad Request`**
  - **Description:** Invalid JSON request body, `key` is empty, `value` is empty (not `{}` or `[]`), or `value` is not a valid JSON document.
  - **Response Body (JSON Examples):**
    ```json
    {
      "success": false,
      "message": "Invalid JSON request body or unknown fields"
    }
    ```
    ```json
    {
      "success": false,
      "message": "Key cannot be empty"
    }
    ```
- **`405 Method Not Allowed`**
  - **Description:** An HTTP method other than `POST` was attempted.
  - **Response Body (JSON Example):**
    ```json
    {
      "success": false,
      "message": "Method not allowed"
    }
    ```

---

### 2. `GET /get`

Retrieves the value associated with a specific key from the main data store. The returned value will be the stored JSON document. If the item has expired, it will be treated as not found.

- **HTTP Method:** `GET`

#### **URL Query Parameters:**

| Name  | Type     | Description                      | Required | Example               |
| ----- | -------- | -------------------------------- | -------- | --------------------- |
| `key` | `string` | The key of the data to retrieve. | Yes      | `user_session:abc123` |

#### **Example Request (using `curl`):**

```bash
curl "http://localhost:8080/get?key=user_session:abc123"
```

#### **Responses:**

- **`200 OK`**
  - **Description:** The key was found, and its JSON value is returned within the `data` field.
  - **Response Body (JSON):**
    ```json
    {
      "success": true,
      "message": "Data retrieved successfully from main store",
      "data": {
        "key": "user_session:abc123",
        "value": {
          "user_id": 101,
          "last_activity": "2025-07-25T09:30:00Z",
          "status": "active"
        }
      }
    }
    ```
- **`400 Bad Request`**
  - **Description:** The `key` parameter is missing from the URL.
  - **Response Body (JSON Example):**
    ```json
    {
      "success": false,
      "message": "'key' query parameter is required"
    }
    ```
- **`404 Not Found`**
  - **Description:** The requested key was not found in the data store **or it has expired**.
  - **Response Body (JSON Example):**
    ```json
    {
      "success": false,
      "message": "Key 'temp_cache_item' not found or expired in main store"
    }
    ```
- **`405 Method Not Allowed`**
  - **Description:** An HTTP method other than `GET` was attempted.
  - **Response Body (JSON Example):**
    ```json
    {
      "success": false,
      "message": "Method not allowed"
    }
    ```

---

## Data Persistence & Snapshots

### Main Store Persistence (`in-memory.mtdb`)

When the application starts, it attempts to load existing data from `in-memory.mtdb`. If the file doesn't exist, it starts with an empty store. Only non-expired items are included in the loaded data.

During runtime, the main store's in-memory data is periodically saved to the `in-memory.mtdb` file. This process is managed by a **Snapshot Manager** and occurs at a configurable `snapshot_interval`. This ensures that even if the application crashes unexpectedly, your data loss is limited to the period since the last snapshot. Only non-expired items are saved during snapshots.

### Collections Persistence (`collections/`)

For **collections**, each collection is persisted to its own file within the `collections/` directory (e.g., `collections/my_collection.cmtdb`).

- **Immediate Saving:** Changes to a collection (like `SET` or `DELETE` operations on collection items, or creating/deleting collections themselves) trigger an **immediate save** of that specific collection's data to its corresponding file on disk. This ensures high durability for collection-specific data.
- **Loading:** At application startup, all existing collection files in the `collections/` directory are loaded into the `CollectionManager` in memory.

### TTL Cleaner

A **TTL Cleaner** runs in the background at a configurable `ttl_clean_interval`. This cleaner physically removes expired items from both the main in-memory store and all active collections, freeing up memory. If items are removed from a collection due due to TTL, that collection's data is also immediately saved to disk.

### Graceful Shutdown

When the application receives a termination signal (e.g., `Ctrl+C` in the terminal or a `SIGTERM` signal in a server/container environment), both the Snapshot Manager and the TTL Cleaner are stopped. A **final snapshot** of the current non-expired in-memory data from the main store is taken and saved to `in-memory.mtdb`, and all active collections are also saved to their respective files before the application exits. This guarantees the freshest possible data state upon shutdown.

You can configure the snapshot and TTL cleaner behavior using the `config.json` file.

---

## Collections API (Multi-Tenant Storage)

Memory Tools also supports named "collections" which act as separate, isolated key-value stores within the application. Each collection is managed independently in memory and persisted to its own file on disk.

### Base URL for Collections

`http://localhost:8080/collections`

---

### 1. `POST /collections/{collectionName}`

Creates a new collection or ensures an existing one is loaded into memory and persisted to disk. If the collection already exists on disk, it will be loaded; otherwise, a new empty collection file will be created.

- **HTTP Method:** `POST`
- **Path Parameters:**

| Name             | Type     | Description                             | Required | Example         |
| ---------------- | -------- | --------------------------------------- | -------- | --------------- |
| `collectionName` | `string` | The unique name for the new collection. | Yes      | `user_settings` |

#### **Example Request (using `curl`):**

```bash
curl -X POST http://localhost:8080/collections/my_new_collection
```

#### **Responses:**

- **`201 Created`**
  - **Description:** The collection was successfully created or ensured to be present and persisted.
  - **Response Body (JSON):**
    ```json
    {
      "success": true,
      "message": "Collection 'my_new_collection' ensured and persisted on disk."
    }
    ```
- **`400 Bad Request`**
  - **Description:** `collectionName` is empty.
  - **Response Body (JSON Example):**
    ```json
    {
      "success": false,
      "message": "Collection name cannot be empty"
    }
    ```
- **`405 Method Not Allowed`**
  - **Description:** An HTTP method other than `POST` was attempted.
- **`500 Internal Server Error`**
  - **Description:** Failed to create or save the collection file to disk.

---

### 2. `DELETE /collections/{collectionName}`

Deletes an entire collection from memory and its corresponding file from disk.

- **HTTP Method:** `DELETE`
- **Path Parameters:**

| Name             | Type     | Description                           | Required | Example    |
| ---------------- | -------- | ------------------------------------- | -------- | ---------- |
| `collectionName` | `string` | The name of the collection to delete. | Yes      | `old_data` |

#### **Example Request (using `curl`):**

```bash
curl -X DELETE http://localhost:8080/collections/old_data
```

#### **Responses:**

- **`200 OK`**
  - **Description:** The collection was successfully deleted from memory and disk.
  - **Response Body (JSON):**
    ```json
    {
      "success": true,
      "message": "Collection 'old_data' deleted from memory and disk."
    }
    ```
- **`400 Bad Request`**
  - **Description:** `collectionName` is empty.
- **`405 Method Not Allowed`**
  - **Description:** An HTTP method other than `DELETE` was attempted.
- **`500 Internal Server Error`**
  - **Description:** Failed to delete the collection file from disk.

---

### 3. `GET /collections`

Lists the names of all currently active (loaded in memory) collections.

- **HTTP Method:** `GET`

#### **Example Request (using `curl`):**

```bash
curl http://localhost:8080/collections
```

#### **Responses:**

- **`200 OK`**
  - **Description:** Returns a list of collection names.
  - **Response Body (JSON):**
    ```json
    {
      "success": true,
      "message": "Collections retrieved successfully",
      "data": ["user_settings", "product_info", "my_new_collection"]
    }
    ```
- **`405 Method Not Allowed`**
  - **Description:** An HTTP method other than `GET` was attempted.

---

### 4. `POST /collections/{collectionName}/set`

Saves a key-value pair within a specific collection. If the key already exists, its value and TTL will be updated. The collection's data is immediately persisted to disk after this operation.

- **HTTP Method:** `POST`
- **Path Parameters:**

| Name             | Type     | Description                               | Required | Example         |
| ---------------- | -------- | ----------------------------------------- | -------- | --------------- |
| `collectionName` | `string` | The name of the collection to operate on. | Yes      | `user_settings` |

- **Content-Type:** `application/json`

#### **Request Body Parameters (JSON):** (Same as `POST /set`)

| Name          | Type                | Description                                                                                     | Required | Example                                    |
| ------------- | ------------------- | ----------------------------------------------------------------------------------------------- | -------- | ------------------------------------------ |
| `key`         | `string`            | The unique key for the data within this collection.                                             | Yes      | `"user_theme"`                             |
| `value`       | `JSON Object/Array` | The value associated with the key, expected to be a valid JSON object or array.                 | Yes      | `{"theme": "dark", "notifications": true}` |
| `ttl_seconds` | `integer`           | **(Optional)** Time-To-Live for the item in seconds. If `0` or omitted, the item never expires. | No       | `300` (for 5 minutes)                      |

#### **Example Request (using `curl`):**

```bash
curl -X POST \
     -H "Content-Type: application/json" \
     -d '{
            "key": "user_101_preferences",
            "value": {
              "theme": "dark",
              "language": "en-US"
            },
            "ttl_seconds": 3600
          }' \
     http://localhost:8080/collections/user_settings/set
```

#### **Responses:**

- **`200 OK`**
  - **Description:** The key-value pair was successfully saved or updated within the collection.
- **`400 Bad Request`**
  - **Description:** Invalid JSON request, empty `key`/`value`, invalid `value` format, or `collectionName` missing from path.
- **`405 Method Not Allowed`**
  - **Description:** An HTTP method other than `POST` was attempted.
- **`500 Internal Server Error`**
  - **Description:** The in-memory `SET` succeeded, but saving the collection to disk failed.

---

### 5. `GET /collections/{collectionName}/get`

Retrieves the value associated with a specific key from a given collection. If the item has expired, it will be treated as not found.

- **HTTP Method:** `GET`
- **Path Parameters:**

| Name             | Type     | Description                               | Required | Example         |
| ---------------- | -------- | ----------------------------------------- | -------- | --------------- |
| `collectionName` | `string` | The name of the collection to operate on. | Yes      | `user_settings` |

- **URL Query Parameters:**

| Name  | Type     | Description                      | Required | Example                |
| ----- | -------- | -------------------------------- | -------- | ---------------------- |
| `key` | `string` | The key of the data to retrieve. | Yes      | `user_101_preferences` |

#### **Example Request (using `curl`):**

```bash
curl "http://localhost:8080/collections/user_settings/get?key=user_101_preferences"
```

#### **Responses:**

- **`200 OK`**
  - **Description:** The key was found in the collection, and its JSON value is returned.
- **`400 Bad Request`**
  - **Description:** `key` or `collectionName` is missing/empty.
- **`404 Not Found`**
  - **Description:** The requested `collectionName` does not exist, or the `key` was not found/expired within the collection.
- **`405 Method Not Allowed`**
  - **Description:** An HTTP method other than `GET` was attempted.

---

### 6. `DELETE /collections/{collectionName}/delete`

Removes a specific key-value pair from a collection. The collection's data is immediately persisted to disk after this operation.

- **HTTP Method:** `DELETE`
- **Path Parameters:**

| Name             | Type     | Description                               | Required | Example         |
| ---------------- | -------- | ----------------------------------------- | -------- | --------------- |
| `collectionName` | `string` | The name of the collection to operate on. | Yes      | `user_settings` |

- **URL Query Parameters:**

| Name  | Type     | Description                    | Required | Example                |
| ----- | -------- | ------------------------------ | -------- | ---------------------- |
| `key` | `string` | The key of the item to delete. | Yes      | `user_101_preferences` |

#### **Example Request (using `curl`):**

```bash
curl -X DELETE "http://localhost:8080/collections/user_settings/delete?key=user_101_preferences"
```

#### **Responses:**

- **`200 OK`**
  - **Description:** The key was successfully deleted from the collection.
- **`400 Bad Request`**
  - **Description:** `key` or `collectionName` is missing/empty.
- **`405 Method Not Allowed`**
  - **Description:** An HTTP method other than `DELETE` was attempted.
- **`500 Internal Server Error`**
  - **Description:** The in-memory `DELETE` succeeded, but saving the collection to disk failed.

---

### 7. `GET /collections/{collectionName}/list`

Retrieves all non-expired key-value pairs from a specific collection.

- **HTTP Method:** `GET`
- **Path Parameters:**

| Name             | Type     | Description                                    | Required | Example         |
| ---------------- | -------- | ---------------------------------------------- | -------- | --------------- |
| `collectionName` | `string` | The name of the collection to list items from. | Yes      | `user_settings` |

#### **Example Request (using `curl`):**

```bash
curl http://localhost:8080/collections/user_settings/list
```

#### **Responses:**

- **`200 OK`**
  - **Description:** Returns all non-expired items from the specified collection as a JSON object where keys are the stored keys and values are the stored JSON documents.
  - **Response Body (JSON Example):**
    ```json
    {
      "success": true,
      "message": "Items from collection 'user_settings' retrieved successfully",
      "data": {
        "user_101_preferences": {
          "theme": "dark",
          "language": "en-US"
        },
        "user_205_data": {
          "last_login": "2025-07-28T12:00:00Z"
        }
      }
    }
    ```
- **`400 Bad Request`**
  - **Description:** `collectionName` is missing/empty.
- **`404 Not Found`**
  - **Description:** The requested `collectionName` does not exist.
- **`405 Method Not Allowed`**
  - **Description:** An HTTP method other than `GET` was attempted.
