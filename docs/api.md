# Memory Tools HTTP REST API

The `memory-tools-api` module provides an HTTP/JSON interface to interact with the core `memory-tools-server`. It acts as a lightweight proxy, translating standard HTTP requests into the server's custom binary protocol and returning JSON responses.

---

## API Endpoint

All commands are handled by a single **POST** endpoint.

- **URL:** `http://<api_listen_address>/command`
- **Method:** `POST`
- **Content-Type:** `application/json`

---

## Request Body (`CommandRequest`)

Send your commands as a JSON object in the request body:

```json
{
  "command": "string", // Required: The specific command (e.g., "set", "get", "collection create").
  "args": "string, optional", // Optional: Space-separated arguments for the command (e.g., "mykey", "users user1").
  "value": "any, optional", // Optional: The JSON value for 'set' or 'collection item set' commands. Can be any valid JSON type.
  "ttl": "integer, optional" // Optional: Time-to-live in seconds for 'set' commands. 0 or omitted means no expiration.
}
```

---

## Response Body (`CommandResponse`)

Responses will always be a JSON object with the following structure:

```json
{
  "status": "string", // Indicates the operation's outcome (e.g., "OK", "NOT_FOUND", "ERROR").
  "message": "string", // A descriptive message from the server.
  "data": "any, optional", // Optional: The data returned by the command, if any. Its type varies.
  "error": "string, optional" // Optional: Provides more details on errors (e.g., "BAD_REQUEST").
}
```

**Common Status Mappings:**

| String Status   | HTTP Code | Description                                      |
| --------------- | --------- | ------------------------------------------------ |
| `"OK"`          | `200`     | Command executed successfully.                   |
| `"NOT_FOUND"`   | `200`     | Key or collection not found.                     |
| `"ERROR"`       | `500`     | Internal server error (e.g., connection issues). |
| `"BAD_COMMAND"` | `400`     | Unknown or malformed command.                    |
| `"BAD_REQUEST"` | `400`     | Missing/invalid arguments or value.              |

---

## Examples

### 1. `set` (Main Store)

Sets a key-value pair in the main in-memory store.

**Request:**

```json
curl -X POST -H "Content-Type: application/json" -d '{
  "command": "set",
  "args": "product:101",
  "value": {"name": "Laptop", "price": 1200},
  "ttl": 3600
}' http://localhost:8081/command
```

**Successful Response:**

```json
{
  "status": "OK",
  "message": "OK: Key 'product:101' set in main store"
}
```

### 2. `get` (Main Store)

Retrieves the value associated with a key from the main store.

**Request:**

```json
curl -X POST -H "Content-Type: application/json" -d '{
  "command": "get",
  "args": "product:101"
}' http://localhost:8081/command
```

**Successful Response:**

```json
{
  "status": "OK",
  "message": "OK: Key 'product:101' retrieved from main store",
  "data": {
    "name": "Laptop",
    "price": 1200
  }
}
```

### 3. `collection item set`

Sets a key-value pair within a specific named collection.

**Request:**

```json
curl -X POST -H "Content-Type: application/json" -d '{
  "command": "collection item set",
  "args": "users user:abc",
  "value": {"username": "alice_smith", "status": "active"},
  "ttl": 86400
}' http://localhost:8081/command
```

**Successful Response:**

```json
{
  "status": "OK",
  "message": "OK: Key 'user:abc' set in collection 'users'"
}
```

### 4. `collection item get`

Retrieves a value from a specific named collection.

**Request:**

```json
curl -X POST -H "Content-Type: application/json" -d '{
  "command": "collection item get",
  "args": "users user:abc"
}' http://localhost:8081/command
```

**Successful Response:**

```json
{
  "status": "OK",
  "message": "OK: Key 'user:abc' retrieved from collection 'users'",
  "data": {
    "username": "alice_smith",
    "status": "active"
  }
}
```
