# Memory Tools CLI Client

The `memory-tools-client` is an interactive command-line interface (CLI) for direct, secure interaction with the `memory-tools-server` via its custom TLS-encrypted TCP protocol.

---

## How to Run

To start the client, provide the address of the `memory-tools-server`. You can also provide credentials for automatic login using flags.

**Locally:**

```bash
./bin/memory-tools-client
./bin/memory-tools-client -u myuser -p mypassword localhost:8080
```

**Via Docker Compose:**

```bash
docker exec -it containerid memory-tools-client localhost:8080
docker exec -it containerid memory-tools-client -u myuser -p mypassword localhost:8080
```

Once connected, you'll see a prompt: `Connected securely to Memory Tools server at <address>. Type 'help' for commands, 'exit' to quit.`

---

## Authentication

The client supports both automatic login via command-line flags and manual login via a command.

- **Automatic Login**: Use the `-u <username>` and `-p <password>` flags when starting the client. If provided, the client will attempt to authenticate immediately upon connection.

  ```bash
  ./bin/memory-tools-client -u admin -p securepassword localhost:8080
  ```

- **Manual Login**: If you don't use the flags, or if automatic login fails, you can log in manually after connecting using the `login` command.

  ```bash
  login <username> <password>
  ```

  - **Example**:
    ```
    login root mysecretpassword
    ```

---

## Available Commands

Here's a list of all commands you can use:

### General Commands

- **`help`**: Displays all available commands and their syntax, including examples for `collection query`.
- **`clear`**: Clears the terminal screen.
- **`exit`**: Disconnects from the server and quits the client.

### User Management Commands

- **`login <username> <password>`**
  - **Description**: Authenticates with the server using the provided username and password. This is necessary to execute most commands.
  - **Example**:
    ```bash
    login admin adminpass
    ```
    ```bash
    login root rootpass
    ```
- **`update password <target_username> <new_password>`**
  - **Description**: Updates the password for a specified user.
  - **Important**: This command can only be executed by the `root` user and requires the client to be connected from `localhost` to the server.
  - **Example**:
    ```bash
    update password user1 newSecurePassword
    ```

### Main Data Store Commands

These commands operate on the primary, default key-value store.

- **`set <key> <value_json> [ttl_seconds]`**
  - **Description**: Sets a key-value pair. `value_json` must be valid JSON. `ttl_seconds` (optional) is in seconds; 0 means no expiration.
  - **Examples**:
    ```bash
    set mykey {"name": "Test Item", "qty": 10}
    set anotherkey "just a string" 60
    ```
- **`get <key>`**
  - **Description**: Retrieves the value for a given key.
  - **Example**:
    ```bash
    get mykey
    ```

### Collection Management Commands

These commands manage named collections, which are essentially independent key-value stores.

- **`collection create <collection_name>`**
  - **Description**: Creates a new collection. If it exists, it ensures its presence.
  - **Example**:
    ```bash
    collection create users
    ```
- **`collection delete <collection_name>`**
  - **Description**: Deletes an entire collection and all its data from both memory and disk.
  - **Example**:
    ```bash
    collection delete users
    ```
- **`collection list`**
  - **Description**: Lists the names of all existing collections.
  - **Example**:
    ```bash
    collection list
    ```

### Collection Item Commands

These commands allow you to manipulate key-value pairs within specific collections.

- **`collection item set <collection_name> <key> <value_json> [ttl_seconds]`**
  - **Description**: Sets a key-value pair within the specified collection. `value_json` must be valid JSON. `ttl_seconds` (optional) is in seconds.
  - **Examples**:
    ```bash
    collection item set users user:123 {"id": "user:123", "email": "a@b.com"} 3600
    collection item set products item:A01 {"name": "Widget"}
    ```
- **`collection item get <collection_name> <key>`**
  - **Description**: Retrieves the value for a key from the specified collection.
  - **Example**:
    ```bash
    collection item get users user:123
    ```
- **`collection item delete <collection_name> <key>`**
  - **Description**: Deletes a key-value pair from the specified collection.
  - **Example**:
    ```bash
    collection item delete users user:123
    ```
- **`collection item list <collection_name>`**
  - **Description**: Lists all non-expired key-value pairs (items) within the specified collection.
  - **Example**:
    ```bash
    collection item list products
    ```

---

### **Collection Query Command (NEW)**

This powerful command allows you to retrieve, filter, sort, and aggregate data from a collection using a flexible JSON-based query language.

- **`collection query <collection_name> <query_json>`**
  - **Description**: Executes a query against the specified collection. The `query_json` must be a valid JSON object defining your query criteria.
  - **Example**:
    ```bash
    collection query products {"filter": {"field": "category", "op": "=", "value": "Electronics"}, "limit": 5}
    ```

---

### Query JSON Examples

When using `collection query`, the `<query_json>` parameter supports various operations:

- **Filter (WHERE clauses):**
  - Equality:
    ```json
    { "filter": { "field": "status", "op": "=", "value": "active" } }
    ```
  - `AND` combined conditions:
    ```json
    {
      "filter": {
        "and": [
          { "field": "age", "op": ">", "value": 30 },
          { "field": "city", "op": "like", "value": "New%" }
        ]
      }
    }
    ```
  - `OR` combined conditions:
    ```json
    {
      "filter": {
        "or": [
          { "field": "category", "op": "=", "value": "Books" },
          { "field": "stock", "op": "<", "value": 10 }
        ]
      }
    }
    ```
  - `IN` operator (value is an array):
    ```json
    { "filter": { "field": "tags", "op": "in", "value": ["A", "B"] } }
    ```
  - `LIKE` operator (use `%` as wildcard):
    ```json
    { "filter": { "field": "name", "op": "like", "value": "%Book%" } }
    ```
  - `BETWEEN` operator (value is a two-element array `[min, max]`):
    ```json
    { "filter": { "field": "price", "op": "between", "value": [100, 200] } }
    ```
  - `IS NULL` / `IS NOT NULL`:
    ```json
    {"filter": {"field": "description", "op": "is null"}}
    {"filter": {"field": "description", "op": "is not null"}}
    ```
- **Order By:**
  - Sort results by one or more fields.
  ```json
  {
    "order_by": [
      { "field": "name", "direction": "asc" },
      { "field": "age", "direction": "desc" }
    ]
  }
  ```
- **Limit/Offset:**
  - Limit the number of results and/or skip a certain number of results.
  ```json
  { "limit": 5, "offset": 10 }
  ```
- **Count:**
  - Get a count of items matching the filter.
  ```json
  { "count": true, "filter": { "field": "active", "op": "=", "value": true } }
  ```
- **Aggregations (SUM, AVG, MIN, MAX):**
  - Perform aggregate functions on numeric fields, optionally grouped by one or more fields.
  - Supported functions: `"sum"`, `"avg"`, `"min"`, `"max"`.
  ```json
  {
    "aggregations": { "total_sales": { "func": "sum", "field": "sales" } },
    "group_by": ["category"]
  }
  ```
- **Distinct:**
  - Get unique values for a specified field.
  ```json
  { "distinct": "city" }
  ```

---

**Tip:** When entering JSON values, especially for `set` or `collection item set`, ensure they are properly formatted and escaped if necessary for your shell. For `collection query`, the entire query object must be valid JSON.
