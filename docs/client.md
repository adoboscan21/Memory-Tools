## Memory Tools CLI Client Documentation

The `memory-tools-client` is an interactive command-line interface (CLI) for direct, secure interaction with the `memory-tools-server` via its custom TLS-encrypted TCP protocol.

---

### How to Run

To start the client, you must provide the address of the `memory-tools-server`. You can also include credentials for automatic login using flags.

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

Once connected, you will see the message: `Connected securely to Memory Tools server at <address>. Type 'help' for commands, 'exit' to quit.`

---

### Authentication

The client supports both automatic login via command-line flags and manual login via a command.

- **Automatic Login**: Use the `-u <username>` and `-p <password>` flags when starting the client. If provided, the client will attempt to authenticate immediately upon connection.
- **Manual Login**: If you don't use the flags, or if automatic login fails, you can log in manually after connecting using the **`login`** command.
  ```bash
  login admin adminpass
  ```
  - **Example**:
    ```bash
    login root rootpass
    ```

---

### Available Commands

Here is a full list of all the commands you can use with the CLI client.

#### General Commands

- **`help`**: Displays a list of all available commands and their syntax, including detailed examples for collection queries.
- **`clear`**: Clears the terminal screen.
- **`exit`**: Disconnects from the server and quits the client.

#### User Management Commands

- **`login <username> <password>`**
  - **Description**: Authenticates the connection with the server. This is necessary to execute most commands.
  - **Example**:
    ```bash
    login admin adminpass
    ```
- **`update password <target_username> <new_password>`**
  - **Description**: Updates the password for a specified user.
  - **Important**: This command can only be executed by the `root` user and requires the client to be connected from `localhost`.
  - **Example**:
    ```bash
    update password user1 newSecurePassword
    ```

#### Main Data Store Commands (Key-Value)

These commands operate on the primary, default key-value store.

- **`set <key> <value_json> [ttl_seconds]`**
  - **Description**: Sets a key-value pair. The `value_json` must be valid JSON. `ttl_seconds` (optional) is the Time-To-Live in seconds; `0` means no expiration.
  - **Examples**:
    ```bash
    set mykey "a simple string"
    set product:123 {"name": "Laptop", "price": 1200} 3600
    ```
- **`get <key>`**
  - **Description**: Retrieves the value associated with a given key.
  - **Example**:
    ```bash
    get product:123
    ```

---

### Collections

A collection is an independent data store that allows for grouping items and performing more advanced operations like queries and bulk deletions.

#### Collection Management Commands

- **`collection create <collection_name>`**
  - **Description**: Creates a new collection. If it already exists, it simply ensures its presence.
  - **Example**:
    ```bash
    collection create users
    ```
- **`collection delete <collection_name>`**
  - **Description**: Deletes an entire collection and all its data, both from memory and disk.
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

#### Collection Item Commands

These commands allow you to manipulate key-value pairs within a specific collection.

- **`collection item set <collection_name> [<key>] <value_json> [ttl_seconds]`**

  - **Description**: Sets a key-value pair within the specified collection. `value_json` must be valid JSON. **If `<key>` is omitted, a unique UUID will be generated and used as the key, and it will also be injected into the JSON as an `_id` field.**
  - **Examples**:

    - With an explicit key:
      ```bash
      collection item set users user:123 {"email": "a@b.com", "name": "User A"} 3600
      ```
    - Without an explicit key (a UUID is generated for the key and the `_id` field):

      ```bash
      collection item set products {"name": "New Gadget", "price": 99.99} 180
      ```

- **`collection item set many <collection_name> <value_json_array>`**
  - **Description**: Inserts multiple items at once into a collection. The `value_json_array` must be an array of JSON objects. **If a JSON object does not contain the `_id` field, a UUID will be generated for it.**
  - **Example**:
    ```bash
    collection item set many products [{"name": "New Product", "price": 199.99},{"name": "Another Product", "price": 50}]
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
- **`collection item delete many <collection_name> <value_json_array>`**
  - **Description**: Deletes multiple items from a collection at once, based on the `_id` field of the objects in the `value_json_array`.
  - **Example**:
    ```bash
    collection item delete many products [ {"_id": "27cb8c82-3cb0-43fc-b93c-399b6aac22f3"}, {"_id": "85c337b2-e05a-4691-97a7-2e58f582145a"} ]
    ```
- **`collection item list <collection_name>`**
  - **Description**: Lists all non-expired key-value pairs (items) within the specified collection.
  - **Example**:
    ```bash
    collection item list products
    ```

---

### Collection Query Command (`collection query`)

This powerful command lets you perform complex queries to filter, sort, paginate, and aggregate data from a collection, similar to operations in a relational database.

- **`collection query <collection_name> <query_json>`**
  - **Description**: Executes a query against a specified collection. The `query_json` must be a valid JSON object defining your query criteria.
  - **Example**:
    ```bash
    collection query products {"filter": {"field": "category", "op": "=", "value": "Electronics"}, "limit": 5}
    ```

---

### Query JSON Examples

The `query_json` parameter supports a variety of operations:

- **Filter (`filter` - WHERE clauses):**
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
  - `LIKE` operator (use `%` as a wildcard):
    ```json
    { "filter": { "field": "name", "op": "like", "value": "%Book%" } }
    ```
  - `BETWEEN` operator (value is a two-element array `[min, max]`):
    ```json
    { "filter": { "field": "price", "op": "between", "value": [100, 200] } }
    ```
  - `IS NULL` / `IS NOT NULL`:
    ```json
    { "filter": { "field": "description", "op": "is null" } }
    { "filter": { "field": "description", "op": "is not null" } }
    ```
- **Ordering (`order_by`):**
  - Sort results by one or more fields.
  ```json
  {
    "order_by": [
      { "field": "name", "direction": "asc" },
      { "field": "age", "direction": "desc" }
    ]
  }
  ```
- **Limit/Offset (`limit`/`offset`):**
  - Limit the number of results and/or skip a certain number of results.
  ```json
  { "limit": 5, "offset": 10 }
  ```
- **Count (`count`):**
  - Get a count of items matching the filter. If no other fields are in the query, an object with the key `count` is returned.
  ```json
  { "count": true, "filter": { "field": "active", "op": "=", "value": true } }
  ```
- **Aggregations (`aggregations`):**
  - Perform aggregate functions on numeric fields. You can optionally group the results by one or more fields (`group_by`).
  - Supported functions: `"sum"`, `"avg"`, `"min"`, `"max"`, `"count"`.
  ```json
  {
    "aggregations": { "total_sales": { "func": "sum", "field": "sales" } },
    "group_by": ["category"]
  }
  ```
- **Distinct (`distinct`):**
  - Get unique values for a specified field. This is a terminal operation and excludes other clauses like `limit` or `order_by`.
  ```json
  { "distinct": "city" }
  ```

---

**Tip**: When entering JSON values, especially for `set` or `collection item set`, make sure they are properly formatted and escaped if necessary for your shell. For `collection query`, the entire query object must be valid JSON.
