# Memory Tools CLI Client

The `memory-tools-client` is an interactive command-line interface (CLI) for direct, secure interaction with the `memory-tools-server` via its custom TLS-encrypted TCP protocol.

---

## How to Run

To start the client, provide the address of the `memory-tools-server`.

**Locally:**

```bash
./bin/memory-tools-client localhost:8080
```

**Via Docker Compose:**

```bash
docker exec -it memory-tools-server memory-tools-client localhost:8080
```

Once connected, you'll see a prompt: `Connected securely to Memory Tools server at <address>. Type 'help' for commands, 'exit' to quit.`

---

## Available Commands

Here's a list of all commands you can use:

### General Commands

- **`help`**: Displays all available commands and their syntax.
- **`clear`**: Clears the terminal screen.
- **`exit`**: Disconnects from the server and quits the client.

### Main Data Store Commands

These commands operate on the primary, default key-value store.

- **`set <key> <value_json> [ttl_seconds]`**
  - **Description**: Sets a key-value pair. `value_json` must be valid JSON. `ttl_seconds` (optional) is in seconds; 0 means no expiration.
  - **Examples**:
    ```
    set mykey {"name": "Test Item", "qty": 10}
    set anotherkey "just a string" 60
    ```
- **`get <key>`**
  - **Description**: Retrieves the value for a given key.
  - **Example**:
    ```
    get mykey
    ```

### Collection Management Commands

These commands manage named collections, which are essentially independent key-value stores.

- **`collection create <collection_name>`**
  - **Description**: Creates a new collection. If it exists, it ensures its presence.
  - **Example**:
    ```
    collection create users
    ```
- **`collection delete <collection_name>`**
  - **Description**: Deletes an entire collection and all its data from both memory and disk.
  - **Example**:
    ```
    collection delete users
    ```
- **`collection list`**
  - **Description**: Lists the names of all existing collections.
  - **Example**:
    ```
    collection list
    ```

### Collection Item Commands

These commands allow you to manipulate key-value pairs within specific collections.

- **`collection item set <collection_name> <key> <value_json> [ttl_seconds]`**
  - **Description**: Sets a key-value pair within the specified collection. `value_json` must be valid JSON. `ttl_seconds` (optional) is in seconds.
  - **Examples**:
    ```
    collection item set users user:123 {"id": "user:123", "email": "a@b.com"} 3600
    collection item set products item:A01 {"name": "Widget"}
    ```
- **`collection item get <collection_name> <key>`**
  - **Description**: Retrieves the value for a key from the specified collection.
  - **Example**:
    ```
    collection item get users user:123
    ```
- **`collection item delete <collection_name> <key>`**
  - **Description**: Deletes a key-value pair from the specified collection.
  - **Example**:
    ```
    collection item delete users user:123
    ```
- **`collection item list <collection_name>`**
  - **Description**: Lists all non-expired key-value pairs (items) within the specified collection.
  - **Example**:
    ```
    collection item list products
    ```

---

**Tip:** When entering JSON values, make sure they are properly formatted. For complex JSON, you might need to enclose it in single quotes in your shell to prevent shell parsing issues.

---

Feel free to experiment with these commands to manage your in-memory data!
