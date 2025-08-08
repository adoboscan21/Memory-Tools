# 🚀 Memory Tools CLI Client Documentation 🚀

The `memory-tools-client` is an interactive command-line interface (CLI) for direct, secure interaction with the `memory-tools-server` via its custom TLS-encrypted TCP protocol.

---

### ▶️ How to Run

To start the client, you must provide the address of the `memory-tools-server`. You can also include credentials for automatic login using flags.

**Locally:**

```bash
./bin/memory-tools-client
./bin/memory-tools-client -u admin -p adminpass -addr localhost:5876
```

**Docker 🐳:**

```bash
docker exec -it <container_id> ./memory-tools-client -u root -p rootpass localhost:5876
```

Once connected, you will see the message: `Connected securely to Memory Tools server at <address>.`

### 🚀 Contextual Mode with `use`

To simplify commands, you can enter a collection's "context". Once inside, you don't need to specify the collection name for most item and index operations, making the CLI faster and easier to use.

- ➡️ **`use <collection_name>`**: Enter the context of a specific collection.
  - **Example**:
    ```bash
    use products
    ```
    The prompt will change to `root/products>`, and now commands like `get laptop-01` are equivalent to `collection item get products laptop-01`.
- ⬅️ **`use exit`**: Exit the current collection context and return to the root prompt.

### 👥 User and Permission Management (Admins)

Authentication is required to execute most commands. User and permission management requires special privileges.

- 🔐 **`login <username> <password>`**
  - **Description**: Authenticates the connection with the server.
- ➕ **`user create <username> <password> <permissions_json>`**
  - **Description**: Creates a new user with a password and a set of permissions.
  - **Example**: `user create salesuser strongpass123 {"sales":"write", "products":"read"}`
- 🔄 **`user update <username> <permissions_json>`**
  - **Description**: Completely replaces an existing user's permissions with the new set provided.
  - **Example**: `user update salesuser {"*":"read"}`
- 🗑️ **`user delete <username>`**
  - **Description**: Permanently deletes a user from the system.
- 🔑 **`update password <target_username> <new_password>`**
  - **Description**: Updates a user's password. The `root` user can change anyone's password.

### 👑 Main Store Commands (Root Only)

These commands operate on the primary key-value store and are **available only to the `root` user**.

- 💾 **`set <key> <value_json> [ttl_seconds]`**
  - **Description**: Sets a key-value pair.
- 📥 **`get <key>`**
  - **Description**: Retrieves the value associated with a key.

### 🛡️ Admin & Maintenance (Root Only)

These commands are for low-level administrative operations and are **available only to the `root` user**.

- 📦 **`backup`**
  - **Description**: Triggers a full, manual backup of all server data immediately.
- 🔙 **`restore <backup_directory_name>`**
  - **Description**: **Destructive Action!** Restores the entire server state from a specific backup.

### 🗂️ Collection Commands

#### Collection Management

- ✨ **`collection create <collection_name>`**
- 🔥 **`collection delete [collection_name]`** (name is optional when in context)
- 📜 **`collection list`**

#### 📄 Collection Item Operations

**Note**: The `<collection>` parameter is optional when you are in a collection's context using the `use` command.

- ✅ **`set [<key>] <value_json> [ttl]`** (or `collection item set ...`)
  - **Description**: Saves an item. If `<key>` is omitted, a UUID is generated.
  - **Example (in `products` context)**: `set laptop-01 {"name": "Laptop Pro", "price": 1500}`
- 📤 **`get <key>`** (or `collection item get ...`)
  - **Description**: Gets an item by its key.
- ✍️ **`update <key> <patch_json>`** (or `collection item update ...`)
  - **Description**: Partially updates an item.
- 🗑️ **`delete <key>`** (or `collection item delete ...`)
  - **Description**: Deletes an item.
- 📋 **`list`** (or `collection item list ...`)
  - **Description**: Lists all items in the current collection.

#### ⚡ Batch Operations

- **`collection item set many <collection> <json_array>`**
- **`collection item update many <collection> <patch_json_array>`**
- **`collection item delete many <collection> <keys_json_array>`**

### 🔍 Index Commands

**Note**: The `<collection>` parameter is optional when in a collection's context.

- 📈 **`index create <field_name>`** (or `collection index create ...`)
- 📜 **`index list`** (or `collection index list ...`)
- 🔥 **`index delete <field_name>`** (or `collection index delete ...`)

### ❓ Collection Query Command (`query`)

This powerful command lets you filter, sort, join, and aggregate data. The `<collection>` name is optional if you are in a context.

- **`query [collection] <query_json>`**
  - **Description**: Executes a complex query defined in the `query_json`.
  - **Example**: `query products {"filter": {"field": "category", "op": "=", "value": "Electronics"}, "limit": 5}`

#### Query Structure

The `query_json` object can contain the following keys:

| Key              | Type      | Description                                         |
| ---------------- | --------- | --------------------------------------------------- |
| `filter`         | object    | Conditions to select items (like a `WHERE` clause). |
| `orderBy`        | array     | Sorts the results.                                  |
| `limit`          | number    | Restricts the number of results.                    |
| `offset`         | number    | Skips results, used for pagination.                 |
| `count`          | boolean   | Returns a count of matching items.                  |
| `distinct`       | string    | Returns unique values for a field.                  |
| `groupBy`        | array     | Groups results for aggregation.                     |
| `aggregations`   | object    | Defines functions like `sum`, `avg`, `count`.       |
| `having`         | object    | Filters results after aggregation.                  |
| **`projection`** | **array** | **Selects which fields to return.**                 |
| **`lookups`**    | **array** | **Joins data from other collections.**              |

### 🧠 Deep Query Examples

Here are advanced examples showcasing the depth of the query engine.

- **Complex Nested Filtering**
  - Find sales in the 'North' region that are either 'pending' OR have an amount greater than 1000.
  ```bash
  query sales {"filter":{"and":[{"field":"region","op":"=","value":"North"},{"or":[{"field":"status","op":"=","value":"pending"},{"field":"amount","op":">","value":1000}]}]}}
  ```
- **Multi-Aggregation Query**
  - For each salesperson, calculate their total sales (`SUM`), average sale amount (`AVG`), and number of sales (`COUNT`).
  ```bash
  query sales {"aggregations":{"total_sold":{"func":"sum","field":"amount"},"average_sale":{"func":"avg","field":"amount"},"deal_count":{"func":"count","field":"_id"}},"group_by":["salesperson"]}
  ```
- **Joining Collections with `lookups` and `projection`**
  - **Goal**: Create a report from an `inventory_status` collection, joining data from `products` and `suppliers` to get a complete view, showing only the product name, stock, and supplier name.
  ```bash
  query inventory_status {"lookups":[{"from":"products","localField":"productId","foreignField":"_id","as":"product"},{"from":"suppliers","localField":"product.supplierId","foreignField":"_id","as":"supplier"}],"projection":["product.name","stock","supplier.name"]}
  ```

### 💻 Client-Side Commands

These are client utilities and are not sent to the server.

- ℹ️ **`help`**: Displays the list of available commands and their usage.
- 💨 **`clear`**: Clears the terminal screen.
- 🚪 **`exit`**: Closes the connection and exits the client.
