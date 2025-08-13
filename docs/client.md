# 🚀 Memory Tools CLI Client Documentation 🚀

The `memory-tools-client` is a command-line interface (CLI) for direct, secure interaction with the `memory-tools-server` via its custom TLS-encrypted TCP protocol.

---

### ▶️ How to Run

To start the client, you must provide the address of the `memory-tools-server`. You can also include credentials for automatic login using flags.

**Locally:**

```bash
./bin/memory-tools-client
./bin/memory-tools-client -u admin -p adminpass localhost:5876
```

**Docker 🐳:**

```bash
sudo docker exec -it <containerId> memory-tools-client -u root -p rootpass localhost:5876
```

Once connected, you will see the message: `Connected securely to Memory Tools server at <address>.`

---

### 👥 User and Permission Management (Admins)

Authentication is required to execute most commands. User and permission management requires special privileges.

- 🔐 **`login <username> <password>`**
  - **Description**: Authenticates the connection with the server.
- ➕ **`user create <username> <password> <permissions_json|path>`**
  - **Description**: Creates a new user with a password and a set of permissions. The permissions can be provided as a JSON string or a path to a `.json` file.
  - **Example**: `user create salesuser strongpass123 {"sales":"write", "products":"read"}`
- 🔄 **`user update <username> <permissions_json|path>`**
  - **Description**: Completely replaces an existing user's permissions with the new set provided.
  - **Example**: `user update salesuser {"*":"read"}`
- 🗑️ **`user delete <username>`**
  - **Description**: Permanently deletes a user from the system.
- 🔑 **`update password <target_username> <new_password>`**
  - **Description**: Updates a user's password. The `root` user can change anyone's password.

---

### 👑 Main Store Commands (Root Only)

These commands operate on the primary key-value store and are **available only to the `root` user**.

- 💾 **`set <key> <value_json> [ttl_seconds]`**
  - **Description**: Sets a key-value pair.
- 📥 **`get <key>`**
  - **Description**: Retrieves the value associated with a key.

---

### 🛡️ Admin & Maintenance (Root Only)

These commands are for low-level administrative operations and are **available only to the `root` user**.

- 📦 **`backup`**
  - **Description**: Triggers a full, manual backup of all server data immediately.
- 🔙 **`restore <backup_directory_name>`**
  - **Description**: **Destructive Action!** Restores the entire server state from a specific backup.

---

### 📦 Transactions

Memory Tools supports ACID-like transactions, allowing you to group multiple write operations (`set`, `update`, `delete`) and execute them as a single, atomic unit. This ensures that either all operations succeed or none do.

- **`begin`**
  - **Description**: Starts a new transaction block. The command prompt will change to include a `[TX]` indicator to show you are in transaction mode.
  - **Note**: While in a transaction, most read commands (`get`, `list`, `query`) are disabled. The focus is on queueing writes.
- **`commit`**
  - **Description**: Atomically applies all the commands queued since `begin` was executed. If any operation fails on the server side, the entire transaction is automatically rolled back.
- **`rollback`**
  - **Description**: Discards all commands queued since `begin` was executed and exits the transaction block.

---

### 🗂️ Collection Commands

#### Collection Management

- ✨ **`collection create <collection_name>`**
- 🔥 **`collection delete <collection_name>`**
- 📜 **`collection list`**

#### 📄 Collection Item Operations

**Note**: The `<value_json>` or `<patch_json>` can be provided as a raw string or a path to a local `.json` file (e.g., `my_data.json`).

- ✅ **`collection item set <collection> [<key>] <value_json|path> [ttl]`**
  - **Description**: Saves an item. If `<key>` is omitted, a UUID is automatically generated.
  - **Example**: `collection item set products laptop-01 {"name": "Laptop Pro", "price": 1500}`
- 📤 **`collection item get <collection> <key>`**
  - **Description**: Gets an item by its key.
- ✍️ **`collection item update <collection> <key> <patch_json|path>`**
  - **Description**: Partially updates an item with the fields from the patch.
- 🗑️ **`collection item delete <collection> <key>`**
  - **Description**: Deletes an item by its key.
- 📋 **`collection item list <collection>`**
  - **Description**: **(Root only)** Lists all items in the specified collection.

#### ⚡ Batch Operations

- **`collection item set many <collection> <json_array|path>`**
- **`collection item update many <collection> <patch_json_array|path>`**
- **`collection item delete many <collection> <keys_json_array|path>`**

---

### 🔍 Index Commands

- 📈 **`collection index create <collection> <field_name>`**
- 📜 **`collection index list <collection>`**
- 🔥 **`collection index delete <collection> <field_name>`**

---

### ❓ Collection Query Command

This powerful command lets you filter, sort, join, and aggregate data. The `<query_json>` can be a raw string or a path to a local `.json` file.

- **`collection query <collection> <query_json|path>`**
  - **Description**: Executes a complex query defined in the `query_json`.
  - **Example**: `collection query products {"filter": {"field": "category", "op": "=", "value": "Electronics"}, "limit": 5}`

#### Query Structure

The `query_json` object can contain the following keys:

| Key            | Type    | Description                                   |
| -------------- | ------- | --------------------------------------------- |
| `filter`       | object  | Conditions to select items (`WHERE` clause).  |
| `order_by`     | array   | Sorts the results.                            |
| `limit`        | number  | Restricts the number of results.              |
| `offset`       | number  | Skips results, used for pagination.           |
| `count`        | boolean | Returns a count of matching items.            |
| `distinct`     | string  | Returns unique values for a field.            |
| `group_by`     | array   | Groups results for aggregation.               |
| `aggregations` | object  | Defines functions like `sum`, `avg`, `count`. |
| `having`       | object  | Filters results after aggregation.            |
| `projection`   | array   | Selects which fields to return.               |
| `lookups`      | array   | Joins data from other collections.            |

---

### 🧠 Deep Query Examples

Here are advanced examples showcasing the depth of the query engine.

- **Complex Nested Filtering**
  - Find sales in the 'North' region that are either 'pending' OR have an amount greater than 1000.
  ```bash
  collection query sales {"filter":{"and":[{"field":"region","op":"=","value":"North"},{"or":[{"field":"status","op":"=","value":"pending"},{"field":"amount","op":">","value":1000}]}]}}
  ```
- **Multi-Aggregation Query**
  - For each salesperson, calculate their total sales (`SUM`), average sale amount (`AVG`), and number of sales (`COUNT`).
  ```bash
  collection query sales {"aggregations":{"total_sold":{"func":"sum","field":"amount"},"average_sale":{"func":"avg","field":"amount"},"deal_count":{"func":"count","field":"_id"}},"group_by":["salesperson"]}
  ```
- **Joining Collections with `lookups` and `projection`**
  - **Goal**: Create a report from an `inventory_status` collection, joining data from `products` and `suppliers` to get a complete view, showing only the product name, stock, and supplier name.
  - The `localField` in the second lookup (`product.supplierId`) can reference a field from a previously joined document.
  ```bash
  collection query inventory_status {"lookups":[{"from":"products","localField":"productId","foreignField":"_id","as":"product"},{"from":"suppliers","localField":"product.supplierId","foreignField":"_id","as":"supplier"}],"projection":["product.name","stock","supplier.name"]}
  ```

---

### 💻 Client-Side Commands

These are client utilities and are not sent to the server.

- ℹ️ **`help`**: Displays the list of available commands and their usage.
- 💨 **`clear`**: Clears the terminal screen.
- 🚪 **`exit`**: Closes the connection and exits the client.
