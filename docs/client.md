## Memory Tools CLI Client Documentation

The `memory-tools-client` is an interactive command-line interface (CLI) for direct, secure interaction with the `memory-tools-server` via its custom TLS-encrypted TCP protocol.

---

### ## How to Run

To start the client, you must provide the address of the `memory-tools-server`. You can also include credentials for automatic login using flags.

**Locally:**

```bash
./bin/memory-tools-client
./bin/memory-tools-client -u admin -p adminpass localhost:5876
```

**Via Docker Compose:**

```bash
docker exec -it <container_id> ./memory-tools-client -u root -p rootpass localhost:5876
```

Once connected, you will see the message: `Connected securely to Memory Tools server at <address>.`

---

### ## User and Permission Management (Admins)

Authentication is required to execute most commands. User and permission management requires special privileges.

- **`login <username> <password>`**
  - **Description**: Authenticates the connection with the server.
  - **Example**:
    ```bash
    login root rootpass
    ```
- **`user create <username> <password> <permissions_json>`**
  - **Description**: Creates a new user with a password and a set of permissions defined in a JSON object.
  - **Permissions Required**: Write access (`"write"`) to the `_system` collection (typically `root` user only).
  - **Example**: Create a user who can write to the `sales` collection and read from the `products` collection.
    ```bash
    user create salesuser strongpass123 {"sales":"write", "products":"read"}
    ```
- **`user update <username> <permissions_json>`**
  - **Description**: Completely replaces an existing user's permissions with the new set provided.
  - **Permissions Required**: Write access to the `_system` collection.
  - **Example**: Update the `salesuser` to have read-only access to all collections.
    ```bash
    user update salesuser {"*":"read"}
    ```
- **`user delete <username>`**
  - **Description**: Permanently deletes a user from the system.
  - **Permissions Required**: Write access to the `_system` collection.
  - **Example**:
    ```bash
    user delete salesuser
    ```
- **`update password <target_username> <new_password>`**
  - **Description**: Updates a user's password. A regular user can only change their own password. The `root` user can change anyone's password.
  - **Permissions Required**: Must be authenticated. To change _another_ user's password, you must be `root`.
  - **Example**:
    ```bash
    update password salesuser newSecurePass456
    ```

---

### ## Main Store Commands (Root Only)

These commands operate on the primary key-value store and are **available only to the `root` user**.

- **`set <key> <value_json> [ttl_seconds]`**
  - **Description**: Sets a key-value pair. `ttl_seconds` (optional) is the time-to-live in seconds.
  - **Example**:
    ```bash
    set server:config {"version": "2.1", "active": true} 3600
    ```
- **`get <key>`**
  - **Description**: Retrieves the value associated with a key.
  - **Example**:
    ```bash
    get server:config
    ```

---

### ## Collection Commands

#### Collection Management

- **`collection create <collection_name>`**
  - **Description**: Creates a new collection.
  - **Example**:
    ```bash
    collection create products
    ```
- **`collection delete <collection_name>`**
  - **Description**: Deletes an entire collection and all its data.
  - **Example**:
    ```bash
    collection delete old_logs
    ```
- **`collection list`**
  - **Description**: Lists the names of all collections you have permission to read.
  - **Example**:
    ```bash
    collection list
    ```

#### Collection Item Operations

- **`collection item set <collection> [<key>] <value_json> [ttl]`**
  - **Description**: Saves an item in a collection. If `<key>` is omitted, a unique UUID is generated and also injected into the JSON as the `_id` field.
  - **Example (with key)**:
    ```bash
    collection item set products laptop-01 {"name": "Laptop Pro", "price": 1500}
    ```
  - **Example (without key)**:
    ```bash
    collection item set products {"name": "RGB Keyboard", "price": 120}
    ```
- **`collection item get <collection> <key>`**
  - **Description**: Gets an item from a collection by its key.
  - **Example**:
    ```bash
    collection item get products laptop-01
    ```
- **`collection item update <collection> <key> <patch_json>`**
  - **Description**: Partially updates an existing item by applying the fields from the `patch_json`.
  - **Example**:
    ```bash
    collection item update products laptop-01 {"price": 1450, "stock": 45}
    ```
- **`collection item delete <collection> <key>`**
  - **Description**: Deletes an item from a collection.
  - **Example**:
    ```bash
    collection item delete products laptop-01
    ```
- **`collection item list <collection>`**
  - **Description**: Lists all items in a collection.
  - **Example**:
    ```bash
    collection item list products
    ```

#### Batch Operations

- **`collection item set many <collection> <json_array>`**
  - **Description**: Inserts multiple items at once.
  - **Example**:
    ```bash
    collection item set many sales [{"salesperson": "ana", "amount": 200}, {"salesperson": "luis", "amount": 350}]
    ```
- **`collection item update many <collection> <patch_json_array>`**
  - **Description**: Updates multiple items at once. The array must contain objects with an `_id` and the `patch` to apply.
  - **Example**:
    ```bash
    collection item update many sales [{"_id": "sale-uuid-1", "patch": {"status": "shipped"}}, {"_id": "sale-uuid-2", "patch": {"status": "shipped"}}]
    ```
- **`collection item delete many <collection> <keys_json_array>`**

  - **Description**: Deletes multiple items at once by providing a JSON array of their key strings.
  - **Example**:

    ```bash
    collection item delete many products ["product-uuid-1", "product-uuid-2"]
    ```

---

### ## Index Commands

- **`collection index create <collection> <field_name>`**

  - **Description**: Creates an index on a field to accelerate queries.
  - **Example**:

    ```bash
    collection index create products category
    ```

- **`collection index list <collection>`**
  - **Description**: Lists indexed fields in a collection.
  - **Example**:
    ```bash
    collection index list products
    ```
- **`collection index delete <collection> <field_name>`**
  - **Description**: Deletes an index.
  - **Example**:

    ```bash
    collection index delete products category
    ```

---

### ## Collection Query Command (`collection query`)

This powerful command lets you filter, sort, paginate, and aggregate data.

- **`collection query <collection> <query_json>`**
  - **Description**: Executes a complex query defined in the `query_json`.
  - **Example**: Find up to 5 products in the "Electronics" category.
    ```bash
    collection query products {"filter": {"field": "category", "op": "=", "value": "Electronics"}, "limit": 5}
    ```

### ### Deep Query Examples

Here are advanced examples showcasing the depth of the query engine. Assume a `sales` collection with fields like `region`, `salesperson`, `amount`, `status`, and `date`.

- **Complex Nested Filtering**
  - Find sales in the 'North' region that are either 'pending' OR have an amount greater than 1000.
  ```
  collection query sales {"filter":{"and":[{"field":"region","op":"=","value":"North"},{"or":[{"field":"status","op":"=","value":"pending"},{"field":"amount","op":">","value":1000}]}]}}
  ```
- **Filtering with `NOT`**
  - Find all sales that are NOT in the 'North' region.
  ```
  collection query sales {"filter":{"not":{"field":"region","op":"=","value":"North"}}}
  ```
- **Multi-Field Sorting**
  - List sales ordered first by region (A-Z), then by amount (highest to lowest).
  ```bash
  collection query sales {"order_by":[{"field":"region","direction":"asc"},{"field":"amount","direction":"desc"}]}
  ```
- **Multi-Aggregation Query**
  - For each salesperson, calculate their total sales (`SUM`), average sale amount (`AVG`), and number of sales (`COUNT`).
  ```bash
  collection query sales {"aggregations":{"total_sold":{"func":"sum","field":"amount"},"average_sale":{"func":"avg","field":"amount"},"deal_count":{"func":"count","field":"_id"}},"group_by":["salesperson"]}
  ```
- **Aggregation with `HAVING` Clause**
  - Find the total sales for each region, but **only show regions where the total is greater than 5000**.
  ```bash
  collection query sales {"aggregations":{"total_regional_sales":{"func":"sum","field":"amount"}},"group_by":["region"],"having":{"field":"total_regional_sales","op":">","value":5000}}
  ```
- **Putting It All Together: A Deep, Combined Query**
  - **Goal**: From sales in the 'East' or 'West' regions, find the top 5 salespersons by their total sales amount, but only include salespersons whose average sale is over $200.
  ```bash
  collection query sales {"filter":{"field":"region","op":"in","value":["East","West"]},"aggregations":{"total_sales":{"func":"sum","field":"amount"},"average_sale":{"func":"avg","field":"amount"}},"group_by":["salesperson"],"having":{"field":"average_sale","op":">","value":200},"order_by":[{"field":"total_sales","direction":"desc"}],"limit":5}
  ```
