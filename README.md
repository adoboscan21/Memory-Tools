# Memory Tools 🚀

**Memory Tools** is a high-performance, sharded in-memory key-value and document store designed for speed, security, and scalability. It provides a robust backend for your applications, supporting flexible data organization through collections, persistent indexing, a powerful query engine, and a granular user permission system, all secured over a TLS-encrypted protocol.

---

## ✨ Features

- 🚀 **High-Performance Concurrent Architecture:** At its core, Memory Tools uses an efficient **sharding design** to distribute data and minimize lock contention, allowing for massive concurrency across CPU cores. Collection writes are handled by an **asynchronous persistence queue**, resulting in non-blocking, lightning-fast write operations for the client.
- 🧠 **Hot/Cold Data Tiering:** Manage datasets far larger than the available RAM. Memory Tools keeps recent and frequently accessed ("hot") data in memory for maximum speed, while older ("cold") data resides on disk. Query and modification operations **transparently access both tiers**, and cold data can be updated or deleted on-disk without needing to be loaded into memory.
- 💾 **Robust Data Persistence:** Data is atomically saved to disk in an optimized binary format. The use of the **write-to-`.tmp`-and-rename strategy** ensures that your data files are never corrupted, even in the event of a crash. The system reloads all "hot" data on startup for complete durability.
- 🛡️ **Automated Backup & Restore System:** Go beyond simple persistence with a full-featured backup system. It performs **periodic, verifiable backups** to timestamped directories, manages a **retention policy** to clean up old files, and allows for a full manual **restore** from any backup point.
- 📈 **High-Performance B-Tree Indexing:** Drastically accelerate query performance by creating indexes on any field within your JSON documents. Unlike simple hash indexes, the use of **B-Trees** enables extremely fast **range scans (`>`, `<`, `between`)** in addition to equality lookups. This avoids costly full-collection scans for filtered queries.
- 🔍 **Advanced Query Engine:** Don't just get keys; query your JSON documents like a relational database. The engine is backed by a **query optimizer** that leverages available indexes to execute operations in the most efficient way possible. It supports:
  - **Rich Filtering**: `WHERE`, `AND`, `OR`, `NOT`, `LIKE`, `IN`, `BETWEEN`, `IS NULL`.
  - **Data Shaping**: `ORDER BY`, `LIMIT`, `OFFSET`, and `DISTINCT`.
  - **Powerful Aggregations**: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` with `GROUP BY`.
  - **Post-Aggregation Filtering**: A full `HAVING` clause to filter your grouped results.
- ⚡ **Efficient Batch Operations:** Execute commands on multiple items at once for greater efficiency and reduced network latency. `set many`, `update many`, and `delete many` commands are fully supported and optimized to work with both hot and cold data.
- 🔐 **Full Security Suite:** Security is built-in, not an afterthought.
  - **TLS Encryption:** All communication is encrypted with TLS 1.2+, protecting data in transit.
  - **Strong Authentication:** Passwords are never stored in plain text, using `bcrypt` hashing.
  - **Granular Permissions:** A robust user management system allows for creating users and assigning specific `read`/`write` permissions per collection, with wildcard support.
  - **Restricted Superuser**: A `root` user with administrative privileges is restricted to **localhost connections only**, preventing remote admin access.
- 🧹 **Automatic Data & Memory Management:** The engine works for you in the background.
  - **TTL (Time-to-Live):** Assign a time-to-live to keys so they expire automatically. A background cleaner periodically purges them.
  - **Data Compaction:** Deletes on "cold" storage use tombstones. A background compaction worker rewrites data files to permanently remove these records and reclaim disk space.
  - **Idle Memory Release:** The server monitors for periods of inactivity and automatically releases unused memory back to the operating system.

---

## ⚙️ Quick Start with Docker Compose

To get the Memory Tools server up and running quickly, follow these steps:

1. **Copy the .env file:**

   ```bash
   cp .example.env .env
   ```

2. **Start the services:**

   ```bash
   docker compose up -d --build
   ```

   This starts the main database server on port `5876`.

---

## 🛠️ Manual Installation and Build

### Prerequisites

You need **Go version 1.21 or higher** to build and run this project.

### 1. Generate TLS Certificates 🔒

Memory Tools uses TLS for all its communications. You must generate a self-signed certificate pair and place it in the `./certificates/` directory.

1. **Create the directory:**
   ```bash
   mkdir -p certificates
   ```
2. **Run the following OpenSSL command to generate a certificate and key:**

   ```bash
   openssl req -x509 -newkey rsa:4096 -nodes -keyout certificates/server.key -out certificates/server.crt -days 3650 -subj "/CN=localhost" -addext "subjectAltName = DNS:localhost,IP:127.0.0.1"
   ```

### 2. Build and Run

- **Build the Database Server and Client:**
  ```bash
  go build -o ./bin/memory-tools-server .
  go build -o ./bin/memory-tools-client ./cmd/client
  ```
- **Run the Server Directly:**
  ```bash
  ./bin/memory-tools-server
  ```

---

## 🖥️ CLI Client

You can use the interactive CLI client to connect to and operate the server.

- **To connect to the server running in Docker:**
  ```bash
  docker exec -it <container-id> ./memory-tools-client
  ```
- **For a direct and authenticated connection:**

  ```bash
  ./bin/memory-tools-client -u admin -p adminpass
  ```

> **Important:** The default password for the `admin` user is `adminpass`, and for the `root` user (only accessible from localhost) is `rootpass`. Please change these immediately in a production environment using the `update password` command.

For a full list of commands and detailed examples, see the **[`docs/client.md`](https://github.com/adoboscan21/Memory-Tools/blob/main/docs/client.md)** file.

Once connected, type `help` for available commands.

---

## Support the Project!

Hello! I'm the developer behind **Memory Tools**. This is an open-source project.

I've dedicated a lot of time and effort to this project, and with your support, I can continue to maintain it, add new features, and make it better for everyone.

---

### How You Can Help

Every contribution, no matter the size, is a great help and is enormously appreciated. If you would like to support the continued development of this project, you can make a donation via PayPal.

You can donate directly to my PayPal account by clicking the link below:

**[Click here to donate](https://paypal.me/AdonayB?locale.x=es_XC&country.x=VE)**

---

### Other Ways to Contribute

If you can't donate, don't worry! You can still help in other ways:

- **Share the project:** Talk about it on social media or with your friends.
- **Report bugs:** If you find a problem, open an issue on GitHub.
- **Contribute code:** If you have coding skills, you can help improve the code.
  Thank you for your support!
