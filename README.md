# Memory Tools 🚀

Memory Tools is a high-performance, sharded in-memory key-value **and document** store designed for speed, security, and scalability. It provides a robust backend for your applications, supporting flexible data organization through collections, persistent indexing, a powerful query engine, and a granular user permission system, all secured over a TLS-encrypted protocol.

---

## ✨ Features

- **Sharded In-Memory Storage:** Utilizes an efficient sharding design to distribute data and ensure fast, concurrent access across multiple CPU cores.
- **Data Persistence:** Atomically saves data to disk in an optimized binary format and reloads on startup, ensuring durability and preventing data corruption.
- **Secure TLS Encryption:** All communications over the TCP interface are encrypted with TLS 1.2+, guaranteeing the security of your data in transit.
- **Time-to-Live (TTL):** Assigns a time-to-live to keys so they expire automatically, ideal for cache data, sessions, or temporary records.
- **Collections:** Organizes your data into named collections, acting as independent, sharded data stores within the same engine.
- **Granular User Permissions:** A robust user management system allows for creating users and assigning specific read/write permissions per collection. Features a superuser (`root`) for administrative tasks.
- **Persistent Field Indexing:** Create indexes on any field within your JSON documents to dramatically accelerate query performance by avoiding full collection scans. Indexes are persisted to disk and rebuilt on startup.
- **Advanced Query Engine:** Performs complex, SQL-like queries on JSON data with filters (`WHERE`, `AND`, `OR`, `NOT`, `LIKE`, `IN`), ordering (`ORDER BY`), pagination (`LIMIT`/`OFFSET`), aggregations (`COUNT`, `SUM`, `AVG` with `GROUP BY`), post-aggregation filtering (`HAVING`), and unique value retrieval (`DISTINCT`).
- **Batch Operations:** Execute commands on multiple items at once, such as `set many`, `update many`, and `delete many`, for greater efficiency and reduced network latency.
- **Graceful Shutdown:** The engine handles a clean application shutdown, ensuring all pending writes are completed and data is safely saved before exiting.
- **Docker-Ready:** Easily deploy and manage the server with Docker and Docker Compose.

---

## ⚙️ Quick Start with Docker Compose

To get the Memory Tools server up and running quickly, follow these steps:

1. **Build the Docker Image:**

   ```bash
   docker-compose build
   ```

2. **Start the Services:**

   ```bash
   docker-compose up -d
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

For a full list of commands and detailed examples, see the **[`docs/client.md`](https://github.com/adoboscan21/Memory-Tools/blob/dev/docs/client.md)** file.

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
