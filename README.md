# Memory Tools

Memory Tools is a high-performance, sharded in-memory key-value store with support for collections and persistent storage. It offers a secure TLS-encrypted TCP interface.

---

## Features

- **Sharded In-Memory Storage:** Efficient data distribution for performance.
- **Time-to-Live (TTL):** Keys can expire automatically.
- **Data Persistence:** Data is saved to disk and loaded on startup.
- **Collections:** Organize data into separate key-value namespaces.
- **TLS Encryption:** Secure communication for all interfaces.
- **Docker-Ready:** Easy deployment with Docker and Docker Compose.

---

## Quick Start with Docker Compose

Get the Memory Tools server and API up and running quickly:

1.  **Build the Docker Image:**
    ```bash
    docker-compose build
    ```
2.  **Start Services:**
    ```bash
    docker-compose up -d
    ```
    This starts the main database server (on port `8080`)

---

### CLI Client

You can use the interactive CLI client inside the running server container to connect directly to the database:

```bash
docker exec -it containerid memory-tools-client
```

```bash
docker exec -it containerid memory-tools-client -u user -p password localhost:8080
```

For a full list of commands and detailed examples, see [`docs/client.md`](https://github.com/adoboscan21/Memory-Tools/blob/main/docs/client.md).

Once connected, type `help` for available commands.
