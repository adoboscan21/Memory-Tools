# Memory Tools

Memory Tools is a high-performance, sharded in-memory key-value store with support for collections and persistent storage. It offers a secure TLS-encrypted TCP interface and a convenient HTTP API.

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
    This starts the main database server (on port `8080`) and the HTTP API server (on port `8081`).

---

## How to Use

### HTTP API  **RECOMMENDED ONLY FOR DEVELOPMENT USE ¡IMPORTANT!**

Access the API server at `http://localhost:8081`. Send `POST` requests to the `/command` endpoint with a JSON body.

**Example: Set a value**

```bash
curl -X POST -H "Content-Type: application/json" -d '{"command": "set", "args": "mykey", "value": {"message": "Hello!"}, "ttl": 300}' http://localhost:8081/command
```

**Example: Get a value**

```bash
curl -X POST -H "Content-Type: application/json" -d '{"command": "get", "args": "mykey"}' http://localhost:8081/command
```

For a full list of commands and detailed request/response examples, see [`docs/api.md`](https://github.com/adoboscan21/Memory-Tools/blob/main/docs/api.md).

### CLI Client

You can use the interactive CLI client inside the running server container to connect directly to the database:

```bash
docker exec -it memory-tools-server memory-tools-client localhost:8080
```

For a full list of commands and detailed examples, see [`docs/client.md`](https://github.com/adoboscan21/Memory-Tools/blob/main/docs/client.md).

Once connected, type `help` for available commands.
