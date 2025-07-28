# Running Memory Tools

This document provides instructions on how to run, compile, and deploy the Memory Tools application.

---

## Prerequisites

Make sure you have [Go installed (version go1.21 or higher)](https://go.dev/doc/install). For Docker deployment, you'll need [Docker Desktop](https://www.docker.com/products/docker-desktop/) or Docker Engine installed.

---

## 1. How to Run (Directly)

1.  **Clone the repository:**

```bash

git clone [https://github.com/adoboscan21/Memory-Tools.git](https://github.com/adoboscan21/Memory-Tools.git)

cd memory-tools

```

2.  **Create a `config.json` file** in the project root with your desired settings. This file specifies how your application behaves (e.g., ports, timeouts, intervals). If omitted or not found, default values will be used.

### Example `config.json`

```json

{

"port": ":8080",

"read_timeout": "5s",

"write_timeout": "10s",

"idle_timeout": "120s",

"shutdown_timeout": "10s",

"snapshot_interval": "5m",

"enable_snapshots": true,

"ttl_clean_interval": "1m"

}

```

3.  **Run the application:**

```bash

go run .

```

You'll see messages in the console indicating the server is listening on the configured port (default `:8080`), that scheduled snapshots are enabled, and the TTL cleaner is starting (if configured).

You can also specify a custom config file path:

```bash

go run . --config=./path/to/your_custom_config.json

```

---

## 2. How to Compile

1.  **Clone the repository** (as described above).

2.  Navigate to the **project root** in your terminal:

```bash

go build .

```

This will create an executable binary (e.g., `memory-tools` on Linux/macOS, `memory-tools.exe` on Windows) in your project root. You can then run it directly:

```bash

./memory-tools # or memory-tools.exe on Windows

```

You can also pass the config flag to the compiled binary:

```bash

./memory-tools --config=./path/to/your_custom_config.json

```

---

## 3. How to Deploy on Docker

1.  **Clone the repository** (as described above).

2.  Navigate to the **project root** in your terminal:

```bash

docker compose up -d --build

```

This command will:

\* Build the Docker image for the application.

\* Create and start the Docker container in detached mode (`-d`).

\* Mount a volume for persistent data, ensuring your `in-memory.mtdb` and `collections` directory are saved outside the container.
