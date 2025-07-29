#!/bin/sh

# Start the main Database server in the background.
echo "Starting Memory Tools DB Server..."
# Use the new binary name: memory-tools-server
# It runs in the background.
memory-tools-server &

# The CLI client (memory-tools-client) is compiled within the container,
# but it's an interactive tool. We don't start it in the background here.
echo "Memory Tools CLI client compiled as 'memory-tools-client'."
echo "To use it, run 'docker exec -it <container_name_or_id> memory-tools-client <db_addr>'."

# Keep the container alive indefinitely.
# This is crucial for the background processes to continue running.
echo "All services (DB) started. Keeping container alive..."
tail -f /dev/null