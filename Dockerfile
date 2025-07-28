# Stage 1: Builder
# Use a Go base image to build the application.
FROM golang:1.24.4-alpine AS builder

# Set the working directory inside the container.
WORKDIR /app

# Copy go.mod and go.sum to download dependencies.
COPY go.mod go.sum ./

# Download dependencies.
RUN go mod tidy

# Copy the application source code.
COPY . .

# Install openssl for certificate generation.
RUN apk add --no-cache openssl

# Generate TLS certificates.
RUN openssl req -x509 -newkey rsa:4096 -nodes -keyout server.key -out server.crt -days 36500 -subj "/CN=localhost" -addext "subjectAltName = DNS:localhost,IP:127.0.0.1"

# Build the Go applications with specific names.
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix nocgo -o memory-tools-server ./main.go
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix nocgo -o memory-tools-client ./cmd/client/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix nocgo -o memory-tools-api ./cmd/api/main.go

# --- Stage 2: Production ---
# Use a minimal base image for the production environment.
FROM alpine:latest

# Create necessary directories and ensure 'certificates' is in /root/certificates
# so binaries can find it if they are in /usr/local/bin.
RUN mkdir -p /root/certificates /root/collections /usr/local/bin/

# Copy the binaries to the directory in the PATH.
COPY --from=builder /app/memory-tools-server /usr/local/bin/
COPY --from=builder /app/memory-tools-client /usr/local/bin/
COPY --from=builder /app/memory-tools-api /usr/local/bin/

# Copy the certificates and key to /root/certificates.
COPY --from=builder /app/server.crt /root/certificates/
COPY --from=builder /app/server.key /root/certificates/

# Copy the configuration file to /root/.
COPY config.json /root/

# Copy the entrypoint script and give it execution permissions.
COPY entrypoint.sh /root/
RUN chmod +x /root/entrypoint.sh

# Set the working directory where the app expects to find config.json and the certificates directory.
WORKDIR /root/

# Expose the server ports.
EXPOSE 8081

# Command to execute the entrypoint script when the container starts.
CMD ["/root/entrypoint.sh"]