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
# NOTE: These are generated inside the builder and copied to the final stage.
RUN mkdir -p certificates && \
    openssl req -x509 -newkey rsa:4096 -nodes \
    -keyout certificates/server.key -out certificates/server.crt \
    -days 3650 -subj "/CN=localhost" \
    -addext "subjectAltName = DNS:localhost,IP:127.0.0.1"

# Build the Go applications with specific names.
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix nocgo -o memory-tools-server ./main.go
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix nocgo -o memory-tools-client ./cmd/client/main.go

# --- Stage 2: Production ---
# Use a minimal base image for the production environment.
FROM alpine:latest

# Set the working directory where the data and config will live.
WORKDIR /data

# Create necessary directories. The WORKDIR will also be the data directory.
RUN mkdir -p certificates collections

# Copy the binaries to a standard location in the PATH.
COPY --from=builder /app/memory-tools-server /usr/local/bin/
COPY --from=builder /app/memory-tools-client /usr/local/bin/

# Copy the certificates. The server looks for a relative 'certificates' directory.
COPY --from=builder /app/certificates/server.crt ./certificates/
COPY --from=builder /app/certificates/server.key ./certificates/

# Copy the configuration file.
COPY config.json .

# Expose the server port.
#EXPOSE 5876

# Command to execute the entrypoint script when the container starts.
CMD "memory-tools-server"