# Use a Go base image for building the application
FROM golang:1.24.4-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files to download dependencies
COPY go.mod go.sum ./

# Download the dependencies
# Use go mod tidy to ensure all necessary dependencies are present
RUN go mod tidy

# Copy the application source code
COPY . .

# Build the Go application
# CGO_ENABLED=0 disables C linking to create a static binary
# -o memory-tools specifies the executable name
# ./main.go specifies the main entry file
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix nocgo -o memory-tools ./main.go

# --- Production Stage ---
# Use a minimal base image for the production environment
FROM alpine:latest

# Set the working directory
WORKDIR /root/

# Copy the executable from the build stage
COPY --from=builder /app/memory-tools .

# If your application needs the configuration file at runtime, copy it too.
# Assuming config.json might be at the root of memory-tools/
# If it's elsewhere or generated dynamically, adjust this line.
COPY config.json .

# Expose the port your application listens on (change 8080 if different)
EXPOSE 8080

# Command to run the application when the container starts
CMD ["./memory-tools"]