# Stage 1: Build
FROM golang:1.22-alpine AS builder

# Set the working directory
WORKDIR /app

# Copy go.mod and go.sum
COPY ../go.mod ../go.sum ./

# Download dependencies
RUN go mod download

# Copy the application source code
COPY ../ ./

# Build the Go application
RUN go build -o main ./cmd

# Stage 2: Runtime
FROM alpine:3.13

# Set the working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/main .

# Copy the local.json configuration file
COPY ../config/local.json ./config/

# Expose the port the app runs on (if applicable)
EXPOSE 8080

# Define the command to run your application
CMD ["./main"]
