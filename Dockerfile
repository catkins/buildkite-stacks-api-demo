FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o scheduler .

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates curl bash git

# Install buildkite-agent
RUN curl -fsSL https://raw.githubusercontent.com/buildkite/agent/main/install.sh | bash -s -- --token=xxx --no-start

# Set buildkite-agent path
ENV BUILDKITE_AGENT_PATH=/root/.buildkite-agent/bin/buildkite-agent

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/scheduler .

ENTRYPOINT ["/app/scheduler"]
