# Build Stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build statically-linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/synchop ./cmd/synchop

# Runtime Stage
FROM alpine:3.21

# Install runtime dependencies required by SyncHop:
# - rsync: for block-level delta transfers
# - openssh-client: for rsync over SSH
# - ca-certificates: for HTTPS / WebDAV
# - tzdata: for accurate schedule & audit timestamps
RUN apk add --no-cache \
    rsync \
    openssh-client \
    ca-certificates \
    tzdata

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/synchop /usr/local/bin/synchop

# Create directory for persistent runtime data (config.json, history.json, staging)
RUN mkdir -p /data /data/staging

ENV PORT=8080 \
    DATA_DIR=/data

EXPOSE 8080

VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/synchop"]
