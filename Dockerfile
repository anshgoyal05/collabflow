# Build stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o websocket-server ./cmd/websocket-server/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o redis-worker ./cmd/redis-worker/main.go

# Run stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/websocket-server .
COPY --from=builder /app/redis-worker .

EXPOSE 8080 8081 8082

CMD ["./websocket-server"]
