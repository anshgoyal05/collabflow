# CollabFlow — A Scalable Real-Time Collaborative Platform

CollabFlow is a production-grade, distributed real-time collaborative document editor. This repository hosts the backend real-time engine built with Go and WebSockets.

---

## Getting Started

### Prerequisites

- Go 1.26 or higher
- Docker & Docker Compose (optional, for containerised execution)

### Installation

1. Clone the repository and navigate into the `collabflow` directory:
   ```bash
   cd collabflow
   ```

2. Tidy the Go module dependencies:
   ```bash
   go mod tidy
   ```

---

## Running the Server

### Local Execution (Standard)

Run the WebSocket server directly using Go:
```bash
go run cmd/websocket-server/main.go
```
The server will start listening on port `8080` (or `PORT` specified in the environment).

### Local Execution (Docker Compose)

Run the server inside a Docker container:
```bash
docker compose up --build
```
This builds the image defined in the multi-stage `Dockerfile` and maps port `8080`.

---

## Testing

### Automated Tests

To run the unit test suite verifying client registration and message broadcasting:
```bash
go test -v ./internal/websocket/...
```

### Manual Verification

Use a WebSocket client CLI such as `wscat` to test real-time communication manually:

1. Install `wscat` if you haven't:
   ```bash
   npm install -g wscat
   ```

2. Open two separate terminal windows.

3. In Terminal 1, connect User A:
   ```bash
   wscat -c "ws://localhost:8080/ws?userId=userA"
   ```

4. In Terminal 2, connect User B:
   ```bash
   wscat -c "ws://localhost:8080/ws?userId=userB"
   ```

5. In Terminal 1, send a message in JSON format:
   ```json
   {"type":"insert","userId":"userA","documentId":"doc123","content":"Hello"}
   ```

6. Verify that Terminal 2 receives the identical message instantly.
