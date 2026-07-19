# CollabFlow Architecture — Week 2 Distributed WebSocket Layer

This document describes the distributed architecture for CollabFlow's horizontally scalable real-time collaboration engine using Redis Pub/Sub.

---

## Distributed System Architecture

```mermaid
flowchart LR

A[User A]
B[User B]

S1[WebSocket Server 1\n:8081]
S2[WebSocket Server 2\n:8082]

R[(Redis Pub/Sub\n:6379)]

A -->|ws://localhost:8081/doc_123| S1
B -->|ws://localhost:8082/doc_123| S2

S1 -->|Publish document:doc_123| R
R -->|PSubscribe document:*| S2
R -->|PSubscribe document:*| S1
```

---

## Sequence Flow: Distributed Edit Broadcast

```mermaid
sequenceDiagram
    autonumber
    actor UserA as User A (Server 1)
    participant S1 as WebSocket Server 1
    participant Redis as Redis Pub/Sub
    participant S2 as WebSocket Server 2
    actor UserB as User B (Server 2)

    UserA->>S1: WebSocket message {"type":"insert", "documentId":"doc_123", "value":"Hello"}
    S1->>S1: Validate & stamp ServerID ("SERVER-1")
    S1->>Redis: PUBLISH document:doc_123 payload
    Redis-->>S1: Redis Pub/Sub Event (document:doc_123)
    Redis-->>S2: Redis Pub/Sub Event (document:doc_123)
    S1->>UserA: Broadcast to local clients in doc_123
    S2->>UserB: Broadcast to local clients in doc_123
```

---

## Core System Components

### 1. Configuration & Environments (`internal/config`)
- Loads `PORT`, `REDIS_ADDR`, and `SERVER_ID` dynamically from environment variables.
- Defaults to local development parameters when unconfigured.

### 2. Messaging & Event Protocol (`internal/messaging`)
- Defines standard payload structure (`Event`) supporting edit type (`insert`, `delete`), `documentId`, `userId`, `position`, `content`, `value`, `serverId`, and `payload` for future CRDT integrations.

### 3. Redis Publisher & Subscriber (`internal/redis`)
- **Publisher**: Serializes edits and publishes to Redis channel `document:<documentID>`.
- **Subscriber**: Subscribes to pattern `document:*`. Listens asynchronously for events across all active document channels and forwards them to the WebSocket engine.

### 4. Stateless WebSocket Hub (`internal/websocket`)
- Does **not** maintain document state or global user presence in server memory.
- Manages local client connections partitioned into document-based rooms (`rooms map[string]map[*Client]bool`).
- Forwards local user actions to Redis Pub/Sub and dispatches incoming Redis events to connected local clients in the specified room.

### 5. Executables (`cmd/`)
- **`cmd/websocket-server/main.go`**: Distributed WebSocket node server listening for client connections.
- **`cmd/redis-worker/main.go`**: Background worker process monitoring and logging Redis Pub/Sub document stream activity.
