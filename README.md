# Distributed Key-Value Store (DKVS)

A distributed key-value store built in Java 21 / Spring Boot, inspired by Dynamo/Cassandra-style architecture: consistent hashing, gossip protocol, quorum-based replication, and an LSM-tree storage engine.

## Architecture

```
Client ──HTTP──▶ [Any Node = Coordinator]
                       │
           ┌───────────┼───────────┐
           ▼           ▼           ▼
       [Replica1]  [Replica2]  [Replica3]
        StorageEngine per node:
          CommitLog → MemTable → SSTable (with BloomFilter)
```

**Key components:**

- **Consistent Hashing** — SHA-256 based hash ring with 150 virtual nodes per physical node. Keys are mapped to replica nodes by walking clockwise around the ring.
- **Gossip Protocol** — Nodes discover each other and share cluster membership via periodic gossip (heartbeat-based failure detection).
- **Quorum Replication** — Configurable N (replication factor), W (write quorum), R (read quorum). Default: N=3, W=2, R=2.
- **LSM-Tree Storage** — Writes go to a commit log (WAL) and in-memory MemTable. When the MemTable reaches the threshold, it flushes to an immutable SSTable on disk. Reads check the MemTable first, then SSTables (newest first, with Bloom filters to skip irrelevant ones).
- **Versioning** — Each write is assigned a version (timestamp-based). Reads return the value with the highest version across replicas.

## Prerequisites

- Java 21
- Docker & Docker Compose

## Building

```bash
./gradlew build
```

## Running a 7-Node Cluster with Docker Compose

```bash
docker compose up --build
```

This starts:
- 7 DKVS nodes on ports 8080–8086
- Prometheus on port 9090
- Grafana on port 3000 (login: admin/admin)

## API Usage

Keys are Base64-URL encoded in the URL path. Values are raw bytes in the request/response body.

### Encoding a key

```bash
# Encode "my-key" to Base64-URL
echo -n "my-key" | base64 | tr '+/' '-_' | tr -d '='
# Output: bXkta2V5
```

### PUT a value

```bash
# Store the value "hello world" under key "my-key"
curl -X PUT http://localhost:8080/api/data/bXkta2V5 \
  -H "Content-Type: application/octet-stream" \
  -d "hello world"
```

### GET a value

```bash
# Retrieve the value for key "my-key"
curl http://localhost:8080/api/data/bXkta2V5
# Output: hello world
```

### Store JSON data

```bash
# Encode key "user:1001"
KEY=$(echo -n "user:1001" | base64 | tr '+/' '-_' | tr -d '=')

# Store JSON value
curl -X PUT "http://localhost:8080/api/data/${KEY}" \
  -H "Content-Type: application/octet-stream" \
  -d '{"name":"Alice","email":"alice@example.com"}'

# Read it back
curl "http://localhost:8080/api/data/${KEY}"
# Output: {"name":"Alice","email":"alice@example.com"}
```

### Read from a different node

```bash
# Write to node 1 (port 8080)
KEY=$(echo -n "distributed-test" | base64 | tr '+/' '-_' | tr -d '=')
curl -X PUT "http://localhost:8080/api/data/${KEY}" \
  -H "Content-Type: application/octet-stream" \
  -d "data written to node 1"

# Read from node 3 (port 8082) — the cluster replicates and routes correctly
curl "http://localhost:8082/api/data/${KEY}"
# Output: data written to node 1
```

### Store binary data

```bash
KEY=$(echo -n "binary-file" | base64 | tr '+/' '-_' | tr -d '=')

# Store a file
curl -X PUT "http://localhost:8080/api/data/${KEY}" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @myfile.bin

# Retrieve it
curl "http://localhost:8080/api/data/${KEY}" -o retrieved.bin
```

### Check version metadata

```bash
# Response headers include version info
KEY=$(echo -n "versioned-key" | base64 | tr '+/' '-_' | tr -d '=')
curl -X PUT "http://localhost:8080/api/data/${KEY}" \
  -H "Content-Type: application/octet-stream" \
  -d "version 1"

curl -v "http://localhost:8080/api/data/${KEY}" 2>&1 | grep X-DKVS
# X-DKVS-Version: 1740700000000
# X-DKVS-Timestamp: 1740700000000
```

## Monitoring

- **Prometheus**: http://localhost:9090 — scrapes `/actuator/prometheus` from all 7 nodes
- **Grafana**: http://localhost:3000 (admin/admin) — pre-provisioned dashboard showing:
  - Key-value pairs per node (time series)
  - Current key count per node (stat panel)
  - MemTable size per node
  - SSTable count per node

## Configuration

| Property | Default | Description |
|---|---|---|
| `dkvs.node-id` | `node-1` | Unique node identifier |
| `dkvs.host` | `localhost` | Node hostname |
| `dkvs.port` | `8080` | Node port |
| `dkvs.seeds` | `localhost:8080` | Comma-separated seed nodes |
| `dkvs.replication-factor` | `3` | Number of replicas (N) |
| `dkvs.write-quorum` | `2` | Write quorum size (W) |
| `dkvs.read-quorum` | `2` | Read quorum size (R) |
| `dkvs.data-dir` | `./data` | Data directory |
| `dkvs.memtable-flush-threshold` | `1048576` | MemTable flush threshold in bytes (1 MB) |
| `dkvs.gossip-interval-ms` | `1000` | Gossip interval in milliseconds |
| `dkvs.virtual-nodes` | `150` | Virtual nodes per physical node |
| `dkvs.node-down-timeout-ms` | `5000` | Node failure detection timeout |

## Running Tests

```bash
./gradlew test
```
