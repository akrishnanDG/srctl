# Continuous Schema Replication Guide

This guide covers setting up and operating `srctl replicate` for continuous, real-time schema replication between Schema Registry instances.

## Overview

`srctl replicate` is a long-running process that replicates schema changes from a source Schema Registry to a target in real-time. It works by consuming the source cluster's `_schemas` Kafka topic (Schema Registry's internal changelog) and applying each change to the target via the REST API.

**Common use cases:**
- On-prem / Community Edition to Confluent Cloud migration
- CP Enterprise to Confluent Cloud replication
- Cross-datacenter schema synchronization
- Disaster recovery (active-passive schema registry setup)

## Architecture

```
Source Schema Registry          Target Schema Registry
        |                              ^
        | (writes to)                  | (REST API)
        v                              |
  _schemas topic  ──>  srctl replicate ┘
                    (Kafka consumer)
```

1. Every schema registration, config change, mode change, or deletion on the source SR produces an event on the `_schemas` Kafka topic
2. `srctl replicate` consumes these events in real-time via a Kafka consumer group
3. Each event is parsed and applied to the target SR via its REST API
4. Consumer group offsets are committed after successful processing

## Setup

### 1. Configure registries

Add both registries to `~/.srctl/srctl.yaml`. Include Kafka connection details under the source registry's `kafka` block:

```yaml
registries:
  - name: on-prem
    url: https://schema-registry.internal:8081
    username: sr-user
    password: sr-pass
    kafka:
      brokers:
        - broker1.internal:9092
        - broker2.internal:9092
        - broker3.internal:9092
      sasl:
        mechanism: PLAIN          # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
        username: kafka-user
        password: kafka-pass
      tls:
        enabled: true
        skip_verify: false        # Set true only for self-signed certs

  - name: ccloud
    url: https://psrc-xxxxx.us-east-2.aws.confluent.cloud
    username: CCLOUD_SR_API_KEY
    password: CCLOUD_SR_API_SECRET
    context: .my-context          # Optional: replicate into a specific context
```

CLI flags (`--kafka-brokers`, `--kafka-sasl-mechanism`, etc.) override config file values.

### 2. Verify connectivity

```bash
# Check source SR
srctl health --registry on-prem

# Check target SR
srctl health --registry ccloud

# Verify Kafka connectivity (the replicate command will fail fast if brokers are unreachable)
```

### 3. Start replication

```bash
# Basic - Kafka config from srctl.yaml
srctl replicate --source on-prem --target ccloud

# With explicit Kafka brokers
srctl replicate --source on-prem --target ccloud \
  --kafka-brokers broker1:9092,broker2:9092

# With subject filtering (only replicate user-* subjects)
srctl replicate --source on-prem --target ccloud --filter "user-*"

# With monitoring
srctl replicate --source on-prem --target ccloud \
  --metrics-port 9090 \
  --status-interval 10s
```

### 4. Verify replication

In another terminal:

```bash
# Register a schema on source
srctl register test-replication-value --file test.avsc --registry on-prem

# Check it appeared on target
srctl get test-replication-value --registry ccloud
```

## How It Works

### Initial sync

On first run, the replicator performs a full clone of all schemas from source to target via the REST API (same logic as `srctl clone`). This ensures the target is fully caught up before streaming begins.

Skip this on subsequent runs with `--no-initial-sync` -- the consumer group remembers where it left off.

### Streaming replication

After the initial sync, the replicator enters streaming mode:

1. **Poll** -- Fetch the next batch of records from the `_schemas` topic
2. **Parse** -- Decode each record's key/value into a typed event (SCHEMA, CONFIG, MODE, DELETE_SUBJECT)
3. **Filter** -- Skip events that don't match the `--filter` glob pattern
4. **Apply** -- Execute the corresponding REST API call on the target
5. **Commit** -- Commit consumer group offsets (only if the entire batch succeeded)

### Event types replicated

| Source event | Target action |
|---|---|
| New schema version registered | `POST /subjects/{subject}/versions` on target |
| Schema soft-deleted | `DELETE /subjects/{subject}` on target |
| Schema permanently deleted (tombstone) | `DELETE /subjects/{subject}?permanent=true` on target |
| Subject compatibility config changed | `PUT /config/{subject}` on target |
| Subject mode changed | `PUT /mode/{subject}` on target |
| Global config changed | `PUT /config` on target |
| Global mode changed | Skipped (to avoid overriding IMPORT mode on target) |

### Schema ID preservation

By default, schema IDs are preserved using IMPORT mode on the target. This ensures producers/consumers referencing schema IDs remain consistent. Use `--no-preserve-ids` if you don't need this (e.g., the target is a separate environment with its own ID space).

### Consumer groups and resumability

The replicator uses a Kafka consumer group (default: `srctl-replicate-<source>-<target>`) to track its position. On restart:

- The consumer resumes from the last committed offset
- Use `--no-initial-sync` to skip the full clone (since the target is already caught up)
- Use `--group-id` to run multiple independent replication streams

```bash
# First run (full sync + streaming)
srctl replicate --source on-prem --target ccloud

# After restart (resume from last offset)
srctl replicate --source on-prem --target ccloud --no-initial-sync
```

## Retry and Error Handling

### Event-level retries

Each event is retried up to 10 times with exponential backoff (1s, 2s, 4s, ... capped at 30s). This covers roughly 5 minutes of retries per event before giving up.

- **Retryable errors** (network timeouts, connection refused, 5xx server errors) -- retried with backoff
- **Non-retryable errors** (400 bad request, 422 incompatible schema) -- fail immediately
- **Idempotent** -- "already exists" / "already registered" responses are treated as success

### Offset safety

Offsets are committed only when the entire batch succeeds. If any event in a batch fails after exhausting retries:

- The error is logged
- Offsets are **not** committed
- On restart, uncommitted events are replayed from the last committed offset

### Kafka consumer resilience

- franz-go handles broker reconnection internally
- Poll errors use exponential backoff (1s → 2s → 4s → ... → 30s cap)
- Backoff resets on successful poll

### What happens during outages

| Scenario | Behavior |
|---|---|
| Target SR down for 2 minutes | Events retry with backoff, recover automatically, no data loss |
| Target SR down for 30+ minutes | Events exhaust retries, errors logged, offsets not committed. On restart, events are replayed |
| Kafka broker down | franz-go reconnects automatically. Poll errors retry with backoff |
| Network partition (intermittent) | Individual requests retry with backoff, recover when connectivity restores |
| Incompatible schema on target | Fails immediately (non-retryable), logged as error, other events continue |

## Monitoring

### CLI status

The replicator prints periodic status lines to the terminal:

```
[15:42:14] on-prem -> ccloud | schemas=142 configs=8 deletes=2 errors=0 events=1523 filtered=45 offset=1568 uptime=2h15m30s
```

Configure the interval with `--status-interval` (default: 30s).

### Shutdown stats

On graceful shutdown (Ctrl+C / SIGTERM), a final summary table is printed:

```
Replication Stopped
──────────────────────────────────────────────────
  METRIC             | VALUE
---------------------+--------
  Schemas Replicated | 142
  Configs Replicated | 8
  Deletes Replicated | 2
  Modes Replicated   | 0
  Errors             | 0
  Total Events       | 1523
  Filtered Events    | 45
  Last Offset        | 1568
  Uptime             | 2h15m30s
```

### Prometheus metrics

Enable with `--metrics-port <port>`. Metrics are available at `http://localhost:<port>/metrics`.

```bash
srctl replicate --source on-prem --target ccloud --metrics-port 9090
```

#### Available metrics

| Metric | Type | Description |
|---|---|---|
| `srctl_replicate_schemas_total` | Counter | Total schemas registered on target |
| `srctl_replicate_configs_total` | Counter | Total config changes applied |
| `srctl_replicate_deletes_total` | Counter | Total deletes applied |
| `srctl_replicate_errors_total` | Counter | Total errors (after retries exhausted) |
| `srctl_replicate_events_processed_total` | Counter | Total events consumed from `_schemas` |
| `srctl_replicate_events_filtered_total` | Counter | Events skipped by `--filter` |
| `srctl_replicate_last_offset` | Gauge | Last processed Kafka offset |
| `srctl_replicate_uptime_seconds` | Gauge | Replicator uptime |

All metrics include `source` and `target` labels.

#### Prometheus scrape config

```yaml
# prometheus.yml
scrape_configs:
  - job_name: srctl-replicate
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost:9090']
```

#### Recommended alerts

```yaml
# Alert if replication errors are occurring
- alert: SchemaReplicationErrors
  expr: rate(srctl_replicate_errors_total[5m]) > 0
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Schema replication errors detected"
    description: "{{ $value }} errors/sec for {{ $labels.source }} -> {{ $labels.target }}"

# Alert if no events processed for 10 minutes (replicator may be stuck)
- alert: SchemaReplicationStalled
  expr: changes(srctl_replicate_events_processed_total[10m]) == 0 and srctl_replicate_uptime_seconds > 600
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Schema replication appears stalled"

# Alert if replicator is down
- alert: SchemaReplicationDown
  expr: up{job="srctl-replicate"} == 0
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "Schema replicator is down"
```

#### Grafana dashboard queries

```
# Replication throughput (schemas/min)
rate(srctl_replicate_schemas_total[5m]) * 60

# Error rate
rate(srctl_replicate_errors_total[5m])

# Consumer lag (approximate - compare offset to topic high watermark)
srctl_replicate_last_offset

# Uptime
srctl_replicate_uptime_seconds / 3600
```

## Running in Production

### Systemd service

```ini
# /etc/systemd/system/srctl-replicate.service
[Unit]
Description=Schema Registry Continuous Replication
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=srctl
ExecStart=/usr/local/bin/srctl replicate \
  --source on-prem \
  --target ccloud \
  --no-initial-sync \
  --metrics-port 9090 \
  --status-interval 60s
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable srctl-replicate
sudo systemctl start srctl-replicate
sudo journalctl -u srctl-replicate -f
```

**Note:** Use `--no-initial-sync` in the service file. Run the initial sync manually first (`srctl replicate --source on-prem --target ccloud`), then enable the service for ongoing streaming.

### Docker

```bash
docker run -d \
  --name srctl-replicate \
  --restart=always \
  -v ~/.srctl:/root/.srctl:ro \
  -p 9090:9090 \
  srctl replicate \
    --source on-prem \
    --target ccloud \
    --no-initial-sync \
    --metrics-port 9090
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: srctl-replicate
spec:
  replicas: 1    # Must be 1 - consumer group handles exactly-once
  selector:
    matchLabels:
      app: srctl-replicate
  template:
    metadata:
      labels:
        app: srctl-replicate
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
    spec:
      containers:
        - name: srctl
          image: your-registry/srctl:latest
          args:
            - replicate
            - --source=on-prem
            - --target=ccloud
            - --no-initial-sync
            - --metrics-port=9090
          ports:
            - containerPort: 9090
              name: metrics
          volumeMounts:
            - name: config
              mountPath: /root/.srctl
              readOnly: true
      volumes:
        - name: config
          secret:
            secretName: srctl-config
```

## Kafka ACLs

The replicator's Kafka consumer needs the following ACLs on the source cluster:

```
# Read the _schemas topic
kafka-acls --add --allow-principal User:<username> \
  --operation Read --topic _schemas

# Consumer group
kafka-acls --add --allow-principal User:<username> \
  --operation Read --group srctl-replicate-on-prem-ccloud
```

## CLI Reference

```
srctl replicate [flags]

Required:
  --source string       Source registry name from config
  --target string       Target registry name from config

Kafka connection (override config file):
  --kafka-brokers strings         Kafka broker addresses
  --kafka-sasl-mechanism string   SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
  --kafka-sasl-user string        SASL username
  --kafka-sasl-password string    SASL password
  --kafka-tls                     Enable TLS
  --kafka-tls-skip-verify         Skip TLS certificate verification

Behavior:
  --topic string            Kafka topic (default "_schemas")
  --group-id string         Consumer group ID (default "srctl-replicate-<source>-<target>")
  --filter string           Filter subjects by glob pattern
  --no-preserve-ids         Do not preserve schema IDs
  --no-initial-sync         Skip initial full sync
  --workers int             Workers for initial sync (default 10)

Monitoring:
  --metrics-port int        Prometheus /metrics port (0 = disabled)
  --status-interval duration  CLI status interval (default 30s)
```
