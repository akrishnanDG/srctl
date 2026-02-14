# srctl - Schema Registry Control CLI

A powerful CLI tool for Confluent Schema Registry that provides advanced capabilities beyond the standard SR CLI, including multi-threaded operations, referential integrity checks, cross-registry operations, and AI-agent-ready commands.

## Features

### Schema Operations
- **list** - List subjects with filtering, sorting, pagination
- **versions** - List all versions of a subject
- **get** - Fetch schemas with rich output (JSON, YAML, table)
- **register** - Register schemas with dry-run, context support
- **delete** - Advanced delete with referential integrity checks
- **diff** - Compare schemas between versions, subjects, or registries
- **evolve** - Analyze schema evolution history with breaking change detection
- **validate** - Validate schema syntax and compatibility offline (no registry needed)
- **search** - Search schemas by field name, type, tag, or content across the registry
- **explain** - Describe a schema in human-readable terms (fields, types, references)
- **suggest** - Propose compatible schema changes from a natural language description
- **generate** - Infer a schema (Avro/Protobuf/JSON) from sample JSON data

### Schema Splitting
- **split analyze** - Analyze a schema and show extractable types, sizes, and dependency tree
- **split extract** - Split a schema into referenced sub-schemas and write to files
- **split register** - Split a schema and register all parts to Schema Registry in dependency order

### AI-Agent-Ready Commands
- **explain** - Describe a schema in human-readable terms (fields, types, docs, references)
- **suggest** - Propose compatible schema changes from a natural language description
- **generate** - Infer a schema (Avro/Protobuf/JSON) from sample JSON data

### Bulk & Backup Operations
- **export** - Export to tar.gz or zip with dependencies (multi-threaded)
- **import** - Import from directory/archive with automatic dependency ordering
- **backup** - Full registry backup including configs, modes, and tags (multi-threaded)
- **restore** - Restore from backup with dependency ordering and optional schema ID preservation

### Cross-Registry Operations
- **compare** - Compare schemas across registries with multi-threading
- **clone** - Clone schemas between registries (preserves schema IDs by default)
- **replicate** - Continuously replicate schemas in real-time by consuming the `_schemas` Kafka topic

### Data Contracts
- **contract** - Manage data contract rules (get, set, validate)

### Configuration & Analysis
- **config** - Manage compatibility settings at all levels
- **mode** - Manage registry mode (READWRITE, READONLY, IMPORT)
- **stats** - Comprehensive statistics with multi-threading
- **health** - Health check for connectivity
- **contexts** - List all contexts in the registry
- **dangling** - Find schemas with broken/dangling references

## Installation

### From Source

```bash
git clone https://github.com/srctl/srctl.git
cd srctl
make build
# or: go build -o srctl .
```

### Verify Installation

```bash
srctl --help
srctl health --url https://your-sr-url --username API_KEY --password API_SECRET
```

## Quick Start

```bash
# 1. Set up config (or use --url, --username, --password flags)
cp srctl.example.yaml ~/.srctl/srctl.yaml
# Edit with your credentials

# 2. Check connectivity
srctl health

# 3. List subjects
srctl list

# 4. Register a schema (see examples/ folder for sample schemas)
srctl register my-topic-value --file schema.avsc

# 5. View schema details
srctl get my-topic-value

# 6. Compare versions
srctl diff my-topic-value@1 my-topic-value@2
```

## Configuration

### Configuration File

Copy `srctl.example.yaml` to `~/.srctl/srctl.yaml` or `./srctl.yaml` and fill in your credentials:

```yaml
registries:
  # Confluent Cloud
  - name: confluent-cloud
    url: https://psrc-xxxxx.us-east-2.aws.confluent.cloud
    username: YOUR_API_KEY
    password: YOUR_API_SECRET
    default: true

  # On-prem / Community with Kafka config (for 'srctl replicate')
  - name: on-prem
    url: https://sr.internal:8081
    username: SR_USER
    password: SR_PASS
    kafka:
      brokers:
        - broker1:9092
        - broker2:9092
      sasl:
        mechanism: PLAIN
        username: kafka-user
        password: kafka-pass
      tls:
        enabled: true

  # Confluent Cloud (target for replication)
  - name: ccloud
    url: https://psrc-xxxxx.confluent.cloud
    username: CCLOUD_API_KEY
    password: CCLOUD_API_SECRET

# Default output format
default_output: table
```

### Environment Variables

```bash
export SCHEMA_REGISTRY_URL=https://your-sr.cloud.confluent.io
export SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO=API_KEY:API_SECRET
```

## Performance Tips

### Using Workers for Large Registries

For registries with many subjects (1000+), increase the `--workers` flag for faster operations:

```bash
# Stats with 100 parallel workers (recommended for large registries)
srctl stats --workers 100

# Export with parallel fetching
srctl export --output ./schemas --workers 50

# Backup with parallel processing
srctl backup --output ./backup --workers 50

# Clone with parallel processing
srctl clone --source dev --target prod --workers 50

# Bulk delete with parallel processing
srctl delete --subjects user-events,order-events --workers 20

# Delete all with parallel processing (DANGEROUS!)
srctl delete --force --all --workers 100
```

**Recommended worker counts:**
| Registry Size | Workers |
|---------------|---------|
| < 100 subjects | 10 (default) |
| 100-500 subjects | 20-30 |
| 500-2000 subjects | 50 |
| 2000-10000 subjects | 100 |
| > 10000 subjects | 100-200 |

⚠️ **Note:** Higher worker counts will execute faster but may hit rate limits on managed services like Confluent Cloud. Adjust based on your environment.

## Command Reference

### Delete Operations

The delete command supports multiple modes with **referential integrity checks**:

```bash
# Soft delete a specific version
srctl delete user-events 3

# Soft delete entire subject
srctl delete user-events

# Permanent delete (hard delete)
srctl delete user-events --permanent

# Force delete specific version (soft + hard)
srctl delete user-events 3 --force

# Force delete entire subject
srctl delete user-events --force

# Delete multiple subjects with multi-threading
srctl delete --subjects user-events,order-events --workers 10

# Force delete entire context (DANGEROUS!)
srctl delete --context .mycontext --force --workers 20

# Empty entire registry (VERY DANGEROUS!)
srctl delete --force --all --workers 50

# Keep only latest 3 versions (soft delete)
srctl delete user-events --keep-latest 3

# Keep only latest 3 versions (permanent delete)
srctl delete user-events --keep-latest 3 --force

# Purge all soft-deleted schemas
srctl delete --purge-soft-deleted --workers 20
```

#### Referential Integrity

By default, delete operations check if schemas are referenced by other schemas:

```bash
# This will fail if user-events is referenced by other schemas
srctl delete user-events

# Skip referential integrity check (not recommended)
srctl delete user-events --skip-ref-check
```

### Clone Operations

Clone schemas between registries with **schema ID preservation** (default):

```bash
# Clone all schemas (preserves schema IDs by default)
srctl clone --source dev --target prod

# Clone with high parallelism for large registries
srctl clone --source dev --target prod --workers 50

# Clone specific subjects
srctl clone --source dev --target prod --subjects user-events,order-events

# Clone WITHOUT preserving schema IDs (new IDs will be assigned)
srctl clone --source dev --target prod --no-preserve-ids

# Clone with configs and tags
srctl clone --source dev --target prod --configs --tags

# Dry run to preview changes
srctl clone --source dev --target prod --dry-run
```

**Note:** Schema ID preservation requires the target registry to be set to IMPORT mode. The clone command handles this automatically.

### Export & Import

Export schemas to a directory or archive for transfer or version control:

```bash
# Export all schemas to directory
srctl export --output ./schemas

# Export to tar.gz archive
srctl export --output schemas.tar.gz --archive tar

# Export to zip archive  
srctl export --output schemas.zip --archive zip

# Export only latest versions
srctl export --versions latest --output ./schemas

# Export with referenced schemas
srctl export --with-refs --output ./schemas

# Export with parallelism
srctl export --output ./schemas --workers 50
```

Import schemas from an export:

```bash
# Import from directory
srctl import ./schemas

# Import from archive
srctl import schemas.tar.gz

# Dry run (validate without importing)
srctl import ./schemas --dry-run

# Skip subjects that already exist
srctl import ./schemas --skip-existing

# Import into specific context
srctl import ./schemas --target-context .production
```

**Important:** Import automatically sorts schemas by dependencies (topological sort) so that referenced schemas are registered before schemas that reference them.

### Backup & Restore

```bash
# Full backup with tags
srctl backup --output ./backup --workers 50

# Backup with schema ID mapping (for exact restoration)
srctl backup --output ./backup --by-id --workers 50

# Backup specific subjects
srctl backup --output ./backup --subjects user-events,order-events

# Restore from backup
srctl restore ./backup/sr-backup-20240115

# Restore with original schema IDs (requires backup with --by-id)
srctl restore ./backup/sr-backup-20240115 --preserve-ids

# Restore with tags
srctl restore ./backup/sr-backup-20240115 --tags

# Dry run restore
srctl restore ./backup/sr-backup-20240115 --dry-run

# Restore specific subjects only
srctl restore ./backup/sr-backup-20240115 --subjects user-events
```

**Important Notes:**
- Restore automatically sorts schemas by dependencies to ensure correct registration order
- `--preserve-ids` requires the backup to be created with `--by-id` and sets the registry to IMPORT mode
- Schema **version numbers may differ** after restore - Schema Registry assigns versions sequentially, so if you backup v1, v3, v5 (with v2, v4 deleted), restore creates v1, v2, v3

### Continuous Replication

Continuously replicate schema changes from a source registry (on-prem, community, or CP) to a target registry (CP Enterprise or Confluent Cloud) in real-time. Consumes the source cluster's `_schemas` Kafka topic to detect every change as it happens.

For a comprehensive guide covering setup, monitoring, alerting, production deployment (systemd/Docker/Kubernetes), and retry behavior, see [docs/continuous-replication-guide.md](docs/continuous-replication-guide.md).

```bash
# Basic replication (Kafka config from srctl.yaml)
srctl replicate --source on-prem --target ccloud

# With explicit Kafka brokers
srctl replicate --source on-prem --target ccloud --kafka-brokers broker1:9092,broker2:9092

# With SASL/TLS authentication
srctl replicate --source on-prem --target ccloud \
  --kafka-brokers broker1:9092 \
  --kafka-sasl-mechanism PLAIN \
  --kafka-sasl-user <user> \
  --kafka-sasl-password <pass> \
  --kafka-tls

# With subject filtering
srctl replicate --source on-prem --target ccloud \
  --kafka-brokers broker1:9092 \
  --filter "user-*"

# With Prometheus metrics endpoint
srctl replicate --source on-prem --target ccloud \
  --kafka-brokers broker1:9092 \
  --metrics-port 9090

# Resume after restart (consumer group tracks offsets)
srctl replicate --source on-prem --target ccloud \
  --kafka-brokers broker1:9092 \
  --no-initial-sync

# Without schema ID preservation
srctl replicate --source on-prem --target ccloud \
  --kafka-brokers broker1:9092 \
  --no-preserve-ids
```

**How it works:**

1. **Initial sync** — On first run, performs a full clone from source to target (like `srctl clone`)
2. **Streaming** — Consumes the `_schemas` Kafka topic for real-time change detection
3. **Apply** — Registers new schemas, replicates config/mode changes, and handles deletes on the target via REST API
4. **Resume** — Kafka consumer group offsets are committed, so restarts pick up where they left off

**What gets replicated:**

| Event | Action |
|-------|--------|
| New schema / version | Registered on target |
| Compatibility config change | Applied to target (global & subject-level) |
| Subject mode change | Applied to target (subject-level only) |
| Subject deletion | Deleted on target |

**Monitoring:**

- **CLI status** — Periodic one-line status printed to terminal (configurable with `--status-interval`)
- **Prometheus** — Optional `/metrics` HTTP endpoint (enabled with `--metrics-port`)

Available Prometheus metrics:
```
srctl_replicate_schemas_total
srctl_replicate_configs_total
srctl_replicate_deletes_total
srctl_replicate_errors_total
srctl_replicate_events_processed_total
srctl_replicate_events_filtered_total
srctl_replicate_last_offset
srctl_replicate_uptime_seconds
```

**Kafka authentication:**

Kafka connection can be configured via CLI flags or in `srctl.yaml` under the source registry's `kafka` block. CLI flags override config file values. Supported SASL mechanisms: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`.

**Graceful shutdown:**

Send `SIGINT` (Ctrl+C) or `SIGTERM` to stop. The replicator commits offsets, restores target registry mode, and prints a final stats table. A second signal forces immediate exit.

### Compare Operations

```bash
# Compare registries with multi-threading
srctl compare --source dev --target prod --workers 50

# Compare by schema ID
srctl compare --source dev --target prod --by-id

# Show only differences
srctl compare --source dev --target prod --diff-only
```

### Statistics

```bash
# Basic stats
srctl stats

# Fast stats with high parallelism
srctl stats --workers 100

# JSON output
srctl stats -o json
```

### Schema Splitting

Split large schemas that exceed the 1MB Schema Registry limit into referenced sub-schemas. Supports Avro, Protobuf, and JSON Schema.

For a comprehensive guide, see [docs/schema-splitting-guide.md](docs/schema-splitting-guide.md).

```bash
# Analyze a schema to see what can be extracted
srctl split analyze --file order.avsc

# Analyze with minimum size threshold (only extract types > 10KB)
srctl split analyze --file order.avsc --min-size 10240

# Top-level split only (extract direct field types, keep nesting intact)
srctl split analyze --file order.avsc --depth 1

# Extract sub-schemas to a directory for review
srctl split extract --file order.avsc --output-dir ./split-schemas/

# Split and register directly to Schema Registry (in dependency order)
srctl split register --file order.avsc --subject orders-value

# Top-level split and register (recommended for large schemas)
srctl split register --file order.avsc --subject orders-value --depth 1

# Dry run -- see what would be registered without making changes
srctl split register --file order.avsc --subject orders-value --dry-run

# Split a Protobuf schema
srctl split register --file order.proto --type PROTOBUF --subject orders-value

# Split a JSON Schema
srctl split register --file order.json --type JSON --subject orders-value
```

**Split depth control:**
- `--depth 0` (default) — extracts every named type recursively (can produce many small subjects)
- `--depth 1` — extracts only top-level field types, keeping nested types inline (fewer, larger subjects)

### Schema Validation

Validate schemas offline without requiring a running Schema Registry. Supports syntax checks, compatibility analysis between local files, and directory validation.

```bash
# Validate syntax of a schema file
srctl validate --file order.avsc

# Validate a Protobuf schema
srctl validate --file order.proto --type PROTOBUF

# Check compatibility between two local files
srctl validate --file order-v2.avsc --against order-v1.avsc

# Check with specific compatibility mode (BACKWARD, FORWARD, FULL)
srctl validate --file order-v2.avsc --against order-v1.avsc --compatibility FULL

# Validate all schemas in a directory
srctl validate --dir ./schemas/

# Check compatibility against latest version in registry
srctl validate --file order-v2.avsc --subject orders-value
```

Compatibility issues include actionable fix suggestions:
```
ERROR [name]: Field 'name' was removed
  Fix: Keep the field 'name', or change compatibility to NONE
WARN [email]: New field 'email' has no default value
  Fix: Add a default value or make 'email' nullable: ["null", "string"]
```

### Schema Search

Search across all schemas in the registry by field name, type, tag, or content. Uses multi-threaded fetching for large registries.

```bash
# Find all schemas with an 'email' field
srctl search --field email

# Find fields matching a glob pattern
srctl search --field "address*"

# Find fields of a specific type
srctl search --field email --field-type string

# Full-text search in schema content
srctl search --text customerId

# Search for tagged schemas (PII, SENSITIVE, etc.)
srctl search --tag PII

# Only search latest versions (default)
srctl search --field email --version latest

# Search all versions
srctl search --field email --version all

# Filter by subject name pattern
srctl search --field email --filter "order-*"

# Use more workers for large registries
srctl search --field email --workers 50

# Output as JSON for scripting
srctl search --field email -o json
```

### Schema Explanation

Describe schemas in human-readable terms. Useful for understanding unfamiliar schemas or for AI coding agents that need schema context before writing producer/consumer code.

```bash
# Explain a schema from the registry (includes referenced schema fields)
srctl explain orders-value

# Explain a specific version
srctl explain orders-value --version 2

# Explain a local file (no registry needed)
srctl explain --file order.avsc

# JSON output for programmatic consumption
srctl explain orders-value -o json
```

Output includes field names, types, documentation, nullability, defaults, and recursively resolved reference fields.

### Schema Suggestions

Propose compatible schema changes from a natural language description. Knows Avro compatibility rules and type promotion (int->long, float->double, string<->bytes).

```bash
# Suggest adding a field
srctl suggest orders-value "add discount code"

# Against a local file
srctl suggest --file order.avsc "add shipping address"

# Warns about breaking changes with alternatives
srctl suggest orders-value "remove the notes field"

# Handles renames (warns, suggests add+deprecate pattern)
srctl suggest --file order.avsc "rename email to emailAddress"

# Type changes (knows promotion rules)
srctl suggest orders-value "change type of count to long"
```

For breaking changes, explains why it breaks and suggests safe alternatives.

### Schema Generation

Infer schemas from sample JSON data. Detects common string formats (ISO dates, UUIDs, emails) and annotates them.

```bash
# Generate Avro schema from JSON (pipe from stdin)
echo '{"orderId": "123", "amount": 49.99, "active": true}' | srctl generate

# Generate with custom name and namespace
echo '{"id": "1", "name": "test"}' | srctl generate --name Order --namespace com.example

# Generate Protobuf
echo '{"id": "1", "count": 5}' | srctl generate --type PROTOBUF

# Generate JSON Schema
echo '{"id": "1", "amount": 9.99}' | srctl generate --type JSON

# From a file
srctl generate --from sample.json --name Event

# Multiple samples (JSONL) for better type inference
cat samples.jsonl | srctl generate --name Event
```

### Data Contracts

```bash
# Get data contract rules
srctl contract get user-events

# Set rules from file
srctl contract set user-events --rules rules.json

# Validate schema against rules
srctl contract validate user-events --schema schema.avsc

# Delete data contract rules
srctl contract delete user-events
```

### Schema Versions & Evolution

```bash
# List all versions of a subject
srctl versions user-events

# Include deleted versions
srctl versions user-events --deleted

# Analyze schema evolution history
srctl evolve user-events

# Show detailed field changes between versions
srctl evolve user-events --detailed
```

### Mode Management

Manage the registry mode at global or subject level:

```bash
# View global mode
srctl mode

# View mode for a subject
srctl mode user-events

# Set global mode to READONLY
srctl mode --set READONLY

# Set subject to IMPORT mode (for restoring with specific IDs)
srctl mode user-events --set IMPORT

# View mode at all levels
srctl mode --all
```

**Modes:**
- `READWRITE` - Normal operation (default)
- `READONLY` - Only allow reads
- `IMPORT` - Allow importing schemas with specific IDs

### Dangling References

Find schemas that reference soft-deleted schemas:

```bash
# Find all dangling references
srctl dangling

# Use more workers for large registries
srctl dangling --workers 50

# Output as JSON
srctl dangling --json
```

This helps identify referential integrity issues before permanent deletion.

### Contexts

```bash
# List all contexts in the registry
srctl contexts

# Output as JSON
srctl contexts -o json
```

## Output Formats

All commands support multiple output formats:

```bash
srctl list -o table    # Default, human-readable
srctl list -o json     # JSON format
srctl list -o yaml     # YAML format
srctl list -o plain    # Plain text (one item per line)
```

## Context Support

Schema Registry supports contexts for logical separation:

```bash
srctl list --context .production
srctl get user-events --context .staging
srctl register user-events --file schema.avsc --context .mycontext
```

## Safety Features

### Delete Safety Levels

1. **Referential Integrity Check** (default) - Prevents deleting schemas that are referenced by others
2. **Soft Delete** (default) - Marks as deleted, can be restored
3. **Permanent Delete** (`--permanent`) - Removes permanently
4. **Force Delete** (`--force`) - Bypasses soft delete, performs full deletion
5. **Confirmation Prompts** - Required for destructive operations
6. **Skip Confirmations** (`--yes`) - For scripted usage

### Clone Safety

- Schema IDs are preserved by default to maintain referential integrity
- Use `--no-preserve-ids` only when you explicitly want new IDs assigned

## AI Agent Integration

srctl is designed to be usable by AI coding agents (Claude Code, Cursor, Copilot, etc.) out of the box. Every command supports `-o json` for structured, parseable output.

### How AI Agents Use srctl

**Before writing producer/consumer code**, an agent can understand the schema:
```bash
srctl explain orders-value -o json    # Get full schema description with field types
srctl search --field email -o json    # Find which schemas have an email field
```

**Before proposing schema changes**, an agent can check compatibility:
```bash
srctl validate --file new-schema.avsc --against old-schema.avsc  # Offline check
srctl suggest --file schema.avsc "add tracking number" -o json   # Get safe change proposal
```

**When bootstrapping a new service**, an agent can generate schemas from sample data:
```bash
echo '{"orderId": "123", "amount": 49.99}' | srctl generate --name Order -o json
```

**When debugging**, an agent can search and inspect:
```bash
srctl search --field customerId -o json     # Find all schemas using this field
srctl diff orders-value@1 orders-value@2    # See what changed between versions
srctl dangling -o json                      # Find broken references
```

### Key Properties for Agent Use

| Property | Detail |
|----------|--------|
| **Structured output** | Every command supports `-o json` and `-o yaml` |
| **Offline capable** | `validate`, `explain --file`, `suggest --file`, `generate` work without a registry |
| **Deterministic** | No LLM inside — all commands produce consistent, reproducible output |
| **Exit codes** | 0 = success, non-zero = error — agents can check `$?` |
| **Actionable errors** | Validation and compatibility errors include fix suggestions |

## Global Flags

```
-u, --url string        Schema Registry URL (overrides config)
    --username string   Basic auth username
    --password string   Basic auth password
-r, --registry string   Registry name from config
-c, --context string    Schema Registry context (e.g., '.mycontext')
-o, --output string     Output format: table, json, yaml, plain (default "table")
```

## Exit Codes

- `0` - Success
- `1` - General error
- `2` - Configuration error
- `3` - Connection error

## License

MIT License
