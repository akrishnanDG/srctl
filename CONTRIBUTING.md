# Contributing to srctl

## Development Setup

### Prerequisites

- Go 1.21+
- Docker and Docker Compose (for integration tests)
- golangci-lint (optional, for linting)

### Build

```bash
git clone https://github.com/akrishnanDG/srctl.git
cd srctl
make build
```

### Run Tests

```bash
# Unit tests
make test

# Unit tests with coverage report
make test-coverage

# Lint
make lint

# Format code
make fmt
```

### Integration Tests

Integration tests run against a local Schema Registry via Docker Compose.

```bash
# Start local Kafka + Schema Registry
cd examples/split-demo
docker compose up -d

# Wait for services
curl -s http://localhost:8081/ > /dev/null

# Run integration tests
./integration-test.sh

# Tear down
docker compose down -v
```

## Project Structure

```
srctl/
├── main.go                    # Entry point
├── cmd/                       # CLI commands (one file per command)
│   ├── root.go                # Root command, global flags, client init
│   ├── list.go                # list, versions commands
│   ├── get.go                 # get, contexts commands
│   ├── register.go            # register command
│   ├── delete.go              # delete command
│   ├── diff.go                # diff, evolve commands
│   ├── split.go               # split analyze/extract/register
│   ├── validate.go            # validate command
│   ├── search.go              # search command
│   ├── explain.go             # explain command
│   ├── suggest.go             # suggest command
│   ├── generate.go            # generate command
│   ├── export.go              # export command
│   ├── import.go              # import command
│   ├── backup.go              # backup command
│   ├── compare.go             # compare command
│   ├── contract.go            # contract command
│   ├── config.go              # config command
│   ├── stats.go               # stats, health commands
│   ├── dangling.go            # dangling command
│   └── *_test.go              # Tests
├── internal/
│   ├── client/                # Schema Registry HTTP client
│   │   ├── client.go          # Client implementation
│   │   ├── interface.go       # Client interface
│   │   └── mock_client.go     # Mock for testing
│   ├── config/                # Configuration (Viper)
│   │   └── config.go
│   └── output/                # Output formatting
│       └── output.go
├── examples/                  # Sample schemas and demos
│   └── split-demo/            # Schema splitting demo
├── docs/                      # Documentation
│   └── schema-splitting-guide.md
└── Makefile                   # Build targets
```

## Adding a New Command

1. Create `cmd/<name>.go` following the existing pattern:

```go
var myCmd = &cobra.Command{
    Use:     "mycommand",
    Short:   "Short description",
    GroupID: groupSchema,  // or groupBulk, groupConfig, etc.
    Long:    `Detailed help text`,
    RunE:    runMyCommand,
}

func init() {
    // Add flags
    myCmd.Flags().StringVarP(&myFlag, "flag", "f", "", "Description")
    rootCmd.AddCommand(myCmd)
}

func runMyCommand(cmd *cobra.Command, args []string) error {
    // Implementation
    return nil
}
```

2. Create `cmd/<name>_test.go` with unit tests
3. Update `README.md` with the command in the features list and command reference
4. Run `make test` and `make lint`

## Code Conventions

- Use `output.Header()`, `output.Info()`, `output.Success()`, `output.Error()` for terminal output
- Support `-o json` via `output.NewPrinter(outputFormat)`
- Use `GetClient()` to get the Schema Registry client (handles flags, config, env vars)
- Use `detectSchemaType()` for auto-detecting Avro/Protobuf/JSON from file extensions or content
- Use the worker pool pattern from `stats.go` for parallel operations with `--workers` flag
- Error messages should be actionable: tell the user what went wrong and how to fix it

## Releasing

Releases are automated via GoReleaser. To create a release:

```bash
# Tag the release
git tag -a v1.1.0 -m "Release v1.1.0"
git push origin v1.1.0

# GoReleaser builds binaries and creates the GitHub release
goreleaser release --clean
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `SCHEMA_REGISTRY_URL` | Schema Registry URL |
| `SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO` | `user:password` for basic auth |
| `SRCTL_*` | Any config key prefixed with SRCTL_ (via Viper) |
