package cmd

import (
	gocontext "context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/srctl/srctl/internal/config"
	"github.com/srctl/srctl/internal/kafka"
	"github.com/srctl/srctl/internal/output"
	"github.com/srctl/srctl/internal/replicator"
)

var (
	replicateSource         string
	replicateTarget         string
	replicateKafkaBrokers   []string
	replicateTopic          string
	replicateGroupID        string
	replicateFilter         string
	replicateNoPreserveIDs  bool
	replicateNoInitialSync  bool
	replicateWorkers        int
	replicateMetricsPort    int
	replicateStatusInterval time.Duration
	replicateKafkaSASLMech  string
	replicateKafkaSASLUser  string
	replicateKafkaSASLPass  string
	replicateKafkaTLS       bool
	replicateKafkaTLSSkip   bool
)

var replicateCmd = &cobra.Command{
	Use:     "replicate",
	Short:   "Continuously replicate schemas between registries",
	GroupID: groupCrossReg,
	Long: `Continuously replicate schema changes from a source to target Schema Registry.

Consumes the _schemas Kafka topic from the source cluster to detect changes
in real-time, then applies them to the target registry via REST API.

Schema IDs are preserved by default using IMPORT mode on the target registry.

On first run, performs a full initial sync (like 'srctl clone'), then switches
to streaming mode for continuous replication.

Kafka connection can be configured via CLI flags or in srctl.yaml under the
source registry's 'kafka' block:

  registries:
    - name: on-prem
      url: https://sr.internal:8081
      kafka:
        brokers: [broker1:9092, broker2:9092]
        sasl:
          mechanism: PLAIN
          username: kafka-user
          password: kafka-pass
        tls:
          enabled: true

Examples:
  # Basic (Kafka config from srctl.yaml)
  srctl replicate --source on-prem --target ccloud

  # With explicit Kafka brokers
  srctl replicate --source dev --target prod --kafka-brokers localhost:9092

  # With SASL/TLS authentication
  srctl replicate --source dev --target prod \
    --kafka-brokers broker1:9092 \
    --kafka-sasl-mechanism PLAIN \
    --kafka-sasl-user myuser \
    --kafka-sasl-password mypass \
    --kafka-tls

  # With subject filter and Prometheus metrics
  srctl replicate --source dev --target prod \
    --kafka-brokers localhost:9092 \
    --filter "user-*" --metrics-port 9090

  # Skip initial sync (resume from last offset)
  srctl replicate --source dev --target prod \
    --kafka-brokers localhost:9092 --no-initial-sync

  # Custom consumer group for multiple replication streams
  srctl replicate --source dev --target staging \
    --kafka-brokers localhost:9092 \
    --group-id my-custom-group`,
	RunE: runReplicate,
}

func init() {
	// Registry flags
	replicateCmd.Flags().StringVar(&replicateSource, "source", "", "Source registry name (required)")
	replicateCmd.Flags().StringVar(&replicateTarget, "target", "", "Target registry name (required)")

	// Kafka flags (override config file)
	replicateCmd.Flags().StringSliceVar(&replicateKafkaBrokers, "kafka-brokers", nil, "Kafka broker addresses (overrides config)")
	replicateCmd.Flags().StringVar(&replicateTopic, "topic", "_schemas", "Kafka topic name for schema changes")
	replicateCmd.Flags().StringVar(&replicateGroupID, "group-id", "", "Kafka consumer group ID (default: srctl-replicate-<source>-<target>)")

	// Kafka auth flags (override config file)
	replicateCmd.Flags().StringVar(&replicateKafkaSASLMech, "kafka-sasl-mechanism", "", "SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 (overrides config)")
	replicateCmd.Flags().StringVar(&replicateKafkaSASLUser, "kafka-sasl-user", "", "SASL username (overrides config)")
	replicateCmd.Flags().StringVar(&replicateKafkaSASLPass, "kafka-sasl-password", "", "SASL password (overrides config)")
	replicateCmd.Flags().BoolVar(&replicateKafkaTLS, "kafka-tls", false, "Enable TLS for Kafka connection (overrides config)")
	replicateCmd.Flags().BoolVar(&replicateKafkaTLSSkip, "kafka-tls-skip-verify", false, "Skip TLS certificate verification")

	// Behavior flags
	replicateCmd.Flags().StringVarP(&replicateFilter, "filter", "f", "", "Filter subjects by glob pattern")
	replicateCmd.Flags().BoolVar(&replicateNoPreserveIDs, "no-preserve-ids", false, "Do NOT preserve schema IDs")
	replicateCmd.Flags().BoolVar(&replicateNoInitialSync, "no-initial-sync", false, "Skip initial full sync")
	replicateCmd.Flags().IntVar(&replicateWorkers, "workers", 10, "Number of parallel workers for initial sync")

	// Metrics/monitoring flags
	replicateCmd.Flags().IntVar(&replicateMetricsPort, "metrics-port", 0, "Port for Prometheus /metrics endpoint (0 = disabled)")
	replicateCmd.Flags().DurationVar(&replicateStatusInterval, "status-interval", 30*time.Second, "Interval for status output")

	replicateCmd.MarkFlagRequired("source")
	replicateCmd.MarkFlagRequired("target")

	rootCmd.AddCommand(replicateCmd)
}

// resolveKafkaConfig resolves Kafka connection settings from CLI flags and config file.
// CLI flags take precedence over the config file.
func resolveKafkaConfig(sourceRegistryName string) (kafka.ConsumerConfig, error) {
	cfg := kafka.ConsumerConfig{
		Topic: replicateTopic,
	}

	// Start with config file values from source registry
	reg := config.GetRegistry(sourceRegistryName)
	if reg != nil && len(reg.Kafka.Brokers) > 0 {
		cfg.Brokers = reg.Kafka.Brokers
		cfg.SASLMechanism = reg.Kafka.SASL.Mechanism
		cfg.SASLUser = reg.Kafka.SASL.Username
		cfg.SASLPassword = reg.Kafka.SASL.Password
		cfg.TLSEnabled = reg.Kafka.TLS.Enabled
		cfg.TLSSkipVerify = reg.Kafka.TLS.SkipVerify
	}

	// CLI flags override config file
	if len(replicateKafkaBrokers) > 0 {
		cfg.Brokers = replicateKafkaBrokers
	}
	if replicateKafkaSASLMech != "" {
		cfg.SASLMechanism = replicateKafkaSASLMech
	}
	if replicateKafkaSASLUser != "" {
		cfg.SASLUser = replicateKafkaSASLUser
	}
	if replicateKafkaSASLPass != "" {
		cfg.SASLPassword = replicateKafkaSASLPass
	}
	if replicateKafkaTLS {
		cfg.TLSEnabled = true
	}
	if replicateKafkaTLSSkip {
		cfg.TLSSkipVerify = true
	}

	if len(cfg.Brokers) == 0 {
		return cfg, fmt.Errorf("no Kafka brokers configured. Use --kafka-brokers flag or configure kafka.brokers in srctl.yaml under the source registry")
	}

	// Consumer group ID
	cfg.GroupID = replicateGroupID
	if cfg.GroupID == "" {
		cfg.GroupID = fmt.Sprintf("srctl-replicate-%s-%s", replicateSource, replicateTarget)
	}

	// Start from beginning unless skipping initial sync
	cfg.FromBeginning = !replicateNoInitialSync

	return cfg, nil
}

func runReplicate(cmd *cobra.Command, args []string) error {
	output.Header("Schema Replication")
	output.Info("Source registry: %s", replicateSource)
	output.Info("Target registry: %s", replicateTarget)

	// Resolve Kafka config (config file + CLI overrides)
	kafkaCfg, err := resolveKafkaConfig(replicateSource)
	if err != nil {
		return err
	}

	output.Info("Kafka brokers: %s", strings.Join(kafkaCfg.Brokers, ", "))
	output.Info("Topic: %s", kafkaCfg.Topic)
	output.Info("Consumer group: %s", kafkaCfg.GroupID)
	if replicateFilter != "" {
		output.Info("Filter: %s", replicateFilter)
	}

	// Build context with signal handling for graceful shutdown
	ctx, cancel := gocontext.WithCancel(gocontext.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		output.Warning("\nReceived %s, shutting down gracefully...", sig)
		cancel()
		// Second signal forces exit
		sig = <-sigCh
		output.Error("Received %s, forcing exit", sig)
		os.Exit(1)
	}()

	// Create registry clients
	sourceClient, err := GetClientForRegistry(replicateSource)
	if err != nil {
		return fmt.Errorf("failed to connect to source registry: %w", err)
	}

	targetClient, err := GetClientForRegistry(replicateTarget)
	if err != nil {
		return fmt.Errorf("failed to connect to target registry: %w", err)
	}

	// Set IMPORT mode on target if preserving IDs
	if !replicateNoPreserveIDs {
		output.Step("Setting target registry to IMPORT mode...")
		if err := targetClient.SetMode("IMPORT"); err != nil {
			return fmt.Errorf("failed to set IMPORT mode on target: %w", err)
		}
		defer func() {
			output.Step("Restoring READWRITE mode on target...")
			if err := targetClient.SetMode("READWRITE"); err != nil {
				output.Warning("Failed to restore READWRITE mode: %v", err)
			}
		}()
	}

	// Create Kafka consumer
	consumer, err := kafka.NewConsumer(kafkaCfg)
	if err != nil {
		return fmt.Errorf("failed to create Kafka consumer: %w", err)
	}
	defer consumer.Close()

	// Create replicator
	rep := replicator.New(replicator.Config{
		SourceClient:       sourceClient,
		TargetClient:       targetClient,
		Consumer:           consumer,
		Filter:             replicateFilter,
		PreserveIDs:        !replicateNoPreserveIDs,
		Workers:            replicateWorkers,
		InitialSync:        !replicateNoInitialSync,
		SourceRegistryName: replicateSource,
		TargetRegistryName: replicateTarget,
	})

	// Start status reporter
	statusReporter := replicator.NewStatusReporter(
		rep.GetStats(),
		replicateStatusInterval,
		replicateSource,
		replicateTarget,
	)
	go statusReporter.Run(ctx)

	// Start Prometheus metrics server if configured
	if replicateMetricsPort > 0 {
		metricsServer := replicator.NewMetricsServer(
			rep.GetStats(),
			replicateMetricsPort,
			replicateSource,
			replicateTarget,
		)
		go func() {
			output.Info("Prometheus metrics at http://0.0.0.0:%d/metrics", replicateMetricsPort)
			if err := metricsServer.Start(ctx); err != nil {
				output.Warning("Metrics server error: %v", err)
			}
		}()
	}

	// Run the replicator (blocks until context is cancelled)
	output.Success("Replication started")
	if err := rep.Run(ctx); err != nil && ctx.Err() == nil {
		return fmt.Errorf("replication error: %w", err)
	}

	// Print final stats
	snap := rep.GetStats().Snapshot()
	output.Header("Replication Stopped")
	output.PrintTable(
		[]string{"Metric", "Value"},
		[][]string{
			{"Schemas Replicated", fmt.Sprintf("%d", snap.SchemasReplicated)},
			{"Configs Replicated", fmt.Sprintf("%d", snap.ConfigsReplicated)},
			{"Deletes Replicated", fmt.Sprintf("%d", snap.DeletesReplicated)},
			{"Modes Replicated", fmt.Sprintf("%d", snap.ModesReplicated)},
			{"Errors", fmt.Sprintf("%d", snap.Errors)},
			{"Total Events", fmt.Sprintf("%d", snap.EventsProcessed)},
			{"Filtered Events", fmt.Sprintf("%d", snap.EventsFiltered)},
			{"Last Offset", fmt.Sprintf("%d", snap.LastOffset)},
			{"Uptime", snap.Uptime.Truncate(time.Second).String()},
		},
	)

	return nil
}
