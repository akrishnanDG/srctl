package kafka

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// ConsumerConfig holds Kafka consumer configuration.
type ConsumerConfig struct {
	Brokers       []string
	Topic         string
	GroupID       string
	SASLMechanism string // "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", or ""
	SASLUser      string
	SASLPassword  string
	TLSEnabled    bool
	TLSSkipVerify bool
	FromBeginning bool // Start from earliest offset
}

// Consumer wraps a franz-go client for consuming the _schemas topic.
type Consumer struct {
	client *kgo.Client
	topic  string
}

// NewConsumer creates a new Kafka consumer with the given configuration.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.DisableAutoCommit(),
	}

	if cfg.FromBeginning {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	} else {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}

	// SASL authentication
	switch cfg.SASLMechanism {
	case "PLAIN":
		mechanism := plain.Auth{
			User: cfg.SASLUser,
			Pass: cfg.SASLPassword,
		}
		opts = append(opts, kgo.SASL(mechanism.AsMechanism()))
	case "SCRAM-SHA-256":
		mechanism := scram.Auth{
			User: cfg.SASLUser,
			Pass: cfg.SASLPassword,
		}
		opts = append(opts, kgo.SASL(mechanism.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		mechanism := scram.Auth{
			User: cfg.SASLUser,
			Pass: cfg.SASLPassword,
		}
		opts = append(opts, kgo.SASL(mechanism.AsSha512Mechanism()))
	case "":
		// No SASL
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s (supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", cfg.SASLMechanism)
	}

	// TLS
	if cfg.TLSEnabled {
		tlsCfg := &tls.Config{
			InsecureSkipVerify: cfg.TLSSkipVerify, // #nosec G402 -- user-controlled flag
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	return &Consumer{
		client: client,
		topic:  cfg.Topic,
	}, nil
}

// Poll fetches the next batch of records from Kafka.
// Blocks until records are available or the context is cancelled.
func (c *Consumer) Poll(ctx context.Context) ([]*kgo.Record, error) {
	fetches := c.client.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		// Return the first error; fetches may still contain records
		for _, e := range errs {
			if e.Err != nil {
				return nil, fmt.Errorf("fetch error on %s[%d]: %w", e.Topic, e.Partition, e.Err)
			}
		}
	}
	return fetches.Records(), nil
}

// CommitOffsets commits the current consumer group offsets.
func (c *Consumer) CommitOffsets(ctx context.Context) error {
	return c.client.CommitUncommittedOffsets(ctx)
}

// Close shuts down the consumer and releases resources.
func (c *Consumer) Close() {
	c.client.Close()
}
