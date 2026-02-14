package replicator

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/srctl/srctl/internal/client"
	"github.com/srctl/srctl/internal/kafka"
	"github.com/srctl/srctl/internal/output"
)

// Config holds replicator configuration.
type Config struct {
	SourceClient       client.SchemaRegistryClientInterface
	TargetClient       client.SchemaRegistryClientInterface
	Consumer           *kafka.Consumer
	Filter             string // Subject glob pattern
	PreserveIDs        bool
	Workers            int
	InitialSync        bool
	SourceRegistryName string
	TargetRegistryName string
}

// Stats tracks replication metrics (thread-safe).
type Stats struct {
	mu                sync.RWMutex
	StartTime         time.Time
	SchemasReplicated int64
	ConfigsReplicated int64
	DeletesReplicated int64
	ModesReplicated   int64
	Errors            int64
	LastOffset        int64
	LastEventTime     time.Time
	EventsProcessed  int64
	EventsFiltered   int64
}

// StatsSnapshot is a point-in-time copy of stats.
type StatsSnapshot struct {
	Uptime            time.Duration
	SchemasReplicated int64
	ConfigsReplicated int64
	DeletesReplicated int64
	ModesReplicated   int64
	Errors            int64
	EventsProcessed  int64
	EventsFiltered   int64
	LastOffset        int64
	LastEventTime     time.Time
}

func (s *Stats) IncrSchemas() {
	s.mu.Lock()
	s.SchemasReplicated++
	s.mu.Unlock()
}

func (s *Stats) IncrConfigs() {
	s.mu.Lock()
	s.ConfigsReplicated++
	s.mu.Unlock()
}

func (s *Stats) IncrDeletes() {
	s.mu.Lock()
	s.DeletesReplicated++
	s.mu.Unlock()
}

func (s *Stats) IncrModes() {
	s.mu.Lock()
	s.ModesReplicated++
	s.mu.Unlock()
}

func (s *Stats) IncrErrors() {
	s.mu.Lock()
	s.Errors++
	s.mu.Unlock()
}

func (s *Stats) IncrProcessed() {
	s.mu.Lock()
	s.EventsProcessed++
	s.mu.Unlock()
}

func (s *Stats) IncrFiltered() {
	s.mu.Lock()
	s.EventsFiltered++
	s.mu.Unlock()
}

func (s *Stats) SetOffset(offset int64) {
	s.mu.Lock()
	s.LastOffset = offset
	s.mu.Unlock()
}

func (s *Stats) SetLastEventTime(t time.Time) {
	s.mu.Lock()
	s.LastEventTime = t
	s.mu.Unlock()
}

// Snapshot returns a point-in-time copy of the stats.
func (s *Stats) Snapshot() StatsSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return StatsSnapshot{
		Uptime:            time.Since(s.StartTime),
		SchemasReplicated: s.SchemasReplicated,
		ConfigsReplicated: s.ConfigsReplicated,
		DeletesReplicated: s.DeletesReplicated,
		ModesReplicated:   s.ModesReplicated,
		Errors:            s.Errors,
		EventsProcessed:  s.EventsProcessed,
		EventsFiltered:   s.EventsFiltered,
		LastOffset:        s.LastOffset,
		LastEventTime:     s.LastEventTime,
	}
}

// Replicator manages continuous schema replication.
type Replicator struct {
	cfg   Config
	stats *Stats
}

// New creates a new Replicator.
func New(cfg Config) *Replicator {
	return &Replicator{
		cfg: cfg,
		stats: &Stats{
			StartTime: time.Now(),
		},
	}
}

// GetStats returns the replication stats.
func (r *Replicator) GetStats() *Stats {
	return r.stats
}

// Run starts the replication loop. Blocks until ctx is cancelled.
func (r *Replicator) Run(ctx context.Context) error {
	// Phase 1: Initial sync
	if r.cfg.InitialSync {
		output.Step("Performing initial sync from %s to %s...", r.cfg.SourceRegistryName, r.cfg.TargetRegistryName)
		if err := r.performInitialSync(ctx); err != nil {
			return fmt.Errorf("initial sync failed: %w", err)
		}
		output.Success("Initial sync complete")
	}

	output.Step("Entering streaming replication mode...")

	// Phase 2: Streaming loop
	pollBackoff := time.Second
	const maxPollBackoff = 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		records, err := r.cfg.Consumer.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // Graceful shutdown
			}
			output.Warning("Poll error (retrying in %s): %v", pollBackoff, err)
			r.stats.IncrErrors()
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(pollBackoff):
			}
			// Exponential backoff on consecutive poll errors, capped at 30s
			pollBackoff = pollBackoff * 2
			if pollBackoff > maxPollBackoff {
				pollBackoff = maxPollBackoff
			}
			continue
		}
		// Reset backoff on successful poll
		pollBackoff = time.Second

		batchOK := true
		for _, record := range records {
			event, err := kafka.ParseRecord(record.Key, record.Value, record.Offset, record.Partition)
			if err != nil {
				output.Warning("Parse error at offset %d: %v", record.Offset, err)
				r.stats.IncrErrors()
				continue
			}

			if event == nil {
				continue // NOOP or empty
			}

			r.stats.IncrProcessed()

			// Apply filter
			if r.cfg.Filter != "" && event.Subject != "" {
				if !matchGlob(strings.ToLower(event.Subject), strings.ToLower(r.cfg.Filter)) {
					r.stats.IncrFiltered()
					continue
				}
			}

			// Apply event with retries. If the target is unreachable, block
			// and keep retrying rather than skipping the event.
			if err := r.applyWithRetry(ctx, event, maxEventRetries); err != nil {
				output.Error("Failed to apply %s event for %s at offset %d: %v",
					event.Type, event.Subject, event.Offset, err)
				r.stats.IncrErrors()
				batchOK = false
			}

			r.stats.SetOffset(event.Offset)
			r.stats.SetLastEventTime(time.Now())
		}

		// Only commit offsets if the entire batch succeeded.
		// On failure, offsets stay uncommitted so events will be replayed on restart.
		if len(records) > 0 && batchOK {
			if err := r.cfg.Consumer.CommitOffsets(ctx); err != nil {
				output.Warning("Failed to commit offsets: %v", err)
			}
		}
	}
}

const (
	// maxEventRetries is the number of retry attempts for transient errors
	// (e.g., network timeouts, 5xx responses). With exponential backoff
	// capped at 30s, this gives roughly 5 minutes of retries before giving up.
	maxEventRetries = 10
	maxRetryBackoff = 30 * time.Second
)

// applyWithRetry applies an event with exponential backoff retries.
// For network/server errors it retries aggressively to ride out transient
// outages. For client errors (4xx other than 409) it fails fast.
func (r *Replicator) applyWithRetry(ctx context.Context, event *kafka.SchemaEvent, maxRetries int) error {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second
			if backoff > maxRetryBackoff {
				backoff = maxRetryBackoff
			}
			if attempt == 1 {
				output.Warning("Retrying %s %s v%d (attempt %d/%d, backoff %s): %v",
					event.Type, event.Subject, event.Version, attempt, maxRetries, backoff, lastErr)
			} else {
				output.Warning("Retry %d/%d for %s %s v%d (backoff %s): %v",
					attempt, maxRetries, event.Type, event.Subject, event.Version, backoff, lastErr)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}

		lastErr = r.applyEvent(ctx, event)
		if lastErr == nil {
			if attempt > 0 {
				output.Success("Recovered after %d retries for %s %s v%d",
					attempt, event.Type, event.Subject, event.Version)
			}
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Don't retry client errors that won't resolve with a retry
		// (e.g., incompatible schema, invalid request). Only retry
		// network errors and server errors (5xx, timeouts, connection refused).
		if isNonRetryableError(lastErr) {
			return lastErr
		}
	}
	return lastErr
}

// isNonRetryableError returns true for errors that won't resolve by retrying
// (client-side errors like incompatible schema, bad request, etc.)
func isNonRetryableError(err error) bool {
	msg := err.Error()
	// 409 Conflict (schema already exists) is handled as success elsewhere
	// 422 Unprocessable (incompatible schema, invalid schema)
	// 400 Bad Request
	if strings.Contains(msg, "status 422") || strings.Contains(msg, "status 400") {
		return true
	}
	// "not found" errors when deleting non-existent subjects are handled elsewhere
	return false
}

// applyEvent applies a single SchemaEvent to the target registry.
func (r *Replicator) applyEvent(ctx context.Context, event *kafka.SchemaEvent) error {
	_ = ctx // reserved for future use

	switch event.Type {
	case kafka.KeyTypeSchema:
		return r.applySchemaEvent(event)
	case kafka.KeyTypeConfig:
		return r.applyConfigEvent(event)
	case kafka.KeyTypeMode:
		return r.applyModeEvent(event)
	case kafka.KeyTypeDeleteSubject, kafka.KeyTypeClearSubject:
		return r.applyDeleteEvent(event)
	}
	return nil
}

func (r *Replicator) applySchemaEvent(event *kafka.SchemaEvent) error {
	if event.Tombstone || event.Deleted {
		if event.Subject == "" {
			return nil
		}
		// Soft-deleted or tombstoned schema: delete on target
		permanent := event.Tombstone
		_, err := r.cfg.TargetClient.DeleteSubject(event.Subject, permanent)
		if err != nil {
			// Subject may not exist on target yet, or target in IMPORT mode -- not an error
			if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "405") {
				return nil
			}
			return fmt.Errorf("failed to delete %s: %w", event.Subject, err)
		}
		r.stats.IncrDeletes()
		return nil
	}

	// Convert references
	refs := make([]client.SchemaReference, len(event.References))
	for i, ref := range event.References {
		refs[i] = client.SchemaReference{
			Name:    ref.Name,
			Subject: ref.Subject,
			Version: ref.Version,
		}
	}

	schema := &client.Schema{
		Schema:     event.Schema,
		SchemaType: event.SchemaType,
		References: refs,
	}

	if r.cfg.PreserveIDs {
		schema.ID = event.SchemaID
		// CCloud requires subject-level IMPORT mode
		_ = r.cfg.TargetClient.SetSubjectMode(event.Subject, "IMPORT")
	}

	_, err := r.cfg.TargetClient.RegisterSchema(event.Subject, schema)
	if err != nil {
		// Idempotent: "already registered" is not an error
		if isAlreadyExistsError(err) {
			return nil
		}
		return fmt.Errorf("failed to register %s v%d: %w", event.Subject, event.Version, err)
	}

	if r.cfg.PreserveIDs {
		// Restore READWRITE mode for the subject
		_ = r.cfg.TargetClient.SetSubjectMode(event.Subject, "READWRITE")
	}

	r.stats.IncrSchemas()
	return nil
}

func (r *Replicator) applyConfigEvent(event *kafka.SchemaEvent) error {
	if event.Subject == "" {
		// Global config
		if err := r.cfg.TargetClient.SetConfig(event.Compatibility); err != nil {
			return fmt.Errorf("failed to set global config: %w", err)
		}
	} else {
		if err := r.cfg.TargetClient.SetSubjectConfig(event.Subject, event.Compatibility); err != nil {
			return fmt.Errorf("failed to set config for %s: %w", event.Subject, err)
		}
	}
	r.stats.IncrConfigs()
	return nil
}

func (r *Replicator) applyModeEvent(event *kafka.SchemaEvent) error {
	if event.Subject == "" {
		// Skip global mode changes -- don't override IMPORT mode on target
		return nil
	}
	if err := r.cfg.TargetClient.SetSubjectMode(event.Subject, event.Mode); err != nil {
		// Subject may not exist yet
		if strings.Contains(err.Error(), "not found") {
			return nil
		}
		return fmt.Errorf("failed to set mode for %s: %w", event.Subject, err)
	}
	r.stats.IncrModes()
	return nil
}

func (r *Replicator) applyDeleteEvent(event *kafka.SchemaEvent) error {
	if event.Subject == "" {
		// Skip empty subject deletes (internal SR bookkeeping)
		return nil
	}
	_, err := r.cfg.TargetClient.DeleteSubject(event.Subject, false)
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "405") {
			return nil
		}
		return fmt.Errorf("failed to delete subject %s: %w", event.Subject, err)
	}
	r.stats.IncrDeletes()
	return nil
}

// performInitialSync does a full clone from source to target.
func (r *Replicator) performInitialSync(ctx context.Context) error {
	source := r.cfg.SourceClient
	target := r.cfg.TargetClient

	subjects, err := source.GetSubjects(false)
	if err != nil {
		return fmt.Errorf("failed to get source subjects: %w", err)
	}

	// Apply filter
	if r.cfg.Filter != "" {
		subjects = filterSubjects(subjects, r.cfg.Filter)
	}

	output.Info("Initial sync: %d subjects to replicate", len(subjects))

	for _, subj := range subjects {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		versions, err := source.GetVersions(subj, false)
		if err != nil {
			output.Warning("Skipping %s: %v", subj, err)
			continue
		}

		if r.cfg.PreserveIDs {
			_ = target.SetSubjectMode(subj, "IMPORT")
		}

		for _, v := range versions {
			schema, err := source.GetSchema(subj, strconv.Itoa(v))
			if err != nil {
				output.Warning("Skipping %s v%d: %v", subj, v, err)
				continue
			}

			regSchema := &client.Schema{
				Schema:     schema.Schema,
				SchemaType: schema.SchemaType,
				References: schema.References,
				Metadata:   schema.Metadata,
				RuleSet:    schema.RuleSet,
			}
			if r.cfg.PreserveIDs {
				regSchema.ID = schema.ID
			}

			_, err = target.RegisterSchema(subj, regSchema)
			if err != nil && !isAlreadyExistsError(err) {
				output.Warning("Failed to register %s v%d: %v", subj, v, err)
				r.stats.IncrErrors()
			} else {
				r.stats.IncrSchemas()
			}
		}

		if r.cfg.PreserveIDs {
			_ = target.SetSubjectMode(subj, "READWRITE")
		}
	}

	return nil
}

func isAlreadyExistsError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "already exists") ||
		strings.Contains(msg, "already registered")
}

// filterSubjects filters subjects by a glob pattern.
func filterSubjects(subjects []string, pattern string) []string {
	var filtered []string
	pattern = strings.ToLower(pattern)
	for _, subj := range subjects {
		if matchGlob(strings.ToLower(subj), pattern) {
			filtered = append(filtered, subj)
		}
	}
	return filtered
}

// matchGlob performs simple glob matching supporting * wildcard.
func matchGlob(s, pattern string) bool {
	if pattern == "*" {
		return true
	}

	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return s == pattern
	}

	if parts[0] != "" && !strings.HasPrefix(s, parts[0]) {
		return false
	}

	if parts[len(parts)-1] != "" && !strings.HasSuffix(s, parts[len(parts)-1]) {
		return false
	}

	idx := len(parts[0])
	for i := 1; i < len(parts)-1; i++ {
		if parts[i] == "" {
			continue
		}
		newIdx := strings.Index(s[idx:], parts[i])
		if newIdx < 0 {
			return false
		}
		idx += newIdx + len(parts[i])
	}

	return true
}
