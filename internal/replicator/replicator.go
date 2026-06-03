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
	"github.com/twmb/franz-go/pkg/kgo"
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
	EventsProcessed   int64
	EventsFiltered    int64
}

// StatsSnapshot is a point-in-time copy of stats.
type StatsSnapshot struct {
	Uptime            time.Duration
	SchemasReplicated int64
	ConfigsReplicated int64
	DeletesReplicated int64
	ModesReplicated   int64
	Errors            int64
	EventsProcessed   int64
	EventsFiltered    int64
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
		EventsProcessed:   s.EventsProcessed,
		EventsFiltered:    s.EventsFiltered,
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

		// Track records that were successfully applied, IN ORDER. We commit
		// only this prefix. The guarantee we maintain is: never commit past a
		// record that has not been successfully applied. On the first
		// unrecoverable failure we stop processing the rest of the batch so a
		// later success can't advance the committed offset past the failure.
		// On restart the consumer group resumes from the first uncommitted
		// (i.e. first unapplied) record, making replay correct.
		applied := make([]*kgo.Record, 0, len(records))
		for _, record := range records {
			event, err := kafka.ParseRecord(record.Key, record.Value, record.Offset, record.Partition)
			if err != nil {
				// A record that cannot be parsed will never become parseable
				// on retry; treat it as applied so we make forward progress
				// rather than blocking the stream forever on a poison record.
				output.Warning("Parse error at offset %d (skipping): %v", record.Offset, err)
				r.stats.IncrErrors()
				applied = append(applied, record)
				continue
			}

			if event == nil {
				// NOOP or empty: nothing to apply, but the offset is safe to commit.
				applied = append(applied, record)
				continue
			}

			r.stats.IncrProcessed()

			// Apply filter. SR subjects are case-sensitive, so match exactly.
			if r.cfg.Filter != "" && event.Subject != "" {
				if !matchGlob(event.Subject, r.cfg.Filter) {
					r.stats.IncrFiltered()
					applied = append(applied, record)
					continue
				}
			}

			// Apply event with retries. If the target is unreachable, block
			// and keep retrying rather than skipping the event.
			if err := r.applyWithRetry(ctx, event, maxEventRetries); err != nil {
				output.Error("Failed to apply %s event for %s at offset %d: %v",
					event.Type, event.Subject, event.Offset, err)
				r.stats.IncrErrors()
				// Stop the batch on the first unrecoverable failure. We do NOT
				// add this record to `applied`, so the commit below stops just
				// before it and it will be re-polled on restart.
				break
			}

			applied = append(applied, record)
			r.stats.SetOffset(event.Offset)
			r.stats.SetLastEventTime(time.Now())
		}

		// Commit exactly the successfully-applied prefix. CommitRecords commits
		// the offset immediately after the highest applied record per
		// partition, so we never commit past an unapplied record.
		if len(applied) > 0 {
			if err := r.cfg.Consumer.CommitRecords(ctx, applied...); err != nil {
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
	lower := strings.ToLower(msg)

	// A missing/unresolved reference also surfaces as 422 (error code 42201),
	// but it is transient during streaming: the referenced subject may simply
	// not have been registered on the target yet. Keep these RETRYABLE so the
	// retry budget can ride out ordering races; only genuine incompatibility
	// errors should fail fast.
	if strings.Contains(lower, "reference") {
		return false
	}

	// 409 Conflict (schema already exists) is handled as success elsewhere.
	// 422 Unprocessable (incompatible schema, invalid schema) and
	// 400 Bad Request are genuine client errors that won't resolve on retry.
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
		if err := r.cfg.TargetClient.SetSubjectMode(event.Subject, "IMPORT"); err != nil {
			// Idempotency: if IMPORT mode can't be set because the subject already
			// exists / has existing subjects (e.g. HTTP initial sync already
			// replicated this, or replication was restarted against a populated
			// target), this is only a real failure if the schema is NOT already
			// present. If the schema is already present on the target, treat the
			// event as a successfully-applied no-op so offset progress isn't
			// blocked. Otherwise it's a genuine error we must not silently drop.
			if isExistingSubjectsImportError(err) && r.schemaAlreadyPresent(event) {
				output.Info("Skipping %s v%d: schema already present on target (import mode unavailable)",
					event.Subject, event.Version)
				return nil
			}
			output.Warning("Failed to set IMPORT mode for %s: %v", event.Subject, err)
			return fmt.Errorf("failed to set IMPORT mode for %s: %w", event.Subject, err)
		}
		// IMPORT mode was set successfully: restore READWRITE on EVERY exit
		// path (including error returns) so the subject is never left stuck in
		// IMPORT mode.
		defer func() {
			if err := r.cfg.TargetClient.SetSubjectMode(event.Subject, "READWRITE"); err != nil {
				output.Warning("Failed to restore READWRITE mode for %s: %v", event.Subject, err)
			}
		}()
	}

	_, err := r.cfg.TargetClient.RegisterSchema(event.Subject, schema)
	if err != nil {
		// Idempotent: "already registered" is not an error
		if isAlreadyExistsError(err) {
			return nil
		}
		return fmt.Errorf("failed to register %s v%d: %w", event.Subject, event.Version, err)
	}

	r.stats.IncrSchemas()
	return nil
}

func (r *Replicator) applyConfigEvent(event *kafka.SchemaEvent) error {
	if event.Tombstone {
		// A tombstoned CONFIG event means the compatibility override was
		// deleted (reset to the global/default). The client interface exposes
		// no delete-config method, so we cannot replicate the reset. Pushing
		// SetConfig("") would register an invalid empty compatibility level, so
		// we skip it and log instead.
		output.Info("Config reset (tombstone) for %q not replicated: deleting config overrides is not supported by the target client", event.Subject)
		return nil
	}
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

	// Subjects whose latest schema has references must be registered AFTER the
	// subjects they reference. Rather than building a full topological sort
	// (which requires resolving every reference's subject across versions), we
	// do a bounded multi-pass: attempt all remaining subjects, defer the ones
	// that fail with a missing-reference error, and repeat until a pass makes
	// no progress. This converges in O(depth) passes for valid DAGs and stops
	// cleanly on genuinely unresolvable subjects.
	remaining := subjects
	for pass := 1; len(remaining) > 0; pass++ {
		var deferred []string
		progress := false

		for _, subj := range remaining {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			retryable, err := r.syncSubject(source, target, subj)
			if err == nil {
				progress = true
				continue
			}
			if retryable {
				// Likely a missing reference; try again on a later pass once
				// its dependencies have been registered.
				deferred = append(deferred, subj)
				continue
			}
			// Non-retryable failure: already logged in syncSubject.
			progress = true
		}

		if len(deferred) == 0 {
			break
		}
		if !progress {
			// No subject made progress this pass: remaining subjects have
			// references we can't resolve. Register them anyway so the error
			// is surfaced per-subject rather than silently dropped.
			output.Warning("Initial sync: %d subjects have unresolved references after %d passes; attempting final registration", len(deferred), pass)
			for _, subj := range deferred {
				if _, err := r.syncSubject(source, target, subj); err != nil {
					output.Warning("Failed to sync %s (unresolved references): %v", subj, err)
				}
			}
			break
		}
		remaining = deferred
	}

	return nil
}

// syncSubject registers all versions of a single subject on the target.
// It returns (retryable=true, err) when the failure looks like a missing
// reference that may resolve once other subjects are registered. Any
// per-version errors that are not retryable are logged and counted here, and
// such a subject is reported as a non-retryable completion (nil error) unless
// IMPORT-mode setup itself failed.
func (r *Replicator) syncSubject(source, target client.SchemaRegistryClientInterface, subj string) (retryable bool, err error) {
	versions, err := source.GetVersions(subj, false)
	if err != nil {
		output.Warning("Skipping %s: %v", subj, err)
		return false, nil
	}

	if r.cfg.PreserveIDs {
		if err := target.SetSubjectMode(subj, "IMPORT"); err != nil {
			output.Warning("Failed to set IMPORT mode for %s, skipping: %v", subj, err)
			r.stats.IncrErrors()
			return false, nil
		}
		// Restore READWRITE on every exit path so the subject is never left
		// stuck in IMPORT mode.
		defer func() {
			if rerr := target.SetSubjectMode(subj, "READWRITE"); rerr != nil {
				output.Warning("Failed to restore READWRITE mode for %s: %v", subj, rerr)
			}
		}()
	}

	for _, v := range versions {
		schema, gerr := source.GetSchema(subj, strconv.Itoa(v))
		if gerr != nil {
			output.Warning("Skipping %s v%d: %v", subj, v, gerr)
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

		_, rerr := target.RegisterSchema(subj, regSchema)
		if rerr != nil && !isAlreadyExistsError(rerr) {
			// A missing reference means a subject this one depends on hasn't
			// been registered yet; signal the caller to retry on a later pass.
			if !isNonRetryableError(rerr) && strings.Contains(strings.ToLower(rerr.Error()), "reference") {
				return true, rerr
			}
			output.Warning("Failed to register %s v%d: %v", subj, v, rerr)
			r.stats.IncrErrors()
		} else {
			r.stats.IncrSchemas()
		}
	}

	return false, nil
}

func isAlreadyExistsError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "already exists") ||
		strings.Contains(msg, "already registered")
}

// isExistingSubjectsImportError reports whether the error indicates the target
// could not be put into IMPORT mode because it already has existing subjects
// (SR error code 42205 / "found existing subjects"). This happens when the
// HTTP initial sync already populated the target, or when replication is
// restarted against an already-populated target.
func isExistingSubjectsImportError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "found existing subjects") ||
		strings.Contains(msg, "42205")
}

// schemaAlreadyPresent reports whether the schema in the event is already
// registered on the target for its subject. It first tries to match by the
// preserved schema ID (the source ID we want on the target), then falls back
// to matching the exact schema content across the subject's versions. A true
// result means re-applying the event would be a no-op, so the event can be
// safely treated as already applied.
func (r *Replicator) schemaAlreadyPresent(event *kafka.SchemaEvent) bool {
	if event.Subject == "" {
		return false
	}

	// Fast path: if a schema with the preserved ID exists and maps to this
	// subject, it's already present.
	if event.SchemaID != 0 {
		if sv, err := r.cfg.TargetClient.GetSchemaSubjectVersionsByID(event.SchemaID); err == nil {
			for _, m := range sv {
				if m.Subject == event.Subject {
					return true
				}
			}
		}
	}

	// Fallback: compare exact schema content across the subject's versions.
	versions, err := r.cfg.TargetClient.GetVersions(event.Subject, false)
	if err != nil {
		return false
	}
	for _, v := range versions {
		s, err := r.cfg.TargetClient.GetSchema(event.Subject, strconv.Itoa(v))
		if err != nil {
			continue
		}
		if s.Schema == event.Schema {
			return true
		}
	}
	return false
}

// filterSubjects filters subjects by a glob pattern. SR subjects are
// case-sensitive, so matching is exact (no case folding).
func filterSubjects(subjects []string, pattern string) []string {
	var filtered []string
	for _, subj := range subjects {
		if matchGlob(subj, pattern) {
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
