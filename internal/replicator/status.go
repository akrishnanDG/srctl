package replicator

import (
	"context"
	"time"

	"github.com/srctl/srctl/internal/output"
)

// StatusReporter periodically prints replication status to the terminal.
type StatusReporter struct {
	stats    *Stats
	interval time.Duration
	source   string
	target   string
}

// NewStatusReporter creates a status reporter that prints every interval.
func NewStatusReporter(stats *Stats, interval time.Duration, source, target string) *StatusReporter {
	return &StatusReporter{
		stats:    stats,
		interval: interval,
		source:   source,
		target:   target,
	}
}

// Run starts the status reporting loop. Blocks until ctx is cancelled.
func (sr *StatusReporter) Run(ctx context.Context) {
	ticker := time.NewTicker(sr.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snap := sr.stats.Snapshot()
			output.Info("[%s] %s -> %s | schemas=%d configs=%d deletes=%d errors=%d events=%d filtered=%d offset=%d uptime=%s",
				time.Now().Format("15:04:05"),
				sr.source,
				sr.target,
				snap.SchemasReplicated,
				snap.ConfigsReplicated,
				snap.DeletesReplicated,
				snap.Errors,
				snap.EventsProcessed,
				snap.EventsFiltered,
				snap.LastOffset,
				snap.Uptime.Truncate(time.Second),
			)
		}
	}
}
