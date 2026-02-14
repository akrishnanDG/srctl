package replicator

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsServer serves Prometheus metrics via HTTP.
type MetricsServer struct {
	stats    *Stats
	port     int
	source   string
	target   string
	server   *http.Server
	registry *prometheus.Registry

	schemasReplicated prometheus.Counter
	configsReplicated prometheus.Counter
	deletesReplicated prometheus.Counter
	modesReplicated   prometheus.Counter
	errors            prometheus.Counter
	eventsProcessed   prometheus.Counter
	eventsFiltered    prometheus.Counter
	lastOffset        prometheus.Gauge
	uptimeSeconds     prometheus.Gauge
}

// NewMetricsServer creates a new metrics server on the given port.
func NewMetricsServer(stats *Stats, port int, source, target string) *MetricsServer {
	reg := prometheus.NewRegistry()
	labels := prometheus.Labels{"source": source, "target": target}

	m := &MetricsServer{
		stats:    stats,
		port:     port,
		source:   source,
		target:   target,
		registry: reg,
		schemasReplicated: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "srctl_replicate_schemas_total",
			Help:        "Total number of schemas replicated",
			ConstLabels: labels,
		}),
		configsReplicated: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "srctl_replicate_configs_total",
			Help:        "Total number of config changes replicated",
			ConstLabels: labels,
		}),
		deletesReplicated: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "srctl_replicate_deletes_total",
			Help:        "Total number of deletes replicated",
			ConstLabels: labels,
		}),
		modesReplicated: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "srctl_replicate_modes_total",
			Help:        "Total number of mode changes replicated",
			ConstLabels: labels,
		}),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "srctl_replicate_errors_total",
			Help:        "Total number of replication errors",
			ConstLabels: labels,
		}),
		eventsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "srctl_replicate_events_processed_total",
			Help:        "Total number of events processed",
			ConstLabels: labels,
		}),
		eventsFiltered: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "srctl_replicate_events_filtered_total",
			Help:        "Total number of events filtered out",
			ConstLabels: labels,
		}),
		lastOffset: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "srctl_replicate_last_offset",
			Help:        "Last processed Kafka offset",
			ConstLabels: labels,
		}),
		uptimeSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "srctl_replicate_uptime_seconds",
			Help:        "Replicator uptime in seconds",
			ConstLabels: labels,
		}),
	}

	reg.MustRegister(
		m.schemasReplicated,
		m.configsReplicated,
		m.deletesReplicated,
		m.modesReplicated,
		m.errors,
		m.eventsProcessed,
		m.eventsFiltered,
		m.lastOffset,
		m.uptimeSeconds,
	)

	return m
}

// Start begins serving metrics. Blocks until ctx is cancelled.
func (m *MetricsServer) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))

	m.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", m.port),
		Handler: mux,
	}

	// Update metrics periodically from stats
	go m.updateLoop(ctx)

	// Shutdown when context is cancelled
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.server.Shutdown(shutdownCtx)
	}()

	if err := m.server.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("metrics server error: %w", err)
	}
	return nil
}

// updateLoop periodically reads from Stats and updates Prometheus metrics.
func (m *MetricsServer) updateLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var prevSnap StatsSnapshot

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snap := m.stats.Snapshot()

			// Counters: add the delta since last update
			if delta := snap.SchemasReplicated - prevSnap.SchemasReplicated; delta > 0 {
				m.schemasReplicated.Add(float64(delta))
			}
			if delta := snap.ConfigsReplicated - prevSnap.ConfigsReplicated; delta > 0 {
				m.configsReplicated.Add(float64(delta))
			}
			if delta := snap.DeletesReplicated - prevSnap.DeletesReplicated; delta > 0 {
				m.deletesReplicated.Add(float64(delta))
			}
			if delta := snap.ModesReplicated - prevSnap.ModesReplicated; delta > 0 {
				m.modesReplicated.Add(float64(delta))
			}
			if delta := snap.Errors - prevSnap.Errors; delta > 0 {
				m.errors.Add(float64(delta))
			}
			if delta := snap.EventsProcessed - prevSnap.EventsProcessed; delta > 0 {
				m.eventsProcessed.Add(float64(delta))
			}
			if delta := snap.EventsFiltered - prevSnap.EventsFiltered; delta > 0 {
				m.eventsFiltered.Add(float64(delta))
			}

			// Gauges: set directly
			m.lastOffset.Set(float64(snap.LastOffset))
			m.uptimeSeconds.Set(snap.Uptime.Seconds())

			prevSnap = snap
		}
	}
}
