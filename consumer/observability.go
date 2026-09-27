package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
)

type metrics struct {
	eventsReceived        atomic.Uint64
	eventsApplied         atomic.Uint64
	transactionsReceived  atomic.Uint64
	transactionsCommitted atomic.Uint64
	transactionsFailed    atomic.Uint64
	transactionsRedeliver atomic.Uint64
	reconnects            atomic.Uint64
	sinkErrors            atomic.Uint64

	lastProcessed atomic.Value
	lastConfirmed atomic.Value
}

func newMetrics() *metrics {
	metrics := &metrics{}
	metrics.lastProcessed.Store("")
	metrics.lastConfirmed.Store("")
	return metrics
}

func (metrics *metrics) setProcessed(lsn pglogrepl.LSN) {
	metrics.lastProcessed.Store(lsn.String())
}

func (metrics *metrics) setConfirmed(lsn pglogrepl.LSN) {
	metrics.lastConfirmed.Store(lsn.String())
}

func (metrics *metrics) snapshot() map[string]any {
	return map[string]any{
		"events_received":          metrics.eventsReceived.Load(),
		"events_applied":           metrics.eventsApplied.Load(),
		"transactions_received":    metrics.transactionsReceived.Load(),
		"transactions_committed":   metrics.transactionsCommitted.Load(),
		"transactions_failed":      metrics.transactionsFailed.Load(),
		"transactions_redelivered": metrics.transactionsRedeliver.Load(),
		"reconnects":               metrics.reconnects.Load(),
		"sink_errors":              metrics.sinkErrors.Load(),
		"last_processed_lsn":       metrics.lastProcessed.Load(),
		"last_confirmed_lsn":       metrics.lastConfirmed.Load(),
	}
}

type readinessState struct {
	mu        sync.RWMutex
	ready     bool
	lastError string
}

func (state *readinessState) setReady() {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.ready = true
	state.lastError = ""
}

func (state *readinessState) setNotReady(err error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.ready = false
	if err != nil {
		state.lastError = err.Error()
	}
}

func (state *readinessState) snapshot() (bool, string) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.ready, state.lastError
}

func startHealthServer(ctx context.Context, addr string, metrics *metrics, readiness *readinessState) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		ready, lastError := readiness.snapshot()
		if !ready {
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = writer.Write([]byte(lastError + "\n"))
			return
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ready\n"))
	})
	mux.HandleFunc("/metrics", func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(metrics.snapshot())
	})

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("level=error event=health_server_failed error=%q\n", err.Error())
		}
	}()
	return server
}
