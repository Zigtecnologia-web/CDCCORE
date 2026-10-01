package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
)

type metrics struct {
	eventsReceived                 atomic.Uint64
	eventsApplied                  atomic.Uint64
	transactionsReceived           atomic.Uint64
	transactionsCommitted          atomic.Uint64
	transactionsFailed             atomic.Uint64
	transactionsRedeliver          atomic.Uint64
	reconnects                     atomic.Uint64
	sinkErrors                     atomic.Uint64
	schemaMetadataChanges          atomic.Uint64
	schemaMetadataUnknown          atomic.Uint64
	schemaMetadataRelationMessages atomic.Uint64
	schemaApplyAttempts            atomic.Uint64
	schemaApplySuccess             atomic.Uint64
	schemaApplyFailure             atomic.Uint64
	schemaApplyRejected            atomic.Uint64
	schemaValidationTotal          schemaValidationMetrics

	lastProcessed atomic.Value
	lastConfirmed atomic.Value
}

type schemaValidationMetrics struct {
	compatible   atomic.Uint64
	incompatible atomic.Uint64
	unknown      atomic.Uint64
}

func (metrics *schemaValidationMetrics) Add(status string) {
	switch status {
	case string(compatibilityCompatible):
		metrics.compatible.Add(1)
	case string(compatibilityIncompatible):
		metrics.incompatible.Add(1)
	default:
		metrics.unknown.Add(1)
	}
}

func (metrics *schemaValidationMetrics) Snapshot() map[string]uint64 {
	return map[string]uint64{
		"compatible":   metrics.compatible.Load(),
		"incompatible": metrics.incompatible.Load(),
		"unknown":      metrics.unknown.Load(),
	}
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
		"events_received":                             metrics.eventsReceived.Load(),
		"events_applied":                              metrics.eventsApplied.Load(),
		"transactions_received":                       metrics.transactionsReceived.Load(),
		"transactions_committed":                      metrics.transactionsCommitted.Load(),
		"transactions_failed":                         metrics.transactionsFailed.Load(),
		"transactions_redelivered":                    metrics.transactionsRedeliver.Load(),
		"reconnects":                                  metrics.reconnects.Load(),
		"sink_errors":                                 metrics.sinkErrors.Load(),
		"cdc_schema_metadata_changes_total":           metrics.schemaMetadataChanges.Load(),
		"cdc_schema_metadata_unknown_total":           metrics.schemaMetadataUnknown.Load(),
		"cdc_schema_metadata_relation_messages_total": metrics.schemaMetadataRelationMessages.Load(),
		"cdc_schema_apply_attempts_total":             metrics.schemaApplyAttempts.Load(),
		"cdc_schema_apply_success_total":              metrics.schemaApplySuccess.Load(),
		"cdc_schema_apply_failure_total":              metrics.schemaApplyFailure.Load(),
		"cdc_schema_apply_rejected_total":             metrics.schemaApplyRejected.Load(),
		"cdc_schema_validation_total":                 metrics.schemaValidationTotal.Snapshot(),
		"last_processed_lsn":                          metrics.lastProcessed.Load(),
		"last_confirmed_lsn":                          metrics.lastConfirmed.Load(),
	}
}

func (metrics *metrics) prometheus() string {
	var builder strings.Builder
	writePrometheusCounter(&builder, "cdc_events_total", "Total de eventos CDC aplicados com sucesso.", metrics.eventsApplied.Load())
	writePrometheusCounter(&builder, "cdc_events_received_total", "Total de eventos CDC recebidos da origem.", metrics.eventsReceived.Load())
	writePrometheusCounter(&builder, "cdc_transactions_total", "Total de transacoes CDC confirmadas no destino.", metrics.transactionsCommitted.Load())
	writePrometheusCounter(&builder, "cdc_transactions_received_total", "Total de transacoes CDC recebidas da origem.", metrics.transactionsReceived.Load())
	writePrometheusCounter(&builder, "cdc_transactions_failed_total", "Total de transacoes CDC que falharam.", metrics.transactionsFailed.Load())
	writePrometheusCounter(&builder, "cdc_transactions_redelivered_total", "Total de transacoes CDC reentregues.", metrics.transactionsRedeliver.Load())
	writePrometheusCounter(&builder, "cdc_reconnects_total", "Total de tentativas de reconexao realizadas.", metrics.reconnects.Load())
	writePrometheusCounter(&builder, "cdc_sink_errors_total", "Total de erros ao gravar no destino.", metrics.sinkErrors.Load())
	writePrometheusCounter(&builder, "cdc_schema_metadata_relation_messages_total", "Total de mensagens de relacao observadas.", metrics.schemaMetadataRelationMessages.Load())
	writePrometheusCounter(&builder, "cdc_schema_metadata_changes_total", "Total de diferencas de metadata observadas.", metrics.schemaMetadataChanges.Load())
	writePrometheusCounter(&builder, "cdc_schema_metadata_unknown_total", "Total de diferencas de metadata classificadas como UNKNOWN.", metrics.schemaMetadataUnknown.Load())
	writePrometheusCounter(&builder, "cdc_schema_apply_attempts_total", "Total de decisoes de apply de schema avaliadas.", metrics.schemaApplyAttempts.Load())
	writePrometheusCounter(&builder, "cdc_schema_apply_success_total", "Total de applies de schema confirmados.", metrics.schemaApplySuccess.Load())
	writePrometheusCounter(&builder, "cdc_schema_apply_failure_total", "Total de falhas durante apply ou revalidacao de schema.", metrics.schemaApplyFailure.Load())
	writePrometheusCounter(&builder, "cdc_schema_apply_rejected_total", "Total de operacoes de schema bloqueadas por modo ou politica.", metrics.schemaApplyRejected.Load())
	writePrometheusCounterWithLabels(&builder, "cdc_schema_validation_total", "Total de validacoes de schema por status de compatibilidade.", []prometheusCounterLabelValue{
		{labels: `status="compatible"`, value: metrics.schemaValidationTotal.compatible.Load()},
		{labels: `status="incompatible"`, value: metrics.schemaValidationTotal.incompatible.Load()},
		{labels: `status="unknown"`, value: metrics.schemaValidationTotal.unknown.Load()},
	})
	return builder.String()
}

type prometheusCounterLabelValue struct {
	labels string
	value  uint64
}

func writePrometheusCounter(builder *strings.Builder, name string, help string, value uint64) {
	builder.WriteString("# HELP ")
	builder.WriteString(name)
	builder.WriteByte(' ')
	builder.WriteString(help)
	builder.WriteByte('\n')
	builder.WriteString("# TYPE ")
	builder.WriteString(name)
	builder.WriteString(" counter\n")
	builder.WriteString(name)
	builder.WriteByte(' ')
	builder.WriteString(fmt.Sprintf("%d", value))
	builder.WriteString("\n\n")
}

func writePrometheusCounterWithLabels(builder *strings.Builder, name string, help string, values []prometheusCounterLabelValue) {
	builder.WriteString("# HELP ")
	builder.WriteString(name)
	builder.WriteByte(' ')
	builder.WriteString(help)
	builder.WriteByte('\n')
	builder.WriteString("# TYPE ")
	builder.WriteString(name)
	builder.WriteString(" counter\n")
	for _, item := range values {
		builder.WriteString(name)
		builder.WriteByte('{')
		builder.WriteString(item.labels)
		builder.WriteString("} ")
		builder.WriteString(fmt.Sprintf("%d", item.value))
		builder.WriteByte('\n')
	}
	builder.WriteByte('\n')
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

func newHealthMux(metrics *metrics, readiness *readinessState) http.Handler {
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
	mux.HandleFunc("/metrics/prometheus", func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = writer.Write([]byte(metrics.prometheus()))
	})

	return mux
}

func startHealthServer(ctx context.Context, addr string, metrics *metrics, readiness *readinessState) *http.Server {
	server := &http.Server{
		Addr:              addr,
		Handler:           newHealthMux(metrics, readiness),
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
