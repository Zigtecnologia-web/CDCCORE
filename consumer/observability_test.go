package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
)

func TestMetricsEndpointKeepsJSONContract(t *testing.T) {
	metrics := newMetrics()
	metrics.eventsApplied.Add(7)
	readiness := &readinessState{}

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()
	newHealthMux(metrics, readiness).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status /metrics=%d, esperado %d", recorder.Code, http.StatusOK)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content-type /metrics=%q, esperado application/json", got)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, `"events_applied":7`) {
		t.Fatalf("payload /metrics nao preservou JSON esperado: %s", body)
	}
	if strings.Contains(body, "# HELP") {
		t.Fatalf("payload /metrics nao deve usar formato Prometheus: %s", body)
	}
}

func TestPrometheusMetricsEndpoint(t *testing.T) {
	metrics := newMetrics()
	metrics.eventsReceived.Add(9)
	metrics.eventsApplied.Add(7)
	metrics.transactionsReceived.Add(4)
	metrics.transactionsCommitted.Add(3)
	metrics.schemaMetadataRelationMessages.Add(2)
	metrics.schemaMetadataChanges.Add(1)
	metrics.schemaValidationTotal.Add(string(compatibilityCompatible))
	readiness := &readinessState{}

	request := httptest.NewRequest(http.MethodGet, "/metrics/prometheus", nil)
	recorder := httptest.NewRecorder()
	newHealthMux(metrics, readiness).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status /metrics/prometheus=%d, esperado %d", recorder.Code, http.StatusOK)
	}
	if got := recorder.Header().Get("Content-Type"); got != "text/plain; version=0.0.4" {
		t.Fatalf("content-type /metrics/prometheus=%q, esperado text/plain; version=0.0.4", got)
	}
	body := recorder.Body.String()
	expected := []string{
		"# HELP cdc_events_total Total de eventos CDC aplicados com sucesso.",
		"# TYPE cdc_events_total counter",
		"cdc_events_total 7",
		"cdc_events_received_total 9",
		"cdc_transactions_total 3",
		"cdc_transactions_received_total 4",
		"cdc_schema_metadata_relation_messages_total 2",
		"cdc_schema_metadata_changes_total 1",
		`cdc_schema_validation_total{status="compatible"} 1`,
		`cdc_schema_validation_total{status="incompatible"} 0`,
		`cdc_schema_validation_total{status="unknown"} 0`,
	}
	for _, text := range expected {
		if !strings.Contains(body, text) {
			t.Fatalf("payload /metrics/prometheus nao contem %q:\n%s", text, body)
		}
	}
	if strings.Contains(body, `"events_applied"`) {
		t.Fatalf("payload /metrics/prometheus nao deve usar formato JSON: %s", body)
	}
}

func TestPrometheusMetricsExposeCounterTypes(t *testing.T) {
	metrics := newMetrics()
	body := metrics.prometheus()

	expectedCounters := []string{
		"cdc_events_total",
		"cdc_events_received_total",
		"cdc_transactions_total",
		"cdc_transactions_received_total",
		"cdc_transactions_failed_total",
		"cdc_transactions_redelivered_total",
		"cdc_reconnects_total",
		"cdc_sink_errors_total",
		"cdc_schema_metadata_relation_messages_total",
		"cdc_schema_metadata_changes_total",
		"cdc_schema_metadata_unknown_total",
		"cdc_schema_apply_attempts_total",
		"cdc_schema_apply_success_total",
		"cdc_schema_apply_failure_total",
		"cdc_schema_apply_rejected_total",
		"cdc_schema_validation_total",
	}
	for _, name := range expectedCounters {
		if !strings.Contains(body, "# HELP "+name+" ") {
			t.Fatalf("metrica %s sem HELP:\n%s", name, body)
		}
		if !strings.Contains(body, "# TYPE "+name+" counter") {
			t.Fatalf("metrica %s sem TYPE counter:\n%s", name, body)
		}
	}
	for _, label := range []string{`status="compatible"`, `status="incompatible"`, `status="unknown"`} {
		if !strings.Contains(body, "cdc_schema_validation_total{"+label+"} 0") {
			t.Fatalf("cdc_schema_validation_total sem label %s:\n%s", label, body)
		}
	}
}

func TestJSONAndPrometheusMetricsStayConsistent(t *testing.T) {
	metrics := newMetrics()
	metrics.eventsReceived.Add(8)
	metrics.eventsApplied.Add(7)
	metrics.transactionsReceived.Add(6)
	metrics.transactionsCommitted.Add(5)
	metrics.transactionsFailed.Add(4)
	metrics.transactionsRedeliver.Add(3)
	metrics.reconnects.Add(2)
	metrics.sinkErrors.Add(1)
	metrics.schemaMetadataRelationMessages.Add(9)
	metrics.schemaMetadataChanges.Add(10)
	metrics.schemaMetadataUnknown.Add(11)
	metrics.schemaApplyAttempts.Add(12)
	metrics.schemaApplySuccess.Add(13)
	metrics.schemaApplyFailure.Add(14)
	metrics.schemaApplyRejected.Add(15)
	metrics.schemaValidationTotal.compatible.Add(16)
	metrics.schemaValidationTotal.incompatible.Add(17)
	metrics.schemaValidationTotal.unknown.Add(18)

	recorder := httptest.NewRecorder()
	newHealthMux(metrics, &readinessState{}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	var snapshot map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &snapshot); err != nil {
		t.Fatalf("/metrics retornou JSON invalido: %v", err)
	}
	prometheusMetrics := parsePrometheusCounters(t, metrics.prometheus())

	expectedMapping := map[string]string{
		"events_received":                             "cdc_events_received_total",
		"events_applied":                              "cdc_events_total",
		"transactions_received":                       "cdc_transactions_received_total",
		"transactions_committed":                      "cdc_transactions_total",
		"transactions_failed":                         "cdc_transactions_failed_total",
		"transactions_redelivered":                    "cdc_transactions_redelivered_total",
		"reconnects":                                  "cdc_reconnects_total",
		"sink_errors":                                 "cdc_sink_errors_total",
		"cdc_schema_metadata_relation_messages_total": "cdc_schema_metadata_relation_messages_total",
		"cdc_schema_metadata_changes_total":           "cdc_schema_metadata_changes_total",
		"cdc_schema_metadata_unknown_total":           "cdc_schema_metadata_unknown_total",
		"cdc_schema_apply_attempts_total":             "cdc_schema_apply_attempts_total",
		"cdc_schema_apply_success_total":              "cdc_schema_apply_success_total",
		"cdc_schema_apply_failure_total":              "cdc_schema_apply_failure_total",
		"cdc_schema_apply_rejected_total":             "cdc_schema_apply_rejected_total",
	}
	for jsonName, prometheusName := range expectedMapping {
		gotJSON := uint64(snapshot[jsonName].(float64))
		if gotPrometheus := prometheusMetrics[prometheusName]; gotPrometheus != gotJSON {
			t.Fatalf("%s=%d em JSON, mas %s=%d em Prometheus", jsonName, gotJSON, prometheusName, gotPrometheus)
		}
	}

	validation := snapshot["cdc_schema_validation_total"].(map[string]any)
	for _, status := range []string{"compatible", "incompatible", "unknown"} {
		jsonValue := uint64(validation[status].(float64))
		prometheusName := `cdc_schema_validation_total{status="` + status + `"}`
		if gotPrometheus := prometheusMetrics[prometheusName]; gotPrometheus != jsonValue {
			t.Fatalf("schema validation %s=%d em JSON, mas Prometheus=%d", status, jsonValue, gotPrometheus)
		}
	}
}

func TestCommitMetricsOnlyCountSuccessfulSinkApply(t *testing.T) {
	metrics := newMetrics()
	decoder := &pgoutputDecoder{
		sourceID: "spec-21",
		tx: &transactionState{
			id: 1,
			events: []event{{
				Type:          eventInsert,
				LSN:           "0/1",
				TransactionID: 1,
				Schema:        "public",
				Table:         "clientes",
				Data:          map[string]any{"id": int64(1)},
			}},
		},
	}

	commitLSN, _, err := decoder.processMessage(
		context.Background(),
		&pglogrepl.CommitMessage{TransactionEndLSN: 0x200},
		0x200,
		&stubSink{},
		metrics,
	)
	if err != nil {
		t.Fatalf("commit valido falhou: %v", err)
	}
	if commitLSN != 0x200 {
		t.Fatalf("commit LSN=%s, esperado %s", commitLSN, pglogrepl.LSN(0x200))
	}
	if got := metrics.eventsApplied.Load(); got != 1 {
		t.Fatalf("eventsApplied=%d, esperado 1", got)
	}
	if got := metrics.transactionsCommitted.Load(); got != 1 {
		t.Fatalf("transactionsCommitted=%d, esperado 1", got)
	}
	if got := metrics.transactionsFailed.Load(); got != 0 {
		t.Fatalf("transactionsFailed=%d, esperado 0", got)
	}

	decoder.tx = &transactionState{id: 2, events: []event{{Type: eventInsert}}}
	if _, _, err := decoder.processMessage(
		context.Background(),
		&pglogrepl.CommitMessage{TransactionEndLSN: 0x300},
		0x300,
		&stubSink{err: errors.New("sink indisponivel")},
		metrics,
	); err == nil {
		t.Fatal("commit com erro de sink deveria falhar")
	}
	if got := metrics.eventsApplied.Load(); got != 1 {
		t.Fatalf("eventsApplied apos falha=%d, esperado 1", got)
	}
	if got := metrics.transactionsCommitted.Load(); got != 1 {
		t.Fatalf("transactionsCommitted apos falha=%d, esperado 1", got)
	}
	if got := metrics.transactionsFailed.Load(); got != 1 {
		t.Fatalf("transactionsFailed apos falha=%d, esperado 1", got)
	}
}

func parsePrometheusCounters(t *testing.T, body string) map[string]uint64 {
	t.Helper()
	values := make(map[string]uint64)
	for _, line := range strings.Split(body, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 2 {
			t.Fatalf("linha Prometheus invalida: %q", line)
		}
		value, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			t.Fatalf("valor Prometheus invalido em %q: %v", line, err)
		}
		values[parts[0]] = value
	}
	return values
}

type stubSink struct {
	err error
}

func (sink *stubSink) ApplyTransaction(context.Context, sourceTransaction) ([]string, error) {
	if sink.err != nil {
		return nil, sink.err
	}
	return nil, nil
}

func (sink *stubSink) Close(context.Context) error {
	return nil
}

func (sink *stubSink) Name() string {
	return "stub"
}

func (sink *stubSink) Ready(context.Context) error {
	return nil
}
