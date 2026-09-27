package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
)

type largeTransactionMeasurement struct {
	events        int
	duration      time.Duration
	heapStart     uint64
	heapBefore    uint64
	heapAfter     uint64
	heapPeak      uint64
	transactions  int
	appliedEvents int
}

// TestLargeTransactionMemoryBaseline is intentionally opt-in because the largest
// scenario allocates enough memory to make the regular test suite noisy.
func TestLargeTransactionMemoryBaseline(t *testing.T) {
	if os.Getenv("CDC_MEASURE_LARGE_TX") != "1" {
		t.Skip("defina CDC_MEASURE_LARGE_TX=1 para medir transacoes grandes")
	}

	scenarios := []struct {
		name   string
		events int
	}{
		{name: "small", events: 1_000},
		{name: "large", events: 25_000},
		{name: "very_large", events: 100_000},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			measurement := measureLargeTransaction(t, scenario.events)
			t.Logf(
				"events=%d duration=%s heap_start=%s heap_before_commit=%s heap_after_commit=%s heap_peak=%s transactions=%d applied_events=%d",
				measurement.events,
				measurement.duration,
				formatBytes(measurement.heapStart),
				formatBytes(measurement.heapBefore),
				formatBytes(measurement.heapAfter),
				formatBytes(measurement.heapPeak),
				measurement.transactions,
				measurement.appliedEvents,
			)
		})
	}
}

func measureLargeTransaction(t *testing.T, events int) largeTransactionMeasurement {
	t.Helper()

	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	measurement := largeTransactionMeasurement{
		events:    events,
		heapStart: stats.Alloc,
		heapPeak:  stats.Alloc,
	}

	decoder := decoderWithClientesRelation()
	output := &countingSink{}
	metrics := newMetrics()
	ctx := context.Background()
	start := time.Now()

	if _, _, err := decoder.processMessage(ctx, &pglogrepl.BeginMessage{FinalLSN: 0x100, Xid: 9001}, 0x100, output, metrics); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}

	for index := 0; index < events; index++ {
		message := &pglogrepl.InsertMessage{
			RelationID: 7,
			Tuple: clientesTuple(
				fmt.Sprintf("%d", index+1),
				fmt.Sprintf("Cliente %06d", index+1),
				fmt.Sprintf("cliente-%06d@example.com", index+1),
			),
		}
		if _, _, err := decoder.processMessage(ctx, message, pglogrepl.LSN(0x101+index), output, metrics); err != nil {
			t.Fatalf("INSERT %d: %v", index+1, err)
		}
		if index%1024 == 0 {
			runtime.ReadMemStats(&stats)
			if stats.Alloc > measurement.heapPeak {
				measurement.heapPeak = stats.Alloc
			}
		}
	}

	runtime.ReadMemStats(&stats)
	measurement.heapBefore = stats.Alloc
	if stats.Alloc > measurement.heapPeak {
		measurement.heapPeak = stats.Alloc
	}

	processedLSN, _, err := decoder.processMessage(ctx, &pglogrepl.CommitMessage{
		CommitLSN:         0x200,
		TransactionEndLSN: 0x208,
	}, 0x200, output, metrics)
	if err != nil {
		t.Fatalf("COMMIT: %v", err)
	}
	if processedLSN != 0x208 {
		t.Fatalf("TransactionEndLSN inesperado: %s", processedLSN)
	}

	runtime.ReadMemStats(&stats)
	measurement.heapAfter = stats.Alloc
	if stats.Alloc > measurement.heapPeak {
		measurement.heapPeak = stats.Alloc
	}
	measurement.duration = time.Since(start)
	measurement.transactions = output.transactions
	measurement.appliedEvents = output.events

	return measurement
}

type countingSink struct {
	transactions int
	events       int
}

func (sink *countingSink) ApplyTransaction(_ context.Context, transaction sourceTransaction) ([]string, error) {
	sink.transactions++
	sink.events += len(transaction.events)
	return nil, nil
}

func (*countingSink) Close(context.Context) error { return nil }
func (*countingSink) Name() string                { return "counting" }
func (*countingSink) Ready(context.Context) error { return nil }

func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	value := float64(bytes)
	for _, suffix := range []string{"KiB", "MiB", "GiB"} {
		value /= unit
		if value < unit {
			return fmt.Sprintf("%.2f %s", value, suffix)
		}
	}
	return fmt.Sprintf("%.2f TiB", value/unit)
}
