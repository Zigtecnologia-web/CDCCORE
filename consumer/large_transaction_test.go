package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
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

type largeTransactionBaseline struct {
	duration   time.Duration
	heapBefore uint64
	heapPeak   uint64
}

var largeTransactionBaselines = map[int]largeTransactionBaseline{
	1_000: {
		duration:   2901083 * time.Nanosecond,
		heapBefore: mib(1.72),
		heapPeak:   mib(1.82),
	},
	25_000: {
		duration:   63408042 * time.Nanosecond,
		heapBefore: mib(18.84),
		heapPeak:   mib(21.51),
	},
	100_000: {
		duration:   229230375 * time.Nanosecond,
		heapBefore: mib(74.35),
		heapPeak:   mib(85.04),
	},
}

// TestLargeTransactionMemoryBaseline is intentionally opt-in because the largest
// scenario allocates enough memory to make the regular test suite noisy.
func TestLargeTransactionMemoryBaseline(t *testing.T) {
	if os.Getenv("CDC_MEASURE_LARGE_TX") != "1" {
		t.Skip("defina CDC_MEASURE_LARGE_TX=1 para medir transacoes grandes")
	}
	tolerance := largeTransactionMemoryTolerance(t)

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
			baseline := largeTransactionBaselines[scenario.events]
			t.Logf(
				"events=%d duration=%s heap_start=%s heap_before_commit=%s heap_after_commit=%s heap_peak=%s baseline_duration=%s baseline_heap_before_commit=%s baseline_heap_peak=%s tolerance=%.2fx transactions=%d applied_events=%d",
				measurement.events,
				measurement.duration,
				formatBytes(measurement.heapStart),
				formatBytes(measurement.heapBefore),
				formatBytes(measurement.heapAfter),
				formatBytes(measurement.heapPeak),
				baseline.duration,
				formatBytes(baseline.heapBefore),
				formatBytes(baseline.heapPeak),
				tolerance,
				measurement.transactions,
				measurement.appliedEvents,
			)
			assertLargeTransactionMemoryBaseline(t, measurement, baseline, tolerance)
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

func mib(value float64) uint64 {
	return uint64(value * 1024 * 1024)
}

func largeTransactionMemoryTolerance(t *testing.T) float64 {
	t.Helper()
	const defaultTolerance = 3.0
	raw := os.Getenv("CDC_LARGE_TX_MEMORY_TOLERANCE")
	if raw == "" {
		return defaultTolerance
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || value < 1 {
		t.Fatalf("CDC_LARGE_TX_MEMORY_TOLERANCE deve ser numero >= 1: %q", raw)
	}
	return value
}

func assertLargeTransactionMemoryBaseline(
	t *testing.T,
	measurement largeTransactionMeasurement,
	baseline largeTransactionBaseline,
	tolerance float64,
) {
	t.Helper()
	if baseline.heapBefore == 0 || baseline.heapPeak == 0 {
		t.Fatalf("baseline de memoria ausente para %d eventos", measurement.events)
	}
	allowedBefore := uint64(float64(baseline.heapBefore) * tolerance)
	allowedPeak := uint64(float64(baseline.heapPeak) * tolerance)
	if measurement.heapBefore > allowedBefore {
		t.Fatalf(
			"heap antes do commit regrediu para %d eventos: atual=%s baseline=%s limite=%s",
			measurement.events,
			formatBytes(measurement.heapBefore),
			formatBytes(baseline.heapBefore),
			formatBytes(allowedBefore),
		)
	}
	if measurement.heapPeak > allowedPeak {
		t.Fatalf(
			"pico de heap regrediu para %d eventos: atual=%s baseline=%s limite=%s",
			measurement.events,
			formatBytes(measurement.heapPeak),
			formatBytes(baseline.heapPeak),
			formatBytes(allowedPeak),
		)
	}
}
