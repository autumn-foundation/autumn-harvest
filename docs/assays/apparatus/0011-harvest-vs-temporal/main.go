// NON-PRODUCTION THROWAWAY APPARATUS. Never build against this.
//
// It is the Temporal arm of assay ledger #11. It drains the same backlog, of
// the same 3-activity shape, at the same concurrency, as the harvest arm in
// docs/assays/apparatus/0010-cross-mode-throughput.
//
// The lines, the shape and the bounding statement come from
// docs/rnd/2026-09-16-harvest-vs-temporal-single-box-preregistration.md.
//
// Shape parity with the harvest arm:
//   - three sequential activities, inert bodies, one ~40-byte input
//   - MaxConcurrentWorkflowTaskExecutionSize 8, matching MAX_CONCURRENT_WORKFLOWS
//   - MaxConcurrentActivityExecutionSize 16, matching MAX_CONCURRENT_ACTIVITIES
//   - one worker
//   - drain shape: every workflow is started BEFORE the worker starts, so the
//     measured window is a backlog drain on both arms rather than a paced feed
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	taskQueue    = "assay11"
	inputPayload = "0123456789abcdef0123456789abcdef"

	workflowSlots = 8
	activitySlots = 16
)

// activityRuns is the correctness ledger, matching the harvest arm's counter.
var activityRuns atomic.Uint64

func Step1(ctx context.Context, in string) (string, error) { activityRuns.Add(1); return in, nil }
func Step2(ctx context.Context, in string) (string, error) { activityRuns.Add(1); return in, nil }
func Step3(ctx context.Context, in string) (string, error) { activityRuns.Add(1); return in, nil }

// BenchWorkflow runs the three steps in sequence, matching wf_three_activities.
func BenchWorkflow(ctx workflow.Context, in string) (string, error) {
	opts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		// The harvest arm's activities carry no retry policy and inert bodies
		// that cannot fail. A retry here would double-count a side effect and
		// fail the correctness precondition.
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1},
	}
	ctx = workflow.WithActivityOptions(ctx, opts)

	var out string
	for _, act := range []any{Step1, Step2, Step3} {
		if err := workflow.ExecuteActivity(ctx, act, in).Get(ctx, &out); err != nil {
			return "", err
		}
	}
	return out, nil
}

func envInt(key string, fallback int) int {
	if raw := os.Getenv(key); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			return parsed
		}
	}
	return fallback
}

func envString(key, fallback string) string {
	if raw := os.Getenv(key); raw != "" {
		return raw
	}
	return fallback
}

func main() {
	hostPort := envString("ASSAY11_TEMPORAL_HOSTPORT", "127.0.0.1:7233")
	workflows := envInt("ASSAY11_WORKFLOWS", 2000)
	reps := envInt("ASSAY11_REPS", 3)
	capSecs := envInt("ASSAY11_CAP_SECS", 900)

	// The harvest arm runs under NoOpMetrics with no tracing subscriber
	// installed. The SDK's default logger writes a line per activity dispatch
	// to stderr, which at this backlog is thousands of synchronized writes
	// the harvest arm never pays. A discarding logger restores parity.
	c, err := client.Dial(client.Options{
		HostPort:  hostPort,
		Namespace: "default",
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		log.Fatalf("dial temporal: %v", err)
	}
	defer c.Close()

	fmt.Printf("# Assay #11 — Temporal arm\n\n")
	fmt.Printf("Backlog %d workflows, %d reps, cap %d s, slots %d/%d.\n\n",
		workflows, reps, capSecs, workflowSlots, activitySlots)

	rates := []float64{}
	for rep := 0; rep < reps; rep++ {
		rate, ok := runRep(c, rep, workflows, capSecs)
		if ok {
			rates = append(rates, rate)
		}
	}

	total := 0.0
	for _, r := range rates {
		total += r
	}
	mean := 0.0
	if len(rates) > 0 {
		mean = total / float64(len(rates))
	}
	fmt.Printf("\n**mean %.2f workflows/sec** over %d valid rep(s).\n", mean, len(rates))
}

// runRep seeds a backlog with no worker running, then starts the worker and
// measures the drain. It returns the rate and whether the correctness
// precondition held.
func runRep(c client.Client, rep, workflows, capSecs int) (float64, bool) {
	ctx := context.Background()
	activityRuns.Store(0)

	// Seed. No worker is polling yet, so these queue in matching and the
	// measured window below is a drain, exactly as on the harvest arm.
	runs := make([]client.WorkflowRun, 0, workflows)
	var seedMu sync.Mutex
	var seedWg sync.WaitGroup
	seedSem := make(chan struct{}, 32)
	for i := 0; i < workflows; i++ {
		seedWg.Add(1)
		go func(i int) {
			defer seedWg.Done()
			seedSem <- struct{}{}
			defer func() { <-seedSem }()
			run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
				ID:        fmt.Sprintf("a11-r%d-%d", rep, i),
				TaskQueue: taskQueue,
			}, BenchWorkflow, inputPayload)
			if err != nil {
				log.Printf("seed %d: %v", i, err)
				return
			}
			seedMu.Lock()
			runs = append(runs, run)
			seedMu.Unlock()
		}(i)
	}
	seedWg.Wait()
	if len(runs) != workflows {
		fmt.Printf("rep %d: seeded %d of %d, discarded\n", rep, len(runs), workflows)
		return 0, false
	}

	// Measure. The worker starts here and the clock starts with it.
	started := time.Now()
	w := worker.New(c, taskQueue, worker.Options{
		MaxConcurrentWorkflowTaskExecutionSize: workflowSlots,
		MaxConcurrentActivityExecutionSize:     activitySlots,
	})
	w.RegisterWorkflow(BenchWorkflow)
	w.RegisterActivity(Step1)
	w.RegisterActivity(Step2)
	w.RegisterActivity(Step3)
	if err := w.Start(); err != nil {
		log.Fatalf("start worker: %v", err)
	}

	var completed atomic.Int64
	var wg sync.WaitGroup
	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(capSecs)*time.Second)
	defer cancel()
	for _, run := range runs {
		wg.Add(1)
		go func(run client.WorkflowRun) {
			defer wg.Done()
			var out string
			if err := run.Get(waitCtx, &out); err == nil {
				completed.Add(1)
			}
		}(run)
	}
	wg.Wait()
	elapsed := time.Since(started).Seconds()
	w.Stop()

	done := completed.Load()
	acts := activityRuns.Load()
	truncated := waitCtx.Err() != nil
	correct := !truncated && done == int64(workflows) && acts == uint64(workflows*3)

	rate := 0.0
	if elapsed > 0 {
		rate = float64(done) / elapsed
	}
	status := "PASS"
	if !correct {
		status = "FAIL"
	}
	trunc := ""
	if truncated {
		trunc = ", TRUNCATED"
	}
	fmt.Printf("rep %d: %.2f workflows/sec (%d completed in %.2f s, %d activity runs, correctness %s%s)\n",
		rep, rate, done, elapsed, acts, status, trunc)
	return rate, correct
}
