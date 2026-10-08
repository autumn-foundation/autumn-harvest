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

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	taskQueue = "assay11"

	// The workflow payload assay #11's Shape table registers.
	//
	// Assay #10's harvest arm seeds the canonical empty object, because its L1
	// compares against a published number taken that way. Assay #11 registered
	// a ~40-byte payload for BOTH arms instead, so its runs use this and the
	// harvest arm is re-run with ASSAY10_INPUT_JSON set to match. Found by
	// review on PR #1617.
	registeredPayload = "0123456789abcdef0123456789abcdef"

	workflowSlots = 8
	activitySlots = 16
)

// okResult is what each activity returns.
//
// The harvest activity returns {"ok": true}, so an equivalent result keeps
// the persisted activity-completion payloads the same size on both arms.
// Found by review on PR #1617.
type okResult struct {
	Ok bool `json:"ok"`
}

// emptyInput is the workflow input both arms seed.
//
// The canonical harness seeds an empty object, and the harvest arm now does
// too. The workflow ignores it, but it is persisted in history and re-read on
// every workflow-task replay, so it has to match. Found by review on PR #1617.
type registeredInput struct {
	P string `json:"p"`
}

// activityRuns is the correctness ledger, matching the harvest arm's counter.
var activityRuns atomic.Uint64

// The three activity bodies return no result, only an error.
//
// The harvest arm's body returns JSON null. An activity that returned its
// input would have Temporal serialize and persist that payload into every
// activity-completion history event, a cost the harvest arm never pays, and
// the registered shape says both arms are inert. Found by review on PR #1617.
// Each takes one nullable argument so the caller can pass an explicit null.
//
// The harvest handler calls execute_activity_raw with an explicit JSON null,
// which harvest persists as the scheduled activity's input. Taking no argument
// at all made Temporal persist no input payload, giving it smaller histories
// than the arm it is matched against. Found by review on PR #1617.
func Step1(ctx context.Context, _ *okResult) (okResult, error) {
	activityRuns.Add(1)
	return okResult{true}, nil
}

func Step2(ctx context.Context, _ *okResult) (okResult, error) {
	activityRuns.Add(1)
	return okResult{true}, nil
}

func Step3(ctx context.Context, _ *okResult) (okResult, error) {
	activityRuns.Add(1)
	return okResult{true}, nil
}

// BenchWorkflow runs the three steps in sequence, matching wf_three_activities.
func BenchWorkflow(ctx workflow.Context, _ registeredInput) (okResult, error) {
	opts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		// The harvest arm's activities carry no retry policy and inert bodies
		// that cannot fail. A retry here would double-count a side effect and
		// fail the correctness precondition.
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1},
	}
	ctx = workflow.WithActivityOptions(ctx, opts)

	// Each activity is invoked with no argument, matching the harvest handler,
	// which passes JSON null to every activity and ignores its own input.
	// Passing the workflow input down would make Temporal serialize it into
	// three activity-scheduled events per run that harvest never writes.
	// Found by review on PR #1617.
	for _, act := range []any{Step1, Step2, Step3} {
		if err := workflow.ExecuteActivity(ctx, act, nil).Get(ctx, nil); err != nil {
			return okResult{}, err
		}
	}
	return okResult{true}, nil
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
			}, BenchWorkflow, registeredInput{P: registeredPayload})
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
	// Anchor the deadline to `started`, not to now. The measured clock starts
	// before the worker is constructed, so a slow worker startup would
	// otherwise let a repetition report an elapsed time over the cap while
	// waitCtx.Err() stayed nil and correctness passed. Found by review on
	// PR #1617.
	waitCtx, cancel := context.WithDeadline(ctx, started.Add(time.Duration(capSecs)*time.Second))
	defer cancel()
	for _, run := range runs {
		wg.Add(1)
		go func(run client.WorkflowRun) {
			defer wg.Done()
			var out okResult
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

	// The pre-registration's third correctness clause: no workflow task
	// failures. A transient failure that Temporal later retries still lets
	// run.Get succeed with an exact activity count, so neither of the other
	// two clauses can see it, and the repetition would be timed with extra
	// retry work in it. Scanned after the measured window closes, so this
	// costs the rate nothing. Found by review on PR #1617.
	taskFailures, scanErrors := countWorkflowTaskFailures(ctx, c, runs)

	correct := !truncated && done == int64(workflows) &&
		acts == uint64(workflows*3) && taskFailures == 0 && scanErrors == 0

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
	fmt.Printf("rep %d: %.2f workflows/sec (%d completed in %.2f s, %d activity runs, %d workflow task failures, %d unread histories, correctness %s%s)\n",
		rep, rate, done, elapsed, acts, taskFailures, scanErrors, status, trunc)
	return rate, correct
}

// countWorkflowTaskFailures scans every execution's history for a
// WorkflowTaskFailed event and returns how many executions carry at least one.
//
// It runs after the measured window, never inside it.
// It returns the failure count and the number of histories it could not read
// to the end. An unread history proves nothing, so a non-zero scan-error count
// invalidates the repetition rather than passing it. Found by review on
// PR #1617.
func countWorkflowTaskFailures(
	ctx context.Context,
	c client.Client,
	runs []client.WorkflowRun,
) (int64, int64) {
	var failures atomic.Int64
	var scanErrors atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for _, run := range runs {
		wg.Add(1)
		go func(run client.WorkflowRun) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			iter := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(),
				false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
			for iter.HasNext() {
				event, err := iter.Next()
				if err != nil {
					scanErrors.Add(1)
					return
				}
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED {
					failures.Add(1)
					return
				}
			}
		}(run)
	}
	wg.Wait()
	return failures.Load(), scanErrors.Load()
}
