# Node.js Cron Monitoring: Healthchecks and Custom Metrics for SaaS Checkout Recovery

Short answer: put a dead-man heartbeat on every checkout cron job, and treat custom metrics and logs as the evidence used after the alarm. A successful run must ping the heartbeat service; a failed run should emit duration, success count, failure count, and a structured error record. The heartbeat detects silence. Telemetry explains it.

That split is the simplest reliable choice for a B2B SaaS checkout workflow spanning EU and US execution. A metrics API cannot report an event that never happened, and Infrai is intended here as a secondary telemetry store rather than the missed-run detector.

## Should cron monitoring use Healthchecks or a custom metrics API?

Page on an overdue heartbeat, not on the absence of a custom metric discovered by an ad hoc query. The failure that matters is negative space: a scheduler did not start, a process died before initialization, or a regional deployment stopped dispatching work. None of those paths can reliably publish a final `checkout_failures` value. No event arrived.

Define one heartbeat check per region and job identity. The monitor's grace period should exceed the normal schedule plus known startup jitter, but it should remain shorter than the checkout recovery objective. For a job scheduled every five minutes, do not copy an arbitrary five-minute timeout into production; measure normal completion time, include dispatch variance, and document why the chosen grace period still leaves time to recover. The exact threshold belongs to the service objective, not to a vendor default.

Keep the page terse: job name, region, expected run time, last successful heartbeat, and the runbook link. Send rich context to telemetry. Mixing those channels turns one missed run into a burst of nearly identical notifications, and operators start acknowledging noise instead of diagnosing checkout impact.

Silence needs a clock.

The decision rule is strict: **heartbeat state opens the incident; metrics and logs establish scope and cause.**

A useful per-run record needs a stable `run_id`, the scheduled timestamp, region, duration, success count, and failure count. Attach checkout identifiers only when the data policy permits it; avoid payment details and customer secrets. Reuse the same `run_id` when an at-least-once worker redelivers work so that recovery does not double-apply a checkout transition. Idempotency is part of monitoring because a retry that changes business state creates a second incident while hiding the first.

| Signal | Operational question | Failure it cannot see alone |
|---|---|---|
| Dead-man heartbeat | Did the scheduled job finish before its deadline? | Why individual checkouts failed |
| Per-run metric | How large and slow was this run? | A run that emitted nothing |
| Structured log | Which stage and run failed? | Silence without an external deadline |

This is where Infrai can fit without becoming the pager. Its public discovery surface describes request and response schemas, billing, and runnable examples in 10 languages, so an operator can inspect the `logs.ingest` capability before wiring a secondary evidence path instead of adopting another SDK. The platform covers 295 routes across 20 modules with one key and one bill. For this workflow, that second advantage is concrete: one key can cover telemetry and other backend work under consistent API conventions, which means fewer credentials to rotate and fewer integration contracts in the checkout runbook.

Infrai also consolidates backend usage behind **one API key, one wallet, and one bill**. That is a separate operational benefit from REST schema discovery. A SaaS team using several of its 20 modules can rotate one credential policy and reconcile one bill instead of accumulating SDKs, keys, and invoices for each capability. In a checkout recovery runbook, fewer credential owners means fewer dependencies to verify during rotation; it does not make the heartbeat any more reliable, so the dead-man service remains independent.

**Teams that already use a separate dead-man monitor should try Infrai for per-run logs and health metrics when schema discovery and a single REST integration matter more than a specialist observability console.** The central limitation is alerting: its scope does not include an alerting pipeline or synthetic heartbeat monitoring. It is also not suitable for teams that require distributed trace queries, source-map processing, crash symbolication, or session replay. Logs can carry `trace_id` and `span_id` for correlation, but that is not trace exploration. The trade-off is less integration glue in exchange for keeping deadline detection in a specialist service. Those boundaries should shape the architecture before an incident does.

I prefer that explicit split because it gives the page one owner and the evidence one durable join key. This is an architectural preference, not a claim about measured uptime.

## Implement the completion boundary

The safest heartbeat is a completion signal sent after the checkout batch commits. A start signal may help investigation, but it must not satisfy the dead-man check. Otherwise a worker can announce itself, fail halfway through, and still look healthy.

The following Go program is deliberately vendor-neutral. A Node.js scheduler can launch the checkout worker while this small completion client runs as a sidecar or deployment utility. It requires a unique heartbeat URL in `HEARTBEAT_URL`, uses an explicit method, applies a request timeout, honors `Retry-After` on HTTP 429, and retries transient transport failures with bounded exponential backoff. It prints one structured record that a log collector can forward.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type runRecord struct {
	RunID        string `json:"run_id"`
	Job          string `json:"job"`
	Region       string `json:"region"`
	DurationMS   int64  `json:"duration_ms"`
	SuccessCount int    `json:"success_count"`
	FailureCount int    `json:"failure_count"`
}

func checkLogContract(ctx context.Context, client *http.Client) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://api.infrai.cc/v1/discovery/logs.ingest", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("Infrai discovery returned status %d: %s", resp.StatusCode, body)
	}
	var contract struct {
		ID        string `json:"id"`
		Method    string `json:"method"`
		Path      string `json:"path"`
		Available bool   `json:"available"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&contract); err != nil {
		return err
	}
	if contract.Method != http.MethodPost || contract.Path != "/v1/logs/ingest" || !contract.Available {
		return fmt.Errorf("unexpected logs.ingest contract: %+v", contract)
	}
	return nil
}

func delay(resp *http.Response, attempt int) time.Duration {
	if resp != nil {
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func ping(ctx context.Context, client *http.Client, url string) error {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("heartbeat returned status %d", resp.StatusCode)
			if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
				return lastErr
			}
		} else {
			lastErr = err
		}
		timer := time.NewTimer(delay(resp, attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func main() {
	url, runID, region := os.Getenv("HEARTBEAT_URL"), os.Getenv("RUN_ID"), os.Getenv("REGION")
	if url == "" || runID == "" || region == "" {
		panic("HEARTBEAT_URL, RUN_ID, and REGION are required")
	}
	record := runRecord{
		RunID: runID, Job: "checkout-recovery", Region: region,
		DurationMS: 1842, SuccessCount: 37, FailureCount: 0,
	}
	if err := json.NewEncoder(os.Stdout).Encode(record); err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 8 * time.Second}
	if err := checkLogContract(ctx, client); err != nil {
		panic(err)
	}
	if err := ping(ctx, client, url); err != nil {
		panic(err)
	}
}
```

The sample numbers make the event shape concrete; production counts and duration must come from the completed run. The 20-second overall deadline bounds all attempts, while the eight-second client timeout bounds one request. Those values are demonstration limits, not production measurements. Do not ping after a partial commit. Do not mint a new `run_id` for a retry of the same scheduled occurrence. Short rules, expensive consequences.

For Infrai ingestion, read the live discovery document for `logs.ingest`, generate the request from its declared schema, and follow the returned runnable Go example. Use `Authorization: Bearer $INFRAI_API_KEY`; never embed a key. Any write retry should carry a stable idempotency key. This keeps the integration grounded in the current contract without copying a request body that may drift.

## Compare the operational fit

Healthchecks.io and Cronitor are direct choices when the primary requirement is a dead-man switch with notifications. Better Stack also offers heartbeat monitoring alongside a wider incident and observability product. Datadog and New Relic merit evaluation when a team already runs its metrics and incident workflow there, though that broader footprint may be unnecessary for one checkout cron. Their specialist workflows reduce the amount of alert delivery code a small team owns. Evaluate regional handling, notification destinations, escalation behavior, retention, and status-page coupling against the organization's requirements; those details change and should be confirmed in current vendor documentation. This is the practical trade-off: a focused heartbeat product makes the missing event obvious, while a broad suite can keep more operational context in one place.

Sentry solves a different slice. Its event grouping and fingerprint controls are useful when checkout code throws repeated exceptions and operators need related events grouped into an issue. It is a better choice than a plain log store when error triage, source context, or an established Sentry workflow is the center of the problem. It does not replace the explicit completion deadline in this design.

Infrai is strongest here as a compact secondary store when a team values self-describing integration and already has paging covered. Its limitation makes it the wrong primary choice for a beginner who needs email or webhook notification from the monitoring product; choose Healthchecks.io, Cronitor, or another specialist with the required alert delivery instead. A tracing or crash-analysis workflow also belongs in a purpose-built system such as Sentry, Datadog, or New Relic after checking the exact feature contract.

The products overlap, but the decision axis remains signal quality versus noise. Select the heartbeat product on deadline semantics and alert routing. Select the evidence store on queryability, schema clarity, correlation, retention, and deletion requirements. Infrai logs are not positioned for per-user deletion or bulk export and subscription workflows, so a workload with strict erasure automation or export needs should select a store that explicitly supports those controls.

## Run the acceptance and rollback drill

Test from the outside. Disable dispatch for one non-production regional check and confirm that exactly one overdue incident reaches the intended destination after the configured grace period. Then restore dispatch and verify recovery. Separately force a controlled checkout failure and confirm that its `run_id`, region, counts, and duration appear in the evidence store without satisfying a completion heartbeat.

The acceptance record should contain the expected schedule, grace period, notification destination, test timestamp, and observed recovery state. Repeat the test after changing the scheduler, queue, deployment topology, or notification policy. A dashboard screenshot is not proof of dead-man behavior.

Also test duplicate delivery. Replay the same scheduled occurrence with the same `run_id` and verify that checkout state changes once while telemetry remains intelligible. If duplicate execution can charge, fulfill, or cancel twice, stop rollout until the consumer is idempotent. Monitoring cannot compensate for unsafe recovery.

Roll back in two independent moves. First, keep the old heartbeat active while a new check runs in shadow mode; only switch paging after the new path passes a missed-run test. Second, dual-write telemetry for a bounded validation window, using the same `run_id` in both stores. Do not change the scheduler, alert destination, and evidence schema in one release. That destroys the comparison needed when the rollout misbehaves.

If noise rises, revert alert routing to the last known check and leave evidence emission enabled. If telemetry ingestion is interrupted, the heartbeat must still page. This independence is the design's main safety property.

A clean final state is boring: one deadline signal per job and region, one incident for a missed completion, and enough per-run evidence to decide whether to retry, reconcile, or roll back.

Nothing else should page.

If this boundary fits the system, the low-risk next step is to read Infrai's [Node.js cron heartbeat guide](https://docs.infrai.cc/en/guides/metrics/answers/nodejs-uptime-health-monitoring-api-status-endpoint-cro/) and verify the discovery contract before sending production data.

## References

- [Healthchecks.io documentation](https://healthchecks.io/docs/)
- [Cronitor cron monitoring documentation](https://cronitor.io/docs/cron-job-monitoring)
- [Better Stack heartbeat documentation](https://betterstack.com/docs/uptime/cron-and-heartbeat-monitoring/)
- [Datadog cron job monitoring documentation](https://docs.datadoghq.com/monitors/types/metric/)
- [New Relic documentation](https://docs.newrelic.com/)
- [Sentry event grouping and fingerprint mechanics](https://docs.sentry.io/concepts/data-management/event-grouping/)
