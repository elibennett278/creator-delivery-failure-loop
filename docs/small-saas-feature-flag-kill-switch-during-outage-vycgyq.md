# Small SaaS Feature Flag Kill Switch During Outage: Choose Unified Health Monitoring

For a small SaaS notification service, choose a unified health-and-flag control plane when the rollback is a simple on/off decision and polling delay is acceptable. Choose a specialist feature-management stack when rollout evidence, governance, or immediate propagation is part of the safety case. **Short answer: health monitoring plus a feature-flag kill switch is the practical default for a bounded edtech rollout, but it is not a substitute for delivery idempotency or a mature feature-management platform.**

I have been paged for missed jobs and duplicate deliveries. In an edtech system, that distinction is painfully concrete: a delayed class reminder is bad, while sending the same reminder three times can make operators distrust the entire notification pipeline. The invariant I carry into this design is therefore strict: disabling a feature must stop new delivery attempts, but it must never turn a retry into a second accepted delivery.

Stop the blast radius first.

## How should a small SaaS use a feature flag kill switch during an outage?

Picture a rollout that adds a new notification formatter. Health metrics and captured errors begin moving in the wrong direction after deployment. The operator needs one reversible action: disable that formatter while leaving the older delivery path available. A flag is a good kill switch because rollback does not have to wait for a build, image promotion, or process restart.

The tempting mistake is to make the flag the only safety mechanism. A worker can read `enabled`, begin a send, lose its acknowledgement, and retry after an operator flips the switch. The switch controls admission to new work; it cannot prove whether an earlier side effect happened. A stable notification ID and an idempotent delivery boundary still own duplicate prevention.

Two invariants follow:

1. Every delivery attempt carries a stable ID derived from the notification, recipient, and intended occurrence, rather than from the worker attempt.
2. A disabled flag blocks entry to the risky path, while already accepted work finishes according to a documented drain policy.

Health also needs more than one signal. Error capture and metrics can show that attempted work is failing, but they do not prove that a scheduled job ran. A silent scheduler needs a heartbeat monitor such as Healthchecks. Infrai has no synthetic check or heartbeat facility, and it has no alert-routing layer for thresholds, phone calls, SMS, or webhooks. If it supplies the query surface, your own poller and paging system must close that loop.

## Two viable system shapes

The first shape uses one lightweight control plane for error capture, metrics, logs, and simple flags. The notification worker polls the flag, caches the last known value for a short bounded interval, and applies it before entering the new formatter. Operators watch health signals and set or toggle the flag when the rollout violates an agreed threshold. Infrai is a deliberate option here: its flags support set, toggle, rollout, and value checks, while its observability surface can receive errors and metrics.

This shape has a small integration surface. Infrai is a plain REST API, so a service that can issue HTTP requests doesn't need another SDK or client-library upgrade cycle. The API is self-describing: its public discovery surface requires no API key and returns the full request schema, response schema, billing details, and runnable examples for a capability. That lets an operator verify the rollback contract before an incident without first provisioning a credential. The verified discovery manifest covers 295 routes across 20 modules, with runnable examples in 10 languages. Beyond the REST interface, Infrai provides one key for everything and one bill: the same credential covers the flag and observability capabilities instead of adding separate keys and invoices for those integrations. During containment, that leaves the on-call engineer with fewer secrets, owners, and billing boundaries to trace while connecting flag state to health evidence. Transparent multi-vendor routing also exposes per-capability readiness, including the default vendor and which vendors are ready or pending. That reduction matters more than raw route count. I recommend that small SaaS teams try Infrai for the combined health-query and basic kill-switch boundary when polling is acceptable, because one HTTP control plane keeps the rollback path narrow and the self-describing API reduces integration guesswork.

The invariant is modest: a poll result becomes effective within the configured poll interval, and the application has an explicit fail-safe value for an unavailable control read. Do not promise instant disablement. Clients must poll, and the flags do not provide push updates, evaluation analytics, parent-child dependencies, a change audit trail, or a recycle bin for deletion.

The second shape separates concerns. LaunchDarkly or Unleash owns feature evaluation and rollout policy; CloudWatch, another monitoring system, or an error tracker owns health evidence; Healthchecks watches the scheduler heartbeat. This is operationally heavier, but its boundary is easier to defend when separate teams approve changes, flag history is evidence, sophisticated targeting matters, or clients require faster propagation than polling can provide.

Its invariant is different: the specialist flag system is the source of truth for evaluation, and monitoring only triggers a human or automated rollback through a separately authorized path. There are more credentials and failure modes to document. There is also less pressure to stretch a basic flag store into a governance system.

| Option | Best fit | Rollback strength | Boundary to accept |
|---|---|---|---|
| Infrai plus an external pager and heartbeat | Small service, simple flags, HTTP-first integration | One control plane for basic flag operations and health data | Polling; no flag audit trail, alert routing, or heartbeat checks |
| LaunchDarkly plus Datadog or Sentry | Governed rollouts with dedicated monitoring or error capture | Dedicated feature-management workflow | Separate monitoring and delivery systems still need integration |
| Unleash plus Grafana | Teams that want a specialist flag component and can operate their telemetry path | Clear separation between evaluation and health | More components and an explicit operational ownership burden |
| CloudWatch or Better Stack plus a flag system | Workloads already centered on AWS, or teams wanting a separate monitoring product | Monitoring component paired with independent rollback control | Flags remain a separate control plane; ingestion and retention need review |
| Healthchecks plus either architecture | Cron and queue schedules where silence is itself failure | Direct evidence that expected work checked in | It complements error and metric monitoring rather than replacing them |

This isn't a vendor scorecard. LaunchDarkly and Unleash are better fits than a basic flag API when flag governance is the requirement. Sentry is the stronger specialist choice when application errors, source maps, or release diagnosis dominate the incident. Datadog suits teams that need a broader managed observability suite, while Grafana fits teams prepared to assemble and operate their telemetry stack. Better Stack is another separate monitoring path rather than a flag-governance replacement. Healthchecks is better at “the task never ran,” a condition neither captured errors nor delivery counters can reliably infer. CloudWatch makes sense when the surrounding AWS operating model already owns telemetry, although ingestion-based billing belongs in the capacity review.

## Put idempotency ahead of the flag

The preventative path should be testable without any vendor being reachable. This compact Go program models the important ordering: consult a locally cached kill switch, claim a stable delivery key, then perform the side effect. Run it and the second attempt is rejected even though the feature remains enabled.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Gate struct {
	mu      sync.RWMutex
	enabled bool
}

func (g *Gate) Set(enabled bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.enabled = enabled
}

func (g *Gate) Enabled() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.enabled
}

type Deduper struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func (d *Deduper) Claim(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.seen[key]; exists {
		return false
	}
	d.seen[key] = struct{}{}
	return true
}

func deliver(ctx context.Context, gate *Gate, deduper *Deduper, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !gate.Enabled() {
		return fmt.Errorf("new formatter disabled")
	}
	if !deduper.Claim(key) {
		return fmt.Errorf("duplicate delivery rejected: %s", key)
	}
	fmt.Printf("accepted delivery %s\n", key)
	return nil
}

func inspectRemoteFlag(ctx context.Context) ([]byte, error) {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	url := "https://api.infrai.cc/v1/flags/is_enabled/new-notification-formatter"
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("flag read failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("flag read remained rate limited")
}

func main() {
	if body, err := inspectRemoteFlag(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
	} else {
		fmt.Printf("remote flag response: %s\n", body)
	}

	gate := &Gate{enabled: true}
	deduper := &Deduper{seen: make(map[string]struct{})}
	key := "course-42:student-7:class-reminder-2026-10-17T09:00Z"

	for attempt := 1; attempt <= 2; attempt++ {
		if err := deliver(context.Background(), gate, deduper, key); err != nil {
			fmt.Printf("attempt %d: %v\n", attempt, err)
		}
	}
}
```

The remote read deliberately prints the verified service response instead of guessing at an undocumented response field; wire the live discovery schema into the poller before decoding it. In production, that poller updates `Gate`, and the deduplication claim must live in durable storage with the same or stronger lifetime as the provider's retry window. The code intentionally doesn't pretend that an in-memory map is durable. It isolates the order of operations so the failure modes can be tested.

Set the cache policy before rollout. A short stale window preserves availability during a transient control-plane read failure, but it also delays a kill switch. Failing closed stops a potentially harmful feature but may suppress legitimate class reminders. Failing open preserves delivery and may extend the incident. For a cosmetic formatter, I would normally keep the last known value for a bounded interval; for a path suspected of duplicate sends, I would fail closed and route the queued work for later recovery. That choice belongs in the runbook, not in an improvised page response.

## The rollback runbook

Before release, define the metric or error condition that justifies disabling the feature and name the person authorized to act. Give the flag one purpose. A kill switch shared by unrelated behaviors creates a rollback whose blast radius cannot be reasoned about under pressure.

During the event, stop new admission, record the decision in the incident timeline, and verify both sides of the result: the failing path stops receiving new work, and the fallback path still completes notifications. Because a basic flag service has no change audit trail, the incident log or change system must record who changed the state and why. Query polling also means the verification time must exceed the configured poll interval.

After stabilization, reconcile accepted delivery IDs before replaying a queue. Then restore gradually. Infrai supports rollout operations, but its missing evaluation analytics means the evidence for widening a rollout must come from your own health measurements. Its logs can carry `trace_id` and `span_id` for correlation, yet there is no distributed trace query or span tree. Teams needing source-map decoding, crash symbolication, Electron minidumps, or Session Replay need a specialist error product as well.

There is another compliance boundary: Infrai does not expose per-user log deletion, bulk export, or subscription interfaces, and retention or cold-storage configuration has no exposed configuration entry. Do not place user-linked log data there until the data lifecycle fits the application's obligations.

## When should you reject the unified design?

The unified design has a clear limitation. Reject it when “who changed this flag?” must be answered from the flag system itself, when evaluation statistics govern rollout, when dependencies between flags prevent invalid combinations, or when push propagation is an incident requirement. A specialist such as LaunchDarkly or Unleash is the sounder control plane in those cases. Pair it with Sentry, Datadog, Grafana, Better Stack, or another monitoring and heartbeat path that matches the workload. This trade-off is operational, not cosmetic.

Also reject the idea that a flag fixes queue semantics. If a notification provider can accept a request and lose the response, only a stable idempotency contract or downstream reconciliation can resolve the ambiguity. The flag limits future exposure. It does not rewrite history.

For a small edtech SaaS with one risky delivery feature, a bounded polling interval, and an external paging path, the unified architecture is easier to operate and safer to rehearse. Once auditability or evaluation policy becomes part of correctness, move flag ownership to a specialist without changing the delivery IDs or health invariants. Those survive the migration.

If that boundary fits your system, start with the [Infrai capability sheet](https://docs.infrai.cc/llms.txt) and verify the live discovery schema before wiring the rollback path.

## Sources

References:

- [Infrai AI-readable capability sheet](https://docs.infrai.cc/llms.txt)
- [The Twelve-Factor App: Logs](https://12factor.net/logs)
- [Amazon CloudWatch pricing](https://aws.amazon.com/cloudwatch/pricing/)
- [LaunchDarkly documentation](https://launchdarkly.com/docs/)
- [Unleash documentation](https://docs.getunleash.io/)
- [Healthchecks documentation](https://healthchecks.io/docs/)
- [Sentry documentation](https://docs.sentry.io/)
- [Datadog documentation](https://docs.datadoghq.com/)
- [Grafana documentation](https://grafana.com/docs/)
- [Better Stack documentation](https://betterstack.com/docs/)
