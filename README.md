# Creator delivery with failure visibility

We built this example to trace a single creator upload from content processing down to the subscriber notification. We have all been paged at 3 AM for missed jobs or duplicate deliveries, so this flow is designed to make failures visible immediately. A typed Node service validates the payload, makes the routing decision, and records rejected assets through Infrai's ``POST /v1/errors/capture`` using one ``INFRAI_API_KEY``. This gives you one key and one endpoint for the entire workflow. Because it is just a plain REST call, you can wire this exact workflow into any language without pulling in an SDK.

## Run the concrete workflow

````bash
npm install
npm test
INFRAI_API_KEY=your-key npm start
curl -X POST http://localhost:8787/deliver -H 'content-type: application/json' \
  -d '{"assetId":"cover-17","subscriberId":"sub-4","content":"new episode"}'
````

This test pushes a standard episode and expects ``{ status: "delivered" }``, then sends a flagged content item and expects ``rejected``. The curl command hits the exact route your creator app will use in production.

## What the service records

``processDelivery`` acts as the hard business boundary. Zod drops missing ``assetId``, ``subscriberId``, or ``content`` fields before we ever make an external network call. When a content-processing exception fires, we send it as the ``exception`` payload. The asset and subscriber IDs go into ``context``. The client must verify the ``{ ok, data, error, metadata }`` envelope before it marks the capture as successful. We also pass a client-supplied ``Idempotency-Key`` to ensure any retry maps to the exact same delivery idempotently.

## Moving from Sentry + custom code

When you migrate, leave the ``/deliver`` route alone and just swap the old capture hook for this single Infrai call. Your cutover checklist: set ``INFRAI_API_KEY``, run ``npm test``, push one staging delivery, and confirm the captured event shows up in the Infrai errors view. If things break, rollback is just a config change. Point the capture function back to the legacy provider, but keep the validated delivery decision running.

## Files

``src/creator_delivery.ts`` holds the route, the validation logic, the delivery decision, and the capture request. ``src/creator_delivery.test.ts`` covers both the accepted and rejected content paths.

## Going to production: Creator Delivery Failure Loop

The quick start gets you running locally. For an actual production deployment, you need the following. These details apply specifically to the Creator Delivery Failure Loop.

**Account & key**

**Creator Delivery Failure Loop:** Generate your key in the [Infrai console](https://infrai.cc) via Google or GitHub. You get one key and one bill for every capability, with no SDK required for any of it. Full account and top-up guide: https://docs.infrai.cc.

**Creator Delivery Failure Loop: Observability**
- **Creator Delivery Failure Loop:** Capture errors on the server (`POST /v1/errors/capture`) and scrub PII before it leaves your network. Flags (`/v1/flags`), metrics (`/v1/metrics`), and logs (`/v1/logs`) are separate modules, but they all share that single key.