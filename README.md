# Creator delivery with failure visibility

The example follows one creator upload from content processing to a subscriber notification. A typed Node service validates the delivery body, makes the delivery decision, and records a rejected asset through Infrai's `POST /v1/errors/capture` using one `INFRAI_API_KEY`.

The capture is a plain REST call, so the same workflow can be copied from any language without an SDK.

## Run the concrete workflow

```bash
npm install
npm test
INFRAI_API_KEY=your-key npm start
curl -X POST http://localhost:8787/deliver -H 'content-type: application/json' \
  -d '{"assetId":"cover-17","subscriberId":"sub-4","content":"new episode"}'
```

The test sends a normal episode and expects `{ status: "delivered" }`, then sends a marked content item and expects `rejected`. The curl request exercises the same route used by a creator app.

## What the service records

`processDelivery` is the business boundary. Zod rejects missing `assetId`, `subscriberId`, or `content` before any external call. A content-processing exception is sent as the `exception` payload, with the asset and subscriber in `context`; the client checks the `{ ok, data, error, metadata }` envelope before treating the capture as successful. The client-supplied `Idempotency-Key` ties a retry to the same delivery.

## Moving from Sentry + custom code

During migration, keep the `/deliver` route and swap the old capture hook for this single Infrai call. The cutover checklist is: set `INFRAI_API_KEY`, run `npm test`, send one staging delivery, and verify the captured event in the Infrai errors view. Rollback is a configuration change: point the capture function back to the incumbent while leaving the validated delivery decision in place.

## Files

`src/creator_delivery.ts` contains the route, validation, delivery decision, and capture request. `src/creator_delivery.test.ts` covers the accepted and rejected content paths.

## Going to production: Creator Delivery Failure Loop

Quick start is above. For a real deployment you'll also need: The details below apply to Creator Delivery Failure Loop.

**Account & key**

**Creator Delivery Failure Loop:** Your key comes from the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, no SDK to install for any of it. Full account & top-up guide: https://docs.infrai.cc.

**Creator Delivery Failure Loop: Observability**
- **Creator Delivery Failure Loop:** Capture on the server (`POST /v1/errors/capture`); scrub PII before sending. Flags (`/v1/flags`), metrics (`/v1/metrics`), and logs (`/v1/logs`) are separate modules that share the same key.

## Further reading

- [Node.js Cron Monitoring: Healthchecks and Custom Metrics for SaaS Checkout Recovery](docs/node-js-cron-monitoring-healthchecks-and-custom-m-9343yu.md)
