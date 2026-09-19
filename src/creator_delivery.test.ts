import assert from "node:assert/strict";
import { processDelivery } from "./creator_delivery.ts";

const accepted = await processDelivery({ assetId: "cover-17", subscriberId: "sub-4", content: "new episode" });
assert.deepEqual(accepted, { status: "delivered", assetId: "cover-17" });
const rejected = await processDelivery({ assetId: "cover-17", subscriberId: "sub-4", content: "[blocked]" });
assert.equal(rejected.status, "rejected");
console.log("delivery decision test passed");
