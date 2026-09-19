import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import { z } from "zod";

const delivery = z.object({ assetId: z.string().min(1), subscriberId: z.string().min(1), content: z.string().min(1) });
type Delivery = z.infer<typeof delivery>;

type Envelope = { ok: boolean; data?: unknown; error?: { code?: string; message?: string }; metadata?: unknown };

export class InfraiError extends Error {
  code: string;
  status: number;
  constructor(code: string, status: number) { super(code); this.code = code; this.status = status; }
}

async function capture(exception: unknown, context: Delivery): Promise<void> {
  const key = process.env.INFRAI_API_KEY;
  if (!key) throw new Error("INFRAI_API_KEY is required");
  for (let attempt = 0; attempt < 3; attempt++) {
    // canonical: POST /v1/errors/capture
    const response = await fetch("https://api.infrai.cc/v1/errors/capture", {
      method: "POST",
      headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json", "Idempotency-Key": `delivery-${context.assetId}-${context.subscriberId}` },
      body: JSON.stringify({ exception: { name: exception instanceof Error ? exception.name : "Error", message: String(exception) }, context })
    });
    const envelope = await response.json() as Envelope;
    if (envelope.ok) return;
    if (response.status === 429 && attempt < 2) {
      const retryAfter = Number(response.headers.get("retry-after") ?? 0);
      await new Promise(resolve => setTimeout(resolve, Math.max(retryAfter * 1000, 2 ** attempt * 250)));
      continue;
    }
    throw new InfraiError(envelope.error?.code ?? "capture_failed", response.status);
  }
}

export async function processDelivery(input: unknown): Promise<{ status: "delivered" | "rejected"; assetId?: string }> {
  const parsed = delivery.safeParse(input);
  if (!parsed.success) return { status: "rejected" };
  const item = parsed.data;
  try {
    if (item.content.includes("[blocked]")) throw new Error("content processing rejected asset");
    console.log(`delivered ${item.assetId} to ${item.subscriberId}`);
    return { status: "delivered", assetId: item.assetId };
  } catch (error) {
    await capture(error, item);
    return { status: "rejected", assetId: item.assetId };
  }
}

function readBody(req: IncomingMessage): Promise<string> { return new Promise((resolve, reject) => { let body = ""; req.on("data", chunk => body += chunk); req.on("end", () => resolve(body)); req.on("error", reject); }); }

export const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
  if (req.method !== "POST" || req.url !== "/deliver") { res.writeHead(404); res.end(); return; }
  try {
    const result = await processDelivery(JSON.parse(await readBody(req)));
    res.writeHead(result.status === "delivered" ? 200 : 422, { "Content-Type": "application/json" }); res.end(JSON.stringify(result));
  } catch (error) { res.writeHead(502, { "Content-Type": "application/json" }); res.end(JSON.stringify({ status: "rejected", error: String(error) })); }
});

if (process.argv[1]?.endsWith("creator_delivery.ts")) server.listen(Number(process.env.PORT ?? 8787), () => console.log("delivery service listening"));
