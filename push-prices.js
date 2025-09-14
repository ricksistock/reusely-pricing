/**
 * Push prices to Reusely from a CSV and log everything to GitHub Actions.
 *
 * CSV format (no header changes, numbers only; blanks mean "skip"):
 * product_id,Brand New,Flawless,Good,Fair,Broken
 * 187569,700,680,520,420,120
 *
 * Secrets required (Settings → Secrets and variables → Actions → New repository secret):
 * - REUSELY_BASE_URL        e.g. https://api-us.reusely.com
 * - REUSELY_TENANT_ID       (string)
 * - REUSELY_SECRET_KEY      (string)
 * Optional (some tenants require it):
 * - REUSELY_API_KEY         (string)
 * Optional endpoint override:
 * - PUT_PRICE_ENDPOINT      default: /v2/admin/pricing
 *
 * How it works:
 * - Reads prices.csv from repo root
 * - For each row, builds a POST /v2/admin/pricing payload with only filled prices
 * - Logs successes & API errors
 */

const fs = require("fs");
const path = require("path");

// ---------- env / config ----------
const BASE = (process.env.REUSELY_BASE_URL || "").replace(/\/+$/, "");
const TENANT = process.env.REUSELY_TENANT_ID || "";
const SECRET = process.env.REUSELY_SECRET_KEY || "";
const APIKEY = process.env.REUSELY_API_KEY || ""; // optional
const PUT_EP = process.env.PUT_PRICE_ENDPOINT || "/v2/admin/pricing";

if (!BASE || !TENANT || !SECRET) {
  console.error("❌ Missing required secrets: REUSELY_BASE_URL, REUSELY_TENANT_ID, REUSELY_SECRET_KEY.");
  process.exit(1);
}

// ---------- tiny CSV reader (no quotes in our numeric sheet) ----------
function readCsv(filePath) {
  const raw = fs.readFileSync(filePath, "utf8").trim();
  const lines = raw.split(/\r?\n/);
  const header = lines.shift().split(",");
  return lines.map((ln) => {
    const cols = ln.split(",");
    const obj = {};
    header.forEach((h, i) => (obj[h.trim()] = (cols[i] || "").trim()));
    return obj;
  });
}

// ---------- helpers ----------
function numOrNull(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.round(n) : null;
}

async function postJson(url, body, headers) {
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-tenant-id": TENANT,
      "x-secret-key": SECRET,
      ...(APIKEY ? { "x-api-key": APIKEY } : {}),
      ...headers,
    },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = { _raw: text };
  }
  return { ok: res.ok, status: res.status, json };
}

// ---------- main ----------
(async () => {
  const csvPath = path.join(process.cwd(), "prices.csv");
  if (!fs.existsSync(csvPath)) {
    console.error("❌ prices.csv not found in repo root.");
    process.exit(1);
  }

  const rows = readCsv(csvPath);
  console.log(`📄 Loaded ${rows.length} row(s) from prices.csv`);

  let posted = 0;
  let skipped = 0;
  let failed = 0;

  for (const r of rows) {
    const pid = (r["product_id"] || "").trim();
    if (!pid) {
      console.log("⏭️  Skipping row with no product_id.");
      skipped++;
      continue;
    }

    // Only include conditions that have a number
    const pairs = [
      ["Brand New", "New"],
      ["Flawless", "Flawless"],
      ["Good", "Good"],
      ["Fair", "Fair"],
      ["Broken", "Broken"],
    ];

    const conditions = [];
    for (const [col, name] of pairs) {
      const val = numOrNull(r[col]);
      if (val !== null) conditions.push({ name, price: val, is_custom_price: 1 });
    }

    if (!conditions.length) {
      console.log(`⏭️  ${pid}: no prices provided (all blank)`);
      skipped++;
      continue;
    }

    const payload = { product_id: Number(pid), conditions };
    const url = `${BASE}${PUT_EP}`;

    console.log(`➡️  POST ${url} :: product_id=${pid} :: ${conditions.map(c => `${c.name}=${c.price}`).join(", ")}`);

    try {
      const { ok, status, json } = await postJson(url, payload);
      if (ok) {
        console.log(`✅  ${pid}: updated. status=${status}`);
        // Optional: show a short summary if present
        if (json && json.data && Array.isArray(json.data.pricing)) {
          const small = json.data.pricing.map(p => `${p.name}:${p.price}`).join(", ");
          console.log(`    → pricing: ${small}`);
        }
        posted++;
      } else {
        console.log(`❌  ${pid}: failed. status=${status}`);
        console.log(`    body: ${JSON.stringify(json).slice(0, 500)}`);
        failed++;
      }
    } catch (e) {
      console.log(`❌  ${pid}: network/error: ${String(e).slice(0, 300)}`);
      failed++;
    }
  }

  console.log("——— SUMMARY ———");
  console.log(`Posted: ${posted}`);
  console.log(`Skipped: ${skipped}`);
  console.log(`Failed: ${failed}`);

  if (failed > 0) process.exit(1);
})();
