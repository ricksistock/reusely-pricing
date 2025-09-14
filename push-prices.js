// push-prices.js
// Reads Google Sheet "Proposals" tab and updates prices in Reusely via API

const { google } = require("googleapis");
const fetch = require("node-fetch");
const fs = require("fs");

// === Load credentials from secrets ===
const SHEET_ID = process.env.SHEET_ID;
const SHEET_TAB = process.env.SHEET_TAB || "Proposals";

const REUSELY_BASE_URL = process.env.REUSELY_BASE_URL;
const REUSELY_TENANT_ID = process.env.REUSELY_TENANT_ID;
const REUSELY_SECRET_KEY = process.env.REUSELY_SECRET_KEY;
const REUSELY_API_KEY = process.env.REUSELY_API_KEY;

// Google service account JSON
const GOOGLE_SHEETS_CREDENTIALS = JSON.parse(process.env.GOOGLE_SHEETS_CREDENTIALS);

// === Setup Google Sheets API client ===
const auth = new google.auth.JWT(
  GOOGLE_SHEETS_CREDENTIALS.client_email,
  null,
  GOOGLE_SHEETS_CREDENTIALS.private_key,
  ["https://www.googleapis.com/auth/spreadsheets.readonly"]
);
const sheets = google.sheets({ version: "v4", auth });

// === Push prices from Proposals tab ===
async function pushPrices() {
  console.log(`Reading prices from sheet: ${SHEET_ID}, tab: ${SHEET_TAB}`);

  // 1. Read all rows from Proposals tab
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: SHEET_TAB,
  });

  const rows = res.data.values;
  if (!rows || rows.length < 2) {
    console.log("No data in Proposals tab.");
    return;
  }

  const header = rows[0];
  const colIndex = (name) => header.indexOf(name);

  const idxProduct = colIndex("product_id");
  const idxCond = colIndex("Condition");
  const idxProposed = colIndex("ProposedPrice");

  if (idxProduct === -1 || idxCond === -1 || idxProposed === -1) {
    throw new Error("Proposals tab missing required columns: product_id, Condition, ProposedPrice");
  }

  // 2. Loop through rows
  for (let i = 1; i < rows.length; i++) {
    const r = rows[i];
    const productId = r[idxProduct];
    const condition = r[idxCond];
    const proposed = r[idxProposed];

    if (!productId || !condition || !proposed) continue;

    const price = Number(proposed);
    if (isNaN(price)) continue;

    // Map condition names
    const condMap = {
      "New": "Brand New",
      "Mint": "Flawless",
      "Good": "Good",
      "Fair": "Fair",
      "Broken": "Broken",
    };
    const reuselyCond = condMap[condition] || condition;

    // 3. Send to Reusely API
    const payload = {
      product_id: Number(productId),
      conditions: [
        {
          name: reuselyCond,
          price: Math.round(price),
          is_custom_price: 1,
        },
      ],
    };

    const url = `${REUSELY_BASE_URL}/api/v2/admin/pricing`;
    console.log(`→ Updating product ${productId}, ${condition} = $${price}`);

    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-tenant-id": REUSELY_TENANT_ID,
          "x-secret-key": REUSELY_SECRET_KEY,
          "x-api-key": REUSELY_API_KEY,
        },
        body: JSON.stringify(payload),
      });

      if (!resp.ok) {
        const text = await resp.text();
        console.error(`❌ Failed for ${productId}: ${resp.status} ${text}`);
      } else {
        console.log(`✅ Updated ${productId} (${condition}) -> $${price}`);
      }
    } catch (err) {
      console.error(`❌ Error updating ${productId}:`, err.message);
    }
  }
}

pushPrices().catch((err) => {
  console.error("Script failed:", err);
  process.exit(1);
});
