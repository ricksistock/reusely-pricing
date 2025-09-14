// push-prices.js
// Reads device pricing from Google Sheets and pushes updates to Reusely API

const fs = require("fs");
const { google } = require("googleapis");
const fetch = require("node-fetch");

// ---------------- CONFIG ----------------
const SHEET_ID = process.env.SHEET_ID;        // Google Sheet ID
const SHEET_TAB = process.env.SHEET_TAB;      // Tab name, e.g. "Prices"
const REUSELY_BASE_URL = process.env.REUSELY_BASE_URL;
const REUSELY_TENANT_ID = process.env.REUSELY_TENANT_ID;
const REUSELY_SECRET_KEY = process.env.REUSELY_SECRET_KEY;
const REUSELY_API_KEY = process.env.REUSELY_API_KEY;

// Path to service account JSON (passed via GitHub secret)
const GOOGLE_CREDS = JSON.parse(process.env.GOOGLE_SHEETS_CREDENTIALS);

// ---------------- AUTH ----------------
const auth = new google.auth.GoogleAuth({
  credentials: GOOGLE_CREDS,
  scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
});
const sheets = google.sheets({ version: "v4", auth });

// ---------------- MAIN ----------------
(async () => {
  try {
    if (!SHEET_ID || !SHEET_TAB) {
      throw new Error("Missing SHEET_ID or SHEET_TAB env vars");
    }

    // 1. Read rows from Google Sheets
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: SHEET_TAB,
    });

    const rows = res.data.values;
    if (!rows || rows.length < 2) {
      throw new Error("No rows found in sheet");
    }

    const headers = rows[0];
    const colIndex = {};
    headers.forEach((h, i) => (colIndex[h.trim()] = i));

    // Required columns
    ["product_id", "Condition", "ProposedPrice"].forEach((col) => {
      if (colIndex[col] == null) {
        throw new Error(`Missing column: ${col}`);
      }
    });

    // 2. Loop rows and push prices
    for (let i = 1; i < rows.length; i++) {
      const r = rows[i];
      const productId = r[colIndex["product_id"]];
      const condition = r[colIndex["Condition"]];
      const price = r[colIndex["ProposedPrice"]];

      if (!productId || !price) continue;

      // Map conditions to Reusely labels
      const condMap = {
        New: "Brand New",
        Mint: "Flawless",
        Good: "Good",
        Fair: "Fair",
        Broken: "Broken",
      };
      const reuselyCond = condMap[condition] || condition;

      const payload = {
        product_id: Number(productId),
        conditions: [
          {
            name: reuselyCond,
            price: Math.round(Number(price)),
            is_custom_price: 1,
          },
        ],
      };

      const url = `${REUSELY_BASE_URL.replace(/\/+$/, "")}/api/v2/admin/pricing`;
      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": REUSELY_API_KEY,
          "x-tenant-id": REUSELY_TENANT_ID,
          "x-secret-key": REUSELY_SECRET_KEY,
        },
        body: JSON.stringify(payload),
      });

      if (!resp.ok) {
        const txt = await resp.text();
        console.error(
          `❌ Failed for ${productId} (${condition}): ${resp.status} ${txt}`
        );
      } else {
        console.log(
          `✅ Updated ${productId} (${condition}) => $${price}`
        );
      }
    }

    console.log("Done pushing prices.");
  } catch (err) {
    console.error("Error:", err.message);
    process.exit(1);
  }
})();
