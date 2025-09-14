// Minimal runner: reads your Google Sheet "Proposals" and pushes prices to Reusely v2 /admin/pricing

const fetch = require('node-fetch');
const { google } = require('googleapis');

const SHEET_ID = process.env.SHEET_ID;            // from repo Secret
const SHEET_TAB = process.env.SHEET_TAB || 'Proposals';

const REUSELY_BASE_URL = (process.env.REUSELY_BASE_URL || '').replace(/\/+$/,'');
const PUT_PRICE_PATH = process.env.PUT_PRICE_BY_PRODUCTID || '/api/v2/admin/pricing';
const REUSELY_API_KEY = process.env.REUSELY_API_KEY || '';
const REUSELY_TENANT_ID = process.env.REUSELY_TENANT_ID || '';
const REUSELY_SECRET_KEY = process.env.REUSELY_SECRET_KEY || '';

if (!SHEET_ID) {
  console.error('❌ SHEET_ID is missing (set it in GitHub Secrets).');
  process.exit(1);
}
if (!REUSELY_BASE_URL || !REUSELY_TENANT_ID || !REUSELY_SECRET_KEY) {
  console.error('❌ Reusely env missing: REUSELY_BASE_URL, REUSELY_TENANT_ID, REUSELY_SECRET_KEY.');
  process.exit(1);
}

const SWAPPA_TO_REUSELY = {
  'New': 'Brand New',
  'Mint': 'Flawless',
  'Good': 'Good',
  'Fair': 'Fair',
  'Broken': 'Broken',
};

async function readSheet() {
  // Service Account JSON from GitHub Secret GOOGLE_CREDENTIALS
  const credsRaw = process.env.GOOGLE_CREDENTIALS;
  if (!credsRaw) throw new Error('GOOGLE_CREDENTIALS secret is missing.');
  let creds;
  try { creds = JSON.parse(credsRaw); } catch (e) {
    throw new Error('GOOGLE_CREDENTIALS is not valid JSON.');
  }

  const scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly'];
  const jwt = new google.auth.JWT(
    creds.client_email,
    null,
    creds.private_key,
    scopes
  );
  const sheets = google.sheets({ version: 'v4', auth: jwt });

  const range = `${SHEET_TAB}`;
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range,
  });

  const rows = res.data.values || [];
  if (rows.length < 2) return [];

  const header = rows[0];
  const col = {};
  header.forEach((h, i) => (col[String(h).trim()] = i));

  // We expect Proposals sheet columns with at least:
  // product_id, Condition, ProposedPrice
  const required = ['product_id', 'Condition', 'ProposedPrice'];
  for (const r of required) {
    if (col[r] == null) {
      throw new Error(`Sheet '${SHEET_TAB}' missing column: ${r}`);
    }
  }

  const out = [];
  for (let i = 1; i < rows.length; i++) {
    const r = rows[i];
    const pid = String(r[col['product_id']] || '').trim();
    const cond = String(r[col['Condition']] || '').trim();
    const proposed = r[col['ProposedPrice']];

    if (!pid) continue;
    if (!cond) continue;
    if (proposed === '' || proposed == null || isNaN(Number(proposed))) continue;

    out.push({
      product_id: Number(pid),
      condition: cond,
      price: Math.round(Number(proposed)),
    });
  }
  return out;
}

function groupByProduct(updates) {
  // { product_id: [ {name, price} ] }
  const byPid = {};
  for (const u of updates) {
    const name = SWAPPA_TO_REUSELY[u.condition] || u.condition;
    if (!byPid[u.product_id]) byPid[u.product_id] = {};
    // last write wins for each condition
    byPid[u.product_id][name] = u.price;
  }
  return byPid;
}

async function pushToReusely(product_id, conditionsMap) {
  const url = `${REUSELY_BASE_URL}${PUT_PRICE_PATH}`;

  const headers = {
    'Content-Type': 'application/json',
    'x-tenant-id': REUSELY_TENANT_ID,
    'x-secret-key': REUSELY_SECRET_KEY,
  };
  if (REUSELY_API_KEY) headers['x-api-key'] = REUSELY_API_KEY;

  const conditions = Object.entries(conditionsMap).map(([name, price]) => ({
    name,
    price,
    is_custom_price: 1,
  }));

  const payload = { product_id, conditions };

  const resp = await fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(payload),
  });

  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`${resp.status} ${text.slice(0,300)}`);
  }
  return text;
}

async function main() {
  console.log(`Reading prices from sheet: ${SHEET_ID}, tab: ${SHEET_TAB}`);
  const rows = await readSheet();
  if (!rows.length) {
    console.log('No rows with ProposedPrice found. Nothing to push.');
    return;
  }

  const grouped = groupByProduct(rows);
  const pids = Object.keys(grouped).map(Number);
  console.log(`Found ${pids.length} products to update.`);

  let ok = 0, fail = 0;
  for (const pid of pids) {
    try {
      const res = await pushToReusely(pid, grouped[pid]);
      ok++;
      console.log(`✅ Updated product ${pid}`);
    } catch (e) {
      fail++;
      console.log(`❌ Failed product ${pid}: ${e.message}`);
    }
  }

  console.log(`Done. Success: ${ok}, Failed: ${fail}`);
  if (fail > 0) process.exit(1);
}

main().catch(err => {
  console.error('Script failed:', err);
  process.exit(1);
});
