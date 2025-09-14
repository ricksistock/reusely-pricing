/******************************************************
 * Reusely – AllGB updater (reads Report_*_AllGB if present)
 * - Builds "Proposals" from your Swappa reports (per storage)
 * - Matches product_id using the Reusely_Catalog tab
 * - Computes ProposedPrice via your rank rules
 * - NEW condition: always target leader - $20 if not #1
 * - Optional: Apply updates to Reusely via Public API
 * - Sequential runner (one carrier per execution) to avoid timeouts
 * - CSV exporter that mirrors Reusely_Catalog and includes ONLY changed rows
 ******************************************************/

/////////////////////// CONFIG ///////////////////////

const REPORT_TABS = [
  { carrier: "Unlocked", tabAll: "Report_Unlocked_AllGB", tabLegacy: "Report_Unlocked" },
  { carrier: "AT&T",     tabAll: "Report_ATT_AllGB",      tabLegacy: "Report_ATT"      },
  { carrier: "T-Mobile", tabAll: "Report_TMobile_AllGB",  tabLegacy: "Report_TMobile"  },
  { carrier: "Verizon",  tabAll: "Report_Verizon_AllGB",  tabLegacy: "Report_Verizon"  },
];

const COL_MODEL   = "Model";
const COL_STORAGE = "Storage";
const CONDITIONS  = ["New", "Mint", "Good", "Fair", "Broken"];

// Pricing
const PRICE_BUMP_ABOVE_SECOND = 1;
const TRIM_LEAD_THRESHOLD = 5.0;
const NEW_UNDERCUT_LEADER_BY = 20;

// Catalog sheet & optional price columns
const CATALOG_SHEET = "Reusely_Catalog";
const PRICE_COLS_IN_CATALOG = {
  "New":     "Brand New",
  "Mint":    "Flawless",
  "Good":    "Good",
  "Fair":    "Fair",
  "Broken":  "Broken",
};

// Secrets
const SECRET_KEYS = [
  "REUSELY_BASE_URL",
  "REUSELY_API_KEY",
  "REUSELY_TENANT_ID",
  "REUSELY_SECRET_KEY",
  "LIST_PRODUCTS_ENDPOINT",
  "GET_PRICE_BY_PRODUCTID",
  "PUT_PRICE_BY_PRODUCTID",
];

const DEFAULT_ENDPOINTS = {
  // leave list + GET fallback as-is for now
  LIST_PRODUCTS_ENDPOINT: "/v1/products?limit=1000",
  GET_PRICE_BY_PRODUCTID: "/v1/products/{productId}/trade-in",

  // ✅ NEW: v2 pricing endpoint (no productId in the path)
  // Reusely docs: POST /api/v2/admin/pricing
  PUT_PRICE_BY_PRODUCTID: "/api/v2/admin/pricing",
};

// Sequential runner state
const QUEUE_KEY = "REUSELY_SEQ_QUEUE";
const APPLY_FLAG_KEY = "REUSELY_SEQ_APPLY";

/////////////////////// MENU ///////////////////////

function onOpen() {
  SpreadsheetApp.getUi().createMenu("Reusely")
    .addItem("Dry Run (ALL tabs – single pass)", "dryRunAllTabs")
    .addItem("Apply (ALL tabs – single pass)", "applyAllTabs")
    .addSeparator()
    .addItem("Dry Run – Sequential (safe)", "startSequentialDryRun")
    .addItem("Apply – Sequential (safe)", "startSequentialApply")
    .addSeparator()
    .addItem("Dry Run – Unlocked only", "dryRunUnlocked")
    .addItem("Apply – Unlocked only", "applyUnlocked")
    .addItem("Dry Run – AT&T only", "dryRunATT")
    .addItem("Apply – AT&T only", "applyATT")
    .addItem("Dry Run – T-Mobile only", "dryRunTMobile")
    .addItem("Apply – T-Mobile only", "applyTMobile")
    .addItem("Dry Run – Verizon only", "dryRunVerizon")
    .addItem("Apply – Verizon only", "applyVerizon")
    .addSeparator()
    .addItem("Set Secrets & Endpoints", "promptAndSetSecrets")
    .addItem("Refresh Reusely_Catalog from API", "refreshCatalogFromApi")
    .addSeparator()
    .addItem("Export CSV (changed only)", "exportCsvForReuselyChangedOnly")
    .addToUi();
}

/////////////////////// SECRETS ///////////////////////

function promptAndSetSecrets() {
  const ui = SpreadsheetApp.getUi();
  const props = PropertiesService.getDocumentProperties();

  for (const k of SECRET_KEYS) {
    const existing = props.getProperty(k) || "";
    const def = DEFAULT_ENDPOINTS[k] || "";
    const current = existing || def;

    const msg =
      (current
        ? `Current value:\n${current}\n\nEnter a new value to change it, or leave blank to keep.`
        : `Enter a value for ${k}.`);

    const resp = ui.prompt(`Set ${k}`, msg, ui.ButtonSet.OK_CANCEL);
    if (resp.getSelectedButton() !== ui.Button.OK) return;

    let v = (resp.getResponseText() || "").trim();
    if (!v) v = current;
    props.setProperty(k, v);
  }

  ui.alert("Saved.");
}

/////////////////////// UTIL ///////////////////////

function _nowIso() {
  return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
}
function _sheetByName(name) { try { return SpreadsheetApp.getActive().getSheetByName(name); } catch(e){ return null; } }
function _ensureSheet(name) { const ss = SpreadsheetApp.getActive(); return ss.getSheetByName(name) || ss.insertSheet(name); }
function _clearExceptHeader(sh) { const lr = sh.getLastRow(); if (lr>1) sh.getRange(2,1,lr-1, sh.getLastColumn()).clearContent(); }
function _titleRowToIndex(h) { const m={}; h.forEach((x,i)=>m[String(x).trim()]=i); return m; }
function _norm(s){ return String(s||"").trim(); }
function _normalizeCarrier(c){ c=(c||"").toLowerCase(); if(c.includes("unlocked"))return"Unlocked"; if(c.includes("at&t")||c.includes("att"))return"AT&T"; if(c.includes("t-mobile")||c.includes("tmobile"))return"T-Mobile"; if(c.includes("verizon"))return"Verizon"; return c; }
function _gbNormalize(s){ s=String(s||"").toUpperCase().replace(/\s+/g,""); const m=s.match(/(\d+)\s*GB/i); return m?`${m[1]}GB`:s; }

// Normalize SE naming only (leave others intact)
function _cleanCatalogModelName_(s){
  const x=(s||"").trim().replace(/\s+/g," ");
  return x
    .replace(/\bSE \(2(nd)?\s*Gen(.*)?\)/i,"SE (2020)")
    .replace(/\bSE \(3(rd)?\s*Gen(.*)?\)/i,"SE (2022)")
    .replace(/\bSE 2(.*)?\b/i,"SE (2020)")
    .replace(/\bSE 3(.*)?\b/i,"SE (2022)");
}

// Strip carrier & size from product_name to get the model-only string.
function _extractModelFromCatalog_(productName, networkName, sizeName){
  let s = String(productName || "");
  if (sizeName) {
    const reSize = new RegExp(String(sizeName).replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&"), "i");
    s = s.replace(reSize, "");
  }
  const carrierTokens = [
    "Unlocked","AT&T","ATT","T-Mobile","TMobile","Verizon","Cricket","Spectrum","MetroPCS","MetroPcs","Metro",
    "Straight Talk","TracFone","Other","Sprint"
  ];
  if (networkName) {
    const reNet = new RegExp(String(networkName).replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&"), "i");
    s = s.replace(reNet, "");
  }
  carrierTokens.forEach(tok=>{
    const re = new RegExp(`\\b${tok.replace(/[-/\\^$*+?.()|[\]{}]/g,"\\$&")}\\b`,"i");
    s = s.replace(re,"");
  });
  s = s.replace(/\b\d+\s*GB\b/gi,"");
  s = s.replace(/\s+/g," ").trim();
  return _cleanCatalogModelName_(s);
}

/////////////////////// CATALOG INDEX ///////////////////////

function buildCatalogIndex_() {
  const sh = _sheetByName(CATALOG_SHEET);
  if (!sh) throw new Error(`Missing sheet: ${CATALOG_SHEET}. Use "Refresh Reusely_Catalog from API" or import CSV first.`);
  const values = sh.getDataRange().getValues();
  if (values.length < 2) throw new Error("Reusely_Catalog has no data.");

  const header = values[0];
  const col = _titleRowToIndex(header);
  ["product_id","product_name","network_name","size_name"].forEach(r=>{
    if (col[r]==null) throw new Error(`Reusely_Catalog missing required column: ${r}`);
  });

  const rows = values.slice(1);
  const idx = {};
  const priceCols = {};
  for (const cond of CONDITIONS) {
    const label = PRICE_COLS_IN_CATALOG[cond];
    if (label && col[label]!=null) priceCols[cond] = col[label];
  }
  const priceLookup = {};

  rows.forEach(r=>{
    const pid  = _norm(r[col["product_id"]]);
    const name = _norm(r[col["product_name"]]);
    const net  = _normalizeCarrier(_norm(r[col["network_name"]]));
    const size = _gbNormalize(_norm(r[col["size_name"]]));
    if (!pid || !name || !net) return;

    const modelOnly = _extractModelFromCatalog_(name, net, size);
    if (!modelOnly) return;

    const keyStrict = `${modelOnly}|${net}|${size}`;
    idx[keyStrict] = { product_id: pid, model: modelOnly, network_name: net, size_name: size };

    const keyLoose = `${modelOnly}|${net}|`;
    if (!idx[keyLoose]) idx[keyLoose] = { product_id: pid, model: modelOnly, network_name: net, size_name: size };

    if (Object.keys(priceCols).length) {
      if (!priceLookup[keyStrict]) priceLookup[keyStrict] = {};
      for (const cond of CONDITIONS) {
        const ix = priceCols[cond];
        if (ix!=null) {
          const v = Number(r[ix]) || 0;
          priceLookup[keyStrict][cond] = v;
        }
      }
    }
  });

  return { idx, priceLookup };
}

/////////////////////// REPORT READING ///////////////////////

function readReportRows_(tabName) {
  const sh = _sheetByName(tabName);
  if (!sh) return [];
  const values = sh.getDataRange().getValues();
  if (values.length < 2) return [];

  const header = values[1]; // row 2 are headers
  const col = _titleRowToIndex(header);
  if (col[COL_MODEL]==null) throw new Error(`${tabName} missing column: ${COL_MODEL}`);

  const hasStorage = (col[COL_STORAGE]!=null);
  const out = [];

  for (let i=2; i<values.length; i++){
    const row = values[i];
    const model = _norm(row[col[COL_MODEL]]);
    if (!model) continue;
    const storage = hasStorage ? _gbNormalize(_norm(row[col[COL_STORAGE]])) : "";

    for (const cond of CONDITIONS) {
      const rankLabel = `${cond} Rank`;
      const dLabel    = `${cond} Δ`;
      if (col[rankLabel]==null || col[dLabel]==null) continue;

      const rankRaw = row[col[rankLabel]];
      const dRaw    = row[col[dLabel]];
      const rank  = (rankRaw===""||rankRaw==null) ? "" : rankRaw;
      const delta = (dRaw===""||dRaw==null) ? "" : dRaw;

      out.push({ model, storage, condition: cond, rank, delta });
    }
  }
  return out;
}

function pickReportTab_(pref, fallback){ return _sheetByName(pref)?pref:(_sheetByName(fallback)?fallback:null); }

/////////////////////// PRICE LOGIC ///////////////////////

function computeNewPrice_(current, rank, delta, condition) {
  if (current==null || current==="" || isNaN(Number(current))) return { proposed:"", reason:"NO CURRENT PRICE" };
  const cur = Number(current);
  const r = (rank===""||rank==null)?null:Number(rank);
  const d = (delta===""||delta==null)?null:Number(delta);
  if (r==null || isNaN(r) || d==null || isNaN(d)) return { proposed:"", reason:"MISSING RANK/Δ" };

  if (condition==="New" && NEW_UNDERCUT_LEADER_BY>0 && r!==1){
    const topPrice = cur + d;
    const target = Math.max(0, topPrice - NEW_UNDERCUT_LEADER_BY);
    return { proposed: target, reason: `NEW: TOP-$${NEW_UNDERCUT_LEADER_BY}` };
  }
  if (r!==1) return { proposed: cur + d + PRICE_BUMP_ABOVE_SECOND, reason:"CHASE #1" };
  if (r===1 && (-d)>TRIM_LEAD_THRESHOLD) return { proposed: cur + d + PRICE_BUMP_ABOVE_SECOND, reason:"TRIM LEAD" };
  return { proposed: cur, reason:"NO CHANGE" };
}

/////////////////////// CURRENT PRICE SOURCE ///////////////////////

function getCurrentPriceFromCatalog_(priceLookup, model, carrier, storage, condition){
  const key = `${_cleanCatalogModelName_(model)}|${carrier}|${storage}`;
  const rec = priceLookup[key];
  if (!rec) return null;
  const v = rec[condition];
  return (v==null || isNaN(Number(v))) ? null : Number(v);
}

function getCurrentPriceViaApi_(productId){
  const base = PropertiesService.getDocumentProperties().getProperty("REUSELY_BASE_URL") || "";
  const pathTpl = PropertiesService.getDocumentProperties().getProperty("GET_PRICE_BY_PRODUCTID") || "/v2/admin/products/{productId}/pricing";
  if (!base || !pathTpl) return null;

  const url = base.replace(/\/+$/,"") + pathTpl.replace("{productId}", encodeURIComponent(productId));

  const headers = {
    "Content-Type": "application/json",
    // v2 docs show x-tenant-id and x-secret-key. If your tenant also uses x-api-key, keep it.
    "x-tenant-id": PropertiesService.getDocumentProperties().getProperty("REUSELY_TENANT_ID") || "",
    "x-secret-key": PropertiesService.getDocumentProperties().getProperty("REUSELY_SECRET_KEY") || "",
    "x-api-key": PropertiesService.getDocumentProperties().getProperty("REUSELY_API_KEY") || ""
  };

  try {
    const resp = UrlFetchApp.fetch(url, { method: "get", headers, muteHttpExceptions: true });
    if (resp.getResponseCode() !== 200) return null;

    const json = JSON.parse(resp.getContentText() || "{}");

    // v2 example shape:
    // { "status_code": 200, "data": { "product_id": 187569, "pricing": [ {name:"New", price:700, ...}, ... ] } }
    const list = (json.data && Array.isArray(json.data.pricing)) ? json.data.pricing : null;
    if (!list) return null;

    // Build { New, Flawless, Good, Fair, Broken } numeric map
    const out = {};
    list.forEach(p => {
      const n = String(p && p.name || "").trim();
      const v = Number(p && p.price);
      if (!isNaN(v)) {
        // Reusely uses "Brand New" / "Flawless" etc. Normalize to our keys.
        const norm = (
          n.toLowerCase() === "brand new" ? "New" :
          n.toLowerCase() === "flawless"  ? "Flawless" :
          n
        );
        out[norm] = v;
      }
    });
    return out; // e.g. { New: 700, Flawless: 680, Good: 520, Fair: 420, Broken: 120 }

  } catch (e) {
    return null;
  }
}

/////////////////////// PROPOSALS ///////////////////////

function ensureProposalsSheet_(){
  const sh = _ensureSheet("Proposals");
  const header = ["When","Carrier","Model","Storage","Condition","Rank","Δ","CurrentPrice","ProposedPrice","Status","product_id"];
  if (sh.getLastRow()===0){ sh.appendRow(header); }
  else{
    const existing = sh.getRange(1,1,1,header.length).getValues()[0];
    if (existing.join("|")!==header.join("|")){ sh.clear(); sh.appendRow(header); }
  }
  return sh;
}
function clearProposals_(){ _clearExceptHeader(ensureProposalsSheet_()); }

function buildProposals_(doApply, carriersFilter){
  const { idx, priceLookup } = buildCatalogIndex_();
  const haveCatalogPrices = Object.keys(priceLookup).length > 0;

  const carriersToRun = carriersFilter && carriersFilter.length
    ? REPORT_TABS.filter(s=>carriersFilter.indexOf(s.carrier)>=0)
    : REPORT_TABS;

  const out = [];
  const when = _nowIso();

  for (const spec of carriersToRun){
    const tab = pickReportTab_(spec.tabAll, spec.tabLegacy);
    if (!tab) continue;

    const hasAllGb = !!_sheetByName(spec.tabAll);
    const carrier = spec.carrier;
    const carrierNorm = _normalizeCarrier(carrier);
    const records = readReportRows_(tab);

    records.forEach(rec=>{
      const model   = _norm(rec.model);
      const storage = hasAllGb ? _gbNormalize(_norm(rec.storage)) : "";
      const cond    = rec.condition;

      const modelKey = _cleanCatalogModelName_(model);
      const keyStrict = `${modelKey}|${carrierNorm}|${storage}`;
      const keyLoose  = `${modelKey}|${carrierNorm}|`;

      const cat = idx[keyStrict] || idx[keyLoose];
      let productId = cat ? cat.product_id : "";
      let currentPrice = null;

      if (haveCatalogPrices) {
        currentPrice = getCurrentPriceFromCatalog_(priceLookup, model, carrierNorm, storage, cond);
      } else if (productId) {
        const apiPrices = getCurrentPriceViaApi_(productId);
        if (apiPrices){
          const map = { "New":"New","Flawless":"Mint","Good":"Good","Fair":"Fair","Broken":"Broken" };
          for (const k in map) if (map[k]===cond && apiPrices[k]!=null) currentPrice = Number(apiPrices[k]);
        }
      }

      const statusParts = [];
      if (!productId) statusParts.push("NO CATALOG MATCH");

      const { proposed, reason } = computeNewPrice_(currentPrice, rec.rank, rec.delta, cond);
      if (proposed==="" || isNaN(Number(proposed))) statusParts.push(reason || "NO PROPOSED PRICE");
      else { statusParts.push(reason); if (!doApply) statusParts.push("DRY-RUN"); }

      let appliedPrice = proposed;
      if (doApply && productId && proposed!=="" && !isNaN(Number(proposed))) {
        const result = putPrice_(productId, cond, Number(proposed));
        statusParts.push(result.ok ? "APPLIED" : `APPLY FAILED${result.note ? " ("+result.note+")" : ""}`);
      } else if (doApply) {
        statusParts.push("SKIPPED");
      }

      out.push([
        when, carrier, model, storage, cond, rec.rank, rec.delta,
        currentPrice==null?"":Number(currentPrice),
        appliedPrice===""?"":Number(appliedPrice),
        statusParts.filter(Boolean).join(" | "),
        productId
      ]);
    });
  }

  if (out.length){
    const sh = ensureProposalsSheet_();
    sh.getRange(sh.getLastRow()+1, 1, out.length, out[0].length).setValues(out);
  }
}

/////////////////////// PUT PRICE (v2 /admin/pricing) ///////////////////////

function putPrice_(productId, condition, price) {
  try {
    const base = PropertiesService.getDocumentProperties().getProperty("REUSELY_BASE_URL") || "";
    const path = PropertiesService.getDocumentProperties().getProperty("PUT_PRICE_BY_PRODUCTID") || "/v2/admin/pricing";
    if (!base || !path) return { ok: false, note: "no-endpoint" };

    const url = base.replace(/\/+$/, "") + path;

    const tenantId = PropertiesService.getDocumentProperties().getProperty("REUSELY_TENANT_ID") || "";
    const headers = {
      "Content-Type": "application/json",
      "x-api-key": PropertiesService.getDocumentProperties().getProperty("REUSELY_API_KEY") || "",
      "x-tenant-id": tenantId,
      "x-secret-key": PropertiesService.getDocumentProperties().getProperty("REUSELY_SECRET_KEY") || "",
    };

    // Map Swappa -> Reusely conditions
    const condMap = { "New": "Brand New", "Mint": "Flawless", "Good": "Good", "Fair": "Fair", "Broken": "Broken" };
    const reuselyCond = condMap[condition] || condition;

    const payload = {
      product_id: Number(productId),
      conditions: [
        {
          name: reuselyCond,
          price: Math.round(Number(price)),
          is_custom_price: 1
        }
      ]
    };

    const resp = UrlFetchApp.fetch(url, {
      method: "post",
      headers,
      contentType: "application/json",
      payload: JSON.stringify(payload),
      muteHttpExceptions: true,
    });

    const code = resp.getResponseCode();
    if (code >= 200 && code < 300) return { ok: true, note: "" };

    const body = String(resp.getContentText() || "").slice(0, 300).replace(/\s+/g, " ").trim();
    return { ok: false, note: `${code} ${body} @ ${url}` };

  } catch (e) {
    return { ok: false, note: String(e && e.message ? e.message : e) };
  }
}
/////////////////////// REFRESH CATALOG (OPTIONAL) ///////////////////////

function refreshCatalogFromApi(){
  const base = PropertiesService.getDocumentProperties().getProperty("REUSELY_BASE_URL") || "";
  const path = PropertiesService.getDocumentProperties().getProperty("LIST_PRODUCTS_ENDPOINT") || DEFAULT_ENDPOINTS.LIST_PRODUCTS_ENDPOINT;
  const url = base.replace(/\/+$/,"") + path;
  const headers = {
    "Content-Type":"application/json",
    "x-api-key": PropertiesService.getDocumentProperties().getProperty("REUSELY_API_KEY") || "",
    "x-tenant-id": PropertiesService.getDocumentProperties().getProperty("REUSELY_TENANT_ID") || "",
    "x-secret-key": PropertiesService.getDocumentProperties().getProperty("REUSELY_SECRET_KEY") || "",
  };
  const resp = UrlFetchApp.fetch(url,{method:"get", headers, muteHttpExceptions:true});
  if (resp.getResponseCode()!==200){ SpreadsheetApp.getUi().alert(`Catalog fetch failed: ${resp.getResponseCode()}`); return; }
  const data = JSON.parse(resp.getContentText()||"{}");
  if (!data || !data.items || !data.items.length){ SpreadsheetApp.getUi().alert("No products returned."); return; }

  const rows = [["product_id","product_name","network_name","size_name"]];
  data.items.forEach(it=>{
    rows.push([
      it.product_id || it.id || "",
      _cleanCatalogModelName_(it.product_name || it.name || ""),
      it.network_name || it.carrier || "",
      it.size_name || it.storage || "",
    ]);
  });

  const sh = _ensureSheet(CATALOG_SHEET);
  sh.clear();
  sh.getRange(1,1,rows.length, rows[0].length).setValues(rows);
}

/////////////////////// SIMPLE (single-pass) ENTRIES ///////////////////////

function dryRunAllTabs(){ clearProposals_(); buildProposals_(false); SpreadsheetApp.getUi().alert("Dry run complete."); }
function applyAllTabs(){ clearProposals_(); buildProposals_(true);  SpreadsheetApp.getUi().alert("Apply complete."); }

function dryRunUnlocked(){ _runCarrierOnce_(false,"Unlocked"); }  function applyUnlocked(){ _runCarrierOnce_(true,"Unlocked"); }
function dryRunATT(){ _runCarrierOnce_(false,"AT&T"); }          function applyATT(){ _runCarrierOnce_(true,"AT&T"); }
function dryRunTMobile(){ _runCarrierOnce_(false,"T-Mobile"); }   function applyTMobile(){ _runCarrierOnce_(true,"T-Mobile"); }
function dryRunVerizon(){ _runCarrierOnce_(false,"Verizon"); }    function applyVerizon(){ _runCarrierOnce_(true,"Verizon"); }

function _runCarrierOnce_(apply, carrier){
  clearProposals_();
  buildProposals_(apply, [carrier]);
  SpreadsheetApp.getUi().alert(`${apply ? "Apply" : "Dry run"} complete for ${carrier}.`);
}

/////////////////////// SEQUENTIAL RUNNER ///////////////////////

function startSequentialDryRun(){ _startSequential_(false); }
function startSequentialApply(){ _startSequential_(true); }

function _startSequential_(apply){
  clearProposals_();
  const queue = [];
  for (const spec of REPORT_TABS){ const tab = pickReportTab_(spec.tabAll, spec.tabLegacy); if (tab) queue.push(spec.carrier); }
  const props = PropertiesService.getDocumentProperties();
  props.setProperty(QUEUE_KEY, JSON.stringify(queue));
  props.setProperty(APPLY_FLAG_KEY, String(!!apply));
  ScriptApp.newTrigger("processNextCarrier_").timeBased().after(1000).create();
  SpreadsheetApp.getUi().alert(`Started ${apply ? "APPLY" : "DRY RUN"} sequence for carriers: ${queue.join(", ")}`);
}

function processNextCarrier_(){
  const props = PropertiesService.getDocumentProperties();
  let queue=[]; try{ queue = JSON.parse(props.getProperty(QUEUE_KEY)||"[]"); }catch(e){ queue=[]; }
  const apply = (props.getProperty(APPLY_FLAG_KEY)||"false")==="true";
  if (!queue.length){ _cleanupSelfTriggers_("processNextCarrier_"); return; }
  const carrier = queue.shift();
  props.setProperty(QUEUE_KEY, JSON.stringify(queue));
  buildProposals_(apply, [carriage=carrier]);
  if (queue.length) ScriptApp.newTrigger("processNextCarrier_").timeBased().after(1500).create();
  else { _cleanupSelfTriggers_("processNextCarrier_"); SpreadsheetApp.getUi().alert(`Sequential ${apply ? "APPLY" : "DRY RUN"} finished.`); }
}
function _cleanupSelfTriggers_(funcName){
  ScriptApp.getProjectTriggers().forEach(t=>{ if (t.getHandlerFunction && t.getHandlerFunction()===funcName) ScriptApp.deleteTrigger(t); });
}

/////////////////////// CSV EXPORT (CHANGED ONLY) ///////////////////////

function exportCsvForReuselyChangedOnly() {
  const ui = SpreadsheetApp.getUi();
  const ss = SpreadsheetApp.getActive();

  const psh = ss.getSheetByName("Proposals");
  if (!psh) { ui.alert("Missing 'Proposals' sheet."); return; }
  const pVals = psh.getDataRange().getValues();
  if (pVals.length < 2) { ui.alert("'Proposals' is empty."); return; }
  const ph = _titleRowToIndex(pVals[0]);

  const condToApi = { "New":"New", "Mint":"Flawless", "Good":"Good", "Fair":"Fair", "Broken":"Broken" };
  const latestByPid = {};
  for (let i = 1; i < pVals.length; i++) {
    const r = pVals[i];
    const pid = String(r[ph["product_id"]] || "").trim();
    const cond = String(r[ph["Condition"]] || "").trim();
    const prop = r[ph["ProposedPrice"]];
    if (!pid || !cond || prop === "" || prop == null || isNaN(Number(prop))) continue;
    const key = condToApi[cond];
    if (!key) continue;
    if (!latestByPid[pid]) latestByPid[pid] = { New:"", Flawless:"", Good:"", Fair:"", Broken:"" };
    latestByPid[pid][key] = Math.round(Number(prop));
  }

  const csh = ss.getSheetByName(CATALOG_SHEET);
  if (!csh) { ui.alert(`Missing '${CATALOG_SHEET}' sheet.`); return; }
  const cVals = csh.getDataRange().getValues();
  if (cVals.length < 2) { ui.alert(`'${CATALOG_SHEET}' has no data.`); return; }
  const ch = _titleRowToIndex(cVals[0]);

  const PRICE_HEADERS = ["Brand New","Flawless","Good","Fair","Broken"];
  const nonPriceHeaders = cVals[0].filter(h => PRICE_HEADERS.indexOf(String(h)) === -1);
  const csvHeader = nonPriceHeaders.concat(PRICE_HEADERS);
  const out = [csvHeader];

  function getCellByName(row, name) { const idx = ch[name]; return (idx == null) ? "" : row[idx]; }

  for (let i = 1; i < cVals.length; i++) {
    const r = cVals[i];
    const pid = String(getCellByName(r, "product_id") || "").trim();
    if (!pid) continue;

    const curPrices = {
      "Brand New": numOrZero(getCellByName(r, "Brand New")),
      "Flawless" : numOrZero(getCellByName(r, "Flawless")),
      "Good"     : numOrZero(getCellByName(r, "Good")),
      "Fair"     : numOrZero(getCellByName(r, "Fair")),
      "Broken"   : numOrZero(getCellByName(r, "Broken")),
    };

    const props = latestByPid[pid] || {};
    const newPrices = {
      "Brand New": numOrKeep(props["New"],       curPrices["Brand New"]),
      "Flawless" : numOrKeep(props["Flawless"],  curPrices["Flawless"]),
      "Good"     : numOrKeep(props["Good"],      curPrices["Good"]),
      "Fair"     : numOrKeep(props["Fair"],      curPrices["Fair"]),
      "Broken"   : numOrKeep(props["Broken"],    curPrices["Broken"]),
    };

    const changed =
      newPrices["Brand New"] !== curPrices["Brand New"] ||
      newPrices["Flawless"]  !== curPrices["Flawless"]  ||
      newPrices["Good"]      !== curPrices["Good"]      ||
      newPrices["Fair"]      !== curPrices["Fair"]      ||
      newPrices["Broken"]    !== curPrices["Broken"];

    if (!changed) continue;

    const rowOut = [];
    nonPriceHeaders.forEach(h => {
      const idx = ch[h];
      rowOut.push(idx == null ? "" : cVals[i][idx]);
    });

    rowOut.push(
      newPrices["Brand New"],
      newPrices["Flawless"],
      newPrices["Good"],
      newPrices["Fair"],
      newPrices["Broken"]
    );

    out.push(rowOut);
  }

  if (out.length === 1) { ui.alert("No changes detected vs. catalog — nothing to export."); return; }

  const csv = toCsv_(out);
  const ts = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyyMMdd_HHmmss");
  const fileName = `reusely_prices_changed_${ts}.csv`;
  const file = DriveApp.createFile(fileName, csv, MimeType.CSV);
  ui.alert(`CSV created (changed rows only):\n${fileName}\n\nOpen in Drive:\nhttps://drive.google.com/open?id=${file.getId()}`);
}

/* ===== helpers for CSV exporter ===== */
function numOrZero(v){ const n = Number(v); return isNaN(n)? 0 : Math.round(n); }
function numOrKeep(v, keep){ const n = Number(v); return (v==null || v==="" || isNaN(n)) ? keep : Math.round(n); }
function toCsv_(rows) {
  const esc = v => { if (v == null) v = ""; v = String(v); if (/[",\n]/.test(v)) v = '"' + v.replace(/"/g, '""') + '"'; return v; };
  return rows.map(r => r.map(esc).join(",")).join("\n") + "\n";
}
