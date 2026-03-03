"""
app.py - FastAPI web service for the Day Trading Signal Bot.

Endpoints:
  GET /              - Live dashboard (dark theme, auto-refreshes via SSE)
  GET /api/signals   - JSON list of recent signals (filterable)
  GET /api/stream    - Server-Sent Events stream for real-time updates
  GET /health        - Health check

Deploy as a Railway Web Service (needs a public URL).
"""

import asyncio
import json
import os
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse

from db import get_recent_signals, init_db

app = FastAPI(title="Day Trading Signal Bot", docs_url="/docs")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    init_db()


# ---------------------------------------------------------------------------
# REST API
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/signals")
def api_signals(
    limit:       int           = Query(50,  ge=1, le=200),
    hours:       int           = Query(24,  ge=1, le=72),
    ticker:      Optional[str] = Query(None),
    signal_type: Optional[str] = Query(None),
):
    """Return recent signals as JSON. Supports filtering by ticker and signal_type."""
    return get_recent_signals(limit=limit, hours=hours, ticker=ticker, signal_type=signal_type)


# ---------------------------------------------------------------------------
# Server-Sent Events (real-time push to dashboard)
# ---------------------------------------------------------------------------

@app.get("/api/stream")
async def stream_signals():
    """
    SSE endpoint. The dashboard subscribes here and receives new signals
    pushed automatically every 20 seconds without a page refresh.
    """
    async def generate():
        last_id = 0
        while True:
            try:
                signals = get_recent_signals(limit=20, hours=1)
                new_signals = [s for s in signals if s["id"] > last_id]
                if new_signals:
                    last_id = max(s["id"] for s in new_signals)
                    for sig in new_signals:
                        yield f"data: {json.dumps(sig)}\n\n"
            except Exception as exc:
                yield f"data: {json.dumps({'error': str(exc)})}\n\n"
            await asyncio.sleep(20)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------------------------------------------------------------------
# Dashboard HTML
# ---------------------------------------------------------------------------

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Day Trading Signal Bot</title>
<style>
  :root {
    --bg:       #0d1117;
    --surface:  #161b22;
    --border:   #30363d;
    --text:     #e6edf3;
    --muted:    #8b949e;
    --green:    #3fb950;
    --red:      #f85149;
    --yellow:   #d29922;
    --blue:     #58a6ff;
    --purple:   #bc8cff;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 14px; }
  header { background: var(--surface); border-bottom: 1px solid var(--border); padding: 16px 24px; display: flex; align-items: center; justify-content: space-between; }
  header h1 { font-size: 18px; font-weight: 600; letter-spacing: -0.3px; }
  header h1 span { color: var(--blue); }
  .status-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green); display: inline-block; margin-right: 6px; animation: pulse 2s infinite; }
  @keyframes pulse { 0%,100% { opacity:1 } 50% { opacity:.4 } }
  .controls { padding: 16px 24px; display: flex; gap: 10px; flex-wrap: wrap; align-items: center; border-bottom: 1px solid var(--border); }
  input, select { background: var(--surface); border: 1px solid var(--border); color: var(--text); padding: 7px 12px; border-radius: 6px; font-size: 13px; }
  input:focus, select:focus { outline: none; border-color: var(--blue); }
  button { padding: 7px 16px; border-radius: 6px; border: 1px solid var(--border); cursor: pointer; font-size: 13px; font-weight: 500; transition: all .15s; }
  .btn-primary { background: var(--blue); color: #fff; border-color: var(--blue); }
  .btn-primary:hover { opacity: .85; }
  .btn-secondary { background: var(--surface); color: var(--text); }
  .btn-secondary:hover { border-color: var(--blue); }
  .stats-bar { display: flex; gap: 0; border-bottom: 1px solid var(--border); }
  .stat { padding: 12px 24px; border-right: 1px solid var(--border); }
  .stat-label { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .5px; }
  .stat-value { font-size: 22px; font-weight: 700; margin-top: 2px; }
  .main { padding: 20px 24px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(380px, 1fr)); gap: 14px; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 16px; position: relative; overflow: hidden; transition: border-color .2s; }
  .card:hover { border-color: var(--blue); }
  .card::before { content:''; position:absolute; top:0; left:0; width:3px; height:100%; }
  .card.bullish::before { background: var(--green); }
  .card.bearish::before  { background: var(--red); }
  .card-header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 10px; }
  .ticker { font-size: 18px; font-weight: 700; margin-right: 8px; }
  .badge { display: inline-block; padding: 3px 9px; border-radius: 20px; font-size: 11px; font-weight: 600; letter-spacing: .4px; }
  .badge.bullish { background: rgba(63,185,80,.15); color: var(--green); }
  .badge.bearish { background: rgba(248,81,73,.15); color: var(--red); }
  .badge.new { background: rgba(88,166,255,.15); color: var(--blue); font-size:10px; margin-left:6px; }
  .signal-type { font-size: 12px; color: var(--muted); margin-top: 2px; }
  .instruction { font-size: 13px; line-height: 1.55; color: var(--text); margin: 10px 0; padding: 10px 12px; background: rgba(255,255,255,.03); border-radius: 6px; border-left: 2px solid var(--border); }
  .levels { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 8px; margin-top: 12px; }
  .level { text-align: center; }
  .level-label { font-size: 10px; color: var(--muted); text-transform: uppercase; letter-spacing:.5px; }
  .level-value { font-size: 14px; font-weight: 600; margin-top: 2px; }
  .level-value.entry { color: var(--blue); }
  .level-value.sl     { color: var(--red); }
  .level-value.tp     { color: var(--green); }
  .card-footer { display: flex; justify-content: space-between; align-items: center; margin-top: 12px; padding-top: 10px; border-top: 1px solid var(--border); }
  .rr { font-size: 12px; font-weight: 700; }
  .rr.good { color: var(--green); }
  .rr.ok   { color: var(--yellow); }
  .time-ago { font-size: 11px; color: var(--muted); }
  .empty-state { text-align: center; padding: 80px 20px; color: var(--muted); }
  .empty-state svg { opacity: .3; margin-bottom: 16px; }
  .toast { position: fixed; bottom: 20px; right: 20px; background: var(--surface); border: 1px solid var(--green); color: var(--green); padding: 10px 16px; border-radius: 8px; font-size: 13px; z-index: 999; opacity: 0; transform: translateY(10px); transition: all .3s; }
  .toast.show { opacity: 1; transform: translateY(0); }
  .live-pill { display: inline-flex; align-items: center; gap: 5px; background: rgba(63,185,80,.1); color: var(--green); border: 1px solid rgba(63,185,80,.3); padding: 3px 10px; border-radius: 20px; font-size: 11px; font-weight: 600; }
  @media (max-width: 600px) { .grid { grid-template-columns: 1fr; } .stats-bar { flex-wrap: wrap; } }
</style>
</head>
<body>

<header>
  <h1><span>&#9650;</span> Day Trading Signal Bot</h1>
  <div class="live-pill"><span class="status-dot"></span>Live</div>
</header>

<div class="controls">
  <input type="text" id="filterTicker" placeholder="Filter ticker (e.g. AAPL)" oninput="applyFilters()">
  <select id="filterType" onchange="applyFilters()">
    <option value="">All signal types</option>
    <option value="BULLISH_FVG">Bullish FVG</option>
    <option value="BEARISH_FVG">Bearish FVG</option>
    <option value="BULLISH_LIQUIDITY_SWEEP">Bullish Sweep</option>
    <option value="BEARISH_LIQUIDITY_SWEEP">Bearish Sweep</option>
    <option value="BULLISH_ORB">Bullish ORB</option>
    <option value="BEARISH_ORB">Bearish ORB</option>
  </select>
  <select id="filterHours" onchange="loadSignals()">
    <option value="6">Last 6 hours</option>
    <option value="24" selected>Last 24 hours</option>
    <option value="48">Last 48 hours</option>
    <option value="72">Last 72 hours</option>
  </select>
  <button class="btn-primary" onclick="loadSignals()">Refresh</button>
  <button class="btn-secondary" onclick="toggleAutoRefresh()" id="autoBtn">Auto-refresh: ON</button>
</div>

<div class="stats-bar">
  <div class="stat">
    <div class="stat-label">Total Signals</div>
    <div class="stat-value" id="statTotal">-</div>
  </div>
  <div class="stat">
    <div class="stat-label">Bullish</div>
    <div class="stat-value" id="statBull" style="color:var(--green)">-</div>
  </div>
  <div class="stat">
    <div class="stat-label">Bearish</div>
    <div class="stat-value" id="statBear" style="color:var(--red)">-</div>
  </div>
  <div class="stat">
    <div class="stat-label">Avg R:R</div>
    <div class="stat-value" id="statRR" style="color:var(--blue)">-</div>
  </div>
</div>

<div class="main">
  <div class="grid" id="signals-grid">
    <div class="empty-state">Loading signals...</div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
let allSignals = [];
let autoRefreshEnabled = true;
let refreshTimer;
let knownIds = new Set();

function isBullish(type) {
  return type.startsWith("BULLISH");
}

function friendlyType(type) {
  const map = {
    BULLISH_FVG:               "Fair Value Gap",
    BEARISH_FVG:               "Fair Value Gap",
    BULLISH_LIQUIDITY_SWEEP:   "Liquidity Sweep",
    BEARISH_LIQUIDITY_SWEEP:   "Liquidity Sweep",
    BULLISH_ORB:               "Opening Range Breakout",
    BEARISH_ORB:               "Opening Range Breakout",
  };
  return map[type] || type;
}

function timeAgo(isoString) {
  const diff = Math.floor((Date.now() - new Date(isoString)) / 1000);
  if (diff < 60)   return diff + "s ago";
  if (diff < 3600) return Math.floor(diff/60) + "m ago";
  return Math.floor(diff/3600) + "h ago";
}

function renderCard(sig, isNew = false) {
  const bull = isBullish(sig.type);
  const dir  = bull ? "bullish" : "bearish";
  const rr   = parseFloat(sig.rr) || 0;
  const rrClass = rr >= 2.5 ? "good" : "ok";

  return `<div class="card ${dir}" data-ticker="${sig.ticker}" data-type="${sig.type}">
    <div class="card-header">
      <div>
        <span class="ticker">${sig.ticker}</span>
        <span class="badge ${dir}">${bull ? "LONG" : "SHORT"}</span>
        ${isNew ? '<span class="badge new">NEW</span>' : ""}
        <div class="signal-type">${friendlyType(sig.type)}</div>
      </div>
    </div>
    <div class="instruction">${sig.instruction}</div>
    <div class="levels">
      <div class="level">
        <div class="level-label">Entry</div>
        <div class="level-value entry">$${sig.entry}</div>
      </div>
      <div class="level">
        <div class="level-label">Stop Loss</div>
        <div class="level-value sl">$${sig.sl}</div>
      </div>
      <div class="level">
        <div class="level-label">Take Profit</div>
        <div class="level-value tp">$${sig.tp}</div>
      </div>
    </div>
    <div class="card-footer">
      <span class="rr ${rrClass}">R:R ${rr.toFixed(1)}</span>
      <span class="time-ago">${timeAgo(sig.time)}</span>
    </div>
  </div>`;
}

function updateStats(signals) {
  const bull = signals.filter(s => isBullish(s.type)).length;
  const bear = signals.length - bull;
  const avgRR = signals.length ? (signals.reduce((a,s) => a + (parseFloat(s.rr)||0), 0) / signals.length).toFixed(1) : "-";
  document.getElementById("statTotal").textContent = signals.length;
  document.getElementById("statBull").textContent  = bull;
  document.getElementById("statBear").textContent  = bear;
  document.getElementById("statRR").textContent    = avgRR;
}

function applyFilters() {
  const ticker = document.getElementById("filterTicker").value.toUpperCase().trim();
  const type   = document.getElementById("filterType").value;
  const filtered = allSignals.filter(s =>
    (!ticker || s.ticker.includes(ticker)) &&
    (!type   || s.type === type)
  );
  renderGrid(filtered);
  updateStats(filtered);
}

function renderGrid(signals) {
  const grid = document.getElementById("signals-grid");
  if (!signals.length) {
    grid.innerHTML = `<div class="empty-state">
      <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
        <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>
      </svg>
      <p>No signals found for the selected filters.<br>The bot scans every minute during market hours.</p>
    </div>`;
    return;
  }
  grid.innerHTML = signals.map(s => renderCard(s, false)).join("");
}

async function loadSignals() {
  const hours  = document.getElementById("filterHours").value;
  const ticker = document.getElementById("filterTicker").value.toUpperCase().trim();
  const type   = document.getElementById("filterType").value;

  let url = `/api/signals?limit=100&hours=${hours}`;
  if (ticker) url += `&ticker=${encodeURIComponent(ticker)}`;
  if (type)   url += `&signal_type=${encodeURIComponent(type)}`;

  try {
    const resp = await fetch(url);
    allSignals = await resp.json();
    allSignals.forEach(s => knownIds.add(s.id));
    renderGrid(allSignals);
    updateStats(allSignals);
  } catch (e) {
    console.error("Failed to load signals:", e);
  }
}

function showToast(message) {
  const el = document.getElementById("toast");
  el.textContent = message;
  el.classList.add("show");
  setTimeout(() => el.classList.remove("show"), 4000);
}

function startSSE() {
  const evtSource = new EventSource("/api/stream");
  evtSource.onmessage = (event) => {
    try {
      const sig = JSON.parse(event.data);
      if (!sig.id || knownIds.has(sig.id)) return;
      knownIds.add(sig.id);
      allSignals.unshift(sig);
      const grid = document.getElementById("signals-grid");
      const newCard = document.createElement("div");
      newCard.innerHTML = renderCard(sig, true);
      const emptyState = grid.querySelector(".empty-state");
      if (emptyState) grid.innerHTML = "";
      grid.prepend(newCard.firstElementChild);
      updateStats(allSignals);
      showToast(`New signal: ${sig.ticker} ${sig.type.replace(/_/g," ")}`);
    } catch(e) { /* skip malformed events */ }
  };
}

function startAutoRefresh() {
  refreshTimer = setInterval(loadSignals, 60000);
}

function toggleAutoRefresh() {
  autoRefreshEnabled = !autoRefreshEnabled;
  document.getElementById("autoBtn").textContent = `Auto-refresh: ${autoRefreshEnabled ? "ON" : "OFF"}`;
  if (autoRefreshEnabled) startAutoRefresh();
  else clearInterval(refreshTimer);
}

// Init
loadSignals();
startSSE();
startAutoRefresh();
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(content=DASHBOARD_HTML)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
