// supabase/functions/stock-scheduler/index.ts
import { serve } from "https://deno.land/std@0.200.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";
const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const PRIMARY_API_URL = Deno.env.get("PRIMARY_API_URL") || "https://stock-api-x35p.vercel.app";
const BATCH_SIZE = 50;
const TOP_N_DELTA = 100;
const FMP_KEY = Deno.env.get("FMP_KEY") ?? "";
const TWELVE_DATA_KEY = Deno.env.get("TWELVE_DATA_KEY") ?? "";
let providerFailures = {
  primary: 0,
  twelvedata: 0,
  fmp: 0
};
const CIRCUIT_BREAKER_THRESHOLD = 10;
// Check if market is closed
async function isMarketClosed() {
  try {
    const now = new Date();
    const etDate = new Date(now.toLocaleString("en-US", {
      timeZone: "America/New_York"
    }));
    const weekday = etDate.getDay();
    if (weekday === 0 || weekday === 6) {
      console.log(`Market closed: Weekend`);
      return true;
    }
    const dateStr = etDate.toISOString().slice(0, 10);
    const { data: holidays } = await supabase.from("us_market_holidays").select("holiday_name").eq("holiday_date", dateStr).maybeSingle();
    if (holidays?.holiday_name) {
      console.log(`Market closed: ${holidays.holiday_name}`);
      return true;
    }
    return false;
  } catch (err) {
    console.error('Holiday check error:', err);
    return false;
  }
}
function computeRelativeVolume(today, avg10) {
  if (!today || !avg10) return null;
  return Number((today / avg10).toFixed(2));
}
// Fetch from Primary API
async function fetchBatchFromPrimaryAPI(symbols) {
  const results = new Map();
  if (providerFailures.primary >= CIRCUIT_BREAKER_THRESHOLD) return results;
  try {
    const symbolList = symbols.join(",");
    const url = `${PRIMARY_API_URL}/quote?symbols=${encodeURIComponent(symbolList)}`;
    console.log(`Fetching ${symbols.length} symbols from Primary API`);
    const res = await fetch(url, {
      signal: AbortSignal.timeout(20000),
      headers: {
        'User-Agent': 'SupabaseScheduler/1.0'
      }
    });
    if (!res.ok) {
      providerFailures.primary++;
      console.error(`Primary API error: ${res.status}`);
      return results;
    }
    const data = await res.json();
    let quotes = Array.isArray(data) ? data : data?.data ?? data?.quotes ?? [];
    providerFailures.primary = 0;
    for (const quote of quotes){
      if (!quote.symbol) continue;
      const volume = quote.volume ?? null;
      const avg10 = quote.averageVolume ?? quote.average_volume ?? null;
      results.set(quote.symbol, {
        symbol: quote.symbol,
        name: quote.name ?? quote.longName ?? quote.shortName ?? null,
        price: quote.price ?? null,
        open: quote.open ?? null,
        high: quote.high ?? null,
        low: quote.low ?? null,
        change_percent: quote.change_percent ?? quote.changePercent ?? null,
        volume,
        market_cap: quote.market_cap ?? quote.marketCap ?? null,
        shares_float: quote.shares_float ?? quote.sharesOutstanding ?? null,
        relative_volume: quote.relative_volume ?? computeRelativeVolume(volume, avg10),
        raw: quote
      });
    }
    console.log(`Primary API: Fetched ${results.size}/${symbols.length} symbols`);
  } catch (err) {
    providerFailures.primary++;
    console.error('Primary API error:', err);
  }
  return results;
}
// Fetch from Twelve Data (fallback)
async function fetchBatchFromTwelveData(symbols) {
  const results = new Map();
  if (!TWELVE_DATA_KEY || providerFailures.twelvedata >= CIRCUIT_BREAKER_THRESHOLD) return results;
  try {
    const symbolList = symbols.join(",");
    const url = `https://api.twelvedata.com/quote?symbol=${encodeURIComponent(symbolList)}&apikey=${TWELVE_DATA_KEY}`;
    const res = await fetch(url, {
      signal: AbortSignal.timeout(10000)
    });
    if (!res.ok) {
      providerFailures.twelvedata++;
      return results;
    }
    const data = await res.json();
    const quotes = Array.isArray(data) ? data : [
      data
    ];
    providerFailures.twelvedata = 0;
    for (const quote of quotes){
      if (!quote.symbol) continue;
      results.set(quote.symbol, {
        symbol: quote.symbol,
        name: quote.name ?? null,
        price: parseFloat(quote.close) || null,
        open: parseFloat(quote.open) || null,
        high: parseFloat(quote.high) || null,
        low: parseFloat(quote.low) || null,
        volume: parseFloat(quote.volume) || null,
        change_percent: parseFloat(quote.percent_change) || null,
        raw: quote
      });
    }
    console.log(`Twelve Data: ${results.size}/${symbols.length}`);
  } catch (err) {
    providerFailures.twelvedata++;
    console.error('Twelve Data error:', err);
  }
  return results;
}
// Fetch from FMP (fundamentals)
async function fetchFundamentalsFromFMP(symbols) {
  const results = new Map();
  if (!FMP_KEY || providerFailures.fmp >= CIRCUIT_BREAKER_THRESHOLD || symbols.length === 0) return results;
  try {
    const symbolList = symbols.join(",");
    const url = `https://financialmodelingprep.com/api/v3/profile/${encodeURIComponent(symbolList)}?apikey=${FMP_KEY}`;
    const res = await fetch(url, {
      signal: AbortSignal.timeout(10000)
    });
    if (!res.ok) {
      providerFailures.fmp++;
      return results;
    }
    const data = await res.json();
    if (!Array.isArray(data)) return results;
    providerFailures.fmp = 0;
    for (const profile of data){
      results.set(profile.symbol, {
        symbol: profile.symbol,
        name: profile.companyName ?? null,
        price: profile.price ?? null,
        market_cap: profile.mktCap ?? profile.marketCap ?? null,
        shares_float: profile.sharesOutstanding ?? null,
        raw: profile
      });
    }
    console.log(`FMP: ${results.size}/${symbols.length}`);
  } catch (err) {
    providerFailures.fmp++;
    console.error('FMP error:', err);
  }
  return results;
}
// Merge data
function mergeData(primary, twelve, fmp, symbol) {
  const source = primary ?? twelve ?? fmp;
  if (!source) return null;
  return {
    symbol,
    name: primary?.name ?? twelve?.name ?? fmp?.name ?? null,
    price: primary?.price ?? twelve?.price ?? fmp?.price ?? null,
    open: primary?.open ?? twelve?.open ?? null,
    high: primary?.high ?? twelve?.high ?? null,
    low: primary?.low ?? twelve?.low ?? null,
    change_percent: primary?.change_percent ?? twelve?.change_percent ?? null,
    volume: primary?.volume ?? twelve?.volume ?? null,
    market_cap: primary?.market_cap ?? fmp?.market_cap ?? null,
    shares_float: primary?.shares_float ?? fmp?.shares_float ?? null,
    relative_volume: primary?.relative_volume ?? null,
    raw: {
      primary: primary?.raw,
      twelve: twelve?.raw,
      fmp: fmp?.raw
    }
  };
}
// Process batches
async function processBatches(symbols) {
  let successCount = 0;
  for(let i = 0; i < symbols.length; i += BATCH_SIZE){
    const batch = symbols.slice(i, i + BATCH_SIZE);
    console.log(`Processing batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(symbols.length / BATCH_SIZE)}`);
    const primaryData = await fetchBatchFromPrimaryAPI(batch);
    const failedSymbols = batch.filter((s)=>!primaryData.has(s));
    const twelveData = failedSymbols.length > 0 ? await fetchBatchFromTwelveData(failedSymbols) : new Map();
    const needsFMP = batch.filter((s)=>{
      const p = primaryData.get(s);
      return p && (!p.market_cap || !p.shares_float);
    });
    const fmpData = needsFMP.length > 0 ? await fetchFundamentalsFromFMP(needsFMP) : new Map();
    const rows = [];
    for (const symbol of batch){
      const merged = mergeData(primaryData.get(symbol), twelveData.get(symbol), fmpData.get(symbol), symbol);
      if (merged && merged.price) {
        rows.push({
          symbol: merged.symbol,
          name: merged.name,
          price: merged.price,
          open: merged.open,
          high: merged.high,
          low: merged.low,
          close: null,
          volume: merged.volume,
          change_percent: merged.change_percent,
          market_cap: merged.market_cap,
          shares_float: merged.shares_float,
          relative_volume: merged.relative_volume,
          raw: merged.raw,
          updated_at: new Date().toISOString()
        });
        successCount++;
      }
    }
    if (rows.length > 0) {
      const { error } = await supabase.from("stocks").upsert(rows, {
        onConflict: "symbol"
      });
      if (error) {
        console.error("Upsert error:", error);
      } else {
        console.log(`Upserted ${rows.length} stocks`);
      }
    }
    // Small delay between batches
    await new Promise((r)=>setTimeout(r, 200));
  }
  return successCount;
}
// Get delta symbols
async function getDeltaSymbols() {
  const symbols = new Set();
  try {
    const { data: topVol } = await supabase.from("stocks").select("symbol").not("volume", "is", null).order("volume", {
      ascending: false
    }).limit(Math.floor(TOP_N_DELTA * 0.5));
    topVol?.forEach((r)=>symbols.add(r.symbol));
    const { data: topVolatile } = await supabase.from("stocks").select("symbol").not("change_percent", "is", null).order("change_percent", {
      ascending: false
    }).limit(Math.floor(TOP_N_DELTA * 0.3));
    topVolatile?.forEach((r)=>symbols.add(r.symbol));
    const { data: topNegative } = await supabase.from("stocks").select("symbol").not("change_percent", "is", null).order("change_percent", {
      ascending: true
    }).limit(Math.floor(TOP_N_DELTA * 0.2));
    topNegative?.forEach((r)=>symbols.add(r.symbol));
    console.log(`Selected ${symbols.size} delta symbols`);
  } catch (err) {
    console.error("Error getting delta symbols:", err);
  }
  return Array.from(symbols).slice(0, TOP_N_DELTA);
}
// Get full symbols
async function getFullSymbols(limit, offset) {
  try {
    const { data } = await supabase.from("stock_tickers").select('"Symbol"').range(offset, offset + limit - 1);
    const symbols = (data ?? []).map((r)=>r.Symbol).filter(Boolean);
    console.log(`Selected ${symbols.length} symbols (offset: ${offset})`);
    return symbols;
  } catch (err) {
    console.error("Error getting full symbols:", err);
    return [];
  }
}
// Main HTTP handler
serve(async (req)=>{
  const startTime = Date.now();
  try {
    const url = new URL(req.url);
    // Health check
    if (url.pathname.endsWith("/health")) {
      return new Response(JSON.stringify({
        status: "healthy",
        timestamp: new Date().toISOString()
      }), {
        headers: {
          "Content-Type": "application/json"
        }
      });
    }
    // Get parameters
    const mode = url.searchParams.get("mode");
    const limit = parseInt(url.searchParams.get("limit") ?? "1000", 10);
    const offset = parseInt(url.searchParams.get("offset") ?? "0", 10);
    const force = url.searchParams.get("force") === "true";
    if (!mode) {
      return new Response(JSON.stringify({
        error: "Missing 'mode' parameter. Use ?mode=delta or ?mode=full"
      }), {
        status: 400,
        headers: {
          "Content-Type": "application/json"
        }
      });
    }
    console.log(`Starting ${mode} update (limit: ${limit}, offset: ${offset})`);
    // Check if market is closed
    if (!force && await isMarketClosed()) {
      console.log("Market is closed, skipping update");
      return new Response(JSON.stringify({
        status: "skipped",
        reason: "Market closed (weekend or holiday)",
        timestamp: new Date().toISOString()
      }), {
        status: 200,
        headers: {
          "Content-Type": "application/json"
        }
      });
    }
    let symbols = [];
    if (mode === "delta") {
      symbols = await getDeltaSymbols();
    } else if (mode === "full") {
      symbols = await getFullSymbols(limit, offset);
    } else {
      return new Response(JSON.stringify({
        error: `Invalid mode: ${mode}. Use 'delta' or 'full'`
      }), {
        status: 400,
        headers: {
          "Content-Type": "application/json"
        }
      });
    }
    if (symbols.length === 0) {
      return new Response(JSON.stringify({
        status: "success",
        mode,
        updated: 0,
        message: "No symbols to process",
        duration_ms: Date.now() - startTime
      }), {
        status: 200,
        headers: {
          "Content-Type": "application/json"
        }
      });
    }
    const updated = await processBatches(symbols);
    return new Response(JSON.stringify({
      status: "success",
      mode,
      requested: symbols.length,
      updated,
      failed: symbols.length - updated,
      duration_ms: Date.now() - startTime,
      duration_sec: ((Date.now() - startTime) / 1000).toFixed(2)
    }), {
      status: 200,
      headers: {
        "Content-Type": "application/json"
      }
    });
  } catch (err) {
    console.error("Error:", err);
    return new Response(JSON.stringify({
      status: "error",
      error: String(err),
      duration_ms: Date.now() - startTime
    }), {
      status: 500,
      headers: {
        "Content-Type": "application/json"
      }
    });
  }
});
