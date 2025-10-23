// supabase/functions/screener/index.ts
import { serve } from "https://deno.land/std@0.200.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";

const FMP_KEY = Deno.env.get("FMP_KEY") ?? "";
const TWELVE_DATA_KEY = Deno.env.get("TWELVE_DATA_KEY") ?? "";
const FINNHUB_KEY = Deno.env.get("FINNHUB_KEY") ?? "";
const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const BATCH_SIZE = Number(Deno.env.get("BATCH_SIZE") || "100");
const FRESHNESS_MS = 5 * 60 * 1000; // 5 minutes
const THROTTLE_MS = 500;
const PRIMARY_API_URL = String(Deno.env.get("PRIMARY_API_URL") || "https://stock-api-x35p.vercel.app");

let providerFailures = { primary: 0, twelvedata: 0, finnhub: 0, fmp: 0 };
const CIRCUIT_BREAKER_THRESHOLD = 10;

// ----------------------------------------------------
// Helper Functions
// ----------------------------------------------------

function computeRelativeVolume(today, avg10) {
  if (today == null || avg10 == null || avg10 === 0) return null;
  return Number((today / avg10).toFixed(2));
}

function buildResponse(source: string, stocks: any[], meta: any = {}) {
  return {
    stats: {
      symbols_checked: meta.candidates ?? stocks.length ?? 0,
      symbols_with_data: stocks.length ?? 0,
      symbols_matched: stocks.length ?? 0,
      api_calls_used: meta.enriched_count ?? 0,
      duration_seconds: Number(((Date.now() - meta.startTime) / 1000).toFixed(2))
    },
    results: stocks,
    source
  };
}

// ----------------------------------------------------
// Fetch helpers
// ----------------------------------------------------

async function fetchBatchFromPrimaryAPI(symbols) {
  const results = new Map();
  if (providerFailures.primary >= CIRCUIT_BREAKER_THRESHOLD) return results;
  try {
    const url = `${PRIMARY_API_URL}/quote?symbols=${encodeURIComponent(symbols.join(','))}`;
    const res = await fetch(url, { headers: { 'Accept': 'application/json' }, signal: AbortSignal.timeout(15000) });
    if (!res.ok) {
      providerFailures.primary++;
      return results;
    }

    const data = await res.json();
    let quotes = Array.isArray(data) ? data : data?.data || data?.quotes || [];
    for (const quote of quotes) {
      if (!quote.symbol) continue;
      const price = quote.price ?? null;
      const prevClose = quote.previousClose ?? quote.previous_close ?? null;
      const change_percent = quote.change_percent ?? quote.changePercent ?? (price && prevClose ? Number(((price - prevClose) / prevClose * 100).toFixed(4)) : null);
      const volume = quote.volume ?? null;
      const avg10 = quote.averageVolume ?? quote.average_volume ?? null;
      const relative_volume = quote.relative_volume ?? computeRelativeVolume(volume, avg10);
      results.set(quote.symbol, {
        symbol: quote.symbol,
        name: quote.name ?? quote.longName ?? quote.shortName ?? null,
        price,
        change_percent,
        volume,
        market_cap: quote.market_cap ?? quote.marketCap ?? null,
        shares_float: quote.shares_float ?? quote.sharesOutstanding ?? null,
        relative_volume,
        raw: quote
      });
    }
  } catch (err) {
    providerFailures.primary++;
    console.error("Primary API error:", err);
  }
  return results;
}

async function fetchBatchFromTwelveData(symbols) {
  const results = new Map();
  if (!TWELVE_DATA_KEY || providerFailures.twelvedata >= CIRCUIT_BREAKER_THRESHOLD) return results;
  try {
    const url = `https://api.twelvedata.com/quote?symbol=${encodeURIComponent(symbols.join(','))}&apikey=${TWELVE_DATA_KEY}`;
    const res = await fetch(url, { signal: AbortSignal.timeout(10000) });
    if (!res.ok) {
      providerFailures.twelvedata++;
      return results;
    }
    const data = await res.json();
    const quotes = Array.isArray(data) ? data : [data];
    for (const quote of quotes) {
      if (!quote.symbol) continue;
      results.set(quote.symbol, {
        symbol: quote.symbol,
        name: quote.name ?? null,
        price: parseFloat(quote.close) || null,
        volume: parseFloat(quote.volume) || null,
        change_percent: parseFloat(quote.percent_change) || null,
        raw: quote
      });
    }
  } catch (err) {
    providerFailures.twelvedata++;
  }
  return results;
}

async function fetchFromFMPProfile(symbol) {
  if (!FMP_KEY || providerFailures.fmp >= CIRCUIT_BREAKER_THRESHOLD) return null;
  try {
    const res = await fetch(`https://financialmodelingprep.com/api/v3/profile/${symbol}?apikey=${FMP_KEY}`, { signal: AbortSignal.timeout(10000) });
    if (!res.ok) {
      providerFailures.fmp++;
      return null;
    }
    const data = await res.json();
    if (!Array.isArray(data) || data.length === 0) return null;
    providerFailures.fmp = 0;
    const p = data[0];
    return {
      symbol,
      name: p.companyName ?? null,
      price: p.price ?? null,
      market_cap: p.mktCap ?? p.marketCap ?? null,
      shares_float: p.sharesOutstanding ?? null,
      raw: p
    };
  } catch {
    providerFailures.fmp++;
    return null;
  }
}

// ----------------------------------------------------
// Cache / Merge Helpers
// ----------------------------------------------------

async function getCachedCandidates(filters, forceRealtime = false) {
  const cached = new Map();
  try {
    let query = supabase.from("stocks").select("*").not("price", "is", null);
    if (filters.price_min != null) query = query.gte("price", filters.price_min);
    if (filters.price_max != null) query = query.lte("price", filters.price_max);
    if (filters.volume_min != null) query = query.gte("volume", filters.volume_min);
    if (filters.market_cap_min != null) query = query.gte("market_cap", filters.market_cap_min);
    if (filters.market_cap_max != null) query = query.lte("market_cap", filters.market_cap_max);
    if (filters.float_max != null) query = query.lte("shares_float", filters.float_max);
    if (filters.symbols?.length) query = query.in("symbol", filters.symbols);

    const { data } = await query.limit(1000);
    const now = Date.now();
    for (const row of data || []) {
      const isStale = !row.updated_at || now - new Date(row.updated_at).getTime() > FRESHNESS_MS;
      if (forceRealtime && isStale) continue;
      cached.set(row.symbol, row);
    }
  } catch (err) {
    console.error("Cache error:", err);
  }
  return cached;
}

async function batchUpsert(stocks) {
  if (stocks.length === 0) return;
  const rows = stocks.map((s) => ({
    ...s,
    updated_at: new Date().toISOString()
  }));
  for (let i = 0; i < rows.length; i += 100) {
    await supabase.from("stocks").upsert(rows.slice(i, i + 100), { onConflict: "symbol" });
  }
}

function mergeData(primary, twelve, fmp, symbol) {
  const s = primary ?? twelve ?? fmp;
  if (!s) return null;
  return {
    symbol,
    name: primary?.name ?? twelve?.name ?? fmp?.name ?? null,
    price: primary?.price ?? twelve?.price ?? fmp?.price ?? null,
    change_percent: primary?.change_percent ?? twelve?.change_percent ?? null,
    volume: primary?.volume ?? twelve?.volume ?? fmp?.volume ?? null,
    market_cap: primary?.market_cap ?? fmp?.market_cap ?? null,
    shares_float: primary?.shares_float ?? fmp?.shares_float ?? null,
    relative_volume: primary?.relative_volume ?? null,
    raw: { primary: primary?.raw, twelve: twelve?.raw, fmp: fmp?.raw }
  };
}

async function enrichSymbols(symbols) {
  const results = [];
  for (let i = 0; i < symbols.length; i += BATCH_SIZE) {
    const batch = symbols.slice(i, i + BATCH_SIZE);
    const primary = await fetchBatchFromPrimaryAPI(batch);
    const failed = batch.filter((s) => !primary.has(s));
    const twelve = failed.length ? await fetchBatchFromTwelveData(failed) : new Map();
    const needsFMP = batch.filter((s) => {
      const p = primary.get(s);
      return p && (!p.market_cap || !p.shares_float);
    });
    const fmp = new Map();
    for (const s of needsFMP) {
      const data = await fetchFromFMPProfile(s);
      if (data) fmp.set(s, data);
    }
    for (const s of batch) {
      const merged = mergeData(primary.get(s), twelve.get(s), fmp.get(s), s);
      if (merged && merged.price) results.push(merged);
    }
    if (i + BATCH_SIZE < symbols.length) await new Promise((r) => setTimeout(r, THROTTLE_MS));
  }
  return results;
}

function applyFilters(stocks, filters) {
  return stocks.filter((q) => {
    if (filters.float_max && q.shares_float && q.shares_float > filters.float_max) return false;
    if (filters.change_min && q.change_percent && q.change_percent < filters.change_min) return false;
    if (filters.change_max && q.change_percent && q.change_percent > filters.change_max) return false;
    if (filters.relative_volume_min && q.relative_volume && q.relative_volume < filters.relative_volume_min) return false;
    if (filters.price_min && q.price && q.price < filters.price_min) return false;
    if (filters.price_max && q.price && q.price > filters.price_max) return false;
    if (filters.volume_min && q.volume && q.volume < filters.volume_min) return false;
    if (filters.market_cap_min && q.market_cap && q.market_cap < filters.market_cap_min) return false;
    if (filters.market_cap_max && q.market_cap && q.market_cap > filters.market_cap_max) return false;
    return true;
  });
}

function buildFmpScreenerQuery(filters, limit = 250) {
  const params = new URLSearchParams();
  params.set("limit", String(limit));
  params.set("exchange", "NASDAQ,NYSE");
  if (filters.price_min) params.set("priceMoreThan", String(filters.price_min));
  if (filters.price_max) params.set("priceLowerThan", String(filters.price_max));
  if (filters.market_cap_min) params.set("marketCapMoreThan", String(filters.market_cap_min));
  if (filters.market_cap_max) params.set("marketCapLowerThan", String(filters.market_cap_max));
  if (filters.volume_min) params.set("volumeMoreThan", String(filters.volume_min));
  return params.toString();
}

// ----------------------------------------------------
// Main Serve Logic
// ----------------------------------------------------

serve(async (req) => {
  const startTime = Date.now();

  if (req.method === "OPTIONS") {
    return new Response(null, {
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization"
      }
    });
  }

  try {
    if (req.method !== "POST") {
      return new Response(JSON.stringify({ error: "POST only" }), {
        status: 405,
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
      });
    }

    const body = await req.json();
    const filters = body.filters || body;
    const limit = Number(body.limit ?? 250);
    const forceRealtime = filters.realtime || false;

    // ------------------------------------------------
    // Specific symbols (realtime)
    // ------------------------------------------------
    if (filters.symbols?.length) {
      const symbols = filters.symbols.slice(0, 500);
      const cached = await getCachedCandidates(filters, forceRealtime);
      const needsUpdate = symbols.filter((s) => !cached.has(s));
      const enriched = (needsUpdate.length > 0 || forceRealtime) ? await enrichSymbols(forceRealtime ? symbols : needsUpdate) : [];
      if (enriched.length > 0) await batchUpsert(enriched);
      const allStocks = [...Array.from(cached.values()), ...enriched];
      const filtered = applyFilters(allStocks, filters);

      // ✅ Unified return
      return new Response(JSON.stringify(buildResponse(forceRealtime ? "realtime" : "hybrid", filtered.slice(0, limit), {
        startTime, candidates: symbols.length, enriched_count: enriched.length
      })), {
        status: 200,
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
      });

      // ❌ OLD:
      // return new Response(JSON.stringify({ source: "hybrid", count: filtered.length, stocks: filtered }), { status: 200 });
    }

    // ------------------------------------------------
    // Cached flow (if valid)
    // ------------------------------------------------
    const cached = await getCachedCandidates(filters, forceRealtime);
    const cachedFiltered = applyFilters(Array.from(cached.values()), filters);
    if (!forceRealtime && cachedFiltered.length >= limit) {
      return new Response(JSON.stringify(buildResponse("cache", cachedFiltered.slice(0, limit), { startTime })), {
        status: 200,
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
      });
    }

    // ------------------------------------------------
    // FMP Screener (hybrid)
    // ------------------------------------------------
    const fmpUrl = `https://financialmodelingprep.com/api/v3/stock-screener?${buildFmpScreenerQuery(filters, limit * 2)}&apikey=${encodeURIComponent(FMP_KEY)}`;
    const fmpRes = await fetch(fmpUrl);
    if (!fmpRes.ok) {
      return new Response(JSON.stringify(buildResponse("cache_fallback", cachedFiltered.slice(0, limit), { startTime })), {
        status: 200,
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
      });
    }

    const fmpData = await fmpRes.json();
    const candidates = Array.isArray(fmpData) ? fmpData.map((r) => r.symbol ?? r.ticker).filter(Boolean) : [];
    if (candidates.length === 0) {
      return new Response(JSON.stringify(buildResponse("fmp_empty", [], { startTime })), {
        status: 200,
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
      });
    }

    const uncached = candidates.filter((s) => !cached.has(s));
    const enriched = uncached.length ? await enrichSymbols(uncached) : [];
    if (enriched.length > 0) await batchUpsert(enriched);

    const allStocks = [...Array.from(cached.values()), ...enriched];
    const filtered = applyFilters(allStocks, filters);

    // ✅ Unified hybrid return
    return new Response(JSON.stringify(buildResponse("hybrid", filtered.slice(0, limit), {
      startTime, candidates: candidates.length, enriched_count: enriched.length
    })), {
      status: 200,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
    });

  } catch (err) {
    console.error("Screener error:", err);
    return new Response(JSON.stringify(buildResponse("error", [], { startTime })), {
      status: 500,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
    });
  }
});
