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
const FRESHNESS_MS = 5 * 60 * 1000; // 5 minutes for realtime screening
const THROTTLE_MS = 500;
const PRIMARY_API_URL = String(Deno.env.get("PRIMARY_API_URL") || "https://stock-api-x35p.vercel.app");
// Circuit breaker
let providerFailures = {
  primary: 0,
  twelvedata: 0,
  finnhub: 0,
  fmp: 0
};
const CIRCUIT_BREAKER_THRESHOLD = 10;
function computeRelativeVolume(today, avg10) {
  if (today == null || avg10 == null || avg10 === 0) return null;
  return Number((today / avg10).toFixed(2));
}
// ✅ FIXED: Batch fetch from Primary API (handles new format)
async function fetchBatchFromPrimaryAPI(symbols) {
  const results = new Map();
  if (providerFailures.primary >= CIRCUIT_BREAKER_THRESHOLD) {
    console.warn('Primary API circuit breaker open');
    return results;
  }
  try {
    const symbolList = symbols.join(',');
    const url = `${PRIMARY_API_URL}/quote?symbols=${encodeURIComponent(symbolList)}`;
    const res = await fetch(url, {
      headers: {
        'User-Agent': 'SupabaseScreener/1.0',
        'Accept': 'application/json'
      },
      signal: AbortSignal.timeout(15000)
    });
    if (!res.ok) {
      providerFailures.primary++;
      console.error(`Primary API error: ${res.status}`);
      return results;
    }
    const data = await res.json();
    // ✅ FIXED: Handle multiple response formats
    let quotes = [];
    if (Array.isArray(data)) {
      quotes = data;
    } else if (data?.data && Array.isArray(data.data)) {
      quotes = data.data; // New format: {count: 50, data: [...]}
    } else if (data?.quotes && Array.isArray(data.quotes)) {
      quotes = data.quotes; // Old format: {quotes: [...]}
    } else {
      console.warn('Primary API returned unexpected format');
      return results;
    }
    providerFailures.primary = 0;
    for (const quote of quotes){
      if (!quote.symbol) continue;
      const symbol = quote.symbol;
      const price = quote.price ?? null;
      const prevClose = quote.previousClose ?? quote.previous_close ?? null;
      const change_percent = quote.change_percent ?? quote.changePercent ?? (price != null && prevClose != null && prevClose !== 0 ? Number(((price - prevClose) / prevClose * 100).toFixed(4)) : null);
      const volume = quote.volume ?? null;
      const avg10 = quote.averageVolume ?? quote.average_volume ?? null;
      const relative_volume = quote.relative_volume ?? computeRelativeVolume(volume, avg10);
      results.set(symbol, {
        symbol,
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
    console.log(`Primary API: fetched ${results.size}/${symbols.length} symbols`);
  } catch (err) {
    providerFailures.primary++;
    console.error('Primary API batch error:', err);
  }
  return results;
}
// Fallback: Twelve Data API
async function fetchBatchFromTwelveData(symbols) {
  const results = new Map();
  if (!TWELVE_DATA_KEY || providerFailures.twelvedata >= CIRCUIT_BREAKER_THRESHOLD) {
    return results;
  }
  try {
    const symbolList = symbols.join(',');
    const url = `https://api.twelvedata.com/quote?symbol=${encodeURIComponent(symbolList)}&apikey=${TWELVE_DATA_KEY}`;
    const res = await fetch(url, {
      signal: AbortSignal.timeout(10000)
    });
    if (!res.ok) {
      providerFailures.twelvedata++;
      return results;
    }
    const data = await res.json();
    providerFailures.twelvedata = 0;
    const quotes = Array.isArray(data) ? data : [
      data
    ];
    for (const quote of quotes){
      if (!quote.symbol) continue;
      const price = parseFloat(quote.close) || null;
      const volume = parseFloat(quote.volume) || null;
      const change = parseFloat(quote.percent_change) || null;
      results.set(quote.symbol, {
        symbol: quote.symbol,
        name: quote.name ?? null,
        price,
        volume,
        change_percent: change,
        raw: quote
      });
    }
    console.log(`Twelve Data: fetched ${results.size}/${symbols.length} symbols`);
  } catch (err) {
    providerFailures.twelvedata++;
    console.error('Twelve Data error:', err);
  }
  return results;
}
// Fetch from FMP Profile (for fundamentals)
async function fetchFromFMPProfile(symbol) {
  if (!FMP_KEY || providerFailures.fmp >= CIRCUIT_BREAKER_THRESHOLD) return null;
  try {
    const url = `https://financialmodelingprep.com/api/v3/profile/${encodeURIComponent(symbol)}?apikey=${encodeURIComponent(FMP_KEY)}`;
    const res = await fetch(url, {
      signal: AbortSignal.timeout(10000)
    });
    if (!res.ok) {
      providerFailures.fmp++;
      return null;
    }
    const data = await res.json();
    if (!Array.isArray(data) || data.length === 0) return null;
    providerFailures.fmp = 0;
    const profile = data[0];
    return {
      symbol,
      name: profile.companyName ?? null,
      price: profile.price ?? null,
      market_cap: profile.mktCap ?? profile.marketCap ?? null,
      shares_float: profile.sharesOutstanding ?? null,
      raw: profile
    };
  } catch  {
    providerFailures.fmp++;
    return null;
  }
}
// ✅ IMPROVED: Get cached stocks with freshness check
async function getCachedCandidates(filters, forceRealtime = false) {
  const cached = new Map();
  try {
    let query = supabase.from("stocks").select("*").not("price", "is", null);
    // Apply filters that can be done in DB
    if (filters.price_min != null) query = query.gte("price", filters.price_min);
    if (filters.price_max != null) query = query.lte("price", filters.price_max);
    if (filters.volume_min != null) query = query.gte("volume", filters.volume_min);
    if (filters.market_cap_min != null) query = query.gte("market_cap", filters.market_cap_min);
    if (filters.market_cap_max != null) query = query.lte("market_cap", filters.market_cap_max);
    if (filters.float_max != null) query = query.lte("shares_float", filters.float_max);
    // If specific symbols requested
    console.log(`requested filters ${filters.price_min} is min price, ${filters.price_max} is max price, ${filters.volume_min} is min volume`); //added line
    console.log(query.toString()); //added line
    if (filters.symbols && filters.symbols.length > 0) {
      query = query.in("symbol", filters.symbols);
    }
    const { data } = await query.limit(1000);
    if (!data) return cached;
    const now = Date.now();
    for (const row of data){
      // If realtime mode, only use very fresh data (<5 min)
      const isStale = !row.updated_at || now - new Date(row.updated_at).getTime() > FRESHNESS_MS;
      if (forceRealtime && isStale) continue; // Skip stale data in realtime mode
      cached.set(row.symbol, {
        symbol: row.symbol,
        name: row.name ?? null,
        price: row.price != null ? Number(row.price) : null,
        change_percent: row.change_percent != null ? Number(row.change_percent) : null,
        volume: row.volume != null ? Number(row.volume) : null,
        market_cap: row.market_cap != null ? Number(row.market_cap) : null,
        shares_float: row.shares_float != null ? Number(row.shares_float) : null,
        relative_volume: row.relative_volume != null ? Number(row.relative_volume) : null,
        raw: row.raw ?? null
      });
    }
  } catch (err) {
    console.error('Cache lookup error:', err);
  }
  return cached;
}
// Batch upsert
async function batchUpsert(stocks) {
  if (stocks.length === 0) return;
  const rows = stocks.map((s)=>({
      symbol: s.symbol,
      name: s.name,
      price: s.price,
      change_percent: s.change_percent,
      volume: s.volume,
      market_cap: s.market_cap,
      shares_float: s.shares_float,
      relative_volume: s.relative_volume,
      raw: s.raw ?? null,
      updated_at: new Date().toISOString()
    }));
  for(let i = 0; i < rows.length; i += 100){
    const chunk = rows.slice(i, i + 100);
    await supabase.from("stocks").upsert(chunk, {
      onConflict: "symbol"
    });
  }
}
// Smart merge
function mergeData(primary, twelve, fmp, symbol) {
  const source = primary ?? twelve ?? fmp;
  if (!source) return null;
  return {
    symbol,
    name: primary?.name ?? twelve?.name ?? fmp?.name ?? null,
    price: primary?.price ?? twelve?.price ?? fmp?.price ?? null,
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
// Process batches with cascading fallbacks
async function enrichSymbols(symbols) {
  const results = [];
  for(let i = 0; i < symbols.length; i += BATCH_SIZE){
    const batch = symbols.slice(i, i + BATCH_SIZE);
    const primaryData = await fetchBatchFromPrimaryAPI(batch);
    const failedSymbols = batch.filter((s)=>!primaryData.has(s));
    let twelveData = new Map();
    if (failedSymbols.length > 0) {
      twelveData = await fetchBatchFromTwelveData(failedSymbols);
    }
    const needsFMP = batch.filter((s)=>{
      const p = primaryData.get(s);
      return p && (!p.market_cap || !p.shares_float);
    });
    const fmpData = new Map();
    for (const symbol of needsFMP){
      const fmp = await fetchFromFMPProfile(symbol);
      if (fmp) fmpData.set(symbol, fmp);
    }
    for (const symbol of batch){
      const merged = mergeData(primaryData.get(symbol), twelveData.get(symbol), fmpData.get(symbol), symbol);
      if (merged && merged.price) {
        results.push(merged);
      }
    }
    if (i + BATCH_SIZE < symbols.length) {
      await new Promise((resolve)=>setTimeout(resolve, THROTTLE_MS));
    }
  }
  return results;
}
// Apply post-enrichment filters
function applyFilters(stocks, filters) {
  return stocks.filter((q)=>{
    if (filters.float_max != null && q.shares_float != null && q.shares_float > filters.float_max) return false;
    if (filters.change_min != null && q.change_percent != null && q.change_percent < filters.change_min) return false;
    if (filters.change_max != null && q.change_percent != null && q.change_percent > filters.change_max) return false;
    if (filters.relative_volume_min != null && q.relative_volume != null && q.relative_volume < filters.relative_volume_min) return false;
    if (filters.price_min != null && q.price != null && q.price < filters.price_min) return false;
    if (filters.price_max != null && q.price != null && q.price > filters.price_max) return false;
    if (filters.volume_min != null && q.volume != null && q.volume < filters.volume_min) return false;
    if (filters.market_cap_min != null && q.market_cap != null && q.market_cap < filters.market_cap_min) return false;
    if (filters.market_cap_max != null && q.market_cap != null && q.market_cap > filters.market_cap_max) return false;
    return true;
  });
}
// Build FMP screener query
function buildFmpScreenerQuery(filters, limit = 250) {
  const params = new URLSearchParams();
  params.set("limit", String(limit));
  params.set("exchange", "NASDAQ,NYSE");
  if (filters.price_min != null) params.set("priceMoreThan", String(filters.price_min));
  if (filters.price_max != null) params.set("priceLowerThan", String(filters.price_max));
  if (filters.market_cap_min != null) params.set("marketCapMoreThan", String(filters.market_cap_min));
  if (filters.market_cap_max != null) params.set("marketCapLowerThan", String(filters.market_cap_max));
  if (filters.volume_min != null) params.set("volumeMoreThan", String(filters.volume_min));
  return params.toString();
}
serve(async (req)=>{
  const startTime = Date.now();
  // CORS
  if (req.method === 'OPTIONS') {
    return new Response(null, {
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization'
      }
    });
  }
  try {
    if (req.method !== "POST") {
      return new Response(JSON.stringify({
        error: "POST only"
      }), {
        status: 405,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*"
        }
      });
    }
    const body = await req.json();
    const filters = body.filters || body; // Support both formats
    const limit = Number(body.limit ?? 250);
    const forceRealtime = filters.realtime || false;
    console.log('Screener request:', {
      filters,
      limit,
      realtime: forceRealtime
    });
    // ✅ NEW: Handle specific symbols request (real-time quotes)
    if (filters.symbols && filters.symbols.length > 0) {
      const symbols = filters.symbols.slice(0, 500);
      const cached = await getCachedCandidates(filters, forceRealtime);
      const needsUpdate = symbols.filter((s)=>!cached.has(s));
      let enriched = [];
      if (needsUpdate.length > 0 || forceRealtime) {
        const toFetch = forceRealtime ? symbols : needsUpdate;
        enriched = await enrichSymbols(toFetch);
        if (enriched.length > 0) {
          await batchUpsert(enriched);
        }
      }
      const allStocks = [
        ...Array.from(cached.values()),
        ...enriched
      ];
      const filtered = applyFilters(allStocks, filters);
      return new Response(JSON.stringify({
        source: forceRealtime ? "realtime" : "hybrid",
        count: filtered.length,
        stocks: filtered.slice(0, limit),
        duration_ms: Date.now() - startTime
      }), {
        status: 200,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*"
        }
      });
    }
    // Original FMP screener flow
    const cached = await getCachedCandidates(filters, forceRealtime);
    const cachedFiltered = applyFilters(Array.from(cached.values()), filters);
    console.log(`Cache: ${cached.size} found, ${cachedFiltered.length} after filters`);
    if (!forceRealtime && cachedFiltered.length >= limit) {
      return new Response(JSON.stringify({
        source: "cache",
        count: cachedFiltered.length,
        stocks: cachedFiltered.slice(0, limit),
        duration_ms: Date.now() - startTime
      }), {
        status: 200,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*"
        }
      });
    }
    const fmpQS = buildFmpScreenerQuery(filters, limit * 2);
    const fmpUrl = `https://financialmodelingprep.com/api/v3/stock-screener?${fmpQS}&apikey=${encodeURIComponent(FMP_KEY)}`;
    console.log(`fmp URL: ${fmpUrl}`);
    const fmpRes = await fetch(fmpUrl);
    if (!fmpRes.ok) {
      if (cachedFiltered.length > 0) {
        return new Response(JSON.stringify({
          source: "cache_fallback",
          count: cachedFiltered.length,
          stocks: cachedFiltered.slice(0, limit),
          warning: "FMP screener unavailable, using cached results",
          duration_ms: Date.now() - startTime
        }), {
          status: 200,
          headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
          }
        });
      }
      return new Response(JSON.stringify({
        error: "FMP screener failed",
        status: fmpRes.status
      }), {
        status: 502,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*"
        }
      });
    }
    const fmpData = await fmpRes.json();
    const candidates = Array.isArray(fmpData) ? fmpData.map((r)=>r.symbol ?? r.ticker).filter(Boolean) : [];
    if (candidates.length === 0) {
      return new Response(JSON.stringify({
        source: "fmp_empty",
        count: 0,
        stocks: [],
        duration_ms: Date.now() - startTime
      }), {
        status: 200,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*"
        }
      });
    }
    const uncached = candidates.filter((s)=>!cached.has(s));
    const enriched = uncached.length > 0 ? await enrichSymbols(uncached) : [];
    const allStocks = [
      ...Array.from(cached.values()),
      ...enriched
    ];
    const filtered = applyFilters(allStocks, filters);
    if (enriched.length > 0) {
      await batchUpsert(enriched);
    }
    return new Response(JSON.stringify({
      source: "hybrid",
      count: filtered.length,
      candidates: candidates.length,
      enriched_count: enriched.length,
      stocks: filtered.slice(0, limit),
      duration_ms: Date.now() - startTime
    }), {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*"
      }
    });
  } catch (err) {
    console.error('Screener error:', err);
    return new Response(JSON.stringify({
      error: String(err),
      duration_ms: Date.now() - startTime
    }), {
      status: 500,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*"
      }
    });
  }
});
