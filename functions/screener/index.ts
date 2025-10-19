// supabase/functions/screener/index.ts
import { serve } from "https://deno.land/std@0.200.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";

type Filters = {
  price_min?: number;
  price_max?: number;
  float_max?: number;
  market_cap_max?: number;
  change_min?: number;
  change_max?: number;
  relative_volume_min?: number;
  volume_min?: number;
};

type QuoteResult = {
  symbol: string;
  name: string | null;
  price: number | null;
  change_percent: number | null;
  volume: number | null;
  market_cap: number | null;
  shares_float: number | null;
  relative_volume: number | null;
  raw?: any;
};

const FMP_KEY = Deno.env.get("FMP_KEY") ?? "";
const TWELVE_DATA_KEY = Deno.env.get("TWELVE_DATA_KEY") ?? "";
const FINNHUB_KEY = Deno.env.get("FINNHUB_KEY") ?? "";
const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const BATCH_SIZE = Number(Deno.env.get("BATCH_SIZE") || "100");
const FRESHNESS_MS = 1000 * 60 * 60; // 1 hour for screener
const THROTTLE_MS = 500;

// Primary API endpoint
//const PRIMARY_API_URL = "https://stock-api-x35p.vercel.app";
const PRIMARY_API_URL = String(Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "https://stock-api-x35p.vercel.app");


// Circuit breaker
let providerFailures = { 
  primary: 0,
  twelvedata: 0,
  finnhub: 0,
  fmp: 0 
};
const CIRCUIT_BREAKER_THRESHOLD = 10;

function computeRelativeVolume(today: number | null, avg10: number | null): number | null {
  if (today == null || avg10 == null || avg10 === 0) return null;
  return Number((today / avg10).toFixed(2));
}

// Batch fetch from Primary API (Vercel - Yahoo internally)
async function fetchBatchFromPrimaryAPI(symbols: string[]): Promise<Map<string, Partial<QuoteResult>>> {
  const results = new Map<string, Partial<QuoteResult>>();
  
  if (providerFailures.primary >= CIRCUIT_BREAKER_THRESHOLD) {
    console.warn('Primary API circuit breaker open');
    return results;
  }
  
  try {
    const symbolList = symbols.join(',');
    const url = `${PRIMARY_API_URL}/quote?symbols=${encodeURIComponent(symbolList)}`;
    
    const res = await fetch(url, {
      headers: { 
        'User-Agent': 'SupabaseFunction/1.0',
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
    
    if (!data?.quotes || !Array.isArray(data.quotes)) {
      console.warn('Primary API returned invalid format');
      return results;
    }
    
    providerFailures.primary = 0;
    
    for (const quote of data.quotes) {
      if (!quote.symbol) continue;
      
      const symbol = quote.symbol;
      const price = quote.price ?? null;
      const prevClose = quote.previousClose ?? null;
      const change_percent = quote.changePercent ?? 
        ((price != null && prevClose != null && prevClose !== 0)
          ? Number((((price - prevClose) / prevClose) * 100).toFixed(4))
          : null);
      const volume = quote.volume ?? null;
      const avg10 = quote.averageVolume ?? null;
      const relative_volume = computeRelativeVolume(volume, avg10);
      
      results.set(symbol, {
        symbol,
        name: quote.name ?? quote.longName ?? quote.shortName ?? null,
        price,
        change_percent,
        volume,
        market_cap: quote.marketCap ?? null,
        shares_float: quote.sharesOutstanding ?? null,
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
async function fetchBatchFromTwelveData(symbols: string[]): Promise<Map<string, Partial<QuoteResult>>> {
  const results = new Map<string, Partial<QuoteResult>>();
  
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
    
    const quotes = Array.isArray(data) ? data : [data];
    
    for (const quote of quotes) {
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

// Fallback: Finnhub API (for individual symbols)
async function fetchFromFinnhub(symbol: string): Promise<Partial<QuoteResult> | null> {
  if (!FINNHUB_KEY || providerFailures.finnhub >= CIRCUIT_BREAKER_THRESHOLD) return null;
  
  try {
    const url = `https://finnhub.io/api/v1/quote?symbol=${encodeURIComponent(symbol)}&token=${FINNHUB_KEY}`;
    const res = await fetch(url, {
      signal: AbortSignal.timeout(5000)
    });
    
    if (!res.ok) {
      providerFailures.finnhub++;
      return null;
    }
    
    const quote = await res.json();
    providerFailures.finnhub = 0;
    
    if (!quote.c) return null;
    
    const price = quote.c;
    const change_percent = quote.dp ?? null;
    
    return {
      symbol,
      name: null,
      price,
      change_percent,
      raw: quote
    };
  } catch (err) {
    providerFailures.finnhub++;
    return null;
  }
}

// Fetch from FMP Profile (only if needed for fundamentals)
async function fetchFromFMPProfile(symbol: string): Promise<Partial<QuoteResult> | null> {
  if (!FMP_KEY || providerFailures.fmp >= CIRCUIT_BREAKER_THRESHOLD) return null;
  
  try {
    const url = `https://financialmodelingprep.com/api/v3/profile/${encodeURIComponent(symbol)}?apikey=${encodeURIComponent(FMP_KEY)}`;
    const res = await fetch(url, { signal: AbortSignal.timeout(10000) });
    
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
  } catch {
    providerFailures.fmp++;
    return null;
  }
}

// Get cached stocks matching basic filters
async function getCachedCandidates(filters: Filters): Promise<Map<string, QuoteResult>> {
  const cached = new Map<string, QuoteResult>();
  
  try {
    let query = supabase
      .from("stocks")
      .select("*")
      .not("price", "is", null);
    
    // Apply filters that can be done in DB
    if (filters.price_min != null) query = query.gte("price", filters.price_min);
    if (filters.price_max != null) query = query.lte("price", filters.price_max);
    if (filters.volume_min != null) query = query.gte("volume", filters.volume_min);
    if (filters.market_cap_max != null) query = query.lte("market_cap", filters.market_cap_max);
    if (filters.float_max != null) query = query.lte("shares_float", filters.float_max);
    
    // Get more than needed since we'll filter by freshness
    const { data } = await query.limit(1000);
    
    if (!data) return cached;
    
    const now = Date.now();
    for (const row of data) {
      const isStale = !row.updated_at || (now - new Date(row.updated_at).getTime()) > FRESHNESS_MS;
      if (isStale) continue;
      
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
async function batchUpsert(stocks: QuoteResult[]): Promise<void> {
  if (stocks.length === 0) return;
  
  const rows = stocks.map(s => ({
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
  
  for (let i = 0; i < rows.length; i += 100) {
    const chunk = rows.slice(i, i + 100);
    await supabase.from("stocks").upsert(chunk, { onConflict: "symbol" });
  }
}

// Smart merge with priority: Primary > TwelveData > Finnhub > FMP
function mergeData(
  primary: Partial<QuoteResult> | undefined,
  twelve: Partial<QuoteResult> | undefined,
  finnhub: Partial<QuoteResult> | undefined,
  fmp: Partial<QuoteResult> | undefined,
  symbol: string
): QuoteResult | null {
  const source = primary ?? twelve ?? finnhub ?? fmp;
  if (!source) return null;
  
  return {
    symbol,
    name: primary?.name ?? twelve?.name ?? fmp?.name ?? null,
    price: primary?.price ?? twelve?.price ?? finnhub?.price ?? fmp?.price ?? null,
    change_percent: primary?.change_percent ?? twelve?.change_percent ?? finnhub?.change_percent ?? null,
    volume: primary?.volume ?? twelve?.volume ?? null,
    market_cap: primary?.market_cap ?? fmp?.market_cap ?? null,
    shares_float: primary?.shares_float ?? fmp?.shares_float ?? null,
    relative_volume: primary?.relative_volume ?? null,
    raw: { 
      primary: primary?.raw, 
      twelve: twelve?.raw,
      finnhub: finnhub?.raw,
      fmp: fmp?.raw 
    }
  };
}

// Process batches with cascading fallbacks
async function enrichSymbols(symbols: string[]): Promise<QuoteResult[]> {
  const results: QuoteResult[] = [];
  const stats = {
    total: symbols.length,
    primary_success: 0,
    twelve_success: 0,
    finnhub_success: 0,
    fmp_success: 0,
    failed: 0
  };
  
  for (let i = 0; i < symbols.length; i += BATCH_SIZE) {
    const batch = symbols.slice(i, i + BATCH_SIZE);
    
    console.log(`Enriching batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(symbols.length / BATCH_SIZE)}`);
    
    // Step 1: Try Primary API
    const primaryData = await fetchBatchFromPrimaryAPI(batch);
    
    // Step 2: Identify failed symbols
    const failedSymbols = batch.filter(s => !primaryData.has(s));
    
    // Step 3: Try Twelve Data for failed symbols
    let twelveData = new Map();
    if (failedSymbols.length > 0) {
      console.log(`Retrying ${failedSymbols.length} symbols with Twelve Data`);
      twelveData = await fetchBatchFromTwelveData(failedSymbols);
    }
    
    // Step 4: Try Finnhub for still-failed symbols (limited to avoid rate limits)
    const stillFailed = failedSymbols.filter(s => !twelveData.has(s));
    const finnhubData = new Map();
    if (stillFailed.length > 0 && stillFailed.length <= 10) {
      console.log(`Retrying ${stillFailed.length} symbols with Finnhub`);
      for (const symbol of stillFailed) {
        const finnhub = await fetchFromFinnhub(symbol);
        if (finnhub) finnhubData.set(symbol, finnhub);
      }
    }
    
    // Step 5: Determine which symbols need FMP for fundamentals
    const needsFMP: string[] = [];
    for (const symbol of batch) {
      const primary = primaryData.get(symbol);
      const twelve = twelveData.get(symbol);
      if ((primary || twelve) && (!primary?.market_cap || !primary?.shares_float)) {
        needsFMP.push(symbol);
      }
    }
    
    // Step 6: Fetch FMP data for fundamentals
    const fmpData = new Map();
    for (const symbol of needsFMP) {
      const fmp = await fetchFromFMPProfile(symbol);
      if (fmp) fmpData.set(symbol, fmp);
    }
    
    // Step 7: Merge all sources
    for (const symbol of batch) {
      const primary = primaryData.get(symbol);
      const twelve = twelveData.get(symbol);
      const finnhub = finnhubData.get(symbol);
      const fmp = fmpData.get(symbol);
      
      const merged = mergeData(primary, twelve, finnhub, fmp, symbol);
      
      if (merged && merged.price) {
        results.push(merged);
        if (primary) stats.primary_success++;
        else if (twelve) stats.twelve_success++;
        else if (finnhub) stats.finnhub_success++;
        else if (fmp) stats.fmp_success++;
      } else {
        stats.failed++;
      }
    }
    
    // Throttle between batches
    if (i + BATCH_SIZE < symbols.length) {
      await new Promise(resolve => setTimeout(resolve, THROTTLE_MS));
    }
  }
  
  console.log('Enrichment stats:', stats);
  return results;
}

// Apply post-enrichment filters
function applyFilters(stocks: QuoteResult[], filters: Filters): QuoteResult[] {
  return stocks.filter(q => {
    if (filters.float_max != null && q.shares_float != null && q.shares_float > filters.float_max) return false;
    if (filters.change_min != null && q.change_percent != null && q.change_percent < filters.change_min) return false;
    if (filters.change_max != null && q.change_percent != null && q.change_percent > filters.change_max) return false;
    if (filters.relative_volume_min != null && q.relative_volume != null && q.relative_volume < filters.relative_volume_min) return false;
    if (filters.price_min != null && q.price != null && q.price < filters.price_min) return false;
    if (filters.price_max != null && q.price != null && q.price > filters.price_max) return false;
    if (filters.volume_min != null && q.volume != null && q.volume < filters.volume_min) return false;
    if (filters.market_cap_max != null && q.market_cap != null && q.market_cap > filters.market_cap_max) return false;
    return true;
  });
}

// Build FMP screener query
function buildFmpScreenerQuery(filters: Filters, limit = 250): string {
  const params = new URLSearchParams();
  params.set("limit", String(limit));
  params.set("exchange", "NASDAQ,NYSE");
  
  if (filters.price_min != null) params.set("priceMoreThan", String(filters.price_min));
  if (filters.price_max != null) params.set("priceLowerThan", String(filters.price_max));
  if (filters.market_cap_max != null) params.set("marketCapLowerThan", String(filters.market_cap_max));
  if (filters.volume_min != null) params.set("volumeMoreThan", String(filters.volume_min));
  
  return params.toString();
}

serve(async (req) => {
  const startTime = Date.now();
  
  try {
    if (req.method !== "POST") {
      return new Response(
        JSON.stringify({ error: "POST only" }),
        { status: 405, headers: { "Content-Type": "application/json" } }
      );
    }
    
    const body = await req.json();
    const filters: Filters = body.filters || {};
    const limit = Number(body.limit ?? 250);
    
    console.log('Screener request:', { filters, limit });
    
    // Step 1: Try to get results from cache first
    const cached = await getCachedCandidates(filters);
    const cachedFiltered = applyFilters(Array.from(cached.values()), filters);
    
    console.log(`Cache: ${cached.size} found, ${cachedFiltered.length} after filters`);
    
    // If cache has enough results, use it
    if (cachedFiltered.length >= limit) {
      const results = cachedFiltered.slice(0, limit);
      return new Response(
        JSON.stringify({
          source: "cache",
          count: results.length,
          cache_hit: true,
          stocks: results,
          duration_ms: Date.now() - startTime
        }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }
    
    // Step 2: Need to fetch more - use FMP screener
    const fmpQS = buildFmpScreenerQuery(filters, limit * 2);
    const fmpUrl = `https://financialmodelingprep.com/api/v3/stock-screener?${fmpQS}&apikey=${encodeURIComponent(FMP_KEY)}`;
    const fmpRes = await fetch(fmpUrl);
    
    if (!fmpRes.ok) {
      // If FMP fails and we have some cached results, return those
      if (cachedFiltered.length > 0) {
        return new Response(
          JSON.stringify({
            source: "cache_fallback",
            count: cachedFiltered.length,
            stocks: cachedFiltered.slice(0, limit),
            warning: "FMP screener unavailable, using cached results",
            duration_ms: Date.now() - startTime
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }
      
      return new Response(
        JSON.stringify({ 
          error: "FMP screener failed and no cached results available",
          status: fmpRes.status 
        }),
        { status: 502, headers: { "Content-Type": "application/json" } }
      );
    }
    
    const fmpData = await fmpRes.json();
    const candidates: string[] = Array.isArray(fmpData) 
      ? fmpData.map((r: any) => r.symbol ?? r.ticker).filter(Boolean)
      : [];
    
    if (candidates.length === 0) {
      return new Response(
        JSON.stringify({ 
          source: "fmp_empty",
          count: 0,
          stocks: [],
          duration_ms: Date.now() - startTime
        }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }
    
    console.log(`FMP returned ${candidates.length} candidates`);
    
    // Step 3: Check which candidates are already in cache
    const uncached = candidates.filter(s => !cached.has(s));
    console.log(`Need to enrich: ${uncached.length} symbols`);
    
    // Step 4: Enrich uncached symbols with cascading fallbacks
    const enriched = uncached.length > 0 ? await enrichSymbols(uncached) : [];
    
    // Step 5: Combine cached + enriched
    const allStocks = [
      ...Array.from(cached.values()),
      ...enriched
    ];
    
    // Step 6: Apply all filters
    const filtered = applyFilters(allStocks, filters);
    
    // Step 7: Upsert new data to cache
    if (enriched.length > 0) {
      await batchUpsert(enriched);
      console.log(`Cached ${enriched.length} new stocks`);
    }
    
    const results = filtered.slice(0, limit);
    
    return new Response(
      JSON.stringify({
        source: "hybrid",
        count: results.length,
        candidates: candidates.length,
        cache_hit_count: cached.size,
        enriched_count: enriched.length,
        stocks: results,
        provider_status: {
          primary_failures: providerFailures.primary,
          twelvedata_failures: providerFailures.twelvedata,
          finnhub_failures: providerFailures.finnhub,
          fmp_failures: providerFailures.fmp
        },
        duration_ms: Date.now() - startTime,
        duration_readable: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
    
  } catch (err) {
    console.error('Screener error:', err);
    return new Response(
      JSON.stringify({ 
        error: String(err),
        duration_ms: Date.now() - startTime
      }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
});