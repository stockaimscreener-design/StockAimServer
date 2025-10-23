// supabase/functions/update-stocks/index.ts
import { serve } from "https://deno.land/std@0.200.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";

type QuoteResult = {
  symbol: string;
  name: string | null;
  price: number | null;
  open?: number | null;
  high?: number | null;
  low?: number | null;
  close?: number | null;
  volume: number | null;
  change_percent: number | null;
  market_cap: number | null;
  shares_float: number | null;
  relative_volume: number | null;
  raw?: any;
};

type UpdateMode = 'full' | 'delta' | 'manual';

const FMP_KEY = Deno.env.get("FMP_KEY") ?? "";
const TWELVE_DATA_KEY = Deno.env.get("TWELVE_DATA_KEY") ?? "";
const FINNHUB_KEY = Deno.env.get("FINNHUB_KEY") ?? "";
const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// Configuration
const BATCH_SIZE = Number(Deno.env.get("BATCH_SIZE") || "100");
const CONCURRENCY = Number(Deno.env.get("CONCURRENCY") || "5");
const TOP_N_DELTA = Number(Deno.env.get("TOP_N_DELTA") || "500");
const FRESHNESS_MS = Number(Deno.env.get("FRESHNESS_MS") || "300000"); // 5 minutes
const THROTTLE_MS = 500; // delay between batches

const PRIMARY_API_URL = String(Deno.env.get("PRIMARY_API_URL") || "https://stock-api-x35p.vercel.app");

// Primary API endpoint
//const PRIMARY_API_URL = ;

// Circuit breaker state
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
    console.warn('Primary API circuit breaker open, skipping');
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
      signal: AbortSignal.timeout(15000) // 15s timeout for batch
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
    
    // Reset failures on success
    providerFailures.primary = 0;
    
    for (const quote of data.quotes) {
      if (!quote.symbol) continue;
      
      const symbol = quote.symbol;
      const price = quote.price ?? null;
      const open = quote.open ?? null;
      const high = quote.high ?? null;
      const low = quote.low ?? null;
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
        open,
        high,
        low,
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
    console.error('Primary API batch fetch error:', err);
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
    
    // Handle both single and multiple symbol responses
    const quotes = Array.isArray(data) ? data : [data];
    
    for (const quote of quotes) {
      if (!quote.symbol) continue;
      
      const price = parseFloat(quote.close) || null;
      const open = parseFloat(quote.open) || null;
      const high = parseFloat(quote.high) || null;
      const low = parseFloat(quote.low) || null;
      const volume = parseFloat(quote.volume) || null;
      const change = parseFloat(quote.percent_change) || null;
      
      results.set(quote.symbol, {
        symbol: quote.symbol,
        name: quote.name ?? null,
        price,
        open,
        high,
        low,
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

// Fallback: Finnhub API
async function fetchBatchFromFinnhub(symbols: string[]): Promise<Map<string, Partial<QuoteResult>>> {
  const results = new Map<string, Partial<QuoteResult>>();
  
  if (!FINNHUB_KEY || providerFailures.finnhub >= CIRCUIT_BREAKER_THRESHOLD) {
    return results;
  }
  
  try {
    // Finnhub doesn't support batch requests, so we need to fetch individually
    // Only fetch up to 10 symbols to avoid rate limits
    const limitedSymbols = symbols.slice(0, 10);
    
    await Promise.all(limitedSymbols.map(async (symbol) => {
      try {
        const url = `https://finnhub.io/api/v1/quote?symbol=${encodeURIComponent(symbol)}&token=${FINNHUB_KEY}`;
        const res = await fetch(url, {
          signal: AbortSignal.timeout(5000)
        });
        
        if (!res.ok) return;
        
        const quote = await res.json();
        
        if (quote.c) { // current price
          const price = quote.c;
          const open = quote.o ?? null;
          const high = quote.h ?? null;
          const low = quote.l ?? null;
          const prevClose = quote.pc ?? null;
          const change_percent = quote.dp ?? null;
          
          results.set(symbol, {
            symbol,
            name: null,
            price,
            open,
            high,
            low,
            change_percent,
            raw: quote
          });
        }
      } catch (err) {
        console.error(`Finnhub error for ${symbol}:`, err);
      }
    }));
    
    providerFailures.finnhub = 0;
    console.log(`Finnhub: fetched ${results.size}/${limitedSymbols.length} symbols`);
  } catch (err) {
    providerFailures.finnhub++;
    console.error('Finnhub error:', err);
  }
  
  return results;
}

// Fallback: FMP for fundamentals
async function fetchFundamentalsFromFMP(symbols: string[]): Promise<Map<string, Partial<QuoteResult>>> {
  const results = new Map<string, Partial<QuoteResult>>();
  if (!FMP_KEY || symbols.length === 0 || providerFailures.fmp >= CIRCUIT_BREAKER_THRESHOLD) return results;
  
  try {
    const symbolList = symbols.join(',');
    const url = `https://financialmodelingprep.com/api/v3/profile/${encodeURIComponent(symbolList)}?apikey=${encodeURIComponent(FMP_KEY)}`;
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
    
    for (const profile of data) {
      results.set(profile.symbol, {
        symbol: profile.symbol,
        name: profile.companyName ?? null,
        price: profile.price ?? null,
        market_cap: profile.mktCap ?? profile.marketCap ?? null,
        shares_float: profile.sharesOutstanding ?? null,
        raw: profile
      });
    }
    
    console.log(`FMP: fetched ${results.size}/${symbols.length} symbols`);
  } catch (err) {
    providerFailures.fmp++;
    console.error('FMP batch fetch error:', err);
  }
  
  return results;
}

// Get symbols for delta mode
async function getDeltaSymbols(): Promise<string[]> {
  const symbols = new Set<string>();
  
  try {
    // 1. Top by volume
    const { data: topVol } = await supabase
      .from("stocks")
      .select("symbol")
      .not("volume", "is", null)
      .order("volume", { ascending: false })
      .limit(Math.floor(TOP_N_DELTA * 0.5));
    
    if (topVol) topVol.forEach(r => symbols.add(r.symbol));
    
    // 2. Most volatile (absolute change)
    const { data: topVolatile } = await supabase
      .from("stocks")
      .select("symbol, change_percent")
      .not("change_percent", "is", null)
      .order("change_percent", { ascending: false })
      .limit(Math.floor(TOP_N_DELTA * 0.3));
    
    if (topVolatile) topVolatile.forEach(r => symbols.add(r.symbol));
    
    // Also get negative movers
    const { data: topNegative } = await supabase
      .from("stocks")
      .select("symbol, change_percent")
      .not("change_percent", "is", null)
      .order("change_percent", { ascending: true })
      .limit(Math.floor(TOP_N_DELTA * 0.2));
    
    if (topNegative) topNegative.forEach(r => symbols.add(r.symbol));
    
    // 3. New symbols not yet in stocks table
    const { data: newSymbols } = await supabase
      .rpc('get_new_symbols', { limit_count: 100 })
      .catch(() => ({ data: null }));
    
    if (newSymbols) {
      newSymbols.forEach((r: any) => symbols.add(r.symbol));
    } else {
      // Fallback: manual query if RPC doesn't exist
      const { data: allTickers } = await supabase
        .from("stock_tickers")
        .select('"Symbol"')
        .limit(100);
      
      const { data: existingStocks } = await supabase
        .from("stocks")
        .select("symbol");
      
      const existingSet = new Set(existingStocks?.map(s => s.symbol) || []);
      allTickers?.forEach((t: any) => {
        if (!existingSet.has(t.Symbol)) {
          symbols.add(t.Symbol);
        }
      });
    }
    
  } catch (err) {
    console.error('Error getting delta symbols:', err);
  }
  
  // Limit to TOP_N_DELTA
  const result = Array.from(symbols).slice(0, TOP_N_DELTA);
  console.log(`Delta mode selected ${result.length} symbols`);
  return result;
}

// Get all symbols for full mode
async function getFullSymbols(): Promise<string[]> {
  try {
    const { data } = await supabase
      .from("stock_tickers")
      .select('"Symbol"');
    
    if (!data) return [];
    
    const symbols = data.map((r: any) => r.Symbol).filter(Boolean);
    console.log(`Full mode: ${symbols.length} total symbols`);
    return symbols;
  } catch (err) {
    console.error('Error getting full symbols:', err);
    return [];
  }
}

// Batch upsert
async function batchUpsert(stocks: QuoteResult[]): Promise<void> {
  if (stocks.length === 0) return;
  
  const rows = stocks.map(s => ({
    symbol: s.symbol,
    name: s.name,
    price: s.price,
    open: s.open ?? null,
    high: s.high ?? null,
    low: s.low ?? null,
    close: null,
    volume: s.volume ?? null,
    change_percent: s.change_percent ?? null,
    market_cap: s.market_cap ?? null,
    shares_float: s.shares_float ?? null,
    relative_volume: s.relative_volume ?? null,
    raw: s.raw ?? null,
    updated_at: new Date().toISOString()
  }));
  
  // Upsert in chunks of 100
  for (let i = 0; i < rows.length; i += 100) {
    const chunk = rows.slice(i, i + 100);
    await supabase.from("stocks").upsert(chunk, { onConflict: "symbol" });
  }
}

// Smart merge with priority: Primary API > TwelveData > Finnhub > FMP
function mergeData(
  primary: Partial<QuoteResult> | undefined,
  twelve: Partial<QuoteResult> | undefined,
  finnhub: Partial<QuoteResult> | undefined,
  fmp: Partial<QuoteResult> | undefined,
  symbol: string
): QuoteResult | null {
  // Need at least one source with the symbol
  const source = primary ?? twelve ?? finnhub ?? fmp;
  if (!source) return null;
  
  return {
    symbol,
    name: primary?.name ?? twelve?.name ?? fmp?.name ?? null,
    price: primary?.price ?? twelve?.price ?? finnhub?.price ?? fmp?.price ?? null,
    open: primary?.open ?? twelve?.open ?? finnhub?.open ?? null,
    high: primary?.high ?? twelve?.high ?? finnhub?.high ?? null,
    low: primary?.low ?? twelve?.low ?? finnhub?.low ?? null,
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
async function processBatches(symbols: string[]): Promise<QuoteResult[]> {
  const results: QuoteResult[] = [];
  const stats = {
    total: symbols.length,
    processed: 0,
    primary_success: 0,
    twelve_success: 0,
    finnhub_success: 0,
    fmp_success: 0,
    failed: 0
  };
  
  for (let i = 0; i < symbols.length; i += BATCH_SIZE) {
    const batch = symbols.slice(i, i + BATCH_SIZE);
    
    console.log(`Processing batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(symbols.length / BATCH_SIZE)}`);
    
    // Step 1: Try Primary API first
    const primaryData = await fetchBatchFromPrimaryAPI(batch);
    
    // Step 2: Identify failed symbols
    const failedSymbols = batch.filter(s => !primaryData.has(s));
    
    // Step 3: Try Twelve Data for failed symbols
    let twelveData = new Map();
    if (failedSymbols.length > 0) {
      console.log(`Retrying ${failedSymbols.length} symbols with Twelve Data`);
      twelveData = await fetchBatchFromTwelveData(failedSymbols);
    }
    
    // Step 4: Try Finnhub for still-failed symbols
    const stillFailed = failedSymbols.filter(s => !twelveData.has(s));
    let finnhubData = new Map();
    if (stillFailed.length > 0 && stillFailed.length <= 10) {
      console.log(`Retrying ${stillFailed.length} symbols with Finnhub`);
      finnhubData = await fetchBatchFromFinnhub(stillFailed);
    }
    
    // Step 5: Fetch FMP for fundamentals if needed
    const needsFMP: string[] = [];
    for (const symbol of batch) {
      const primary = primaryData.get(symbol);
      const twelve = twelveData.get(symbol);
      if ((primary || twelve) && (!primary?.market_cap || !primary?.shares_float)) {
        needsFMP.push(symbol);
      }
    }
    
    const fmpData = needsFMP.length > 0 ? await fetchFundamentalsFromFMP(needsFMP) : new Map();
    
    // Step 6: Merge all data sources
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
    
    stats.processed += batch.length;
    
    // Throttle between batches
    if (i + BATCH_SIZE < symbols.length) {
      await new Promise(resolve => setTimeout(resolve, THROTTLE_MS));
    }
  }
  
  console.log('Processing stats:', stats);
  return results;
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
    
    const url = new URL(req.url);
    const body = await req.json().catch(() => ({}));
    
    // Determine mode
    const modeParam = url.searchParams.get("mode") || body.mode;
    const manualSymbols = body.symbols;
    
    let mode: UpdateMode;
    let symbolsToProcess: string[];
    
    if (manualSymbols && Array.isArray(manualSymbols)) {
      mode = 'manual';
      symbolsToProcess = manualSymbols;
    } else if (modeParam === 'full') {
      mode = 'full';
      symbolsToProcess = await getFullSymbols();
    } else if (modeParam === 'delta' || !modeParam) {
      mode = 'delta';
      symbolsToProcess = await getDeltaSymbols();
    } else {
      return new Response(
        JSON.stringify({ error: "Invalid mode. Use 'full', 'delta', or provide 'symbols' array" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }
    
    console.log(`Starting ${mode} update for ${symbolsToProcess.length} symbols`);
    
    if (symbolsToProcess.length === 0) {
      return new Response(
        JSON.stringify({
          success: true,
          mode,
          message: "No symbols to process",
          duration_ms: Date.now() - startTime
        }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }
    
    // Process all symbols
    const results = await processBatches(symbolsToProcess);
    
    // Batch upsert to database
    if (results.length > 0) {
      await batchUpsert(results);
    }
    
    const duration = Date.now() - startTime;
    
    return new Response(
      JSON.stringify({
        success: true,
        mode,
        requested: symbolsToProcess.length,
        updated: results.length,
        failed: symbolsToProcess.length - results.length,
        duration_ms: duration,
        duration_readable: `${(duration / 1000).toFixed(2)}s`,
        provider_status: {
          primary_failures: providerFailures.primary,
          twelvedata_failures: providerFailures.twelvedata,
          finnhub_failures: providerFailures.finnhub,
          fmp_failures: providerFailures.fmp
        },
        stats: {
          avg_time_per_symbol: `${(duration / symbolsToProcess.length).toFixed(0)}ms`,
          batches_processed: Math.ceil(symbolsToProcess.length / BATCH_SIZE)
        }
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
    
  } catch (err) {
    console.error('Update stocks error:', err);
    return new Response(
      JSON.stringify({ 
        error: String(err),
        duration_ms: Date.now() - startTime
      }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
});