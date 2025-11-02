// =====================================================
// FILE 1: supabase/functions/polygon-stock-scheduler/index.ts
// =====================================================
import { serve } from "https://deno.land/std@0.200.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const POLYGON_API_KEY = Deno.env.get("POLYGON_API_KEY")!;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// =====================================================
// MARKET HOURS CHECK
// =====================================================
async function isMarketClosed(): Promise<boolean> {
  try {
    const now = new Date();
    const etDate = new Date(
      now.toLocaleString("en-US", { timeZone: "America/New_York" })
    );

    // Weekend check
    const weekday = etDate.getDay();
    if (weekday === 0 || weekday === 6) {
      console.log("Market closed: Weekend");
      return true;
    }

    // Holiday check
    const dateStr = etDate.toISOString().slice(0, 10);
    const { data: holidays } = await supabase
      .from("us_market_holidays")
      .select("holiday_name")
      .eq("holiday_date", dateStr)
      .maybeSingle();

    if (holidays?.holiday_name) {
      console.log(`Market closed: ${holidays.holiday_name}`);
      return true;
    }

    return false;
  } catch (err) {
    console.error("Holiday check error:", err);
    return false;
  }
}

// =====================================================
// GET PREVIOUS TRADING DAY
// =====================================================
function getPreviousTradingDay(): string {
  const now = new Date();
  const etDate = new Date(
    now.toLocaleString("en-US", { timeZone: "America/New_York" })
  );

  // Go back 1 day initially
  etDate.setDate(etDate.getDate() - 1);

  // If it's Sunday (0), go back to Friday
  if (etDate.getDay() === 0) {
    etDate.setDate(etDate.getDate() - 2);
  }
  // If it's Saturday (6), go back to Friday
  else if (etDate.getDay() === 6) {
    etDate.setDate(etDate.getDate() - 1);
  }

  return etDate.toISOString().slice(0, 10);
}

// =====================================================
// FETCH ALL STOCKS FROM POLYGON (1 API CALL!)
// =====================================================
async function fetchAllStocksFromPolygon(date: string) {
  console.log(`📊 Fetching ALL stocks from Polygon for date: ${date}`);
  
  try {
    // VERIFIED: This is the correct endpoint
    const url = `https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/${date}?adjusted=true&apiKey=${POLYGON_API_KEY}`;
    
    console.log(`Calling: ${url.replace(POLYGON_API_KEY, 'API_KEY_HIDDEN')}`);
    
    const response = await fetch(url, {
      signal: AbortSignal.timeout(30000),
      headers: {
        'User-Agent': 'SupabaseScheduler/2.0'
      }
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error(`❌ Polygon API error: ${response.status} - ${errorText}`);
      return null;
    }

    const data = await response.json();
    
    console.log("Response status:", data.status);
    console.log("Results count:", data.resultsCount);
    
    if (data.status !== "OK") {
      console.error("❌ Invalid response status from Polygon:", data.status);
      console.error("Response:", JSON.stringify(data, null, 2));
      return null;
    }

    if (!data.results || !Array.isArray(data.results)) {
      console.error("❌ No results array in response");
      return null;
    }

    console.log(`✅ Fetched ${data.resultsCount} stocks from Polygon`);
    console.log("Sample ticker:", data.results[0]);
    
    return data.results;
  } catch (err) {
    console.error("❌ Polygon fetch error:", err);
    return null;
  }
}

// =====================================================
// GET VALID TICKERS FROM DATABASE
// =====================================================
async function getValidTickers(): Promise<Set<string>> {
  try {
    const { data, error } = await supabase
      .from("stock_tickers")
      .select('"Symbol"');

    if (error) {
      console.error("Error fetching tickers:", error);
      throw error;
    }

    const tickers = new Set(
      (data || [])
        .map((row) => row.Symbol)
        .filter((symbol) => symbol && typeof symbol === 'string')
    );
    
    console.log(`📋 Loaded ${tickers.size} valid tickers from database`);
    
    // Log sample tickers for verification
    const sampleTickers = Array.from(tickers).slice(0, 5);
    console.log("Sample tickers:", sampleTickers);
    
    return tickers;
  } catch (err) {
    console.error("❌ Error loading tickers:", err);
    return new Set();
  }
}

// =====================================================
// GET PREVIOUS CLOSE PRICES
// =====================================================
async function getPreviousCloses(): Promise<Map<string, number>> {
  try {
    const { data, error } = await supabase
      .from("stocks")
      .select("symbol, price")
      .not("price", "is", null);

    if (error) {
      console.error("Error fetching previous closes:", error);
      return new Map();
    }

    const map = new Map<string, number>();
    (data || []).forEach((row) => {
      if (row.symbol && row.price && typeof row.price === 'number') {
        map.set(row.symbol, row.price);
      }
    });

    console.log(`📊 Loaded ${map.size} previous close prices`);
    return map;
  } catch (err) {
    console.error("Error loading previous closes:", err);
    return new Map();
  }
}

// =====================================================
// CALCULATE METRICS
// =====================================================
function calculateMetrics(bar: any, previousClose?: number) {
  // Polygon returns: T (ticker), v (volume), vw (vwap), o (open), c (close), h (high), l (low), t (timestamp), n (trades)
  const price = bar.c; // close price
  const open = bar.o;
  const high = bar.h;
  const low = bar.l;
  const volume = bar.v;
  const vwap = bar.vw;

  let changePercent = null;
  
  // Calculate change % from previous close if available
  if (previousClose && previousClose > 0 && price) {
    changePercent = ((price - previousClose) / previousClose) * 100;
  } 
  // Otherwise calculate from open to close
  else if (open && open > 0 && price) {
    changePercent = ((price - open) / open) * 100;
  }

  return {
    price,
    open,
    high,
    low,
    volume,
    vwap,
    change_percent: changePercent ? parseFloat(changePercent.toFixed(4)) : null,
  };
}

// =====================================================
// PROCESS & UPDATE DATABASE
// =====================================================
async function processAndUpdateStocks(
  polygonData: any[],
  validTickers: Set<string>,
  previousCloses: Map<string, number>
) {
  const rows = [];
  let skipped = 0;

  console.log(`Processing ${polygonData.length} tickers from Polygon...`);

  for (const bar of polygonData) {
    const symbol = bar.T; // Polygon returns symbol as "T"

    if (!symbol) {
      skipped++;
      continue;
    }

    // Skip if not in our valid tickers list
    if (!validTickers.has(symbol)) {
      skipped++;
      continue;
    }

    const metrics = calculateMetrics(bar, previousCloses.get(symbol));

    // Skip if no valid price data
    if (!metrics.price) {
      skipped++;
      continue;
    }

    rows.push({
      symbol: symbol,
      name: null, // Will be populated from stock_tickers via join in queries
      price: metrics.price,
      open: metrics.open,
      high: metrics.high,
      low: metrics.low,
      close: metrics.price, // Close = current price for daily bars
      volume: metrics.volume,
      change_percent: metrics.change_percent,
      market_cap: null, // Not available from grouped daily
      shares_float: null, // Not available from grouped daily
      relative_volume: null, // Need to calculate separately with avg volume
      raw: {
        polygon_bar: bar,
        vwap: metrics.vwap,
        timestamp: bar.t,
        num_trades: bar.n,
      },
      updated_at: new Date().toISOString(),
    });
  }

  console.log(`💾 Prepared ${rows.length} stocks for database update (skipped ${skipped})`);

  // Batch insert/update
  const BATCH_SIZE = 500;
  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(rows.length / BATCH_SIZE);

    try {
      const { error } = await supabase.from("stocks").upsert(batch, {
        onConflict: "symbol",
      });

      if (error) {
        console.error(`❌ Batch ${batchNum}/${totalBatches} error:`, error);
        errorCount += batch.length;
      } else {
        successCount += batch.length;
        console.log(`✅ Batch ${batchNum}/${totalBatches} updated successfully (${batch.length} stocks)`);
      }
    } catch (err) {
      console.error(`❌ Batch update error:`, err);
      errorCount += batch.length;
    }

    // Small delay between batches to avoid overwhelming DB
    if (i + BATCH_SIZE < rows.length) {
      await new Promise((r) => setTimeout(r, 100));
    }
  }

  return { successCount, errorCount, totalProcessed: rows.length };
}

// =====================================================
// FETCH TICKERS LIST FROM POLYGON (ONE-TIME SETUP)
// =====================================================
async function updateTickersFromPolygon() {
  console.log("📋 Updating tickers list from Polygon...");
  
  let allTickers: any[] = [];
  let nextUrl: string | null = `https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&limit=1000&apiKey=${POLYGON_API_KEY}`;
  
  let pageCount = 0;
  const maxPages = 20; // Safety limit
  
  // Polygon paginates ticker list, need to follow next_url
  while (nextUrl && pageCount < maxPages) {
    try {
      pageCount++;
      console.log(`Fetching page ${pageCount}...`);
      
      const response = await fetch(nextUrl, {
        signal: AbortSignal.timeout(30000)
      });
      
      if (!response.ok) {
        console.error(`Error fetching page ${pageCount}: ${response.status}`);
        break;
      }
      
      const data = await response.json();
      
      if (data.results && Array.isArray(data.results)) {
        allTickers = allTickers.concat(data.results);
        console.log(`📊 Total tickers so far: ${allTickers.length}`);
      }
      
      nextUrl = data.next_url ? `${data.next_url}&apiKey=${POLYGON_API_KEY}` : null;
      
      // Respect rate limit (5 calls/min on free tier = 12 seconds between calls)
      if (nextUrl) {
        console.log("Waiting 12 seconds for rate limit...");
        await new Promise((r) => setTimeout(r, 12000));
      }
    } catch (err) {
      console.error("Error fetching tickers:", err);
      break;
    }
  }
  
  console.log(`✅ Total tickers fetched: ${allTickers.length}`);
  
  // Filter and prepare for database
  const rows = allTickers
    .filter((t) => {
      // Only NYSE and NASDAQ, active stocks
      return (
        (t.primary_exchange === "XNYS" || t.primary_exchange === "XNAS") &&
        t.market === "stocks" &&
        t.active === true &&
        t.type !== "ETF" && // Exclude ETFs
        t.type !== "ETN" && // Exclude ETNs
        t.type !== "UNIT" && // Exclude Units
        t.type !== "WARRANT" // Exclude Warrants
      );
    })
    .map((t) => ({
      Symbol: t.ticker,
      "Security Name": t.name,
      Market: t.primary_exchange === "XNYS" ? "NYSE" : "NASDAQ",
      ETF: "N", // We filtered out ETFs above
    }));

  console.log(`💾 Inserting ${rows.length} filtered tickers into database...`);

  // Batch insert
  const BATCH_SIZE = 500;
  let insertedCount = 0;
  
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    
    const { error } = await supabase.from("stock_tickers").upsert(batch, {
      onConflict: "Symbol",
    });

    if (error) {
      console.error(`Insert error on batch ${batchNum}:`, error);
    } else {
      insertedCount += batch.length;
      console.log(`✅ Batch ${batchNum} inserted (${batch.length} tickers)`);
    }
  }

  return insertedCount;
}

// =====================================================
// MAIN HTTP HANDLER
// =====================================================
serve(async (req) => {
  const startTime = Date.now();

  try {
    const url = new URL(req.url);

    // Health check
    if (url.pathname.endsWith("/health")) {
      return new Response(
        JSON.stringify({
          status: "healthy",
          timestamp: new Date().toISOString(),
          polygon_configured: !!POLYGON_API_KEY,
        }),
        { headers: { "Content-Type": "application/json" } }
      );
    }

    const mode = url.searchParams.get("mode") || "daily";
    const force = url.searchParams.get("force") === "true";
    const dateParam = url.searchParams.get("date"); // Allow manual date override

    console.log(`🚀 Starting Polygon scheduler - mode: ${mode}, force: ${force}`);

    // Mode: Update tickers list (run once or weekly)
    if (mode === "update-tickers") {
      const count = await updateTickersFromPolygon();
      return new Response(
        JSON.stringify({
          status: "success",
          mode: "update-tickers",
          tickers_updated: count,
          duration_ms: Date.now() - startTime,
          duration_sec: ((Date.now() - startTime) / 1000).toFixed(2),
        }),
        { headers: { "Content-Type": "application/json" } }
      );
    }

    // Check market hours (skip on force)
    if (!force && (await isMarketClosed())) {
      return new Response(
        JSON.stringify({
          status: "skipped",
          reason: "Market closed (weekend or holiday)",
          timestamp: new Date().toISOString(),
        }),
        { headers: { "Content-Type": "application/json" } }
      );
    }

    // Determine which date to fetch
    const dateToFetch = dateParam || getPreviousTradingDay();
    console.log(`Fetching data for date: ${dateToFetch}`);

    // Fetch ALL stocks from Polygon in ONE API CALL
    const polygonData = await fetchAllStocksFromPolygon(dateToFetch);

    if (!polygonData) {
      return new Response(
        JSON.stringify({
          status: "error",
          error: "Failed to fetch data from Polygon",
          duration_ms: Date.now() - startTime,
        }),
        { status: 500, headers: { "Content-Type": "application/json" } }
      );
    }

    // Load valid tickers and previous closes in parallel
    const [validTickers, previousCloses] = await Promise.all([
      getValidTickers(),
      getPreviousCloses(),
    ]);

    if (validTickers.size === 0) {
      return new Response(
        JSON.stringify({
          status: "error",
          error: "No valid tickers found in database. Run mode=update-tickers first.",
          duration_ms: Date.now() - startTime,
        }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    // Process and update database
    const result = await processAndUpdateStocks(
      polygonData,
      validTickers,
      previousCloses
    );

    return new Response(
      JSON.stringify({
        status: "success",
        mode: "daily",
        date: dateToFetch,
        polygon_results: polygonData.length,
        valid_tickers_in_db: validTickers.size,
        processed: result.totalProcessed,
        updated: result.successCount,
        errors: result.errorCount,
        duration_ms: Date.now() - startTime,
        duration_sec: ((Date.now() - startTime) / 1000).toFixed(2),
      }),
      { headers: { "Content-Type": "application/json" } }
    );
  } catch (err) {
    console.error("❌ Fatal error:", err);
    return new Response(
      JSON.stringify({
        status: "error",
        error: String(err),
        stack: err instanceof Error ? err.stack : undefined,
        duration_ms: Date.now() - startTime,
      }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
});

// =====================================================
// DEPLOYMENT COMMANDS
// =====================================================
/*

1. Deploy the function:
   supabase functions deploy polygon-stock-scheduler

2. Set environment variables:
   supabase secrets set POLYGON_API_KEY=your_polygon_api_key_here

3. Test health check:
   curl "https://YOUR_PROJECT.supabase.co/functions/v1/polygon-stock-scheduler/health"

4. Update tickers (run this FIRST!):
   curl "https://YOUR_PROJECT.supabase.co/functions/v1/polygon-stock-scheduler?mode=update-tickers" \
     -H "Authorization: Bearer YOUR_ANON_KEY"

5. Test daily stock fetch:
   curl "https://YOUR_PROJECT.supabase.co/functions/v1/polygon-stock-scheduler?mode=daily&force=true" \
     -H "Authorization: Bearer YOUR_ANON_KEY"

6. Verify in database:
   SELECT COUNT(*) FROM stock_tickers;  -- Should be ~5000+
   SELECT COUNT(*) FROM stocks;         -- Should be ~5000+
   SELECT * FROM stocks ORDER BY volume DESC LIMIT 10;

*/