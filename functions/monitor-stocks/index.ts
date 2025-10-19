// supabase/functions/monitor-stocks/index.ts
// Monitoring endpoint for tracking update performance
import { serve } from "https://deno.land/std@0.200.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

serve(async (req) => {
  try {
    if (req.method !== "GET") {
      return new Response(
        JSON.stringify({ error: "GET only" }),
        { status: 405, headers: { "Content-Type": "application/json" } }
      );
    }

    // Get overall stats
    const { data: statsData } = await supabase
      .rpc('get_update_stats')
      .single();

    // Get freshness breakdown
    const now = new Date();
    const fiveMinAgo = new Date(now.getTime() - 5 * 60 * 1000);
    const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
    const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);

    const { count: veryFresh } = await supabase
      .from('stocks')
      .select('*', { count: 'exact', head: true })
      .gte('updated_at', fiveMinAgo.toISOString());

    const { count: fresh } = await supabase
      .from('stocks')
      .select('*', { count: 'exact', head: true })
      .gte('updated_at', oneHourAgo.toISOString())
      .lt('updated_at', fiveMinAgo.toISOString());

    const { count: stale } = await supabase
      .from('stocks')
      .select('*', { count: 'exact', head: true })
      .gte('updated_at', oneDayAgo.toISOString())
      .lt('updated_at', oneHourAgo.toISOString());

    const { count: veryStale } = await supabase
      .from('stocks')
      .select('*', { count: 'exact', head: true })
      .lt('updated_at', oneDayAgo.toISOString());

    const { count: neverUpdated } = await supabase
      .from('stocks')
      .select('*', { count: 'exact', head: true })
      .is('updated_at', null);

    // Get top movers
    const { data: topGainers } = await supabase
      .from('stocks')
      .select('symbol, name, price, change_percent, volume')
      .not('change_percent', 'is', null)
      .order('change_percent', { ascending: false })
      .limit(10);

    const { data: topLosers } = await supabase
      .from('stocks')
      .select('symbol, name, price, change_percent, volume')
      .not('change_percent', 'is', null)
      .order('change_percent', { ascending: true })
      .limit(10);

    // Get most active by volume
    const { data: mostActive } = await supabase
      .from('stocks')
      .select('symbol, name, price, volume, change_percent')
      .not('volume', 'is', null)
      .order('volume', { ascending: false })
      .limit(10);

    // Check for symbols in stock_tickers but not in stocks
    const { count: missingCount } = await supabase
      .from('stock_tickers')
      .select('"Symbol"', { count: 'exact', head: true });

    const { count: existingCount } = await supabase
      .from('stocks')
      .select('symbol', { count: 'exact', head: true });

    const coverage = existingCount && missingCount 
      ? ((existingCount / missingCount) * 100).toFixed(2)
      : 0;

    return new Response(
      JSON.stringify({
        timestamp: new Date().toISOString(),
        stats: statsData,
        freshness: {
          very_fresh_5min: veryFresh || 0,
          fresh_1hour: fresh || 0,
          stale_1day: stale || 0,
          very_stale: veryStale || 0,
          never_updated: neverUpdated || 0
        },
        coverage: {
          total_tickers: missingCount || 0,
          stocks_tracked: existingCount || 0,
          coverage_percent: coverage
        },
        market_snapshot: {
          top_gainers: topGainers || [],
          top_losers: topLosers || [],
          most_active: mostActive || []
        }
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );

  } catch (err) {
    return new Response(
      JSON.stringify({ error: String(err) }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
});