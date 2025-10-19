-- supabase/migrations/create_stocks_functions.sql

-- Function to get new symbols not yet in stocks table
CREATE OR REPLACE FUNCTION get_new_symbols(limit_count INT DEFAULT 100)
RETURNS TABLE (symbol TEXT) AS $$
BEGIN
  RETURN QUERY
  SELECT st."Symbol" as symbol
  FROM stock_tickers st
  LEFT JOIN stocks s ON st."Symbol" = s.symbol
  WHERE s.symbol IS NULL
  LIMIT limit_count;
END;
$$ LANGUAGE plpgsql;

-- Index optimizations for the stocks table
CREATE INDEX IF NOT EXISTS idx_stocks_volume ON stocks(volume DESC NULLS LAST) WHERE volume IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_stocks_change_percent ON stocks(change_percent DESC NULLS LAST) WHERE change_percent IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_stocks_updated_at ON stocks(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_stocks_symbol ON stocks(symbol);

-- Index for stock_tickers
CREATE INDEX IF NOT EXISTS idx_stock_tickers_symbol ON stock_tickers("Symbol");

-- Optional: Create a watchlist table for user-specific tracking
CREATE TABLE IF NOT EXISTS watchlists (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
  symbol TEXT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  UNIQUE(user_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_watchlists_user_id ON watchlists(user_id);
CREATE INDEX IF NOT EXISTS idx_watchlists_symbol ON watchlists(symbol);

-- Function to get watchlist symbols for delta updates
CREATE OR REPLACE FUNCTION get_watchlist_symbols(limit_count INT DEFAULT 100)
RETURNS TABLE (symbol TEXT) AS $$
BEGIN
  RETURN QUERY
  SELECT DISTINCT w.symbol
  FROM watchlists w
  ORDER BY w.created_at DESC
  LIMIT limit_count;
END;
$$ LANGUAGE plpgsql;

-- View for most active stocks (useful for dashboards)
CREATE OR REPLACE VIEW active_stocks AS
SELECT 
  s.*,
  CASE 
    WHEN s.updated_at > NOW() - INTERVAL '5 minutes' THEN 'fresh'
    WHEN s.updated_at > NOW() - INTERVAL '1 hour' THEN 'stale'
    ELSE 'very_stale'
  END as freshness_status
FROM stocks s
WHERE s.volume IS NOT NULL
ORDER BY s.volume DESC;

-- Materialized view for top movers (refresh periodically)
CREATE MATERIALIZED VIEW IF NOT EXISTS top_movers AS
SELECT 
  symbol,
  name,
  price,
  change_percent,
  volume,
  market_cap,
  relative_volume,
  updated_at
FROM stocks
WHERE change_percent IS NOT NULL
  AND volume IS NOT NULL
  AND price IS NOT NULL
ORDER BY ABS(change_percent) DESC
LIMIT 100;

CREATE UNIQUE INDEX IF NOT EXISTS idx_top_movers_symbol ON top_movers(symbol);

-- Function to refresh materialized view (call from cron)
CREATE OR REPLACE FUNCTION refresh_top_movers()
RETURNS void AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY top_movers;
END;
$$ LANGUAGE plpgsql;

-- Stats function for monitoring
CREATE OR REPLACE FUNCTION get_update_stats()
RETURNS JSON AS $$
DECLARE
  result JSON;
BEGIN
  SELECT json_build_object(
    'total_stocks', COUNT(*),
    'fresh_stocks', COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '5 minutes'),
    'stale_stocks', COUNT(*) FILTER (WHERE updated_at <= NOW() - INTERVAL '5 minutes'),
    'never_updated', COUNT(*) FILTER (WHERE updated_at IS NULL),
    'avg_price', AVG(price),
    'total_volume', SUM(volume),
    'last_update', MAX(updated_at)
  ) INTO result
  FROM stocks;
  
  RETURN result;
END;
$$ LANGUAGE plpgsql;