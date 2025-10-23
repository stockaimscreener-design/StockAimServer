set check_function_bodies = off;

CREATE OR REPLACE FUNCTION public.get_new_symbols(limit_count integer DEFAULT 100)
 RETURNS TABLE(symbol text)
 LANGUAGE plpgsql
AS $function$
BEGIN
  RETURN QUERY
  SELECT st."Symbol" as symbol
  FROM stock_tickers st
  LEFT JOIN stocks s ON st."Symbol" = s.symbol
  WHERE s.symbol IS NULL
  LIMIT limit_count;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_update_stats()
 RETURNS json
 LANGUAGE plpgsql
AS $function$
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
$function$
;

CREATE OR REPLACE FUNCTION public.get_watchlist_symbols(limit_count integer DEFAULT 100)
 RETURNS TABLE(symbol text)
 LANGUAGE plpgsql
AS $function$
BEGIN
  RETURN QUERY
  SELECT DISTINCT w.symbol
  FROM watchlists w
  ORDER BY w.created_at DESC
  LIMIT limit_count;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.refresh_top_movers()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY top_movers;
END;
$function$
;



