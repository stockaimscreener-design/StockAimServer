revoke delete on table "public"."watchlists" from "anon";

revoke insert on table "public"."watchlists" from "anon";

revoke references on table "public"."watchlists" from "anon";

revoke select on table "public"."watchlists" from "anon";

revoke trigger on table "public"."watchlists" from "anon";

revoke truncate on table "public"."watchlists" from "anon";

revoke update on table "public"."watchlists" from "anon";

revoke delete on table "public"."watchlists" from "authenticated";

revoke insert on table "public"."watchlists" from "authenticated";

revoke references on table "public"."watchlists" from "authenticated";

revoke select on table "public"."watchlists" from "authenticated";

revoke trigger on table "public"."watchlists" from "authenticated";

revoke truncate on table "public"."watchlists" from "authenticated";

revoke update on table "public"."watchlists" from "authenticated";

revoke delete on table "public"."watchlists" from "service_role";

revoke insert on table "public"."watchlists" from "service_role";

revoke references on table "public"."watchlists" from "service_role";

revoke select on table "public"."watchlists" from "service_role";

revoke trigger on table "public"."watchlists" from "service_role";

revoke truncate on table "public"."watchlists" from "service_role";

revoke update on table "public"."watchlists" from "service_role";

create table "public"."us_market_holidays" (
    "holiday_date" date not null,
    "holiday_name" text not null
);


CREATE UNIQUE INDEX us_market_holidays_pkey ON public.us_market_holidays USING btree (holiday_date);

alter table "public"."us_market_holidays" add constraint "us_market_holidays_pkey" PRIMARY KEY using index "us_market_holidays_pkey";

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



