drop policy "Allow public read access on stock_tickers" on "public"."stock_tickers";

drop policy "Allow public read access on stocks" on "public"."stocks";

revoke delete on table "public"."stock_tickers" from "anon";

revoke insert on table "public"."stock_tickers" from "anon";

revoke references on table "public"."stock_tickers" from "anon";

revoke select on table "public"."stock_tickers" from "anon";

revoke trigger on table "public"."stock_tickers" from "anon";

revoke truncate on table "public"."stock_tickers" from "anon";

revoke update on table "public"."stock_tickers" from "anon";

revoke delete on table "public"."stock_tickers" from "authenticated";

revoke insert on table "public"."stock_tickers" from "authenticated";

revoke references on table "public"."stock_tickers" from "authenticated";

revoke select on table "public"."stock_tickers" from "authenticated";

revoke trigger on table "public"."stock_tickers" from "authenticated";

revoke truncate on table "public"."stock_tickers" from "authenticated";

revoke update on table "public"."stock_tickers" from "authenticated";

revoke delete on table "public"."stock_tickers" from "service_role";

revoke insert on table "public"."stock_tickers" from "service_role";

revoke references on table "public"."stock_tickers" from "service_role";

revoke select on table "public"."stock_tickers" from "service_role";

revoke trigger on table "public"."stock_tickers" from "service_role";

revoke truncate on table "public"."stock_tickers" from "service_role";

revoke update on table "public"."stock_tickers" from "service_role";

revoke delete on table "public"."stocks" from "anon";

revoke insert on table "public"."stocks" from "anon";

revoke references on table "public"."stocks" from "anon";

revoke select on table "public"."stocks" from "anon";

revoke trigger on table "public"."stocks" from "anon";

revoke truncate on table "public"."stocks" from "anon";

revoke update on table "public"."stocks" from "anon";

revoke delete on table "public"."stocks" from "authenticated";

revoke insert on table "public"."stocks" from "authenticated";

revoke references on table "public"."stocks" from "authenticated";

revoke select on table "public"."stocks" from "authenticated";

revoke trigger on table "public"."stocks" from "authenticated";

revoke truncate on table "public"."stocks" from "authenticated";

revoke update on table "public"."stocks" from "authenticated";

revoke delete on table "public"."stocks" from "service_role";

revoke insert on table "public"."stocks" from "service_role";

revoke references on table "public"."stocks" from "service_role";

revoke select on table "public"."stocks" from "service_role";

revoke trigger on table "public"."stocks" from "service_role";

revoke truncate on table "public"."stocks" from "service_role";

revoke update on table "public"."stocks" from "service_role";

create table "public"."watchlists" (
    "id" uuid not null default gen_random_uuid(),
    "user_id" uuid,
    "symbol" text not null,
    "created_at" timestamp with time zone default now()
);


alter table "public"."stocks" add column "relative_volume" numeric(6,3);

CREATE INDEX idx_stock_tickers_symbol ON public.stock_tickers USING btree ("Symbol");

CREATE INDEX idx_stocks_change_percent ON public.stocks USING btree (change_percent DESC NULLS LAST) WHERE (change_percent IS NOT NULL);

CREATE INDEX idx_stocks_symbol ON public.stocks USING btree (symbol);

CREATE INDEX idx_stocks_updated_at ON public.stocks USING btree (updated_at DESC);

CREATE INDEX idx_stocks_volume ON public.stocks USING btree (volume DESC NULLS LAST) WHERE (volume IS NOT NULL);

CREATE INDEX idx_watchlists_symbol ON public.watchlists USING btree (symbol);

CREATE INDEX idx_watchlists_user_id ON public.watchlists USING btree (user_id);

CREATE UNIQUE INDEX watchlists_pkey ON public.watchlists USING btree (id);

CREATE UNIQUE INDEX watchlists_user_id_symbol_key ON public.watchlists USING btree (user_id, symbol);

alter table "public"."watchlists" add constraint "watchlists_pkey" PRIMARY KEY using index "watchlists_pkey";

alter table "public"."watchlists" add constraint "watchlists_user_id_fkey" FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE not valid;

alter table "public"."watchlists" validate constraint "watchlists_user_id_fkey";

alter table "public"."watchlists" add constraint "watchlists_user_id_symbol_key" UNIQUE using index "watchlists_user_id_symbol_key";

set check_function_bodies = off;

create or replace view "public"."active_stocks" as  SELECT symbol,
    name,
    price,
    open,
    high,
    low,
    close,
    volume,
    change_percent,
    raw,
    updated_at,
    float_shares,
    market_cap,
    premarket_change,
    postmarket_change,
    day_volume,
    shares_float,
    relative_volume,
        CASE
            WHEN (updated_at > (now() - '00:05:00'::interval)) THEN 'fresh'::text
            WHEN (updated_at > (now() - '01:00:00'::interval)) THEN 'stale'::text
            ELSE 'very_stale'::text
        END AS freshness_status
   FROM stocks s
  WHERE (volume IS NOT NULL)
  ORDER BY volume DESC;


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

create materialized view "public"."top_movers" as  SELECT symbol,
    name,
    price,
    change_percent,
    volume,
    market_cap,
    relative_volume,
    updated_at
   FROM stocks
  WHERE ((change_percent IS NOT NULL) AND (volume IS NOT NULL) AND (price IS NOT NULL))
  ORDER BY (abs(change_percent)) DESC
 LIMIT 100;


CREATE UNIQUE INDEX idx_top_movers_symbol ON public.top_movers USING btree (symbol);

create policy "public_select"
on "public"."stocks"
as permissive
for select
to public
using (true);




