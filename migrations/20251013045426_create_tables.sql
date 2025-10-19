-- Create exchange type enum
create type public.exchange_type as enum ('NASDAQ', 'NYSE');

-- Create the stocks table
create table public.stocks (
  symbol text not null,
  name text null,
  price numeric null,
  open numeric null,
  high numeric null,
  low numeric null,
  close numeric null,
  volume bigint null,
  change_percent numeric null,
  raw jsonb null,
  updated_at timestamp with time zone null default now(),
  float_shares bigint null,
  market_cap numeric null,
  premarket_change numeric null,
  postmarket_change numeric null,
  day_volume bigint null,
  shares_float bigint null,
  constraint stocks_pkey primary key (symbol)
);

-- Create the stock_tickers table
create table public.stock_tickers (
  "Symbol" text not null,
  "Company Name" text null,
  "Security Name" text null,
  "Market Category" text null,
  "Test Issue" text null,
  "Financial Status" text null,
  "Round Lot Size" bigint null,
  "ETF" text null,
  "NextShares" text null,
  exchange public.exchange_type null default 'NASDAQ'::exchange_type,
  constraint nasdaq_tickers_pkey primary key ("Symbol")
);

-- Enable Row Level Security (RLS)
ALTER TABLE public.stocks ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.stock_tickers ENABLE ROW LEVEL SECURITY;

-- Create policies to allow public read access
CREATE POLICY "Allow public read access on stocks" ON public.stocks FOR SELECT USING (true);
CREATE POLICY "Allow public read access on stock_tickers" ON public.stock_tickers FOR SELECT USING (true);
