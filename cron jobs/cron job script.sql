-- =====================================================
-- AUTO-ADJUSTING STOCK SYNC CRON JOBS
-- ✅ Handles DST automatically (no manual updates needed)
-- ✅ Uses simple schedules with ET time checks inside
-- =====================================================

-- Remove all existing stock jobs
SELECT cron.unschedule(jobname) FROM cron.job WHERE jobname LIKE 'stocks-%';

-- =====================================================
-- 1. DAILY FULL SYNC - 6:00 AM ET
-- =====================================================

SELECT cron.unschedule('stocks-full-daily');
SELECT cron.schedule('stocks-full-daily', '0 10,11 * * 1-5',
  $$SELECT CASE 
    WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL
    WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL
    WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') != 6 THEN NULL
    ELSE net.http_post(
      url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=5000&offset=0',
      headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')
    ) END AS r;$$
);


-- =====================================================
-- 2. PRE-MARKET: Every 15 min, 4:00-9:30 AM ET
-- =====================================================
SELECT cron.schedule('stocks-premarket-batch-0', '0,15,30,45 * * * 1-5',
  $$SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL
    WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL
    WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 4 THEN NULL
    WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') > 9 THEN NULL
    WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') >= 30 THEN NULL
    ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=0',
      headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$$
);

SELECT cron.schedule('stocks-premarket-batch-1', '1,16,31,46 * * * 1-5',
  $$SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL
    WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL
    WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 4 THEN NULL
    WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') > 9 THEN NULL
    WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') >= 30 THEN NULL
    ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=500',
      headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$$
);

SELECT cron.schedule('stocks-premarket-batch-2', '2,17,32,47 * * * 1-5',
  $$SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL
    WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL
    WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 4 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') > 9 THEN NULL
    WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') >= 30 THEN NULL
    ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=1000',
      headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$$
);

SELECT cron.schedule('stocks-premarket-batch-3', '3,18,33,48 * * * 1-5',
  $$SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 4 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') > 9 THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') >= 30 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=1500', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$$
);

SELECT cron.schedule('stocks-premarket-batch-4', '4,19,34,49 * * * 1-5',
  $$SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 4 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') > 9 THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') >= 30 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=2000', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$$
);

SELECT cron.schedule('stocks-premarket-batch-5', '5,20,35,50 * * * 1-5',
  $$SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 4 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') > 9 THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') >= 30 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=2500', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$$
);

SELECT cron.schedule('stocks-premarket-batch-6', '6,21,36,51 * * * 1-5',
  $$SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 4 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') > 9 THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') >= 30 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=3000', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$$
);

SELECT cron.schedule('stocks-premarket-batch-7', '7,22,37,52 * * * 1-5',
  $$SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 4 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') > 9 THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') >= 30 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=3500', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$$
);

SELECT cron.schedule('stocks-premarket-batch-8', '8,23,38,53 * * * 1-5',
  $$SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 4 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') > 9 THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') >= 30 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=4000', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$$
);

-- =====================================================
-- 3. MARKET HOURS: Every 5 min, 9:30 AM-4:00 PM ET
-- =====================================================
SELECT cron.schedule('stocks-market-batch-0', '*/5 * * * 1-5',
  $$SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 9 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 16 THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') < 30 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=0', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$$
);

SELECT cron.schedule('stocks-market-batch-1', '1,6,11,16,21,26,31,36,41,46,51,56 * * * 1-5',
  $$SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 9 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 16 THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') < 30 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=500', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$$
);

SELECT cron.schedule('stocks-market-batch-2', '2,7,12,17,22,27,32,37,42,47,52,57 * * * 1-5',
  $$SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 9 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 16 THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') < 30 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=1000', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$$
);

SELECT cron.schedule('stocks-market-batch-3', '3,8,13,18,23,28,33,38,43,48,53,58 * * * 1-5',
  $$SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 9 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 16 THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') < 30 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=1500', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$$
);

SELECT cron.schedule('stocks-market-batch-4', '4,9,14,19,24,29,34,39,44,49,54,59 * * * 1-5',
  $$SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 9 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 16 THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') < 30 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=2000', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$$
);

SELECT cron.schedule('stocks-market-batch-5', '*/5 * * * 1-5',
  $SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 9 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 16 THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') < 30 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=2500', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$
);

SELECT cron.schedule('stocks-market-batch-6', '*/5 * * * 1-5',
  $SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 9 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 16 THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') < 30 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=3000', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$
);

SELECT cron.schedule('stocks-market-batch-7', '*/5 * * * 1-5',
  $SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 9 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 16 THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') < 30 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=3500', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$
);

SELECT cron.schedule('stocks-market-batch-8', '*/5 * * * 1-5',
  $SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 9 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 16 THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') < 30 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=4000', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$
);

-- =====================================================
-- 4. AFTER-HOURS: Every 30 min, 4:00-8:00 PM ET
-- =====================================================
SELECT cron.schedule('stocks-afterhours-batch-0', '0,30 * * * 1-5',
  $SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 16 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 20 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=0', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$
);

SELECT cron.schedule('stocks-afterhours-batch-1', '1,31 * * * 1-5',
  $SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 16 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 20 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=500', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$
);

SELECT cron.schedule('stocks-afterhours-batch-2', '2,32 * * * 1-5',
  $SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 16 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 20 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=1000', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$
);

SELECT cron.schedule('stocks-afterhours-batch-3', '3,33 * * * 1-5',
  $SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 16 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 20 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=1500', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$
);

SELECT cron.schedule('stocks-afterhours-batch-4', '4,34 * * * 1-5',
  $SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 16 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 20 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=2000', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$
);

SELECT cron.schedule('stocks-afterhours-batch-5', '5,35 * * * 1-5',
  $SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 16 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 20 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=2500', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$
);

SELECT cron.schedule('stocks-afterhours-batch-6', '6,36 * * * 1-5',
  $SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 16 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 20 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=3000', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$
);

SELECT cron.schedule('stocks-afterhours-batch-7', '7,37 * * * 1-5',
  $SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 16 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 20 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=3500', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$
);

SELECT cron.schedule('stocks-afterhours-batch-8', '8,38 * * * 1-5',
  $SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 16 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 20 THEN NULL ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=4000', headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) END AS r;$
);


-- =====================================================
-- ADD BATCH-9 TO COVER 5000 STOCKS (4500-5000)
-- Run this to extend coverage from 4500 to 5000
-- =====================================================

-- 2. Add Pre-market Batch-9 (offset=4500)
SELECT cron.schedule('stocks-premarket-batch-9', '9,24,39,54 * * * 1-5',
  $$SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL 
    WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL 
    WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 4 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') > 9 THEN NULL 
    WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') >= 30 THEN NULL 
    ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=4500', 
      headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) 
    END AS r;$$
);

-- 3. Add Market Hours Batch-9 (offset=4500)
SELECT cron.schedule('stocks-market-batch-9', '*/5 * * * 1-5',
  $$SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL 
    WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL 
    WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 9 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 16 THEN NULL 
    WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') = 9 AND EXTRACT(MINUTE FROM NOW() AT TIME ZONE 'America/New_York') < 30 THEN NULL 
    ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=4500', 
      headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) 
    END AS r;$$
);

-- 4. Add After-hours Batch-9 (offset=4500)
SELECT cron.schedule('stocks-afterhours-batch-9', '9,39 * * * 1-5',
  $$SELECT CASE WHEN EXTRACT(DOW FROM NOW() AT TIME ZONE 'America/New_York') IN (0, 6) THEN NULL 
    WHEN EXISTS (SELECT 1 FROM us_market_holidays WHERE holiday_date = (NOW() AT TIME ZONE 'America/New_York')::DATE) THEN NULL 
    WHEN EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') < 16 OR EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/New_York') >= 20 THEN NULL 
    ELSE net.http_post(url := 'https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler?mode=full&limit=500&offset=4500', 
      headers := jsonb_build_object('Content-Type', 'application/json', 'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o')) 
    END AS r;$$
);

-- =====================================================
-- VERIFY ALL JOBS (Should show 30 jobs total)
-- =====================================================
SELECT jobid, jobname, schedule, active 
FROM cron.job 
WHERE jobname LIKE 'stocks-%' 
ORDER BY jobname;

-- =====================================================
-- ✅ DONE! Coverage Summary:
-- - Pre-market (9 batches): 4500-5000 stocks
-- - Market hours (10 batches): 5000 stocks  
-- - After-hours (10 batches): 5000 stocks
-- - Daily full sync: 5000 stocks
-- =====================================================

-- =====================================================
-- ✅ DONE! Summary:
-- - Pre-market: Every 15 min (4:00-9:30 AM ET)
-- - Market: Every 5 min (9:30 AM-4:00 PM ET)  
-- - After-hours: Every 30 min (4:00-8:00 PM ET)
-- - Auto-adjusts for DST (no manual updates needed!)
-- =====================================================