#!/bin/bash
# =============================================
# Supabase Cron Job Setup for stock-scheduler
# =============================================

SERVICE_ROLE_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o"
BASE_URL="https://bmovpltzachccyougkdw.supabase.co/functions/v1/stock-scheduler"

# -------------------------------
# 1) Delta Updates (every 5 mins)
# -------------------------------
supabase cron schedule create stocks-delta \
  --cron "*/5 * * * *" \
  --request-body '' \
  --http-method POST \
  --url "${BASE_URL}?mode=delta" \
  --headers "Authorization: Bearer ${SERVICE_ROLE_KEY}" \
  --headers "Content-Type: application/json"

# -------------------------------
# 2) Full Updates (5 chunks)
# -------------------------------

# Chunk 1
supabase cron schedule create stocks-full-chunk-1 \
  --cron "0 6 * * *" \
  --request-body '' \
  --http-method POST \
  --url "${BASE_URL}?mode=full&limit=1000&offset=0" \
  --headers "Authorization: Bearer ${SERVICE_ROLE_KEY}" \
  --headers "Content-Type: application/json"

# Chunk 2
supabase cron schedule create stocks-full-chunk-2 \
  --cron "10 6 * * *" \
  --request-body '' \
  --http-method POST \
  --url "${BASE_URL}?mode=full&limit=1000&offset=1000" \
  --headers "Authorization: Bearer ${SERVICE_ROLE_KEY}" \
  --headers "Content-Type: application/json"

# Chunk 3
supabase cron schedule create stocks-full-chunk-3 \
  --cron "20 6 * * *" \
  --request-body '' \
  --http-method POST \
  --url "${BASE_URL}?mode=full&limit=1000&offset=2000" \
  --headers "Authorization: Bearer ${SERVICE_ROLE_KEY}" \
  --headers "Content-Type: application/json"

# Chunk 4
supabase cron schedule create stocks-full-chunk-4 \
  --cron "30 6 * * *" \
  --request-body '' \
  --http-method POST \
  --url "${BASE_URL}?mode=full&limit=1000&offset=3000" \
  --headers "Authorization: Bearer ${SERVICE_ROLE_KEY}" \
  --headers "Content-Type: application/json"

# Chunk 5
supabase cron schedule create stocks-full-chunk-5 \
  --cron "40 6 * * *" \
  --request-body '' \
  --http-method POST \
  --url "${BASE_URL}?mode=full&limit=1000&offset=4000" \
  --headers "Authorization: Bearer ${SERVICE_ROLE_KEY}" \
  --headers "Content-Type: application/json"

echo "✅ All cron jobs have been created!"
