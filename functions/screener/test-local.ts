// Test script for local debugging
// Run with: deno run --allow-net --allow-env test-local.ts

import { serve } from "https://deno.land/std@0.200.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

// Set environment variables for local testing
const FINNHUB_KEY = "d3ktp81r01qp3ucpk3ngd3ktp81r01qp3ucpk3o0"; // Replace with your actual key
const SUPABASE_URL = "http://localhost:54321"; // Local Supabase URL
const SUPABASE_SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJtb3ZwbHR6YWNoY2N5b3Vna2R3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDA0NTY5MywiZXhwIjoyMDc1NjIxNjkzfQ.eHkqNd-W-kJWu9AVtdMLrlU8oTAjKESw5Yu8Q9XNY1o"; // Replace with your actual key

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

console.log("🚀 Starting local Supabase Edge Function server...");
console.log("📡 Server will be available at: http://localhost:8000");
console.log("🔧 Make sure to set your FINNHUB_KEY and SUPABASE_SERVICE_ROLE_KEY in this file");

serve(async (req: Request) => {
  console.log(`📥 ${req.method} ${req.url}`);
  
  // Simple test endpoint
  if (req.method === "GET") {
    return new Response(JSON.stringify({
      message: "Supabase Edge Function is running locally!",
      timestamp: new Date().toISOString(),
      environment: {
        has_finnhub_key: !!FINNHUB_KEY && FINNHUB_KEY !== "your_finnhub_api_key_here",
        has_supabase_url: !!SUPABASE_URL,
        has_service_key: !!SUPABASE_SERVICE_ROLE_KEY && SUPABASE_SERVICE_ROLE_KEY !== "your_service_role_key_here"
      }
    }), {
      headers: { "content-type": "application/json" }
    });
  }
  
  // For POST requests, you can test your actual screener logic here
  return new Response(JSON.stringify({ error: "POST endpoint not implemented in test script" }), {
    status: 501,
    headers: { "content-type": "application/json" }
  });
}, { port: 8000 });
