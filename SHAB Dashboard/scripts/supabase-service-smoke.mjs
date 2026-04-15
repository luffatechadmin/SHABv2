import dotenv from "dotenv";
import path from "node:path";

const projectRoot = path.resolve(import.meta.dirname, "..");
dotenv.config({ path: path.join(projectRoot, ".env.local"), quiet: true });

const supabaseUrl = String(process.env.SUPABASE_URL || process.env.VITE_SUPABASE_URL || "").trim();
const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY || "").trim();

if (!supabaseUrl) throw new Error("Missing SUPABASE_URL (or VITE_SUPABASE_URL) in SHAB Dashboard/.env.local");
if (!serviceRoleKey) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY in SHAB Dashboard/.env.local");

const base = supabaseUrl.replace(/\/+$/, "");

const requestJson = async (url) => {
  const res = await fetch(url, {
    headers: {
      apikey: serviceRoleKey,
      Authorization: `Bearer ${serviceRoleKey}`,
    },
  });
  const text = await res.text();
  return { status: res.status, statusText: res.statusText, text };
};

const main = async () => {
  const ping = await requestJson(`${base}/rest/v1/attendance_events?select=id&limit=1`);
  console.log(`service-role rest ping: ${ping.status} ${ping.statusText}`);
  console.log(ping.text.slice(0, 250));
};

await main();
