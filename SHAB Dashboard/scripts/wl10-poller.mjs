import "dotenv/config";
import crypto from "node:crypto";
import process from "node:process";

import { createClient } from "@supabase/supabase-js";
import Papa from "papaparse";

const argv = process.argv.slice(2);

const getArg = (key) => {
  const idx = argv.findIndex((a) => a === key || a.startsWith(`${key}=`));
  if (idx === -1) return undefined;
  const entry = argv[idx];
  if (entry.includes("=")) return entry.split("=").slice(1).join("=");
  return argv[idx + 1];
};

const hasFlag = (flag) => argv.includes(flag);

const requiredEnv = (key) => {
  const value = String(process.env[key] ?? "").trim();
  if (!value) throw new Error(`Missing required env var: ${key}`);
  return value;
};

const optionalEnv = (key, fallback = "") => String(process.env[key] ?? fallback).trim();

const sha256Hex = (value) => crypto.createHash("sha256").update(value).digest("hex");

const toIso = (value) => {
  const raw = String(value ?? "").trim();
  if (!raw) return "";
  const ms = Date.parse(raw);
  if (Number.isFinite(ms)) return new Date(ms).toISOString();
  const numeric = Number(raw);
  if (Number.isFinite(numeric) && numeric > 1000000000) return new Date(numeric).toISOString();
  return "";
};

const normalizePunch = (row, { deviceId, userIdField, occurredAtField }) => {
  const staffId = String(row?.[userIdField] ?? row?.userId ?? row?.user_id ?? row?.pin ?? row?.enrollNumber ?? "").trim();
  const occurredAtRaw = row?.[occurredAtField] ?? row?.occurredAt ?? row?.occurred_at ?? row?.timestamp ?? row?.time ?? row?.dateTime;
  const occurredAt = toIso(occurredAtRaw);
  if (!staffId || !occurredAt) return null;
  return { staffId, deviceId, occurredAt };
};

const parseTextAsCsv = (text) => {
  const parsed = Papa.parse(text, { header: true, skipEmptyLines: true });
  if (!parsed?.data || !Array.isArray(parsed.data)) return [];
  return parsed.data.filter((r) => r && typeof r === "object");
};

const parseTextAsJson = (text) => {
  const trimmed = String(text ?? "").trim();
  if (!trimmed) return [];
  const parsed = JSON.parse(trimmed);
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed?.data)) return parsed.data;
  if (Array.isArray(parsed?.records)) return parsed.records;
  return [];
};

const fetchDeviceLogs = async ({ baseUrl, path, authHeader, timeoutMs }) => {
  const url = `${baseUrl.replace(/\/+$/, "")}/${String(path ?? "").replace(/^\/+/, "")}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method: "GET",
      headers: authHeader ? { Authorization: authHeader } : undefined,
      signal: controller.signal,
    });
    const contentType = String(res.headers.get("content-type") ?? "").toLowerCase();
    const text = await res.text();
    if (!res.ok) throw new Error(`WL10 request failed: ${res.status} ${res.statusText}`);
    return { contentType, text };
  } finally {
    clearTimeout(timer);
  }
};

const loadPunches = async ({ baseUrl, path, authHeader, mode, deviceId, userIdField, occurredAtField, timeoutMs }) => {
  const { contentType, text } = await fetchDeviceLogs({ baseUrl, path, authHeader, timeoutMs });
  const looksJson = mode === "json" || contentType.includes("application/json") || text.trim().startsWith("[") || text.trim().startsWith("{");
  const rows = looksJson ? parseTextAsJson(text) : parseTextAsCsv(text);

  const punches = [];
  for (const row of rows) {
    const punch = normalizePunch(row, { deviceId, userIdField, occurredAtField });
    if (punch) punches.push(punch);
  }
  return punches;
};

const buildEventId = ({ deviceId, staffId, occurredAt }) => sha256Hex(`${deviceId}|${staffId}|${occurredAt}`);

const main = async () => {
  const wl10BaseUrl = optionalEnv("WL10_BASE_URL", "http://192.168.1.170");
  const wl10Path = optionalEnv("WL10_LOG_PATH", "/");
  const wl10Mode = optionalEnv("WL10_LOG_MODE", "auto").toLowerCase();
  const wl10UserIdField = optionalEnv("WL10_USER_ID_FIELD", "userId");
  const wl10OccurredAtField = optionalEnv("WL10_OCCURRED_AT_FIELD", "occurredAt");
  const wl10DeviceId = optionalEnv("WL10_DEVICE_ID", "WL10-192.168.1.170");
  const wl10TimeoutMs = Number(optionalEnv("WL10_TIMEOUT_MS", "8000")) || 8000;
  const wl10Username = optionalEnv("WL10_USERNAME");
  const wl10Password = optionalEnv("WL10_PASSWORD");
  const authHeader = wl10Username && wl10Password ? `Basic ${Buffer.from(`${wl10Username}:${wl10Password}`).toString("base64")}` : "";

  const supabaseUrl = requiredEnv("SUPABASE_URL");
  const supabaseServiceRoleKey = requiredEnv("SUPABASE_SERVICE_ROLE_KEY");
  const supabase = createClient(supabaseUrl, supabaseServiceRoleKey, { auth: { persistSession: false } });

  const intervalSeconds = Number(getArg("--interval") ?? optionalEnv("WL10_INTERVAL_SECONDS", "30")) || 30;
  const once = hasFlag("--once");
  const dryRun = hasFlag("--dry-run");

  const runOnce = async () => {
    const punches = await loadPunches({
      baseUrl: wl10BaseUrl,
      path: wl10Path,
      authHeader,
      mode: wl10Mode,
      deviceId: wl10DeviceId,
      userIdField: wl10UserIdField,
      occurredAtField: wl10OccurredAtField,
      timeoutMs: wl10TimeoutMs,
    });

    const rows = punches.map((p) => ({
      id: buildEventId(p),
      staff_id: p.staffId,
      device_id: p.deviceId,
      occurred_at: p.occurredAt,
    }));

    if (dryRun) {
      const sample = rows.slice(0, 5).map((r) => ({ ...r, staff_id: r.staff_id, occurred_at: r.occurred_at }));
      console.log(JSON.stringify({ fetched: rows.length, sample }, null, 2));
      return;
    }

    if (!rows.length) return;

    const { error } = await supabase.from("attendance_events").upsert(rows, { onConflict: "id" });
    if (error) throw error;
  };

  if (once) {
    await runOnce();
    return;
  }

  for (;;) {
    try {
      await runOnce();
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e ?? "Unknown error");
      console.error(`WL10 poll error: ${message}`);
    }
    await new Promise((r) => setTimeout(r, intervalSeconds * 1000));
  }
};

await main();
