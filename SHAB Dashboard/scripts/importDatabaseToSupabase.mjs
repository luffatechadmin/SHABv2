import fs from "node:fs";
import path from "node:path";
import dotenv from "dotenv";
import Papa from "papaparse";
import { createClient } from "@supabase/supabase-js";

const projectRoot = path.resolve(import.meta.dirname, "..");
dotenv.config({ path: path.join(projectRoot, ".env.local"), quiet: true });

const supabaseUrl = process.env.VITE_SUPABASE_URL;
const supabaseServiceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl) throw new Error("Missing VITE_SUPABASE_URL in SHAB Dashboard/.env.local");
if (!supabaseServiceRoleKey) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY in SHAB Dashboard/.env.local");

const supabase = createClient(supabaseUrl, supabaseServiceRoleKey, { auth: { persistSession: false } });

const databaseDir = path.resolve(projectRoot, "Database");

const shouldDeletePlaceholderStaff = (row) => {
  const id = typeof row?.id === "string" ? row.id.trim() : "";
  if (!id || !id.startsWith("U")) return false;
  const fullName = typeof row?.full_name === "string" ? row.full_name.trim() : "";
  const email = typeof row?.email === "string" ? row.email.trim() : "";
  return !email || !fullName || fullName === id;
};

const readCsv = (fileName) => {
  const filePath = path.join(databaseDir, fileName);
  const csv = fs.readFileSync(filePath, "utf8");
  const parsed = Papa.parse(csv, { header: true, skipEmptyLines: true, dynamicTyping: false });
  if (parsed.errors?.length) {
    const first = parsed.errors[0];
    throw new Error(`${fileName} parse error at row ${first.row}: ${first.message}`);
  }
  return parsed.data;
};

const toTrimmedString = (value) => {
  if (value === null || value === undefined) return null;
  const s = String(value).trim();
  return s.length ? s : null;
};

const toNumberOrNull = (value) => {
  const s = toTrimmedString(value);
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
};

const parseUsDateTimeToIsoOrNull = (value) => {
  const s = toTrimmedString(value);
  if (!s) return null;
  const [datePart, timePart] = s.split(" ");
  if (!datePart || !timePart) return null;
  const [mRaw, dRaw, yRaw] = datePart.split("/");
  const [hRaw, minRaw] = timePart.split(":");
  const m = Number(mRaw);
  const d = Number(dRaw);
  const y = Number(yRaw);
  const h = Number(hRaw);
  const min = Number(minRaw);
  if (![m, d, y, h, min].every((n) => Number.isFinite(n))) return null;
  const dt = new Date(y, m - 1, d, h, min, 0, 0);
  if (Number.isNaN(dt.getTime())) return null;
  return dt.toISOString();
};

const extractUnitSymbol = (unitName) => {
  const s = toTrimmedString(unitName);
  if (!s) return null;
  const match = s.match(/\(([^)]+)\)/);
  return match?.[1]?.trim() || null;
};

const chunk = (arr, size) => {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
};

const upsertAll = async (table, records) => {
  if (!records.length) return { inserted: 0 };
  let total = 0;
  for (const part of chunk(records, 500)) {
    const { error } = await supabase.from(table).upsert(part, { onConflict: "id" });
    if (error) throw new Error(`${table} upsert failed: ${error.message}`);
    total += part.length;
  }
  return { inserted: total };
};

const ensureTableReachable = async (table) => {
  const { error } = await supabase.from(table).select("id").limit(1);
  if (!error) return;
  throw new Error(
    `${table} is not reachable via PostgREST: ${error.message}. Run SHAB Dashboard/supabase_schema.sql in Supabase SQL Editor, then re-run this script.`
  );
};

const run = async () => {
  const resetStaff = ["1", "true", "yes", "y", "on"].includes(String(process.env.RESET_STAFF || "").trim().toLowerCase());

  const tables = ["customers", "suppliers", "staff", "units", "products", "raw_materials", "bom"];
  const missing = [];
  for (const table of tables) {
    try {
      await ensureTableReachable(table);
    } catch {
      missing.push(table);
    }
  }
  if (missing.length) {
    throw new Error(
      `Missing tables: ${missing.join(", ")}. Run SHAB Dashboard/supabase_schema.sql in Supabase SQL Editor, then re-run this script.`
    );
  }

  const customersRows = readCsv("Database - Customers.csv");
  const customers = customersRows
    .map((r) => ({
      id: toTrimmedString(r["Customer ID"]),
      company_name: toTrimmedString(r["Company Name"]),
      brand_name: toTrimmedString(r["Brand Name"]),
      country: toTrimmedString(r["Country"]),
      contact_person: toTrimmedString(r["Contact Person"]),
      email: toTrimmedString(r["Email"]),
      phone: toTrimmedString(r["Phone"]),
      type: toTrimmedString(r["Type"]),
    }))
    .filter((r) => r.id);

  const suppliersRows = readCsv("Database - Suppliers.csv");
  const suppliers = suppliersRows
    .map((r) => ({
      id: toTrimmedString(r["Supplier ID"]),
      name: toTrimmedString(r["Supplier Name"]),
      country: toTrimmedString(r["Country"]),
      contact_person: toTrimmedString(r["Contact Person"]),
      email: toTrimmedString(r["Email"]),
      phone: toTrimmedString(r["Phone"]),
      lead_time_days: toNumberOrNull(r["Lead Time (Days)"]),
      status: toTrimmedString(r["Status"]),
    }))
    .filter((r) => r.id);

  const staffRows = readCsv("Database - Staff.csv");
  const staff = staffRows
    .map((r) => ({
      id: toTrimmedString(r["User ID"]),
      full_name: toTrimmedString(r["Full Name"]),
      email: toTrimmedString(r["Email"]),
      role: toTrimmedString(r["Role"]),
      department: toTrimmedString(r["Department"]),
      username: toTrimmedString(r["Username"]),
      password_hash: toTrimmedString(r["Password Hash"]),
      status: toTrimmedString(r["Status"]),
      date_joined: parseUsDateTimeToIsoOrNull(r["Date Joined"]),
      last_login: parseUsDateTimeToIsoOrNull(r["Last Login"]),
    }))
    .filter((r) => r.id);

  const unitsRows = readCsv("Database - Measurement Unit.csv");
  const units = unitsRows
    .map((r) => ({
      id: toTrimmedString(r["Unit Code"]),
      name: toTrimmedString(r["Unit Name"]),
      unit_type: toTrimmedString(r["Type"]),
      conversion_base: toTrimmedString(r["Conversion Base"]),
      conversion_rate: toNumberOrNull(r["Conversion Rate"]),
      symbol: extractUnitSymbol(r["Unit Name"]),
    }))
    .filter((r) => r.id);

  const productsRows = readCsv("Database - Products.csv");
  const products = productsRows
    .map((r) => ({
      id: toTrimmedString(r["SKU"]),
      name: toTrimmedString(r["Product Name"]),
      variant: toTrimmedString(r["Variant"]),
      category: toTrimmedString(r["Category"]),
      size: toNumberOrNull(r["Size"]),
      unit: toTrimmedString(r["Unit"]),
      version: toTrimmedString(r["Version"]),
      status: toTrimmedString(r["Status"]),
    }))
    .filter((r) => r.id);

  const rawMaterialsRows = readCsv("Database - Raw Materials.csv");
  const rawMaterials = rawMaterialsRows
    .map((r) => ({
      id: toTrimmedString(r["Material Code"]),
      name: toTrimmedString(r["Material Name"]),
      inci_name: toTrimmedString(r["INCI Name"]),
      spec_grade: toTrimmedString(r["Spec Grade"]),
      unit: toTrimmedString(r["Unit"]),
      shelf_life_months: toNumberOrNull(r["Shelf Life (Months)"]),
      storage_condition: toTrimmedString(r["Storage Condition"]),
      status: toTrimmedString(r["Status"]),
    }))
    .filter((r) => r.id);

  const bomRows = readCsv("Database - Bill of Materials.csv");
  const bom = bomRows
    .map((r) => ({
      id: toTrimmedString(r["BOM ID"]),
      product_id: toTrimmedString(r["Product SKU"]),
      raw_material_id: toTrimmedString(r["Material Code"]),
      raw_material_name: toTrimmedString(r["Material Name"]),
      quantity: toNumberOrNull(r["Quantity"]),
      unit: toTrimmedString(r["Unit"]),
      stage: toTrimmedString(r["Stage"]),
    }))
    .filter((r) => r.id);

  const results = [];
  results.push(["customers", await upsertAll("customers", customers)]);
  results.push(["suppliers", await upsertAll("suppliers", suppliers)]);
  results.push(["staff", await upsertAll("staff", staff)]);
  results.push(["units", await upsertAll("units", units)]);
  results.push(["products", await upsertAll("products", products)]);
  results.push(["raw_materials", await upsertAll("raw_materials", rawMaterials)]);
  results.push(["bom", await upsertAll("bom", bom)]);

  if (resetStaff) {
    const csvIds = new Set(staff.map((s) => s.id));
    const { data: existingStaff, error: staffFetchError } = await supabase.from("staff").select("id,full_name,email").limit(1000);
    if (staffFetchError) throw new Error(`staff select failed: ${staffFetchError.message}`);

    const rows = Array.isArray(existingStaff) ? existingStaff : [];
    const toDelete = rows
      .filter((r) => !csvIds.has(r.id) && shouldDeletePlaceholderStaff(r))
      .map((r) => r.id)
      .filter(Boolean);

    let deleted = 0;
    for (const part of chunk(toDelete, 200)) {
      const { error } = await supabase.from("staff").delete().in("id", part);
      if (error) throw new Error(`staff delete failed: ${error.message}`);
      deleted += part.length;
    }

    process.stdout.write(`staff_cleanup: deleted ${deleted}\n`);
  }

  for (const [table, r] of results) {
    process.stdout.write(`${table}: upserted ${r.inserted}\n`);
  }
};

run().catch((e) => {
  process.stderr.write(`${e instanceof Error ? e.message : String(e)}\n`);
  process.exitCode = 1;
});
