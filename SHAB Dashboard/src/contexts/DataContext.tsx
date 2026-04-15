import { ReactNode, useCallback, useEffect, useMemo, useState } from "react";

import { DataContext } from "./data-context";
import { isSupabaseConfigured, supabase } from "../lib/supabase";
import type {
  AttendanceEvent,
  AttendanceInterval,
  AttendanceRecord,
  AuditActor,
  AuditEvent,
  AppModule,
  ApBill,
  ApPayment,
  DataContextType,
  FinanceAccount,
  FinanceJournalEntry,
  FinanceJournalLine,
  GoodsReceipt,
  ArReceipt,
  MasterDataKey,
  MasterDataState,
  PurchaseOrder,
  Staff,
  StockLot,
  StockMovement,
  WorkOrder,
  ProductLot,
  ProductMovement,
  SalesOrder,
  DeliveryNote,
  Invoice,
  UserRole,
} from "./master-data-types";
import { defaultModulesForRole, defaultModulesForRoles } from "./master-data-types";

const STORAGE_KEY_V1 = "shab:masterData:v1";
const STORAGE_KEY_V2 = "shab:appData:v2";

const normalizeModules = (value: unknown, roles: UserRole[]): AppModule[] => {
  if (Array.isArray(value)) {
    const cleaned = value.map((v) => String(v).trim()).filter(Boolean);
    const allowed = new Set<AppModule>(["inventory", "production", "sales", "finance", "hr"]);
    const unique = Array.from(new Set(cleaned)).filter((v): v is AppModule => allowed.has(v as AppModule));
    return unique.length ? unique : defaultModulesForRoles(roles);
  }
  return defaultModulesForRoles(roles);
};

const normalizeStaff = (raw: unknown): Staff | null => {
  if (!raw || typeof raw !== "object") return null;
  const row = raw as Partial<Staff> & {
    name?: unknown;
    password?: unknown;
    full_name?: unknown;
    password_hash?: unknown;
    date_joined?: unknown;
    last_login?: unknown;
  };

  const id = String(row.id ?? "").trim();
  const fullName = String((row.fullName ?? row.full_name ?? row.name ?? "") as unknown).trim();
  const username = String(row.username ?? "").trim();
  const passwordHash = String((row.passwordHash ?? row.password_hash ?? row.password ?? "") as unknown).trim();
  const email = String(row.email ?? "").trim() || undefined;
  const department = String(row.department ?? "").trim() || undefined;

  const toRole = (value: unknown): UserRole | null => {
    const raw = String(value ?? "").trim().toLowerCase();
    if (!raw) return null;
    const mapped = raw === "operations" || raw === "staff" ? "operator" : raw === "procurement" ? "procurement_manager" : raw === "sales" ? "sales_manager" : raw === "manager" ? "production_manager" : raw;
    const allowed: UserRole[] = [
      "superadmin",
      "supervisor",
      "operator",
      "production_manager",
      "procurement_manager",
      "finance_manager",
      "sales_manager",
      "manager",
      "staff",
      "operations",
      "procurement",
      "sales",
    ];
    return allowed.includes(mapped as UserRole) ? (mapped as UserRole) : null;
  };

  const rawRoles = (row as { roles?: unknown }).roles;
  const roles: UserRole[] = Array.isArray(rawRoles) ? rawRoles.map(toRole).filter(Boolean) as UserRole[] : [];
  const primary = toRole(row.role) ?? "operator";
  const effectiveRoles = roles.length ? roles : [primary];
  const role = effectiveRoles[0] ?? primary;

  const rawStatus = String(row.status ?? "active").trim().toLowerCase();
  const status: Staff["status"] = rawStatus === "inactive" ? "inactive" : "active";

  const dateJoined = String((row.dateJoined ?? row.date_joined ?? "") as unknown).trim() || undefined;
  const lastLogin = String((row.lastLogin ?? row.last_login ?? "") as unknown).trim() || undefined;

  if (!id || !fullName || !username || !passwordHash) return null;
  return {
    id,
    fullName,
    email,
    username,
    passwordHash,
    role,
    roles: effectiveRoles,
    department,
    status,
    dateJoined,
    lastLogin,
    modules: normalizeModules((row as { modules?: unknown }).modules, effectiveRoles),
  };
};

const normalizeAttendanceRecord = (raw: unknown): AttendanceRecord | null => {
  if (!raw || typeof raw !== "object") return null;
  const row = raw as Partial<AttendanceRecord> & {
    staff_id?: unknown;
    clock_in?: unknown;
    clock_out?: unknown;
  };
  const id = String(row.id ?? "").trim();
  const staffId = String((row.staffId ?? row.staff_id ?? "") as unknown).trim();
  const date = String(row.date ?? "").trim();
  const clockIn = String((row.clockIn ?? row.clock_in ?? "") as unknown).trim();
  const clockOut = String((row.clockOut ?? row.clock_out ?? "") as unknown).trim() || undefined;
  if (!id || !staffId || !date || !clockIn) return null;
  return { id, staffId, date, clockIn, clockOut };
};

const normalizeAttendanceEvent = (raw: unknown): AttendanceEvent | null => {
  if (!raw || typeof raw !== "object") return null;
  const row = raw as Partial<AttendanceEvent> & {
    staff_id?: unknown;
    device_id?: unknown;
    occurred_at?: unknown;
    event_date?: unknown;
  };

  const id = String(row.id ?? "").trim();
  const staffId = String((row.staffId ?? row.staff_id ?? "") as unknown).trim();
  const deviceId = String((row.deviceId ?? row.device_id ?? "") as unknown).trim();
  const occurredAt = String((row.occurredAt ?? row.occurred_at ?? "") as unknown).trim();
  const eventDate = String((row.eventDate ?? row.event_date ?? "") as unknown).trim();
  if (!id || !staffId || !deviceId || !occurredAt || !eventDate) return null;
  return { id, staffId, deviceId, occurredAt, eventDate };
};

const normalizeAttendanceInterval = (raw: unknown): AttendanceInterval | null => {
  if (!raw || typeof raw !== "object") return null;
  const row = raw as Partial<AttendanceInterval> & {
    staff_id?: unknown;
    event_date?: unknown;
    start_at?: unknown;
    end_at?: unknown;
  };

  const id = String(row.id ?? "").trim();
  const staffId = String((row.staffId ?? row.staff_id ?? "") as unknown).trim();
  const date = String((row.date ?? row.event_date ?? "") as unknown).trim();
  const kind = String(row.kind ?? "").trim().toLowerCase();
  const startAt = String((row.startAt ?? row.start_at ?? "") as unknown).trim();
  const endAt = String((row.endAt ?? row.end_at ?? "") as unknown).trim() || undefined;
  if (!id || !staffId || !date || !startAt) return null;
  if (kind !== "work" && kind !== "break") return null;
  return { id, staffId, date, kind: kind as AttendanceInterval["kind"], startAt, endAt };
};

const defaultStaff: Staff[] = [
  {
    id: "SA0001",
    fullName: "Superadmin",
    email: "superadmin@company.com",
    role: "superadmin",
    department: "Management",
    username: "superadmin",
    passwordHash: "abcd1234",
    status: "active",
    modules: defaultModulesForRole("superadmin"),
  },
  {
    id: "MG0001",
    fullName: "Manager",
    email: "manager@company.com",
    role: "production_manager",
    roles: ["production_manager"],
    department: "Management",
    username: "manager",
    passwordHash: "abcd1234",
    status: "active",
    modules: defaultModulesForRole("production_manager"),
  },
  {
    id: "OP0001",
    fullName: "Operations",
    email: "operations@company.com",
    role: "operator",
    roles: ["operator"],
    department: "Operations",
    username: "operations",
    passwordHash: "abcd1234",
    status: "active",
    modules: defaultModulesForRole("operator"),
  },
  {
    id: "PR0001",
    fullName: "Procurement",
    email: "procurement@company.com",
    role: "procurement_manager",
    roles: ["procurement_manager"],
    department: "Procurement",
    username: "procurement",
    passwordHash: "abcd1234",
    status: "active",
    modules: defaultModulesForRole("procurement_manager"),
  },
  {
    id: "SL0001",
    fullName: "Sales",
    email: "sales@company.com",
    role: "sales_manager",
    roles: ["sales_manager"],
    department: "Sales",
    username: "sales",
    passwordHash: "abcd1234",
    status: "active",
    modules: defaultModulesForRole("sales_manager"),
  },
];

const defaultAttendanceRecords: AttendanceRecord[] = (() => {
  const today = new Date();
  const ymd = (d: Date) => d.toISOString().slice(0, 10);
  const daysAgo = (n: number) => {
    const d = new Date(today);
    d.setDate(d.getDate() - n);
    return ymd(d);
  };

  return [
    { id: "AT0001", staffId: "SA0001", date: daysAgo(0), clockIn: "08:45", clockOut: "17:30" },
    { id: "AT0002", staffId: "MG0001", date: daysAgo(0), clockIn: "08:20", clockOut: "18:05" },
    { id: "AT0003", staffId: "OP0001", date: daysAgo(0), clockIn: "09:05" },
    { id: "AT0004", staffId: "PR0001", date: daysAgo(1), clockIn: "08:35", clockOut: "17:40" },
    { id: "AT0005", staffId: "SL0001", date: daysAgo(1), clockIn: "08:55", clockOut: "17:15" },
    { id: "AT0006", staffId: "OP0001", date: daysAgo(2), clockIn: "09:10", clockOut: "17:05" },
    { id: "AT0007", staffId: "MG0001", date: daysAgo(2), clockIn: "08:25", clockOut: "18:10" },
  ];
})();

const defaultAttendanceEvents: AttendanceEvent[] = [];

const defaultAttendanceIntervals: AttendanceInterval[] = defaultAttendanceRecords.map((r) => ({
  id: `AI-${r.id}`,
  staffId: r.staffId,
  date: r.date,
  kind: "work",
  startAt: `${r.date}T${r.clockIn}:00`,
  endAt: r.clockOut ? `${r.date}T${r.clockOut}:00` : undefined,
}));

const emptyState: MasterDataState = {
  staff: defaultStaff,
  customers: [],
  suppliers: [],
  products: [],
  rawMaterials: [],
  units: [],
  bom: [],
};

type AppState = MasterDataState & {
  purchaseOrders: PurchaseOrder[];
  goodsReceipts: GoodsReceipt[];
  stockLots: StockLot[];
  stockMovements: StockMovement[];
  attendanceRecords: AttendanceRecord[];
  attendanceEvents: AttendanceEvent[];
  attendanceIntervals: AttendanceInterval[];
  workOrders: WorkOrder[];
  productLots: ProductLot[];
  productMovements: ProductMovement[];
  salesOrders: SalesOrder[];
  deliveryNotes: DeliveryNote[];
  invoices: Invoice[];
  financeAccounts: FinanceAccount[];
  financeJournalEntries: FinanceJournalEntry[];
  financeJournalLines: FinanceJournalLine[];
  apBills: ApBill[];
  apPayments: ApPayment[];
  arReceipts: ArReceipt[];
};

type StoredState = AppState & { audit: AuditEvent[] };

const emptyAudit: AuditEvent[] = [];
const emptyAppState: AppState = {
  ...emptyState,
  purchaseOrders: [],
  goodsReceipts: [],
  stockLots: [],
  stockMovements: [],
  attendanceRecords: defaultAttendanceRecords,
  attendanceEvents: defaultAttendanceEvents,
  attendanceIntervals: defaultAttendanceIntervals,
  workOrders: [],
  productLots: [],
  productMovements: [],
  salesOrders: [],
  deliveryNotes: [],
  invoices: [],
  financeAccounts: [],
  financeJournalEntries: [],
  financeJournalLines: [],
  apBills: [],
  apPayments: [],
  arReceipts: [],
};

const parseStoredState = (raw: string | null): StoredState | null => {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as Partial<StoredState & AppState>;
    if (!parsed || typeof parsed !== "object") return null;
    const normalizedStaff = Array.isArray(parsed.staff) ? parsed.staff.map(normalizeStaff).filter(Boolean) : [];
    const normalizedAttendance = Array.isArray(parsed.attendanceRecords)
      ? parsed.attendanceRecords.map(normalizeAttendanceRecord).filter(Boolean)
      : [];
    const normalizedAttendanceEvents = Array.isArray((parsed as { attendanceEvents?: unknown }).attendanceEvents)
      ? ((parsed as { attendanceEvents?: unknown[] }).attendanceEvents ?? []).map(normalizeAttendanceEvent).filter(Boolean)
      : [];
    const normalizedAttendanceIntervals = Array.isArray((parsed as { attendanceIntervals?: unknown }).attendanceIntervals)
      ? ((parsed as { attendanceIntervals?: unknown[] }).attendanceIntervals ?? []).map(normalizeAttendanceInterval).filter(Boolean)
      : [];
    return {
      staff: normalizedStaff.length ? (normalizedStaff as Staff[]) : defaultStaff,
      customers: Array.isArray(parsed.customers) ? parsed.customers : [],
      suppliers: Array.isArray(parsed.suppliers) ? parsed.suppliers : [],
      products: Array.isArray(parsed.products) ? parsed.products : [],
      rawMaterials: Array.isArray(parsed.rawMaterials) ? parsed.rawMaterials : [],
      units: Array.isArray(parsed.units) ? parsed.units : [],
      bom: Array.isArray(parsed.bom) ? parsed.bom : [],
      purchaseOrders: Array.isArray(parsed.purchaseOrders) ? parsed.purchaseOrders : [],
      goodsReceipts: Array.isArray(parsed.goodsReceipts) ? parsed.goodsReceipts : [],
      stockLots: Array.isArray(parsed.stockLots) ? parsed.stockLots : [],
      stockMovements: Array.isArray(parsed.stockMovements) ? parsed.stockMovements : [],
      attendanceRecords: normalizedAttendance.length ? (normalizedAttendance as AttendanceRecord[]) : defaultAttendanceRecords,
      attendanceEvents: normalizedAttendanceEvents.length ? (normalizedAttendanceEvents as AttendanceEvent[]) : defaultAttendanceEvents,
      attendanceIntervals: normalizedAttendanceIntervals.length
        ? (normalizedAttendanceIntervals as AttendanceInterval[])
        : defaultAttendanceIntervals,
      workOrders: Array.isArray(parsed.workOrders) ? (parsed.workOrders as WorkOrder[]) : [],
      productLots: Array.isArray(parsed.productLots) ? (parsed.productLots as ProductLot[]) : [],
      productMovements: Array.isArray(parsed.productMovements) ? (parsed.productMovements as ProductMovement[]) : [],
      salesOrders: Array.isArray(parsed.salesOrders) ? (parsed.salesOrders as SalesOrder[]) : [],
      deliveryNotes: Array.isArray(parsed.deliveryNotes) ? (parsed.deliveryNotes as DeliveryNote[]) : [],
      invoices: Array.isArray(parsed.invoices) ? (parsed.invoices as Invoice[]) : [],
      financeAccounts: Array.isArray(parsed.financeAccounts) ? (parsed.financeAccounts as FinanceAccount[]) : [],
      financeJournalEntries: Array.isArray(parsed.financeJournalEntries) ? (parsed.financeJournalEntries as FinanceJournalEntry[]) : [],
      financeJournalLines: Array.isArray(parsed.financeJournalLines) ? (parsed.financeJournalLines as FinanceJournalLine[]) : [],
      apBills: Array.isArray(parsed.apBills) ? (parsed.apBills as ApBill[]) : [],
      apPayments: Array.isArray(parsed.apPayments) ? (parsed.apPayments as ApPayment[]) : [],
      arReceipts: Array.isArray(parsed.arReceipts) ? (parsed.arReceipts as ArReceipt[]) : [],
      audit: Array.isArray(parsed.audit) ? parsed.audit : emptyAudit,
    };
  } catch {
    return null;
  }
};

const newId = (): string => {
  try {
    return crypto.randomUUID();
  } catch {
    return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  }
};

const nowIso = (): string => new Date().toISOString();

type SupabaseSyncKey = "staff" | "suppliers" | "units" | "products" | "rawMaterials" | "bom";

const isSupabaseSyncKey = (key: MasterDataKey): key is SupabaseSyncKey =>
  key === "staff" || key === "suppliers" || key === "units" || key === "products" || key === "rawMaterials" || key === "bom";

const upsertToSupabase = async (key: SupabaseSyncKey, records: Array<Record<string, unknown>>) => {
  if (!isSupabaseConfigured) return;

  if (key === "staff") {
    const payload = records.map((r) => ({
      id: r.id,
      full_name: r.fullName,
      email: r.email ?? null,
      role: r.role ?? null,
      roles: Array.isArray(r.roles) ? r.roles : r.role ? [r.role] : null,
      department: r.department ?? null,
      username: r.username,
      password_hash: r.passwordHash,
      status: r.status ?? null,
      date_joined: r.dateJoined ?? null,
      last_login: r.lastLogin ?? null,
      modules: Array.isArray(r.modules) ? r.modules : null,
      updated_at: nowIso(),
    }));
    const { error } = await supabase.from("staff").upsert(payload, { onConflict: "id" });
    if (error) {
      const basePayload = records.map((r) => ({
        id: r.id,
        full_name: r.fullName,
        email: r.email ?? null,
        role: r.role ?? null,
        department: r.department ?? null,
        username: r.username,
        password_hash: r.passwordHash,
        status: r.status ?? null,
        date_joined: r.dateJoined ?? null,
        last_login: r.lastLogin ?? null,
        updated_at: nowIso(),
      }));
      const { error: fallbackError } = await supabase.from("staff").upsert(basePayload, { onConflict: "id" });
      if (fallbackError) console.error(fallbackError);
    }
    return;
  }

  if (key === "suppliers") {
    const payload = records.map((r) => ({
      id: r.id,
      name: r.name,
      country: r.country ?? null,
      contact_person: r.contactPerson ?? null,
      email: r.email ?? null,
      phone: r.phone ?? null,
      lead_time_days: r.leadTimeDays ?? null,
      status: r.status ?? null,
      updated_at: nowIso(),
    }));
    const { error } = await supabase.from("suppliers").upsert(payload, { onConflict: "id" });
    if (error) console.error(error);
    return;
  }

  if (key === "units") {
    const payload = records.map((r) => ({
      id: r.id,
      name: r.name,
      unit_type: r.unitType ?? null,
      conversion_base: r.conversionBase ?? null,
      conversion_rate: r.conversionRate ?? null,
      symbol: r.symbol ?? null,
      updated_at: nowIso(),
    }));
    const { error } = await supabase.from("units").upsert(payload, { onConflict: "id" });
    if (error) console.error(error);
    return;
  }

  if (key === "products") {
    const payload = records.map((r) => ({
      id: r.id,
      name: r.name,
      variant: r.variant ?? null,
      category: r.category ?? null,
      size: r.size ?? null,
      unit: r.unit ?? null,
      version: r.version ?? null,
      status: r.status ?? null,
      updated_at: nowIso(),
    }));
    const { error } = await supabase.from("products").upsert(payload, { onConflict: "id" });
    if (error) console.error(error);
    return;
  }

  if (key === "rawMaterials") {
    const payload = records.map((r) => ({
      id: r.id,
      name: r.name,
      inci_name: r.inciName ?? null,
      spec_grade: r.specGrade ?? null,
      unit: r.unit ?? null,
      shelf_life_months: r.shelfLifeMonths ?? null,
      storage_condition: r.storageCondition ?? null,
      status: r.status ?? null,
      updated_at: nowIso(),
    }));
    const { error } = await supabase.from("raw_materials").upsert(payload, { onConflict: "id" });
    if (error) console.error(error);
    return;
  }

  if (key === "bom") {
    const payload = records.map((r) => ({
      id: r.id,
      product_id: r.productId,
      raw_material_id: r.rawMaterialId,
      raw_material_name: r.rawMaterialName ?? null,
      quantity: r.quantity,
      unit: r.unit ?? null,
      stage: r.stage ?? null,
      updated_at: nowIso(),
    }));
    const { error } = await supabase.from("bom").upsert(payload, { onConflict: "id" });
    if (error) console.error(error);
  }
};

const upsertAttendanceToSupabase = async (records: AttendanceRecord[]) => {
  if (!isSupabaseConfigured) return;
  const payload = records.map((r) => ({
    id: r.id,
    staff_id: r.staffId,
    date: r.date,
    clock_in: r.clockIn,
    clock_out: r.clockOut ?? null,
    updated_at: nowIso(),
  }));
  const { error } = await supabase.from("attendance_records").upsert(payload, { onConflict: "id" });
  if (error) console.error(error);
};

const deleteFromSupabase = async (key: SupabaseSyncKey, id: string) => {
  if (!isSupabaseConfigured) return;
  if (key === "rawMaterials") {
    const { error } = await supabase.from("raw_materials").delete().eq("id", id);
    if (error) console.error(error);
    return;
  }
  const table = key === "bom" ? "bom" : key;
  const { error } = await supabase.from(table).delete().eq("id", id);
  if (error) console.error(error);
};

export function DataProvider({ children }: { children: ReactNode }) {
  const [isLoading, setIsLoading] = useState(true);
  const [state, setState] = useState<AppState>(emptyAppState);
  const [audit, setAudit] = useState<AuditEvent[]>(emptyAudit);

  const refreshAttendanceFromSupabase = useCallback(async () => {
    if (!isSupabaseConfigured) return;
    const [attendanceRes, attendanceEventsRes, attendanceIntervalsRes] = await Promise.all([
      supabase.from("attendance_records").select("*"),
      supabase.from("attendance_events").select("*"),
      supabase.from("attendance_intervals").select("*"),
    ]);

    const attendanceRows = Array.isArray(attendanceRes.data) ? attendanceRes.data : [];
    const attendanceEventsRows = Array.isArray(attendanceEventsRes.data) ? attendanceEventsRes.data : [];
    const attendanceIntervalsRows = Array.isArray(attendanceIntervalsRes.data) ? attendanceIntervalsRes.data : [];

    const attendanceLoaded = attendanceRows.map(normalizeAttendanceRecord).filter(Boolean) as AttendanceRecord[];
    const attendanceEventsLoaded = attendanceEventsRows.map(normalizeAttendanceEvent).filter(Boolean) as AttendanceEvent[];
    const attendanceIntervalsLoaded = attendanceIntervalsRows.map(normalizeAttendanceInterval).filter(Boolean) as AttendanceInterval[];

    setState((prev) => ({
      ...prev,
      attendanceRecords: attendanceLoaded.length ? attendanceLoaded : defaultAttendanceRecords,
      attendanceEvents: attendanceEventsLoaded.length ? attendanceEventsLoaded : defaultAttendanceEvents,
      attendanceIntervals: attendanceIntervalsLoaded.length ? attendanceIntervalsLoaded : defaultAttendanceIntervals,
    }));
  }, []);

  useEffect(() => {
    const boot = async () => {
      if (isSupabaseConfigured) {
        try {
          const [staffRes, customersRes, suppliersRes, unitsRes, productsRes, rawRes, bomRes, attendanceRes, attendanceEventsRes, attendanceIntervalsRes] = await Promise.all([
            supabase.from("staff").select("*"),
            supabase.from("customers").select("*"),
            supabase.from("suppliers").select("*"),
            supabase.from("units").select("*"),
            supabase.from("products").select("*"),
            supabase.from("raw_materials").select("*"),
            supabase.from("bom").select("*"),
            supabase.from("attendance_records").select("*"),
            supabase.from("attendance_events").select("*"),
            supabase.from("attendance_intervals").select("*"),
          ]);
          const staffRows = Array.isArray(staffRes.data) ? staffRes.data : [];
          const customersRows = Array.isArray(customersRes.data) ? customersRes.data : [];
          const suppliersRows = Array.isArray(suppliersRes.data) ? suppliersRes.data : [];
          const unitsRows = Array.isArray(unitsRes.data) ? unitsRes.data : [];
          const productsRows = Array.isArray(productsRes.data) ? productsRes.data : [];
          const rawRows = Array.isArray(rawRes.data) ? rawRes.data : [];
          const bomRows = Array.isArray(bomRes.data) ? bomRes.data : [];
          const attendanceRows = Array.isArray(attendanceRes.data) ? attendanceRes.data : [];
          const attendanceEventsRows = Array.isArray(attendanceEventsRes.data) ? attendanceEventsRes.data : [];
          const attendanceIntervalsRows = Array.isArray(attendanceIntervalsRes.data) ? attendanceIntervalsRes.data : [];
          const staffLoaded = staffRows.map(normalizeStaff).filter(Boolean) as Staff[];
          const staffNeedsRoleSync = staffRows.some((row) => {
            const normalized = normalizeStaff(row);
            if (!normalized) return false;
            const rawRole = String((row as { role?: unknown }).role ?? "").trim().toLowerCase();
            const normalizedRole = normalized.role.trim().toLowerCase();
            if (rawRole && rawRole !== normalizedRole) return true;
            const rawRoles = Array.isArray((row as { roles?: unknown }).roles)
              ? ((row as { roles?: unknown[] }).roles ?? []).map((r) => String(r ?? "").trim().toLowerCase()).filter(Boolean)
              : [];
            const normalizedRoles = (normalized.roles?.length ? normalized.roles : [normalized.role]).map((r) => r.trim().toLowerCase()).filter(Boolean);
            const rawSet = Array.from(new Set(rawRoles)).sort().join("|");
            const normalizedSet = Array.from(new Set(normalizedRoles)).sort().join("|");
            return rawSet !== normalizedSet;
          });
          const customersLoaded = customersRows.map((r) => ({
            id: String((r as { id: unknown }).id ?? "").trim(),
            name: String((r as { name?: unknown }).name ?? "").trim(),
            phone: String((r as { phone?: unknown }).phone ?? "").trim() || undefined,
            email: String((r as { email?: unknown }).email ?? "").trim() || undefined,
            address: String((r as { address?: unknown }).address ?? "").trim() || undefined,
          })).filter((c) => c.id && c.name);
          const suppliersLoaded = suppliersRows.map((r) => ({
            id: String((r as { id: unknown }).id ?? "").trim(),
            name: String((r as { name?: unknown }).name ?? "").trim(),
            country: String((r as { country?: unknown }).country ?? "").trim() || undefined,
            contactPerson: String((r as { contact_person?: unknown }).contact_person ?? "").trim() || undefined,
            email: String((r as { email?: unknown }).email ?? "").trim() || undefined,
            phone: String((r as { phone?: unknown }).phone ?? "").trim() || undefined,
            leadTimeDays: Number((r as { lead_time_days?: unknown }).lead_time_days ?? "") || undefined,
            status: String((r as { status?: unknown }).status ?? "").trim() || undefined,
          })).filter((s) => s.id && s.name);
          const unitsLoaded = unitsRows.map((r) => ({
            id: String((r as { id: unknown }).id ?? "").trim(),
            name: String((r as { name?: unknown }).name ?? "").trim(),
            unitType: String((r as { unit_type?: unknown }).unit_type ?? "").trim() || undefined,
            conversionBase: String((r as { conversion_base?: unknown }).conversion_base ?? "").trim() || undefined,
            conversionRate: Number((r as { conversion_rate?: unknown }).conversion_rate ?? "") || undefined,
            symbol: String((r as { symbol?: unknown }).symbol ?? "").trim() || undefined,
          })).filter((u) => u.id && u.name);
          const productsLoaded = productsRows.map((r) => ({
            id: String((r as { id: unknown }).id ?? "").trim(),
            name: String((r as { name?: unknown }).name ?? "").trim(),
            variant: String((r as { variant?: unknown }).variant ?? "").trim() || undefined,
            category: String((r as { category?: unknown }).category ?? "").trim() || undefined,
            size: Number((r as { size?: unknown }).size ?? "") || undefined,
            unit: String((r as { unit?: unknown }).unit ?? "").trim() || undefined,
            version: String((r as { version?: unknown }).version ?? "").trim() || undefined,
            status: String((r as { status?: unknown }).status ?? "").trim() || undefined,
          })).filter((p) => p.id && p.name);
          const rawMaterialsLoaded = rawRows.map((r) => ({
            id: String((r as { id: unknown }).id ?? "").trim(),
            name: String((r as { name?: unknown }).name ?? "").trim(),
            inciName: String((r as { inci_name?: unknown }).inci_name ?? "").trim() || undefined,
            specGrade: String((r as { spec_grade?: unknown }).spec_grade ?? "").trim() || undefined,
            unit: String((r as { unit?: unknown }).unit ?? "").trim() || undefined,
            shelfLifeMonths: Number((r as { shelf_life_months?: unknown }).shelf_life_months ?? "") || undefined,
            storageCondition: String((r as { storage_condition?: unknown }).storage_condition ?? "").trim() || undefined,
            status: String((r as { status?: unknown }).status ?? "").trim() || undefined,
          })).filter((m) => m.id && m.name);
          const bomLoaded = bomRows.map((r) => ({
            id: String((r as { id: unknown }).id ?? "").trim(),
            productId: String((r as { product_id?: unknown }).product_id ?? "").trim(),
            rawMaterialId: String((r as { raw_material_id?: unknown }).raw_material_id ?? "").trim(),
            rawMaterialName: String((r as { raw_material_name?: unknown }).raw_material_name ?? "").trim() || undefined,
            quantity: Number((r as { quantity?: unknown }).quantity ?? 0),
            unit: String((r as { unit?: unknown }).unit ?? "").trim() || undefined,
            stage: String((r as { stage?: unknown }).stage ?? "").trim() || undefined,
          })).filter((b) => b.id && b.productId && b.rawMaterialId && Number.isFinite(b.quantity));
          const attendanceLoaded = attendanceRows.map((r) => ({
            id: String((r as { id?: unknown }).id ?? "").trim(),
            staffId: String((r as { staff_id?: unknown }).staff_id ?? "").trim(),
            date: String((r as { date?: unknown }).date ?? "").trim(),
            clockIn: String((r as { clock_in?: unknown }).clock_in ?? "").trim(),
            clockOut: String((r as { clock_out?: unknown }).clock_out ?? "").trim() || undefined,
          })).filter((a) => a.id && a.staffId && a.date && a.clockIn);
          const attendanceEventsLoaded = attendanceEventsRows.map(normalizeAttendanceEvent).filter(Boolean) as AttendanceEvent[];
          const attendanceIntervalsLoaded = attendanceIntervalsRows.map(normalizeAttendanceInterval).filter(Boolean) as AttendanceInterval[];
          if (!attendanceRes.error && attendanceRows.length === 0 && defaultAttendanceRecords.length) void upsertAttendanceToSupabase(defaultAttendanceRecords);
          if (!staffRes.error && staffNeedsRoleSync && staffLoaded.length) void upsertToSupabase("staff", staffLoaded as unknown as Array<Record<string, unknown>>);
          const [poRes, polRes, grRes, grlRes, lotRes, smRes, woRes, plRes, pmRes, soRes, solRes, dnRes, dnlRes, invRes] = await Promise.all([
            supabase.from("purchase_orders").select("*"),
            supabase.from("purchase_order_lines").select("*"),
            supabase.from("goods_receipts").select("*"),
            supabase.from("goods_receipt_lines").select("*"),
            supabase.from("stock_lots").select("*"),
            supabase.from("stock_movements").select("*"),
            supabase.from("work_orders").select("*"),
            supabase.from("product_lots").select("*"),
            supabase.from("product_movements").select("*"),
            supabase.from("sales_orders").select("*"),
            supabase.from("sales_order_lines").select("*"),
            supabase.from("delivery_notes").select("*"),
            supabase.from("delivery_note_lines").select("*"),
            supabase.from("invoices").select("*"),
          ]);
          const [faRes, fjeRes, fjlRes, apbRes, appRes, arrRes] = await Promise.all([
            supabase.from("finance_accounts").select("*"),
            supabase.from("finance_journal_entries").select("*"),
            supabase.from("finance_journal_lines").select("*"),
            supabase.from("ap_bills").select("*"),
            supabase.from("ap_payments").select("*"),
            supabase.from("ar_receipts").select("*"),
          ]);
          const polRows = Array.isArray(polRes.data) ? polRes.data : [];
          const poLoaded = (Array.isArray(poRes.data) ? poRes.data : []).map((r) => {
            const id = String((r as { id: unknown }).id ?? "").trim();
            const lines = polRows
              .filter((l) => String((l as { purchase_order_id?: unknown }).purchase_order_id ?? "") === id)
              .map((l) => ({
                id: String((l as { id?: unknown }).id ?? "").trim(),
                rawMaterialId: String((l as { raw_material_id?: unknown }).raw_material_id ?? "").trim(),
                unitId: String((l as { unit_id?: unknown }).unit_id ?? "").trim() || undefined,
                quantity: Number((l as { quantity?: unknown }).quantity ?? 0),
                unitPrice: Number((l as { unit_price?: unknown }).unit_price ?? ""),
              }))
              .filter((l) => l.id && l.rawMaterialId && Number.isFinite(l.quantity));
            return {
              id,
              supplierId: String((r as { supplier_id?: unknown }).supplier_id ?? "").trim(),
              orderedAt: String((r as { ordered_at?: unknown }).ordered_at ?? "").trim(),
              expectedAt: String((r as { expected_at?: unknown }).expected_at ?? "").trim() || undefined,
              status: String((r as { status?: unknown }).status ?? "").trim() as PurchaseOrder["status"],
              notes: String((r as { notes?: unknown }).notes ?? "").trim() || undefined,
              lines,
            };
          }).filter((o) => o.id && o.supplierId && o.orderedAt);
          const grlRows = Array.isArray(grlRes.data) ? grlRes.data : [];
          const grLoaded = (Array.isArray(grRes.data) ? grRes.data : []).map((r) => ({
            id: String((r as { id?: unknown }).id ?? "").trim(),
            purchaseOrderId: String((r as { purchase_order_id?: unknown }).purchase_order_id ?? "").trim(),
            receivedAt: String((r as { received_at?: unknown }).received_at ?? "").trim(),
            lines: grlRows
              .filter((l) => String((l as { goods_receipt_id?: unknown }).goods_receipt_id ?? "") === String((r as { id?: unknown }).id ?? "").trim())
              .map((l) => ({
                id: String((l as { id?: unknown }).id ?? "").trim(),
                rawMaterialId: String((l as { raw_material_id?: unknown }).raw_material_id ?? "").trim(),
                unitId: String((l as { unit_id?: unknown }).unit_id ?? "").trim() || undefined,
                quantity: Number((l as { quantity?: unknown }).quantity ?? 0),
                batchNo: String((l as { batch_no?: unknown }).batch_no ?? "").trim() || undefined,
                expiryDate: String((l as { expiry_date?: unknown }).expiry_date ?? "").trim() || undefined,
              }))
              .filter((l) => l.id && l.rawMaterialId && Number.isFinite(l.quantity)),
          })).filter((g) => g.id && g.purchaseOrderId && g.receivedAt);
          const stockLotsLoaded = (Array.isArray(lotRes.data) ? lotRes.data : []).map((r) => ({
            id: String((r as { id?: unknown }).id ?? "").trim(),
            rawMaterialId: String((r as { raw_material_id?: unknown }).raw_material_id ?? "").trim(),
            unitId: String((r as { unit_id?: unknown }).unit_id ?? "").trim() || undefined,
            receivedAt: String((r as { received_at?: unknown }).received_at ?? "").trim(),
            batchNo: String((r as { batch_no?: unknown }).batch_no ?? "").trim() || undefined,
            expiryDate: String((r as { expiry_date?: unknown }).expiry_date ?? "").trim() || undefined,
            quantityOnHand: Number((r as { quantity_on_hand?: unknown }).quantity_on_hand ?? 0),
            sourceReceiptId: String((r as { source_receipt_id?: unknown }).source_receipt_id ?? "").trim() || undefined,
          })).filter((l) => l.id && l.rawMaterialId && l.receivedAt && Number.isFinite(l.quantityOnHand));
          const stockMovementsLoaded = (Array.isArray(smRes.data) ? smRes.data : []).map((r) => ({
            id: String((r as { id?: unknown }).id ?? "").trim(),
            at: String((r as { at?: unknown }).at ?? "").trim(),
            type: String((r as { type?: unknown }).type ?? "").trim() as StockMovement["type"],
            rawMaterialId: String((r as { raw_material_id?: unknown }).raw_material_id ?? "").trim(),
            lotId: String((r as { lot_id?: unknown }).lot_id ?? "").trim() || undefined,
            quantityDelta: Number((r as { quantity_delta?: unknown }).quantity_delta ?? 0),
            referenceId: String((r as { reference_id?: unknown }).reference_id ?? "").trim() || undefined,
          })).filter((m) => m.id && m.at && m.rawMaterialId && Number.isFinite(m.quantityDelta));
          const workOrdersLoaded = (Array.isArray(woRes.data) ? woRes.data : []).map((r) => ({
            id: String((r as { id?: unknown }).id ?? "").trim(),
            productId: String((r as { product_id?: unknown }).product_id ?? "").trim(),
            quantity: Number((r as { quantity?: unknown }).quantity ?? 0),
            status: String((r as { status?: unknown }).status ?? "").trim() as WorkOrder["status"],
            createdAt: String((r as { created_at?: unknown }).created_at ?? "").trim(),
            startedAt: String((r as { started_at?: unknown }).started_at ?? "").trim() || undefined,
            completedAt: String((r as { completed_at?: unknown }).completed_at ?? "").trim() || undefined,
          })).filter((w) => w.id && w.productId && w.createdAt && Number.isFinite(w.quantity));
          const productLotsLoaded = (Array.isArray(plRes.data) ? plRes.data : []).map((r) => ({
            id: String((r as { id?: unknown }).id ?? "").trim(),
            productId: String((r as { product_id?: unknown }).product_id ?? "").trim(),
            unitId: String((r as { unit_id?: unknown }).unit_id ?? "").trim() || undefined,
            producedAt: String((r as { produced_at?: unknown }).produced_at ?? "").trim(),
            batchNo: String((r as { batch_no?: unknown }).batch_no ?? "").trim() || undefined,
            expiryDate: String((r as { expiry_date?: unknown }).expiry_date ?? "").trim() || undefined,
            quantityOnHand: Number((r as { quantity_on_hand?: unknown }).quantity_on_hand ?? 0),
            sourceWorkOrderId: String((r as { source_work_order_id?: unknown }).source_work_order_id ?? "").trim() || undefined,
          })).filter((l) => l.id && l.productId && l.producedAt && Number.isFinite(l.quantityOnHand));
          const productMovementsLoaded = (Array.isArray(pmRes.data) ? pmRes.data : []).map((r) => ({
            id: String((r as { id?: unknown }).id ?? "").trim(),
            at: String((r as { at?: unknown }).at ?? "").trim(),
            type: String((r as { type?: unknown }).type ?? "").trim() as ProductMovement["type"],
            productId: String((r as { product_id?: unknown }).product_id ?? "").trim(),
            lotId: String((r as { lot_id?: unknown }).lot_id ?? "").trim() || undefined,
            quantityDelta: Number((r as { quantity_delta?: unknown }).quantity_delta ?? 0),
            referenceId: String((r as { reference_id?: unknown }).reference_id ?? "").trim() || undefined,
          })).filter((m) => m.id && m.at && m.productId && Number.isFinite(m.quantityDelta));
          const solRows = Array.isArray(solRes.data) ? solRes.data : [];
          const salesOrdersLoaded = (Array.isArray(soRes.data) ? soRes.data : []).map((r) => {
            const id = String((r as { id?: unknown }).id ?? "").trim();
            const lines = solRows
              .filter((l) => String((l as { sales_order_id?: unknown }).sales_order_id ?? "") === id)
              .map((l) => ({
                id: String((l as { id?: unknown }).id ?? "").trim(),
                productId: String((l as { product_id?: unknown }).product_id ?? "").trim(),
                unitId: String((l as { unit_id?: unknown }).unit_id ?? "").trim() || undefined,
                quantity: Number((l as { quantity?: unknown }).quantity ?? 0),
                unitPrice: Number((l as { unit_price?: unknown }).unit_price ?? ""),
              }))
              .filter((l) => l.id && l.productId && Number.isFinite(l.quantity));
            return {
              id,
              customerId: String((r as { customer_id?: unknown }).customer_id ?? "").trim(),
              orderedAt: String((r as { ordered_at?: unknown }).ordered_at ?? "").trim(),
              status: String((r as { status?: unknown }).status ?? "").trim() as SalesOrder["status"],
              notes: String((r as { notes?: unknown }).notes ?? "").trim() || undefined,
              lines,
            };
          }).filter((o) => o.id && o.customerId && o.orderedAt);
          const dnlRows = Array.isArray(dnlRes.data) ? dnlRes.data : [];
          const deliveryNotesLoaded = (Array.isArray(dnRes.data) ? dnRes.data : []).map((r) => ({
            id: String((r as { id?: unknown }).id ?? "").trim(),
            salesOrderId: String((r as { sales_order_id?: unknown }).sales_order_id ?? "").trim(),
            shippedAt: String((r as { shipped_at?: unknown }).shipped_at ?? "").trim(),
            lines: dnlRows
              .filter((l) => String((l as { delivery_note_id?: unknown }).delivery_note_id ?? "") === String((r as { id?: unknown }).id ?? "").trim())
              .map((l) => ({
                id: String((l as { id?: unknown }).id ?? "").trim(),
                productId: String((l as { product_id?: unknown }).product_id ?? "").trim(),
                unitId: String((l as { unit_id?: unknown }).unit_id ?? "").trim() || undefined,
                quantity: Number((l as { quantity?: unknown }).quantity ?? 0),
              }))
              .filter((l) => l.id && l.productId && Number.isFinite(l.quantity)),
          })).filter((d) => d.id && d.salesOrderId && d.shippedAt);
          const invoicesLoaded = (Array.isArray(invRes.data) ? invRes.data : []).map((r) => ({
            id: String((r as { id?: unknown }).id ?? "").trim(),
            salesOrderId: String((r as { sales_order_id?: unknown }).sales_order_id ?? "").trim(),
            invoicedAt: String((r as { invoiced_at?: unknown }).invoiced_at ?? "").trim(),
            totalAmount: Number((r as { total_amount?: unknown }).total_amount ?? ""),
          })).filter((i) => i.id && i.salesOrderId && i.invoicedAt);
          const financeAccountsLoaded = (Array.isArray(faRes.data) ? faRes.data : []).map((r) => ({
            id: String((r as { id?: unknown }).id ?? "").trim(),
            code: String((r as { code?: unknown }).code ?? "").trim(),
            name: String((r as { name?: unknown }).name ?? "").trim(),
            type: String((r as { type?: unknown }).type ?? "").trim(),
            isActive: Boolean((r as { is_active?: unknown }).is_active ?? true),
          })).filter((a) => a.id && a.code && a.name && a.type);
          const financeJournalEntriesLoaded = (Array.isArray(fjeRes.data) ? fjeRes.data : []).map((r) => ({
            id: String((r as { id?: unknown }).id ?? "").trim(),
            postedAt: String((r as { posted_at?: unknown }).posted_at ?? "").trim(),
            memo: String((r as { memo?: unknown }).memo ?? "").trim() || undefined,
            sourceTable: String((r as { source_table?: unknown }).source_table ?? "").trim() || undefined,
            sourceId: String((r as { source_id?: unknown }).source_id ?? "").trim() || undefined,
          })).filter((j) => j.id && j.postedAt);
          const financeJournalLinesLoaded = (Array.isArray(fjlRes.data) ? fjlRes.data : []).map((r) => ({
            id: String((r as { id?: unknown }).id ?? "").trim(),
            journalEntryId: String((r as { journal_entry_id?: unknown }).journal_entry_id ?? "").trim(),
            accountId: String((r as { account_id?: unknown }).account_id ?? "").trim(),
            description: String((r as { description?: unknown }).description ?? "").trim() || undefined,
            debit: Number((r as { debit?: unknown }).debit ?? 0),
            credit: Number((r as { credit?: unknown }).credit ?? 0),
          })).filter((l) => l.id && l.journalEntryId && l.accountId && Number.isFinite(l.debit) && Number.isFinite(l.credit));
          const apBillsLoaded = (Array.isArray(apbRes.data) ? apbRes.data : []).map((r) => ({
            id: String((r as { id?: unknown }).id ?? "").trim(),
            supplierId: String((r as { supplier_id?: unknown }).supplier_id ?? "").trim() || undefined,
            purchaseOrderId: String((r as { purchase_order_id?: unknown }).purchase_order_id ?? "").trim() || undefined,
            billedAt: String((r as { billed_at?: unknown }).billed_at ?? "").trim(),
            dueAt: String((r as { due_at?: unknown }).due_at ?? "").trim() || undefined,
            status: String((r as { status?: unknown }).status ?? "").trim(),
            totalAmount: Number((r as { total_amount?: unknown }).total_amount ?? 0),
            notes: String((r as { notes?: unknown }).notes ?? "").trim() || undefined,
          })).filter((b) => b.id && b.billedAt && b.status && Number.isFinite(b.totalAmount));
          const apPaymentsLoaded = (Array.isArray(appRes.data) ? appRes.data : []).map((r) => ({
            id: String((r as { id?: unknown }).id ?? "").trim(),
            billId: String((r as { bill_id?: unknown }).bill_id ?? "").trim() || undefined,
            paidAt: String((r as { paid_at?: unknown }).paid_at ?? "").trim(),
            method: String((r as { method?: unknown }).method ?? "").trim() || undefined,
            amount: Number((r as { amount?: unknown }).amount ?? 0),
            reference: String((r as { reference?: unknown }).reference ?? "").trim() || undefined,
          })).filter((p) => p.id && p.paidAt && Number.isFinite(p.amount));
          const arReceiptsLoaded = (Array.isArray(arrRes.data) ? arrRes.data : []).map((r) => ({
            id: String((r as { id?: unknown }).id ?? "").trim(),
            customerId: String((r as { customer_id?: unknown }).customer_id ?? "").trim() || undefined,
            invoiceId: String((r as { invoice_id?: unknown }).invoice_id ?? "").trim() || undefined,
            receivedAt: String((r as { received_at?: unknown }).received_at ?? "").trim(),
            method: String((r as { method?: unknown }).method ?? "").trim() || undefined,
            amount: Number((r as { amount?: unknown }).amount ?? 0),
            reference: String((r as { reference?: unknown }).reference ?? "").trim() || undefined,
          })).filter((rcpt) => rcpt.id && rcpt.receivedAt && Number.isFinite(rcpt.amount));
          const next: AppState = {
            staff: staffLoaded.length ? staffLoaded : defaultStaff,
            customers: customersLoaded,
            suppliers: suppliersLoaded,
            products: productsLoaded,
            rawMaterials: rawMaterialsLoaded,
            units: unitsLoaded,
            bom: bomLoaded,
            purchaseOrders: poLoaded,
            goodsReceipts: grLoaded,
            stockLots: stockLotsLoaded,
            stockMovements: stockMovementsLoaded,
            attendanceRecords: attendanceLoaded.length ? attendanceLoaded : defaultAttendanceRecords,
            attendanceEvents: attendanceEventsLoaded.length ? attendanceEventsLoaded : defaultAttendanceEvents,
            attendanceIntervals: attendanceIntervalsLoaded.length ? attendanceIntervalsLoaded : defaultAttendanceIntervals,
            workOrders: workOrdersLoaded,
            productLots: productLotsLoaded,
            productMovements: productMovementsLoaded,
            salesOrders: salesOrdersLoaded,
            deliveryNotes: deliveryNotesLoaded,
            invoices: invoicesLoaded,
            financeAccounts: financeAccountsLoaded,
            financeJournalEntries: financeJournalEntriesLoaded,
            financeJournalLines: financeJournalLinesLoaded,
            apBills: apBillsLoaded,
            apPayments: apPaymentsLoaded,
            arReceipts: arReceiptsLoaded,
          };
          setState(next);
          setAudit(emptyAudit);
          setIsLoading(false);
          return;
        } catch (e) {
          console.error(e);
        }
      }
      const stored = parseStoredState(window.localStorage.getItem(STORAGE_KEY_V2)) ?? parseStoredState(window.localStorage.getItem(STORAGE_KEY_V1));
      setState(stored ?? emptyAppState);
      setAudit(stored?.audit ?? emptyAudit);
      setIsLoading(false);
    };
    void boot();
  }, []);

  useEffect(() => {
    if (!isSupabaseConfigured) return;

    let refreshTimer: ReturnType<typeof setTimeout> | undefined;
    const requestRefresh = () => {
      if (refreshTimer) clearTimeout(refreshTimer);
      refreshTimer = setTimeout(() => {
        void refreshAttendanceFromSupabase();
      }, 250);
    };

    const channel = supabase
      .channel("attendance-changes")
      .on("postgres_changes", { event: "*", schema: "public", table: "attendance_events" }, requestRefresh)
      .on("postgres_changes", { event: "*", schema: "public", table: "attendance_intervals" }, requestRefresh)
      .on("postgres_changes", { event: "*", schema: "public", table: "attendance_records" }, requestRefresh)
      .subscribe();

    return () => {
      if (refreshTimer) clearTimeout(refreshTimer);
      void supabase.removeChannel(channel);
    };
  }, [refreshAttendanceFromSupabase]);

  const persist = useCallback((nextState: AppState, nextAudit: AuditEvent[]) => {
    const payload: StoredState = { ...nextState, audit: nextAudit };
    window.localStorage.setItem(STORAGE_KEY_V2, JSON.stringify(payload));
  }, []);

  const importRecords = useCallback<DataContextType["importRecords"]>(
    (key, records, actor) => {
      if (isSupabaseSyncKey(key)) void upsertToSupabase(key, records as unknown as Array<Record<string, unknown>>);
      setState((prev) => {
        const next: AppState = { ...prev, [key]: records } as AppState;
        setAudit((prevAudit) => {
          const nextAudit: AuditEvent[] = [
            {
              id: newId(),
              at: nowIso(),
              actor,
              action: "import",
              entity: key,
              meta: { count: records.length },
            } as AuditEvent,
            ...prevAudit,
          ].slice(0, 5000);
          persist(next, nextAudit);
          return nextAudit;
        });
        return next;
      });
    },
    [persist],
  );

  const upsertRecord = useCallback<DataContextType["upsertRecord"]>(
    (key, record, actor) => {
      if (isSupabaseSyncKey(key as MasterDataKey))
        void upsertToSupabase(key as SupabaseSyncKey, [record as unknown as Record<string, unknown>]);
      setState((prev) => {
        const list = prev[key] as Array<{ id: string }>;
        const existingIndex = list.findIndex((r) => r.id === (record as { id: string }).id);
        const action: AuditEvent["action"] = existingIndex === -1 ? "create" : "update";
        const before = existingIndex === -1 ? undefined : list[existingIndex];
        const nextList =
          existingIndex === -1
            ? [...list, record as { id: string }]
            : list.map((r) => (r.id === (record as { id: string }).id ? (record as { id: string }) : r));

        const next: AppState = { ...prev, [key]: nextList } as AppState;

        setAudit((prevAudit) => {
          const nextAudit: AuditEvent[] = [
            {
              id: newId(),
              at: nowIso(),
              actor,
              action,
              entity: key,
              entityId: (record as { id: string }).id,
              before,
              after: record,
            } as AuditEvent,
            ...prevAudit,
          ].slice(0, 5000);
          persist(next, nextAudit);
          return nextAudit;
        });

        return next;
      });
    },
    [persist],
  );

  const deleteRecord = useCallback<DataContextType["deleteRecord"]>(
    (key, id, actor) => {
      if (isSupabaseSyncKey(key as MasterDataKey)) void deleteFromSupabase(key as SupabaseSyncKey, id);
      setState((prev) => {
        const list = prev[key] as Array<{ id: string }>;
        const existing = list.find((r) => r.id === id);
        const nextList = list.filter((r) => r.id !== id);
        const next: AppState = { ...prev, [key]: nextList } as AppState;

        setAudit((prevAudit) => {
          const nextAudit: AuditEvent[] = [
            {
              id: newId(),
              at: nowIso(),
              actor,
              action: "delete",
              entity: key,
              entityId: id,
              before: existing,
            } as AuditEvent,
            ...prevAudit,
          ].slice(0, 5000);
          persist(next, nextAudit);
          return nextAudit;
        });

        return next;
      });
    },
    [persist],
  );

  const upsertPurchaseOrder = useCallback<DataContextType["upsertPurchaseOrder"]>(
    (order, actor) => {
      setState((prev) => {
        const existingIndex = prev.purchaseOrders.findIndex((o) => o.id === order.id);
        const action: AuditEvent["action"] = existingIndex === -1 ? "create" : "update";
        const before = existingIndex === -1 ? undefined : prev.purchaseOrders[existingIndex];
        const nextOrders =
          existingIndex === -1 ? [...prev.purchaseOrders, order] : prev.purchaseOrders.map((o) => (o.id === order.id ? order : o));

        const next: AppState = { ...prev, purchaseOrders: nextOrders };
        setAudit((prevAudit) => {
          const nextAudit: AuditEvent[] = [
            { id: newId(), at: nowIso(), actor, action, entity: "purchaseOrders", entityId: order.id, before, after: order } as AuditEvent,
            ...prevAudit,
          ].slice(0, 5000);
          persist(next, nextAudit);
          return nextAudit;
        });
        return next;
      });
    },
    [persist],
  );

  const deletePurchaseOrder = useCallback<DataContextType["deletePurchaseOrder"]>(
    (id, actor) => {
      setState((prev) => {
        const existing = prev.purchaseOrders.find((o) => o.id === id);
        const nextOrders = prev.purchaseOrders.filter((o) => o.id !== id);
        const next: AppState = { ...prev, purchaseOrders: nextOrders };
        setAudit((prevAudit) => {
          const nextAudit: AuditEvent[] = [
            { id: newId(), at: nowIso(), actor, action: "delete", entity: "purchaseOrders", entityId: id, before: existing } as AuditEvent,
            ...prevAudit,
          ].slice(0, 5000);
          persist(next, nextAudit);
          return nextAudit;
        });
        return next;
      });
    },
    [persist],
  );

  const receivePurchaseOrder = useCallback<DataContextType["receivePurchaseOrder"]>(
    (input, actor) => {
      const receivedAt = input.receivedAt ?? nowIso();
      if (!input.purchaseOrderId.trim()) return "Purchase order is required.";
      if (!input.lines.length) return "At least one receipt line is required.";
      const invalidLine = input.lines.find((l) => !l.rawMaterialId.trim() || !Number.isFinite(l.quantity) || l.quantity <= 0);
      if (invalidLine) return "Receipt lines must include raw material and a quantity > 0.";

      setState((prev) => {
        const poIndex = prev.purchaseOrders.findIndex((o) => o.id === input.purchaseOrderId);
        if (poIndex === -1) return prev;

        const po = prev.purchaseOrders[poIndex];
        const receiptId = newId();
        const receipt: GoodsReceipt = {
          id: receiptId,
          purchaseOrderId: po.id,
          receivedAt,
          lines: input.lines.map((l) => ({
            id: newId(),
            rawMaterialId: l.rawMaterialId,
            unitId: l.unitId,
            quantity: l.quantity,
            batchNo: l.batchNo?.trim() || undefined,
            expiryDate: l.expiryDate?.trim() || undefined,
          })),
        };

        const nextLots: StockLot[] = receipt.lines.map((l) => ({
          id: newId(),
          rawMaterialId: l.rawMaterialId,
          unitId: l.unitId,
          receivedAt,
          batchNo: l.batchNo,
          expiryDate: l.expiryDate,
          quantityOnHand: l.quantity,
          sourceReceiptId: receipt.id,
        }));

        const nextMovements: StockMovement[] = [
          ...nextLots.map(
            (lot): StockMovement => ({
            id: newId(),
            at: receivedAt,
            type: "receive",
            rawMaterialId: lot.rawMaterialId,
            lotId: lot.id,
            quantityDelta: lot.quantityOnHand,
            referenceId: receipt.id,
          }),
          ),
          ...prev.stockMovements,
        ];

        const updatedPo: PurchaseOrder = { ...po, status: "received" };
        const nextOrders = prev.purchaseOrders.map((o) => (o.id === po.id ? updatedPo : o));

        const next: AppState = {
          ...prev,
          purchaseOrders: nextOrders,
          goodsReceipts: [receipt, ...prev.goodsReceipts],
          stockLots: [...nextLots, ...prev.stockLots],
          stockMovements: nextMovements,
        };

        setAudit((prevAudit) => {
          const nextAudit: AuditEvent[] = [
            { id: newId(), at: nowIso(), actor, action: "create", entity: "goodsReceipts", entityId: receipt.id, after: receipt } as AuditEvent,
            { id: newId(), at: nowIso(), actor, action: "update", entity: "purchaseOrders", entityId: updatedPo.id, before: po, after: updatedPo } as AuditEvent,
            ...prevAudit,
          ].slice(0, 5000);
          persist(next, nextAudit);
          return nextAudit;
        });

        return next;
      });

      return null;
    },
    [persist],
  );

  const adjustStock = useCallback<DataContextType["adjustStock"]>(
    (input, actor) => {
      const at = input.at ?? nowIso();
      if (!input.rawMaterialId.trim()) return "Raw material is required.";
      if (!Number.isFinite(input.quantityDelta) || input.quantityDelta === 0) return "Quantity delta must be a non-zero number.";

      let error: string | null = null;
      setState((prev) => {
        const delta = input.quantityDelta;
        if (delta > 0) {
          const lot: StockLot = {
            id: newId(),
            rawMaterialId: input.rawMaterialId,
            unitId: input.unitId,
            receivedAt: at,
            batchNo: input.batchNo?.trim() || undefined,
            expiryDate: input.expiryDate?.trim() || undefined,
            quantityOnHand: delta,
          };
          const movement: StockMovement = {
            id: newId(),
            at,
            type: "adjust",
            rawMaterialId: input.rawMaterialId,
            lotId: lot.id,
            quantityDelta: delta,
          };
          const next: AppState = {
            ...prev,
            stockLots: [lot, ...prev.stockLots],
            stockMovements: [movement, ...prev.stockMovements],
          };
          setAudit((prevAudit) => {
            const entry: AuditEvent = { id: newId(), at: nowIso(), actor, action: "create", entity: "stockLots", entityId: lot.id, after: lot };
            const nextAudit: AuditEvent[] = [entry, ...prevAudit].slice(0, 5000);
            persist(next, nextAudit);
            return nextAudit;
          });
          return next;
        }

        const needed = Math.abs(delta);
        const candidates = prev.stockLots
          .filter((l) => l.rawMaterialId === input.rawMaterialId && l.quantityOnHand > 0)
          .slice()
          .sort((a, b) => {
            const aExp = a.expiryDate ? Date.parse(a.expiryDate) : Number.POSITIVE_INFINITY;
            const bExp = b.expiryDate ? Date.parse(b.expiryDate) : Number.POSITIVE_INFINITY;
            if (aExp !== bExp) return aExp - bExp;
            return Date.parse(a.receivedAt) - Date.parse(b.receivedAt);
          });

        const total = candidates.reduce((sum, l) => sum + l.quantityOnHand, 0);
        if (total < needed) {
          error = "Insufficient stock to issue.";
          return prev;
        }

        let remaining = needed;
        const deductions = new Map<string, number>();
        for (const lot of candidates) {
          if (remaining <= 0) break;
          const take = Math.min(lot.quantityOnHand, remaining);
          remaining -= take;
          deductions.set(lot.id, (deductions.get(lot.id) ?? 0) + take);
        }

        const nextLots = prev.stockLots.map((lot) => {
          const take = deductions.get(lot.id);
          if (!take) return lot;
          return { ...lot, quantityOnHand: lot.quantityOnHand - take };
        });

        const movement: StockMovement = {
          id: newId(),
          at,
          type: "issue",
          rawMaterialId: input.rawMaterialId,
          quantityDelta: delta,
        };

        const next: AppState = { ...prev, stockLots: nextLots, stockMovements: [movement, ...prev.stockMovements] };
        setAudit((prevAudit) => {
          const entry: AuditEvent = {
            id: newId(),
            at: nowIso(),
            actor,
            action: "create",
            entity: "stockMovements",
            entityId: movement.id,
            after: movement,
          };
          const nextAudit: AuditEvent[] = [entry, ...prevAudit].slice(0, 5000);
          persist(next, nextAudit);
          return nextAudit;
        });
        return next;
      });

      return error;
    },
    [persist],
  );

  const upsertWorkOrder = useCallback<DataContextType["upsertWorkOrder"]>(
    (order, actor) => {
      setState((prev) => {
        const existingIndex = prev.workOrders.findIndex((o) => o.id === order.id);
        const action: AuditEvent["action"] = existingIndex === -1 ? "create" : "update";
        const before = existingIndex === -1 ? undefined : prev.workOrders[existingIndex];
        const nextOrders = existingIndex === -1 ? [...prev.workOrders, order] : prev.workOrders.map((o) => (o.id === order.id ? order : o));
        const next: AppState = { ...prev, workOrders: nextOrders };
        setAudit((prevAudit) => {
          const entry: AuditEvent = { id: newId(), at: nowIso(), actor, action, entity: "workOrders", entityId: order.id, before, after: order };
          const nextAudit: AuditEvent[] = [entry, ...prevAudit].slice(0, 5000);
          persist(next, nextAudit);
          return nextAudit;
        });
        return next;
      });
    },
    [persist],
  );

  const startWorkOrder = useCallback<DataContextType["startWorkOrder"]>(
    (id, actor) => {
      setState((prev) => {
        const wo = prev.workOrders.find((o) => o.id === id);
        if (!wo) return prev;
        const updated: WorkOrder = { ...wo, status: "in_progress", startedAt: nowIso() };
        const next: AppState = { ...prev, workOrders: prev.workOrders.map((o) => (o.id === id ? updated : o)) };
        setAudit((prevAudit) => {
          const entry: AuditEvent = { id: newId(), at: nowIso(), actor, action: "update", entity: "workOrders", entityId: id, before: wo, after: updated };
          const nextAudit: AuditEvent[] = [entry, ...prevAudit].slice(0, 5000);
          persist(next, nextAudit);
          return nextAudit;
        });
        return next;
      });
    },
    [persist],
  );

  const issueWorkOrderMaterials = useCallback<DataContextType["issueWorkOrderMaterials"]>(
    (id, actor) => {
      const at = nowIso();
      let error: string | null = null;

      setState((prev) => {
        const wo = prev.workOrders.find((o) => o.id === id);
        if (!wo) {
          error = "Work order not found.";
          return prev;
        }
        if (wo.status !== "in_progress") {
          error = "Work order must be in progress before issuing materials.";
          return prev;
        }

        const bomReqs = prev.bom.filter((b) => b.productId === wo.productId);
        if (!bomReqs.length) {
          error = "No BOM defined for product.";
          return prev;
        }

        const requiredByMaterial = new Map<string, number>();
        for (const b of bomReqs) {
          const qty = b.quantity * wo.quantity;
          if (!Number.isFinite(qty) || qty <= 0) continue;
          requiredByMaterial.set(b.rawMaterialId, (requiredByMaterial.get(b.rawMaterialId) ?? 0) + qty);
        }
        if (!requiredByMaterial.size) {
          error = "Invalid BOM requirements.";
          return prev;
        }

        let nextLots = prev.stockLots.slice();
        const createdMovements: StockMovement[] = [];

        for (const [rawMaterialId, needed] of requiredByMaterial.entries()) {
          const candidates = nextLots
            .filter((l) => l.rawMaterialId === rawMaterialId && l.quantityOnHand > 0)
            .slice()
            .sort((a, b) => {
              const aExp = a.expiryDate ? Date.parse(a.expiryDate) : Number.POSITIVE_INFINITY;
              const bExp = b.expiryDate ? Date.parse(b.expiryDate) : Number.POSITIVE_INFINITY;
              if (aExp !== bExp) return aExp - bExp;
              return Date.parse(a.receivedAt) - Date.parse(b.receivedAt);
            });

          const total = candidates.reduce((sum, l) => sum + l.quantityOnHand, 0);
          if (total < needed) {
            error = `Insufficient material ${rawMaterialId}`;
            return prev;
          }

          let remaining = needed;
          const deductions = new Map<string, number>();
          for (const lot of candidates) {
            if (remaining <= 0) break;
            const take = Math.min(lot.quantityOnHand, remaining);
            remaining -= take;
            deductions.set(lot.id, (deductions.get(lot.id) ?? 0) + take);
          }

          nextLots = nextLots.map((lot) => {
            const take = deductions.get(lot.id);
            if (!take) return lot;
            return { ...lot, quantityOnHand: lot.quantityOnHand - take };
          });

          createdMovements.push({
            id: newId(),
            at,
            type: "issue",
            rawMaterialId,
            quantityDelta: -needed,
            referenceId: id,
          });
        }

        const next: AppState = {
          ...prev,
          stockLots: nextLots,
          stockMovements: [...createdMovements, ...prev.stockMovements],
        };

        setAudit((prevAudit) => {
          const nextAudit = [
            ...createdMovements.map((m) => ({
              id: newId(),
              at: nowIso(),
              actor,
              action: "create" as const,
              entity: "stockMovements" as const,
              entityId: m.id,
              after: m,
            })),
            ...prevAudit,
          ].slice(0, 5000);
          persist(next, nextAudit);
          return nextAudit;
        });

        return next;
      });

      return error;
    },
    [persist],
  );

  const completeWorkOrder = useCallback<DataContextType["completeWorkOrder"]>(
    (id, outputQuantity, batchNo, expiryDate, actor) => {
      const producedAt = nowIso();
      setState((prev) => {
        const wo = prev.workOrders.find((o) => o.id === id);
        if (!wo) return prev;
        if (!Number.isFinite(outputQuantity) || outputQuantity <= 0) return prev;
        const lot: ProductLot = {
          id: newId(),
          productId: wo.productId,
          producedAt,
          quantityOnHand: outputQuantity,
          batchNo: batchNo?.trim() || undefined,
          expiryDate: expiryDate?.trim() || undefined,
          sourceWorkOrderId: id,
        };
        const movement: ProductMovement = {
          id: newId(),
          at: producedAt,
          type: "produce",
          productId: wo.productId,
          lotId: lot.id,
          quantityDelta: outputQuantity,
          referenceId: id,
        };
        const updated: WorkOrder = { ...wo, status: "completed", completedAt: producedAt };
        const next: AppState = {
          ...prev,
          workOrders: prev.workOrders.map((o) => (o.id === id ? updated : o)),
          productLots: [lot, ...prev.productLots],
          productMovements: [movement, ...prev.productMovements],
        };
        setAudit((prevAudit) => {
          const createLot: AuditEvent = { id: newId(), at: nowIso(), actor, action: "create", entity: "productLots", entityId: lot.id, after: lot };
          const updateWo: AuditEvent = { id: newId(), at: nowIso(), actor, action: "update", entity: "workOrders", entityId: id, before: wo, after: updated };
          const nextAudit: AuditEvent[] = [createLot, updateWo, ...prevAudit].slice(0, 5000);
          persist(next, nextAudit);
          return nextAudit;
        });
        return next;
      });
      return null;
    },
    [persist],
  );

  const upsertSalesOrder = useCallback<DataContextType["upsertSalesOrder"]>(
    (order, actor) => {
      setState((prev) => {
        const existingIndex = prev.salesOrders.findIndex((o) => o.id === order.id);
        const action: AuditEvent["action"] = existingIndex === -1 ? "create" : "update";
        const before = existingIndex === -1 ? undefined : prev.salesOrders[existingIndex];
        const nextOrders = existingIndex === -1 ? [...prev.salesOrders, order] : prev.salesOrders.map((o) => (o.id === order.id ? order : o));
        const next: AppState = { ...prev, salesOrders: nextOrders };
        setAudit((prevAudit) => {
          const entry: AuditEvent = { id: newId(), at: nowIso(), actor, action, entity: "salesOrders", entityId: order.id, before, after: order };
          const nextAudit: AuditEvent[] = [entry, ...prevAudit].slice(0, 5000);
          persist(next, nextAudit);
          return nextAudit;
        });
        return next;
      });
    },
    [persist],
  );

  const deleteSalesOrder = useCallback<DataContextType["deleteSalesOrder"]>(
    (id, actor) => {
      setState((prev) => {
        const existing = prev.salesOrders.find((o) => o.id === id);
        const nextOrders = prev.salesOrders.filter((o) => o.id !== id);
        const next: AppState = { ...prev, salesOrders: nextOrders };
        setAudit((prevAudit) => {
          const entry: AuditEvent = { id: newId(), at: nowIso(), actor, action: "delete", entity: "salesOrders", entityId: id, before: existing };
          const nextAudit: AuditEvent[] = [entry, ...prevAudit].slice(0, 5000);
          persist(next, nextAudit);
          return nextAudit;
        });
        return next;
      });
    },
    [persist],
  );

  const fulfillSalesOrder = useCallback<DataContextType["fulfillSalesOrder"]>(
    (input, actor) => {
      const shippedAt = input.shippedAt ?? nowIso();
      if (!input.salesOrderId.trim()) return "Sales order is required.";
      if (!input.lines.length) return "At least one delivery line is required.";
      const invalid = input.lines.find((l) => !l.productId.trim() || !Number.isFinite(l.quantity) || l.quantity <= 0);
      if (invalid) return "Delivery lines must include product and a quantity > 0.";
      let error: string | null = null;
      setState((prev) => {
        const soIndex = prev.salesOrders.findIndex((o) => o.id === input.salesOrderId);
        if (soIndex === -1) return prev;
        const deliveryId = newId();
        const delivery: DeliveryNote = {
          id: deliveryId,
          salesOrderId: input.salesOrderId,
          shippedAt,
          lines: input.lines.map((l) => ({ id: newId(), productId: l.productId, unitId: l.unitId, quantity: l.quantity })),
        };
        const nextMovements: ProductMovement[] = [];
        const nextLots = prev.productLots.slice();
        for (const line of delivery.lines) {
          const needed = line.quantity;
          const candidates = nextLots
            .filter((l) => l.productId === line.productId && l.quantityOnHand > 0)
            .slice()
            .sort((a, b2) => {
              const aExp = a.expiryDate ? Date.parse(a.expiryDate) : Number.POSITIVE_INFINITY;
              const bExp = b2.expiryDate ? Date.parse(b2.expiryDate) : Number.POSITIVE_INFINITY;
              if (aExp !== bExp) return aExp - bExp;
              return Date.parse(a.producedAt) - Date.parse(b2.producedAt);
            });
          const total = candidates.reduce((sum, l) => sum + l.quantityOnHand, 0);
          if (total < needed) {
            error = "Insufficient finished goods to ship.";
            break;
          }
          let remaining = needed;
          for (let i = 0; i < nextLots.length && remaining > 0; i++) {
            const lot = nextLots[i];
            if (lot.productId !== line.productId || lot.quantityOnHand <= 0) continue;
            const take = Math.min(lot.quantityOnHand, remaining);
            remaining -= take;
            nextLots[i] = { ...lot, quantityOnHand: lot.quantityOnHand - take };
          }
          nextMovements.push({
            id: newId(),
            at: shippedAt,
            type: "ship",
            productId: line.productId,
            quantityDelta: -needed,
            referenceId: delivery.id,
          });
        }
        if (error) return prev;
        const so = prev.salesOrders[soIndex];
        const updatedSo: SalesOrder = { ...so, status: "fulfilled" };
        const next: AppState = {
          ...prev,
          salesOrders: prev.salesOrders.map((o) => (o.id === so.id ? updatedSo : o)),
          deliveryNotes: [delivery, ...prev.deliveryNotes],
          productLots: nextLots,
          productMovements: [...nextMovements, ...prev.productMovements],
        };
        setAudit((prevAudit) => {
          const createDn: AuditEvent = { id: newId(), at: nowIso(), actor, action: "create", entity: "deliveryNotes", entityId: delivery.id, after: delivery };
          const updateSo: AuditEvent = { id: newId(), at: nowIso(), actor, action: "update", entity: "salesOrders", entityId: updatedSo.id, before: so, after: updatedSo };
          const nextAudit: AuditEvent[] = [createDn, updateSo, ...prevAudit].slice(0, 5000);
          persist(next, nextAudit);
          return nextAudit;
        });
        return next;
      });
      return error;
    },
    [persist],
  );

  const generateInvoice = useCallback<DataContextType["generateInvoice"]>(
    (salesOrderId, invoicedAt, actor) => {
      const at = invoicedAt ?? nowIso();
      setState((prev) => {
        const so = prev.salesOrders.find((o) => o.id === salesOrderId);
        if (!so) return prev;
        const total = so.lines.reduce((sum, l) => sum + (l.unitPrice ?? 0) * l.quantity, 0);
        const invoice: Invoice = { id: newId(), salesOrderId, invoicedAt: at, totalAmount: total };
        const next: AppState = { ...prev, invoices: [invoice, ...prev.invoices] };
        setAudit((prevAudit) => {
          const entry: AuditEvent = { id: newId(), at: nowIso(), actor, action: "create", entity: "invoices", entityId: invoice.id, after: invoice };
          const nextAudit: AuditEvent[] = [entry, ...prevAudit].slice(0, 5000);
          persist(next, nextAudit);
          return nextAudit;
        });
        return next;
      });
    },
    [persist],
  );

  const clearAudit = useCallback(() => {
    setAudit(emptyAudit);
    persist(state, emptyAudit);
  }, [persist, state]);

  const resetAll = useCallback(() => {
    setState(emptyAppState);
    setAudit(emptyAudit);
    window.localStorage.removeItem(STORAGE_KEY_V2);
    window.localStorage.removeItem(STORAGE_KEY_V1);
  }, []);

  const value = useMemo<DataContextType>(
    () => ({
      ...state,
      isLoading,
      audit,
      importRecords: <T extends MasterDataKey>(key: T, records: MasterDataState[T], actor?: AuditActor) =>
        importRecords(key, records, actor),
      upsertRecord: <T extends MasterDataKey>(key: T, record: MasterDataState[T][number], actor?: AuditActor) =>
        upsertRecord(key, record, actor),
      deleteRecord,
      upsertPurchaseOrder,
      deletePurchaseOrder,
      receivePurchaseOrder,
      adjustStock,
      upsertWorkOrder,
      startWorkOrder,
      issueWorkOrderMaterials,
      completeWorkOrder,
      upsertSalesOrder,
      deleteSalesOrder,
      fulfillSalesOrder,
      generateInvoice,
      clearAudit,
      resetAll,
    }),
    [
      state,
      isLoading,
      audit,
      importRecords,
      upsertRecord,
      deleteRecord,
      upsertPurchaseOrder,
      deletePurchaseOrder,
      receivePurchaseOrder,
      adjustStock,
      upsertWorkOrder,
      startWorkOrder,
      issueWorkOrderMaterials,
      completeWorkOrder,
      upsertSalesOrder,
      deleteSalesOrder,
      fulfillSalesOrder,
      generateInvoice,
      clearAudit,
      resetAll,
    ],
  );

  return <DataContext.Provider value={value}>{children}</DataContext.Provider>;
}
