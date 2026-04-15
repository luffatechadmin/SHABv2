import { useMemo, useRef, useState } from "react";
import Papa from "papaparse";
import { z } from "zod";
import { Navigate } from "react-router-dom";
import { Factory, ShoppingCart, Users, Wallet, Warehouse } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useAuth } from "@/contexts/useAuth";
import { useData } from "@/contexts/useData";
import type {
  AttendanceRecord,
  AdjustStockInput,
  AuditActor,
  AppModule,
  BillOfMaterialLine,
  Customer,
  GoodsReceiptLine,
  MeasurementUnit,
  Product,
  PurchaseOrder,
  PurchaseOrderLine,
  RawMaterial,
  WorkOrder,
  SalesOrder,
  SalesOrderLine,
  Staff,
  StaffStatus,
  Supplier,
  UserRole,
} from "@/contexts/master-data-types";
import { ALL_MODULES, defaultModulesForRole, defaultModulesForRoles } from "@/contexts/master-data-types";

const normalizeKey = (value: string): string => value.toLowerCase().replace(/[^a-z0-9]/g, "");

const labelForModule = (m: AppModule): string => {
  if (m === "inventory") return "Procurement";
  if (m === "hr") return "Human Resource";
  return m.slice(0, 1).toUpperCase() + m.slice(1);
};

const labelForRole = (r: UserRole): string => {
  if (r === "superadmin") return "Superadmin";
  if (r === "operator") return "Operator";
  if (r === "supervisor") return "Supervisor";
  if (r === "production_manager") return "Production Manager";
  if (r === "procurement_manager") return "Procurement Manager";
  if (r === "finance_manager") return "Finance Manager";
  if (r === "sales_manager") return "Sales Manager";
  if (r === "manager") return "Manager";
  if (r === "staff") return "Staff";
  if (r === "operations") return "Operations";
  if (r === "procurement") return "Procurement";
  if (r === "sales") return "Sales";
  return r;
};

const labelForEntityKey = (key: Exclude<EntityTabKey, "procurement" | "inventory" | "production" | "sales" | "audit" | "finance" | "hr">): string => {
  if (key === "rawMaterials") return "Raw Materials";
  if (key === "bom") return "BOM";
  if (key === "staff") return "Staff";
  if (key === "customers") return "Customers";
  if (key === "suppliers") return "Suppliers";
  if (key === "products") return "Products";
  if (key === "units") return "Units";
  return key;
};

const toOptionalString = (value: unknown): string | undefined => {
  if (value === null || value === undefined) return undefined;
  const raw = String(value).trim();
  return raw ? raw : undefined;
};

const toRequiredString = (value: unknown): string => String(value ?? "").trim();

const toNumber = (value: unknown): number => {
  const raw = String(value ?? "").trim();
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : NaN;
};

const parseCsvDateTime = (value: unknown): string | undefined => {
  const raw = toOptionalString(value);
  if (!raw) return undefined;

  const trimmed = raw.trim();
  if (!trimmed) return undefined;

  const parts = trimmed.split(/\s+/);
  if (parts.length >= 2) {
    const datePart = parts[0] ?? "";
    const timePart = parts[1] ?? "";
    const m = datePart.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    const t = timePart.match(/^(\d{1,2}):(\d{2})$/);
    if (m && t) {
      const month = Number(m[1]);
      const day = Number(m[2]);
      const year = Number(m[3]);
      const hour = Number(t[1]);
      const minute = Number(t[2]);
      const d = new Date(year, month - 1, day, hour, minute);
      if (!Number.isNaN(d.getTime())) return d.toISOString();
    }
  }

  const parsed = Date.parse(trimmed);
  if (Number.isFinite(parsed)) return new Date(parsed).toISOString();

  return trimmed;
};

const formatCsvDateTime = (value: string | undefined): string => {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  const m = d.getMonth() + 1;
  const day = d.getDate();
  const year = d.getFullYear();
  const hour = d.getHours();
  const minute = String(d.getMinutes()).padStart(2, "0");
  return `${m}/${day}/${year} ${hour}:${minute}`;
};

const formatMoney = (value: number | undefined): string => {
  const n = typeof value === "number" ? value : Number(value ?? 0);
  const safe = Number.isFinite(n) ? n : 0;
  return new Intl.NumberFormat("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(safe);
};

const formatStatusText = (value: string | undefined | null): string => {
  const raw = String(value ?? "").trim();
  if (!raw) return "-";
  return raw
    .replace(/[_-]+/g, " ")
    .split(" ")
    .filter(Boolean)
    .map((w) => w.slice(0, 1).toUpperCase() + w.slice(1).toLowerCase())
    .join(" ");
};

const formatDateTimeParts = (value: string | undefined | null): { date: string; time: string } => {
  const raw = String(value ?? "").trim();
  if (!raw) return { date: "-", time: "-" };
  const dateMatch = raw.match(/\d{4}-\d{2}-\d{2}/);
  const timeMatch = raw.match(/\b\d{2}:\d{2}\b/);
  const date = dateMatch?.[0] ?? raw.slice(0, 10) ?? "-";
  const time = timeMatch?.[0] ?? "-";
  return { date, time };
};

const pickNormalized = (row: Record<string, unknown>, aliases: string[]): unknown => {
  const normalized: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(row)) {
    normalized[normalizeKey(k)] = v;
  }
  for (const alias of aliases) {
    const key = normalizeKey(alias);
    if (key in normalized) return normalized[key];
  }
  return undefined;
};

const customerSchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  phone: z.string().optional(),
  email: z.string().optional(),
  address: z.string().optional(),
});

const supplierSchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  country: z.string().optional(),
  contactPerson: z.string().optional(),
  phone: z.string().optional(),
  email: z.string().optional(),
  leadTimeDays: z.number().int().nonnegative().optional(),
  status: z.string().optional(),
});

const unitSchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  unitType: z.string().optional(),
  conversionBase: z.string().optional(),
  conversionRate: z.number().optional(),
  symbol: z.string().optional(),
});

const productSchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  variant: z.string().optional(),
  category: z.string().optional(),
  size: z.number().optional(),
  unit: z.string().optional(),
  version: z.string().optional(),
  status: z.string().optional(),
});

const rawMaterialSchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  inciName: z.string().optional(),
  specGrade: z.string().optional(),
  unit: z.string().optional(),
  shelfLifeMonths: z.number().int().nonnegative().optional(),
  storageCondition: z.string().optional(),
  status: z.string().optional(),
});

const bomSchema = z.object({
  id: z.string().min(1),
  productId: z.string().min(1),
  rawMaterialId: z.string().min(1),
  rawMaterialName: z.string().optional(),
  quantity: z.number().positive(),
  unit: z.string().optional(),
  stage: z.string().optional(),
});

const staffRoleSchema = z.enum([
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
]);

const staffSchema = z.object({
  id: z.string().min(1),
  fullName: z.string().min(1),
  email: z.string().optional(),
  role: staffRoleSchema,
  roles: z.array(staffRoleSchema).min(1),
  department: z.string().optional(),
  username: z.string().min(1),
  passwordHash: z.string().min(1),
  status: z.enum(["active", "inactive"]),
  dateJoined: z.string().optional(),
  lastLogin: z.string().optional(),
  modules: z.array(z.enum(["inventory", "production", "sales", "finance", "hr"])),
});

const buildActor = (user: { id: string; fullName: string; role: UserRole } | null): AuditActor | undefined => {
  if (!user) return undefined;
  return { id: user.id, name: user.fullName, role: user.role };
};

type EntityTabKey =
  | "procurement"
  | "inventory"
  | "production"
  | "finishedGoods"
  | "sales"
  | "finance"
  | "journal"
  | "hr"
  | "attendance"
  | "staff"
  | "customers"
  | "suppliers"
  | "products"
  | "rawMaterials"
  | "units"
  | "bom"
  | "audit";
type EditableEntityKey = Exclude<
  EntityTabKey,
  "audit" | "procurement" | "inventory" | "production" | "finishedGoods" | "sales" | "finance" | "journal" | "hr" | "attendance"
>;
type ReceiveDraftState = { purchaseOrderId: string; receivedAt: string; lines: GoodsReceiptLine[] };

export default function SuperadminDashboard() {
  const { user, logout } = useAuth();
  const {
    staff,
    customers,
    suppliers,
    products,
    rawMaterials,
    units,
    bom,
    purchaseOrders,
    goodsReceipts,
    stockLots,
    attendanceRecords,
    productLots,
    workOrders,
    productMovements,
    salesOrders,
    deliveryNotes,
    invoices,
    financeAccounts,
    financeJournalEntries,
    financeJournalLines,
    apBills,
    apPayments,
    arReceipts,
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
  } = useData();

  const actor = buildActor(user);

  const [activeModule, setActiveModule] = useState<AppModule | null>(null);
  const [tab, setTab] = useState<EntityTabKey>("procurement");
  const [search, setSearch] = useState("");
  const [sort, setSort] = useState<{ key: string; dir: "asc" | "desc" } | null>(null);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingKey, setEditingKey] = useState<EditableEntityKey | null>(null);

  const [staffDraft, setStaffDraft] = useState<Staff>({
    id: "",
    fullName: "",
    email: "",
    role: "operator",
    roles: ["operator"],
    department: "",
    username: "",
    passwordHash: "abcd1234",
    status: "active",
    dateJoined: "",
    lastLogin: "",
    modules: defaultModulesForRole("operator"),
  });
  const [customerDraft, setCustomerDraft] = useState<Customer>({ id: "", name: "" });
  const [supplierDraft, setSupplierDraft] = useState<Supplier>({ id: "", name: "" });
  const [unitDraft, setUnitDraft] = useState<MeasurementUnit>({ id: "", name: "" });
  const [productDraft, setProductDraft] = useState<Product>({ id: "", name: "" });
  const [rawMaterialDraft, setRawMaterialDraft] = useState<RawMaterial>({ id: "", name: "" });
  const [bomDraft, setBomDraft] = useState<BillOfMaterialLine>({
    id: "",
    productId: "",
    rawMaterialId: "",
    quantity: 1,
    unit: undefined,
    stage: undefined,
  });
  const [formError, setFormError] = useState<string | null>(null);

  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const [importKey, setImportKey] = useState<EditableEntityKey | null>(null);
  const [importError, setImportError] = useState<string | null>(null);

  const newId = (): string => {
    try {
      return crypto.randomUUID();
    } catch {
      return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    }
  };

  const openModule = (m: AppModule) => {
    setActiveModule(m);
    setSearch("");
    if (m === "inventory") return setTab("inventory");
    if (m === "production") return setTab("production");
    if (m === "sales") return setTab("sales");
    if (m === "finance") return setTab("finance");
    return setTab("staff");
  };

  const [poDialogOpen, setPoDialogOpen] = useState(false);
  const [poDraft, setPoDraft] = useState<PurchaseOrder>({
    id: "",
    supplierId: "",
    orderedAt: new Date().toISOString().slice(0, 10),
    expectedAt: "",
    status: "draft",
    notes: "",
    lines: [],
  });
  const [poError, setPoError] = useState<string | null>(null);

  const [receiveDialogOpen, setReceiveDialogOpen] = useState(false);
  const [receiveDraft, setReceiveDraft] = useState<ReceiveDraftState>({
    purchaseOrderId: "",
    receivedAt: new Date().toISOString().slice(0, 10),
    lines: [],
  });
  const [receiveError, setReceiveError] = useState<string | null>(null);

  const [adjustDialogOpen, setAdjustDialogOpen] = useState(false);
  const [adjustDraft, setAdjustDraft] = useState<AdjustStockInput>({
    rawMaterialId: "",
    unitId: undefined,
    quantityDelta: 0,
    at: new Date().toISOString().slice(0, 10),
    batchNo: "",
    expiryDate: "",
  });
  const [adjustError, setAdjustError] = useState<string | null>(null);

  const [woDialogOpen, setWoDialogOpen] = useState(false);
  const [woDraft, setWoDraft] = useState<WorkOrder>({
    id: "",
    productId: "",
    quantity: 1,
    status: "planned",
    createdAt: new Date().toISOString().slice(0, 10),
  });
  const [woError, setWoError] = useState<string | null>(null);
  const [productionError, setProductionError] = useState<string | null>(null);

  const [completeDialogOpen, setCompleteDialogOpen] = useState(false);
  const [completeDraft, setCompleteDraft] = useState<{ workOrderId: string; quantity: number; batchNo?: string; expiryDate?: string }>({
    workOrderId: "",
    quantity: 1,
    batchNo: "",
    expiryDate: "",
  });
  const [completeError, setCompleteError] = useState<string | null>(null);

  const [salesDialogOpen, setSalesDialogOpen] = useState(false);
  const [salesDraft, setSalesDraft] = useState<SalesOrder>({
    id: "",
    customerId: "",
    orderedAt: new Date().toISOString().slice(0, 10),
    status: "draft",
    notes: "",
    lines: [],
  });
  const [salesError, setSalesError] = useState<string | null>(null);

  const [shipDialogOpen, setShipDialogOpen] = useState(false);
  const [shipDraft, setShipDraft] = useState<{ salesOrderId: string; shippedAt?: string; lines: SalesOrderLine[] }>({
    salesOrderId: "",
    shippedAt: new Date().toISOString().slice(0, 10),
    lines: [],
  });
  const [shipError, setShipError] = useState<string | null>(null);

  const staffById = useMemo(() => new Map(staff.map((s) => [s.id, s] as const)), [staff]);

  const activeList = useMemo(() => {
    if (tab === "staff") return staff;
    if (tab === "customers") return customers;
    if (tab === "suppliers") return suppliers;
    if (tab === "products") return products;
    if (tab === "rawMaterials") return rawMaterials;
    if (tab === "units") return units;
    if (tab === "bom") return bom;
    if (tab === "attendance") return attendanceRecords;
    return [];
  }, [attendanceRecords, bom, customers, products, rawMaterials, staff, suppliers, tab, units]);

  const filteredList = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return activeList;
    if (tab === "attendance") {
      return (activeList as AttendanceRecord[]).filter((r) => {
        const staffRow = staffById.get(r.staffId);
        const staffName = String(staffRow?.fullName ?? "").toLowerCase();
        const staffEmail = String(staffRow?.email ?? "").toLowerCase();
        return (
          r.id.toLowerCase().includes(q) ||
          r.staffId.toLowerCase().includes(q) ||
          r.date.toLowerCase().includes(q) ||
          r.clockIn.toLowerCase().includes(q) ||
          String(r.clockOut ?? "").toLowerCase().includes(q) ||
          staffName.includes(q) ||
          staffEmail.includes(q)
        );
      });
    }
    return activeList.filter((r) => {
      const id = String((r as { id: string }).id ?? "").toLowerCase();
      const name = String((r as { name?: string }).name ?? "").toLowerCase();
      const role = String((r as { role?: string }).role ?? "").toLowerCase();
      const roles = Array.isArray((r as { roles?: unknown }).roles)
        ? String(((r as { roles?: unknown[] }).roles ?? []).join(",")).toLowerCase()
        : "";
      const username = String((r as { username?: string }).username ?? "").toLowerCase();
      return id.includes(q) || name.includes(q) || role.includes(q) || roles.includes(q) || username.includes(q);
    });
  }, [activeList, search, staffById, tab]);

  const toggleSort = (key: string) => {
    setSort((prev) => {
      if (!prev || prev.key !== key) return { key, dir: "asc" };
      return { key, dir: prev.dir === "asc" ? "desc" : "asc" };
    });
  };

  const sortIndicator = (key: string): string => {
    if (!sort || sort.key !== key) return "";
    return sort.dir === "asc" ? " ▲" : " ▼";
  };

  const sortedList = useMemo(() => {
    if (!sort) return filteredList;
    const dir = sort.dir === "asc" ? 1 : -1;
    const key = sort.key;

    const valueFor = (record: unknown): string | number => {
      const r = record as Record<string, unknown>;
      if (tab === "staff") {
        if (key === "id") return String(r.id ?? "");
        if (key === "fullName") return String(r.fullName ?? "");
        if (key === "roles") return Array.isArray(r.roles) ? (r.roles as unknown[]).join(",") : String(r.role ?? "");
        if (key === "department") return String(r.department ?? "");
        if (key === "username") return String(r.username ?? "");
        if (key === "status") return String(r.status ?? "");
        if (key === "dateJoined") return String(r.dateJoined ?? "");
        if (key === "lastLogin") return String(r.lastLogin ?? "");
      }
      if (tab === "attendance") {
        const a = record as AttendanceRecord;
        const staffRow = staffById.get(a.staffId);
        if (key === "date") return a.date;
        if (key === "staffId") return a.staffId;
        if (key === "fullName") return String(staffRow?.fullName ?? "");
        if (key === "department") return String(staffRow?.department ?? "");
        if (key === "clockIn") return a.clockIn;
        if (key === "clockOut") return String(a.clockOut ?? "");
      }
      if (tab === "customers") {
        if (key === "id") return String(r.id ?? "");
        if (key === "name") return String(r.name ?? "");
        if (key === "phone") return String(r.phone ?? "");
        if (key === "email") return String(r.email ?? "");
        if (key === "address") return String(r.address ?? "");
      }
      if (tab === "suppliers") {
        if (key === "id") return String(r.id ?? "");
        if (key === "name") return String(r.name ?? "");
        if (key === "country") return String(r.country ?? "");
        if (key === "contactPerson") return String(r.contactPerson ?? "");
        if (key === "email") return String(r.email ?? "");
        if (key === "phone") return String(r.phone ?? "");
        if (key === "leadTimeDays") return Number(r.leadTimeDays ?? 0);
        if (key === "status") return String(r.status ?? "");
      }
      if (tab === "units") {
        if (key === "id") return String(r.id ?? "");
        if (key === "name") return String(r.name ?? "");
        if (key === "unitType") return String(r.unitType ?? "");
        if (key === "conversionBase") return String(r.conversionBase ?? "");
        if (key === "conversionRate") return Number(r.conversionRate ?? 0);
        if (key === "symbol") return String(r.symbol ?? "");
      }
      if (tab === "products") {
        if (key === "id") return String(r.id ?? "");
        if (key === "name") return String(r.name ?? "");
        if (key === "variant") return String(r.variant ?? "");
        if (key === "category") return String(r.category ?? "");
        if (key === "size") return Number(r.size ?? 0);
        if (key === "unit") return String(r.unit ?? "");
        if (key === "version") return String(r.version ?? "");
        if (key === "status") return String(r.status ?? "");
      }
      if (tab === "rawMaterials") {
        if (key === "id") return String(r.id ?? "");
        if (key === "name") return String(r.name ?? "");
        if (key === "inciName") return String(r.inciName ?? "");
        if (key === "specGrade") return String(r.specGrade ?? "");
        if (key === "unit") return String(r.unit ?? "");
        if (key === "shelfLifeMonths") return Number(r.shelfLifeMonths ?? 0);
        if (key === "storageCondition") return String(r.storageCondition ?? "");
        if (key === "status") return String(r.status ?? "");
      }
      if (tab === "bom") {
        if (key === "id") return String(r.id ?? "");
        if (key === "productId") return String(r.productId ?? "");
        if (key === "rawMaterialId") return String(r.rawMaterialId ?? "");
        if (key === "rawMaterialName") return String(r.rawMaterialName ?? "");
        if (key === "quantity") return Number(r.quantity ?? 0);
        if (key === "unit") return String(r.unit ?? "");
        if (key === "stage") return String(r.stage ?? "");
      }
      return String(r[key] ?? "");
    };

    const next = filteredList
      .map((item, idx) => ({ item, idx }))
      .sort((a, b) => {
        const av = valueFor(a.item);
        const bv = valueFor(b.item);
        if (typeof av === "number" && typeof bv === "number") {
          if (av !== bv) return (av - bv) * dir;
          return (a.idx - b.idx) * dir;
        }
        const as = String(av).toLowerCase();
        const bs = String(bv).toLowerCase();
        if (as < bs) return -1 * dir;
        if (as > bs) return 1 * dir;
        return (a.idx - b.idx) * dir;
      })
      .map((x) => x.item);

    return next;
  }, [filteredList, sort, staffById, tab]);

  const procurementRows = useMemo(() => {
    const supplierNameById = new Map(suppliers.map((s) => [s.id, s.name]));
    const q = search.trim().toLowerCase();
    const rows = purchaseOrders
      .slice()
      .sort((a, b) => Date.parse(b.orderedAt) - Date.parse(a.orderedAt))
      .map((po) => ({
        id: po.id,
        supplier: supplierNameById.get(po.supplierId) ?? po.supplierId,
        status: po.status,
        orderedAt: po.orderedAt,
        lineCount: po.lines.length,
        receiptCount: goodsReceipts.filter((r) => r.purchaseOrderId === po.id).length,
      }));
    if (!q) return rows;
    return rows.filter((r) => r.id.toLowerCase().includes(q) || r.supplier.toLowerCase().includes(q));
  }, [goodsReceipts, purchaseOrders, search, suppliers]);

  const inventoryRows = useMemo(() => {
    const unitNameById = new Map(units.map((u) => [u.id, u.symbol ? `${u.name} (${u.symbol})` : u.name]));
    const qtyByMaterial = new Map<string, number>();
    const lotCountByMaterial = new Map<string, number>();
    const earliestExpiryByMaterial = new Map<string, string>();
    for (const lot of stockLots) {
      qtyByMaterial.set(lot.rawMaterialId, (qtyByMaterial.get(lot.rawMaterialId) ?? 0) + (lot.quantityOnHand ?? 0));
      lotCountByMaterial.set(lot.rawMaterialId, (lotCountByMaterial.get(lot.rawMaterialId) ?? 0) + 1);
      if (lot.expiryDate) {
        const current = earliestExpiryByMaterial.get(lot.rawMaterialId);
        if (!current || Date.parse(lot.expiryDate) < Date.parse(current)) earliestExpiryByMaterial.set(lot.rawMaterialId, lot.expiryDate);
      }
    }
    const q = search.trim().toLowerCase();
    const rows = rawMaterials.map((m) => {
      const qty = qtyByMaterial.get(m.id) ?? 0;
      return {
        id: m.id,
        name: m.name,
        unit: m.unit ? unitNameById.get(m.unit) ?? m.unit : "-",
        onHand: qty,
        reorderLevel: null,
        isLow: false,
        lotCount: lotCountByMaterial.get(m.id) ?? 0,
        earliestExpiry: earliestExpiryByMaterial.get(m.id) ?? null,
      };
    });
    if (!q) return rows;
    return rows.filter((r) => r.id.toLowerCase().includes(q) || r.name.toLowerCase().includes(q));
  }, [rawMaterials, search, stockLots, units]);

  const poById = useMemo(() => new Map(purchaseOrders.map((po) => [po.id, po])), [purchaseOrders]);
  const materialById = useMemo(() => new Map(rawMaterials.map((m) => [m.id, m])), [rawMaterials]);
  const productById = useMemo(() => new Map(products.map((p) => [p.id, p])), [products]);
  const customerById = useMemo(() => new Map(customers.map((c) => [c.id, c])), [customers]);
  const woRows = useMemo(() => {
    const rows = workOrders
      .slice()
      .sort((a, b) => Date.parse(b.createdAt) - Date.parse(a.createdAt))
      .map((wo) => ({
        id: wo.id,
        product: productById.get(wo.productId)?.name ?? wo.productId,
        quantity: wo.quantity,
        status: wo.status,
        createdAt: wo.createdAt,
      }));
    if (!search.trim()) return rows;
    const q = search.trim().toLowerCase();
    return rows.filter((r) => r.id.toLowerCase().includes(q) || r.product.toLowerCase().includes(q));
  }, [workOrders, productById, search]);

  const finishedGoodsRows = useMemo(() => {
    const lotCountByProduct = new Map<string, number>();
    const qtyByProduct = new Map<string, number>();
    const earliestExpiryByProduct = new Map<string, string>();
    for (const lot of productLots) {
      lotCountByProduct.set(lot.productId, (lotCountByProduct.get(lot.productId) ?? 0) + 1);
      qtyByProduct.set(lot.productId, (qtyByProduct.get(lot.productId) ?? 0) + (lot.quantityOnHand ?? 0));
      if (lot.expiryDate) {
        const current = earliestExpiryByProduct.get(lot.productId);
        if (!current || Date.parse(lot.expiryDate) < Date.parse(current)) earliestExpiryByProduct.set(lot.productId, lot.expiryDate);
      }
    }
    const lastMoveAtByProduct = new Map<string, string>();
    for (const m of productMovements) {
      const current = lastMoveAtByProduct.get(m.productId);
      if (!current || Date.parse(m.at) > Date.parse(current)) lastMoveAtByProduct.set(m.productId, m.at);
    }
    const q = search.trim().toLowerCase();
    const rows = products.map((p) => ({
      id: p.id,
      name: p.name,
      onHand: qtyByProduct.get(p.id) ?? 0,
      lots: lotCountByProduct.get(p.id) ?? 0,
      earliestExpiry: earliestExpiryByProduct.get(p.id) ?? null,
      lastMoveAt: lastMoveAtByProduct.get(p.id) ?? null,
    }));
    if (!q) return rows;
    return rows.filter((r) => r.id.toLowerCase().includes(q) || r.name.toLowerCase().includes(q));
  }, [productLots, productMovements, products, search]);
  const salesRows = useMemo(() => {
    const rows = salesOrders
      .slice()
      .sort((a, b) => Date.parse(b.orderedAt) - Date.parse(a.orderedAt))
      .map((so) => ({
        id: so.id,
        customer: customerById.get(so.customerId)?.name ?? so.customerId,
        status: so.status,
        orderedAt: so.orderedAt,
        lineCount: so.lines.length,
        deliveries: deliveryNotes.filter((d) => d.salesOrderId === so.id).length,
        invoices: invoices.filter((inv) => inv.salesOrderId === so.id).length,
      }));
    if (!search.trim()) return rows;
    const q = search.trim().toLowerCase();
    return rows.filter((r) => r.id.toLowerCase().includes(q) || r.customer.toLowerCase().includes(q));
  }, [salesOrders, deliveryNotes, invoices, customerById, search]);

  const supplierNameById = useMemo(() => new Map(suppliers.map((s) => [s.id, s.name])), [suppliers]);
  const soById = useMemo(() => new Map(salesOrders.map((so) => [so.id, so])), [salesOrders]);

  const apPaidByBillId = useMemo(() => {
    const paidByBillId = new Map<string, number>();
    for (const p of apPayments) {
      if (!p.billId) continue;
      paidByBillId.set(p.billId, (paidByBillId.get(p.billId) ?? 0) + (p.amount ?? 0));
    }
    return paidByBillId;
  }, [apPayments]);

  const arReceivedByInvoiceId = useMemo(() => {
    const receivedByInvoiceId = new Map<string, number>();
    for (const r of arReceipts) {
      if (!r.invoiceId) continue;
      receivedByInvoiceId.set(r.invoiceId, (receivedByInvoiceId.get(r.invoiceId) ?? 0) + (r.amount ?? 0));
    }
    return receivedByInvoiceId;
  }, [arReceipts]);

  const apRows = useMemo(() => {
    const q = search.trim().toLowerCase();
    const rows = apBills
      .slice()
      .sort((a, b) => Date.parse(b.billedAt) - Date.parse(a.billedAt))
      .map((b) => {
        const paid = apPaidByBillId.get(b.id) ?? 0;
        const balance = (b.totalAmount ?? 0) - paid;
        return {
          id: b.id,
          supplier: b.supplierId ? supplierNameById.get(b.supplierId) ?? b.supplierId : "-",
          purchaseOrderId: b.purchaseOrderId ?? "-",
          status: b.status,
          billedAt: b.billedAt,
          totalAmount: b.totalAmount ?? 0,
          paid,
          balance,
        };
      });
    if (!q) return rows;
    return rows.filter((r) => r.id.toLowerCase().includes(q) || r.supplier.toLowerCase().includes(q));
  }, [apBills, apPaidByBillId, supplierNameById, search]);

  const arRows = useMemo(() => {
    const q = search.trim().toLowerCase();
    const rows = invoices
      .slice()
      .sort((a, b) => Date.parse(b.invoicedAt) - Date.parse(a.invoicedAt))
      .map((inv) => {
        const so = soById.get(inv.salesOrderId);
        const customerId = so?.customerId ?? "";
        const customerName = customerId ? customerById.get(customerId)?.name ?? customerId : "-";
        const received = arReceivedByInvoiceId.get(inv.id) ?? 0;
        const total = inv.totalAmount ?? 0;
        const balance = total - received;
        const status = balance <= 0 ? "Paid" : received > 0 ? "Partially Paid" : "Open";
        return {
          id: inv.id,
          salesOrderId: inv.salesOrderId,
          customer: customerName,
          status,
          invoicedAt: inv.invoicedAt,
          totalAmount: total,
          received,
          balance,
        };
      });
    if (!q) return rows;
    return rows.filter((r) => r.id.toLowerCase().includes(q) || r.customer.toLowerCase().includes(q));
  }, [invoices, soById, customerById, arReceivedByInvoiceId, search]);

  const cashAccountId = useMemo(() => financeAccounts.find((a) => a.code === "1000")?.id ?? "ACC-1000", [financeAccounts]);
  const cashBalance = useMemo(
    () => financeJournalLines.filter((l) => l.accountId === cashAccountId).reduce((sum, l) => sum + (l.debit - l.credit), 0),
    [financeJournalLines, cashAccountId],
  );
  const apOutstanding = useMemo(() => apRows.reduce((sum, r) => sum + r.balance, 0), [apRows]);
  const arOutstanding = useMemo(() => arRows.reduce((sum, r) => sum + r.balance, 0), [arRows]);

  const journalRows = useMemo(() => {
    const totalsByJeId = new Map<string, { debit: number; credit: number }>();
    for (const l of financeJournalLines) {
      const current = totalsByJeId.get(l.journalEntryId) ?? { debit: 0, credit: 0 };
      totalsByJeId.set(l.journalEntryId, { debit: current.debit + (l.debit ?? 0), credit: current.credit + (l.credit ?? 0) });
    }
    return financeJournalEntries
      .slice()
      .sort((a, b) => Date.parse(b.postedAt) - Date.parse(a.postedAt))
      .map((je) => {
        const totals = totalsByJeId.get(je.id) ?? { debit: 0, credit: 0 };
        const source = je.sourceTable && je.sourceId ? `${je.sourceTable}:${je.sourceId}` : je.sourceTable ?? "-";
        return { ...je, source, debit: totals.debit, credit: totals.credit };
      });
  }, [financeJournalEntries, financeJournalLines]);

  if (!user) return <Navigate to="/login" replace />;
  if (String(user.role ?? "").toLowerCase() !== "superadmin") return <Navigate to="/manager" replace />;

  const startCreate = (key: EditableEntityKey) => {
    setFormError(null);
    setEditingKey(key);
    if (key === "staff") {
      setStaffDraft({
        id: "",
        fullName: "",
        email: "",
        role: "operator",
        roles: ["operator"],
        department: "",
        username: "",
        passwordHash: "abcd1234",
        status: "active",
        dateJoined: "",
        lastLogin: "",
        modules: defaultModulesForRole("operator"),
      });
    }
    if (key === "customers") setCustomerDraft({ id: "", name: "" });
    if (key === "suppliers") setSupplierDraft({ id: "", name: "", status: "Active" });
    if (key === "units") setUnitDraft({ id: "", name: "" });
    if (key === "products") setProductDraft({ id: "", name: "", status: "Active" });
    if (key === "rawMaterials") setRawMaterialDraft({ id: "", name: "", status: "Active" });
    if (key === "bom") setBomDraft({ id: "", productId: "", rawMaterialId: "", quantity: 1 });
    setDialogOpen(true);
  };

  const startEdit = (key: EditableEntityKey, record: unknown) => {
    setFormError(null);
    setEditingKey(key);
    if (key === "staff") setStaffDraft(record as Staff);
    if (key === "customers") setCustomerDraft(record as Customer);
    if (key === "suppliers") setSupplierDraft(record as Supplier);
    if (key === "units") setUnitDraft(record as MeasurementUnit);
    if (key === "products") setProductDraft(record as Product);
    if (key === "rawMaterials") setRawMaterialDraft(record as RawMaterial);
    if (key === "bom") setBomDraft(record as BillOfMaterialLine);
    setDialogOpen(true);
  };

  const submit = () => {
    if (!editingKey) return;
    setFormError(null);
    try {
      if (editingKey === "staff") {
        const roles = (staffDraft.roles?.length ? staffDraft.roles : [staffDraft.role]).filter(Boolean) as UserRole[];
        const normalizedRoles = roles.length ? roles : (["operator"] as UserRole[]);
        const normalizedRole = normalizedRoles[0] ?? "operator";
        const normalizedModules = Array.from(new Set([...(staffDraft.modules ?? []), ...defaultModulesForRoles(normalizedRoles)]));
        const parsed = staffSchema.parse({
          id: toRequiredString(staffDraft.id),
          fullName: toRequiredString(staffDraft.fullName),
          email: toOptionalString(staffDraft.email),
          role: normalizedRole,
          roles: normalizedRoles,
          department: toOptionalString(staffDraft.department),
          username: toRequiredString(staffDraft.username),
          passwordHash: toRequiredString(staffDraft.passwordHash),
          status: staffDraft.status,
          dateJoined: parseCsvDateTime(staffDraft.dateJoined),
          lastLogin: parseCsvDateTime(staffDraft.lastLogin),
          modules: normalizedModules,
        });
        upsertRecord("staff", parsed as Staff, actor);
      }
      if (editingKey === "customers") {
        const parsed = customerSchema.parse({
          id: toRequiredString(customerDraft.id),
          name: toRequiredString(customerDraft.name),
          phone: toOptionalString(customerDraft.phone),
          email: toOptionalString(customerDraft.email),
          address: toOptionalString(customerDraft.address),
        });
        upsertRecord("customers", parsed as Customer, actor);
      }
      if (editingKey === "suppliers") {
        const leadTimeDaysParsed =
          supplierDraft.leadTimeDays === undefined || supplierDraft.leadTimeDays === null ? undefined : toNumber(supplierDraft.leadTimeDays);
        const parsed = supplierSchema.parse({
          id: toRequiredString(supplierDraft.id),
          name: toRequiredString(supplierDraft.name),
          country: toOptionalString(supplierDraft.country),
          contactPerson: toOptionalString(supplierDraft.contactPerson),
          phone: toOptionalString(supplierDraft.phone),
          email: toOptionalString(supplierDraft.email),
          leadTimeDays: Number.isFinite(leadTimeDaysParsed) ? leadTimeDaysParsed : undefined,
          status: toOptionalString(supplierDraft.status),
        });
        upsertRecord("suppliers", parsed as Supplier, actor);
      }
      if (editingKey === "units") {
        const conversionRateParsed =
          unitDraft.conversionRate === undefined || unitDraft.conversionRate === null ? undefined : toNumber(unitDraft.conversionRate);
        const parsed = unitSchema.parse({
          id: toRequiredString(unitDraft.id),
          name: toRequiredString(unitDraft.name),
          unitType: toOptionalString(unitDraft.unitType),
          conversionBase: toOptionalString(unitDraft.conversionBase),
          conversionRate: Number.isFinite(conversionRateParsed) ? conversionRateParsed : undefined,
          symbol: toOptionalString(unitDraft.symbol),
        });
        upsertRecord("units", parsed as MeasurementUnit, actor);
      }
      if (editingKey === "products") {
        const sizeParsed = productDraft.size === undefined || productDraft.size === null ? undefined : toNumber(productDraft.size);
        const parsed = productSchema.parse({
          id: toRequiredString(productDraft.id),
          name: toRequiredString(productDraft.name),
          variant: toOptionalString(productDraft.variant),
          category: toOptionalString(productDraft.category),
          size: Number.isFinite(sizeParsed) ? sizeParsed : undefined,
          unit: toOptionalString(productDraft.unit),
          version: toOptionalString(productDraft.version),
          status: toOptionalString(productDraft.status),
        });
        upsertRecord("products", parsed as Product, actor);
      }
      if (editingKey === "rawMaterials") {
        const shelfLifeParsed =
          rawMaterialDraft.shelfLifeMonths === undefined || rawMaterialDraft.shelfLifeMonths === null
            ? undefined
            : toNumber(rawMaterialDraft.shelfLifeMonths);
        const parsed = rawMaterialSchema.parse({
          id: toRequiredString(rawMaterialDraft.id),
          name: toRequiredString(rawMaterialDraft.name),
          inciName: toOptionalString(rawMaterialDraft.inciName),
          specGrade: toOptionalString(rawMaterialDraft.specGrade),
          unit: toOptionalString(rawMaterialDraft.unit),
          shelfLifeMonths: Number.isFinite(shelfLifeParsed) ? shelfLifeParsed : undefined,
          storageCondition: toOptionalString(rawMaterialDraft.storageCondition),
          status: toOptionalString(rawMaterialDraft.status),
        });
        upsertRecord("rawMaterials", parsed as RawMaterial, actor);
      }
      if (editingKey === "bom") {
        const parsed = bomSchema.parse({
          id: toRequiredString(bomDraft.id),
          productId: toRequiredString(bomDraft.productId),
          rawMaterialId: toRequiredString(bomDraft.rawMaterialId),
          rawMaterialName: toOptionalString(bomDraft.rawMaterialName),
          quantity: toNumber(bomDraft.quantity),
          unit: toOptionalString(bomDraft.unit),
          stage: toOptionalString(bomDraft.stage),
        });
        upsertRecord("bom", parsed as BillOfMaterialLine, actor);
      }
      setDialogOpen(false);
    } catch (e) {
      const message = e instanceof Error ? e.message : "Invalid form";
      setFormError(message);
    }
  };

  const startImport = (key: EditableEntityKey) => {
    setImportKey(key);
    setImportError(null);
    fileInputRef.current?.click();
  };

  const handleImportFile = async (file: File) => {
    if (!importKey) return;
    setImportError(null);

    const result = await new Promise<Papa.ParseResult<Record<string, unknown>>>((resolve) => {
      Papa.parse<Record<string, unknown>>(file, { header: true, skipEmptyLines: true, complete: resolve });
    });

    if (result.errors.length) {
      setImportError(result.errors[0]?.message ?? "Failed to parse CSV");
      return;
    }

    const rows = result.data ?? [];
    try {
      if (importKey === "staff") {
        const normalizeRole = (value: unknown): UserRole => {
          const raw = String(value ?? "").trim().toLowerCase();
          const allowed: UserRole[] = ["superadmin", "manager", "supervisor", "staff", "operations", "procurement", "sales"];
          return allowed.includes(raw as UserRole) ? (raw as UserRole) : "staff";
        };
        const normalizeStatus = (value: unknown): StaffStatus => {
          const raw = String(value ?? "").trim().toLowerCase();
          if (raw === "inactive") return "inactive";
          if (raw === "active") return "active";
          if (raw.startsWith("in")) return "inactive";
          return "active";
        };
        const next: Staff[] = rows.map((row) => {
          const role = normalizeRole(pickNormalized(row, ["role"]));
          const modules = defaultModulesForRole(role);
          const parsed = staffSchema.parse({
            id: toRequiredString(pickNormalized(row, ["user id", "userid", "user_id", "id"])),
            fullName: toRequiredString(pickNormalized(row, ["full name", "fullname", "full_name", "name"])),
            email: toOptionalString(pickNormalized(row, ["email", "emailaddress", "email_address"])),
            role,
            department: toOptionalString(pickNormalized(row, ["department", "dept"])),
            username: toRequiredString(pickNormalized(row, ["username", "user"])),
            passwordHash: toRequiredString(pickNormalized(row, ["password hash", "passwordhash", "password_hash", "password", "pass"])),
            status: normalizeStatus(pickNormalized(row, ["status"])),
            dateJoined: parseCsvDateTime(pickNormalized(row, ["date joined", "datejoined", "date_joined"])),
            lastLogin: parseCsvDateTime(pickNormalized(row, ["last login", "lastlogin", "last_login"])),
            modules,
          });
          return parsed as Staff;
        });
        const merged = new Map<string, Staff>();
        staff.forEach((s) => merged.set(s.id, s));
        next.forEach((s) => merged.set(s.id, s));
        importRecords("staff", Array.from(merged.values()) as Staff[], actor);
      }
      if (importKey === "customers") {
        const next: Customer[] = rows.map((row) => {
          const parsed = customerSchema.parse({
            id: toRequiredString(pickNormalized(row, ["id", "customerid", "customer_id", "customer id"])),
            name: toRequiredString(pickNormalized(row, ["name", "customername", "customer_name", "customer name"])),
            phone: toOptionalString(pickNormalized(row, ["phone", "phone_no", "phoneno", "mobile"])),
            email: toOptionalString(pickNormalized(row, ["email", "emailaddress", "email_address"])),
            address: toOptionalString(pickNormalized(row, ["address", "addr"])),
          });
          return parsed as Customer;
        });
        const merged = new Map<string, Customer>();
        customers.forEach((c) => merged.set(c.id, c));
        next.forEach((c) => merged.set(c.id, c));
        importRecords("customers", Array.from(merged.values()) as Customer[], actor);
      }
      if (importKey === "suppliers") {
        const next: Supplier[] = rows.map((row) => {
          const leadTimeRaw = pickNormalized(row, ["lead time (days)", "leadtimedays", "lead_time_days", "lead time days"]);
          const leadTimeParsed = toNumber(leadTimeRaw);
          const parsed = supplierSchema.parse({
            id: toRequiredString(pickNormalized(row, ["supplier id", "supplierid", "supplier_id", "id"])),
            name: toRequiredString(pickNormalized(row, ["supplier name", "suppliername", "supplier_name", "name"])),
            country: toOptionalString(pickNormalized(row, ["country"])),
            contactPerson: toOptionalString(pickNormalized(row, ["contact person", "contactperson", "contact_person"])),
            email: toOptionalString(pickNormalized(row, ["email", "emailaddress", "email_address"])),
            phone: toOptionalString(pickNormalized(row, ["phone", "phone_no", "phoneno", "mobile"])),
            leadTimeDays: Number.isFinite(leadTimeParsed) ? leadTimeParsed : undefined,
            status: toOptionalString(pickNormalized(row, ["status"])),
          });
          return parsed as Supplier;
        });
        const merged = new Map<string, Supplier>();
        suppliers.forEach((s) => merged.set(s.id, s));
        next.forEach((s) => merged.set(s.id, s));
        importRecords("suppliers", Array.from(merged.values()) as Supplier[], actor);
      }
      if (importKey === "units") {
        const next: MeasurementUnit[] = rows.map((row) => {
          const conversionRateRaw = pickNormalized(row, ["conversion rate", "conversionrate", "conversion_rate"]);
          const conversionRateParsed = toNumber(conversionRateRaw);
          const parsed = unitSchema.parse({
            id: toRequiredString(pickNormalized(row, ["unit code", "unitcode", "unit_id", "unit id", "id"])),
            name: toRequiredString(pickNormalized(row, ["unit name", "unitname", "unit_name", "name"])),
            unitType: toOptionalString(pickNormalized(row, ["type", "unit type", "unit_type"])),
            conversionBase: toOptionalString(pickNormalized(row, ["conversion base", "conversionbase", "conversion_base"])),
            conversionRate: Number.isFinite(conversionRateParsed) ? conversionRateParsed : undefined,
            symbol: toOptionalString(pickNormalized(row, ["symbol", "abbr", "abbreviation"])),
          });
          return parsed as MeasurementUnit;
        });
        const merged = new Map<string, MeasurementUnit>();
        units.forEach((u) => merged.set(u.id, u));
        next.forEach((u) => merged.set(u.id, u));
        importRecords("units", Array.from(merged.values()) as MeasurementUnit[], actor);
      }
      if (importKey === "products") {
        const next: Product[] = rows.map((row) => {
          const sizeRaw = pickNormalized(row, ["size"]);
          const sizeParsed = toNumber(sizeRaw);
          const parsed = productSchema.parse({
            id: toRequiredString(pickNormalized(row, ["sku", "product sku", "productsku", "id"])),
            name: toRequiredString(pickNormalized(row, ["product name", "productname", "product_name", "name"])),
            variant: toOptionalString(pickNormalized(row, ["variant"])),
            category: toOptionalString(pickNormalized(row, ["category"])),
            size: Number.isFinite(sizeParsed) ? sizeParsed : undefined,
            unit: toOptionalString(pickNormalized(row, ["unit"])),
            version: toOptionalString(pickNormalized(row, ["version"])),
            status: toOptionalString(pickNormalized(row, ["status"])),
          });
          return parsed as Product;
        });
        const merged = new Map<string, Product>();
        products.forEach((p) => merged.set(p.id, p));
        next.forEach((p) => merged.set(p.id, p));
        importRecords("products", Array.from(merged.values()) as Product[], actor);
      }
      if (importKey === "rawMaterials") {
        const next: RawMaterial[] = rows.map((row) => {
          const shelfLifeRaw = pickNormalized(row, ["shelf life (months)", "shelf life months", "shelflifemonths", "shelf_life_months"]);
          const shelfLifeParsed = toNumber(shelfLifeRaw);
          const parsed = rawMaterialSchema.parse({
            id: toRequiredString(pickNormalized(row, ["material code", "materialcode", "material_id", "material id", "raw_material_id", "id"])),
            name: toRequiredString(pickNormalized(row, ["material name", "materialname", "raw material name", "name"])),
            inciName: toOptionalString(pickNormalized(row, ["inci name", "inciname", "inci_name"])),
            specGrade: toOptionalString(pickNormalized(row, ["spec grade", "specgrade", "spec_grade"])),
            unit: toOptionalString(pickNormalized(row, ["unit"])),
            shelfLifeMonths: Number.isFinite(shelfLifeParsed) ? shelfLifeParsed : undefined,
            storageCondition: toOptionalString(pickNormalized(row, ["storage condition", "storagecondition", "storage_condition"])),
            status: toOptionalString(pickNormalized(row, ["status"])),
          });
          return parsed as RawMaterial;
        });
        const merged = new Map<string, RawMaterial>();
        rawMaterials.forEach((m) => merged.set(m.id, m));
        next.forEach((m) => merged.set(m.id, m));
        importRecords("rawMaterials", Array.from(merged.values()) as RawMaterial[], actor);
      }
      if (importKey === "bom") {
        const next: BillOfMaterialLine[] = rows.map((row) => {
          const parsed = bomSchema.parse({
            id: toRequiredString(pickNormalized(row, ["bom id", "bomid", "bom_id", "id"])),
            productId: toRequiredString(pickNormalized(row, ["product sku", "productsku", "product_id", "product id", "productid"])),
            rawMaterialId: toRequiredString(pickNormalized(row, ["material code", "materialcode", "raw material id", "rawmaterialid", "raw_material_id"])),
            rawMaterialName: toOptionalString(pickNormalized(row, ["material name", "materialname", "raw material name"])),
            quantity: toNumber(pickNormalized(row, ["quantity", "qty"])),
            unit: toOptionalString(pickNormalized(row, ["unit"])),
            stage: toOptionalString(pickNormalized(row, ["stage"])),
          });
          return parsed as BillOfMaterialLine;
        });
        const merged = new Map<string, BillOfMaterialLine>();
        bom.forEach((b) => merged.set(b.id, b));
        next.forEach((b) => merged.set(b.id, b));
        importRecords("bom", Array.from(merged.values()) as BillOfMaterialLine[], actor);
      }
      setImportKey(null);
    } catch (e) {
      const message = e instanceof Error ? e.message : "Import failed";
      setImportError(message);
    }
  };

  const downloadCsv = (filename: string, csvText: string) => {
    const blob = new Blob([`\ufeff${csvText}`], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  };

  const exportCsv = (key: EditableEntityKey) => {
    if (key === "staff") {
      const fields = ["User ID", "Full Name", "Email", "Role", "Department", "Username", "Password Hash", "Status", "Date Joined", "Last Login"];
      const data = staff.map((s) => [
        s.id,
        s.fullName,
        s.email ?? "",
        s.role,
        s.department ?? "",
        s.username,
        s.passwordHash,
        s.status,
        formatCsvDateTime(s.dateJoined),
        formatCsvDateTime(s.lastLogin),
      ]);
      const csv = Papa.unparse({ fields, data });
      return downloadCsv("staff.csv", csv);
    }
    if (key === "suppliers") {
      const fields = ["Supplier ID", "Supplier Name", "Country", "Contact Person", "Email", "Phone", "Lead Time (Days)", "Status"];
      const data = suppliers.map((s) => [s.id, s.name, s.country ?? "", s.contactPerson ?? "", s.email ?? "", s.phone ?? "", s.leadTimeDays ?? "", s.status ?? ""]);
      const csv = Papa.unparse({ fields, data });
      return downloadCsv("suppliers.csv", csv);
    }
    if (key === "units") {
      const fields = ["Unit Code", "Unit Name", "Type", "Conversion Base", "Conversion Rate"];
      const data = units.map((u) => [u.id, u.name, u.unitType ?? "", u.conversionBase ?? "", u.conversionRate ?? ""]);
      const csv = Papa.unparse({ fields, data });
      return downloadCsv("units.csv", csv);
    }
    if (key === "products") {
      const fields = ["SKU", "Product Name", "Variant", "Category", "Size", "Unit", "Version", "Status"];
      const data = products.map((p) => [p.id, p.name, p.variant ?? "", p.category ?? "", p.size ?? "", p.unit ?? "", p.version ?? "", p.status ?? ""]);
      const csv = Papa.unparse({ fields, data });
      return downloadCsv("products.csv", csv);
    }
    if (key === "rawMaterials") {
      const fields = ["Material Code", "Material Name", "INCI Name", "Spec Grade", "Unit", "Shelf Life (Months)", "Storage Condition", "Status"];
      const data = rawMaterials.map((m) => [
        m.id,
        m.name,
        m.inciName ?? "",
        m.specGrade ?? "",
        m.unit ?? "",
        m.shelfLifeMonths ?? "",
        m.storageCondition ?? "",
        m.status ?? "",
      ]);
      const csv = Papa.unparse({ fields, data });
      return downloadCsv("raw-materials.csv", csv);
    }
    if (key === "bom") {
      const rawMaterialById = new Map(rawMaterials.map((m) => [m.id, m] as const));
      const fields = ["BOM ID", "Product SKU", "Material Code", "Material Name", "Quantity", "Unit", "Stage"];
      const data = bom.map((b) => [
        b.id,
        b.productId,
        b.rawMaterialId,
        b.rawMaterialName ?? rawMaterialById.get(b.rawMaterialId)?.name ?? "",
        b.quantity,
        b.unit ?? "",
        b.stage ?? "",
      ]);
      const csv = Papa.unparse({ fields, data });
      return downloadCsv("bom.csv", csv);
    }
  };

  const isMasterDataTab = (key: EntityTabKey): key is EditableEntityKey =>
    key === "staff" || key === "customers" || key === "suppliers" || key === "products" || key === "rawMaterials" || key === "units" || key === "bom";

  const deleteById = (entity: EditableEntityKey, id: string) => {
    deleteRecord(entity, id, actor);
  };

  const canCreatePo = true;
  const canReceivePo = true;
  const canAdjustStock = true;

  const makePoId = (): string => {
    const date = new Date().toISOString().slice(0, 10).replace(/-/g, "");
    const rand = Math.random().toString(16).slice(2, 6).toUpperCase();
    return `PO-${date}-${rand}`;
  };

  const startNewPo = () => {
    setPoError(null);
    setPoDraft({
      id: makePoId(),
      supplierId: "",
      orderedAt: new Date().toISOString().slice(0, 10),
      expectedAt: "",
      status: "draft",
      notes: "",
      lines: [{ id: newId(), rawMaterialId: "", unitId: undefined, quantity: 1 }],
    });
    setPoDialogOpen(true);
  };

  const startEditPo = (po: PurchaseOrder) => {
    setPoError(null);
    setPoDraft({
      ...po,
      expectedAt: po.expectedAt ?? "",
      notes: po.notes ?? "",
      lines: po.lines.length ? po.lines : [{ id: newId(), rawMaterialId: "", unitId: undefined, quantity: 1 }],
    });
    setPoDialogOpen(true);
  };

  const addPoLine = () => setPoDraft((p) => ({ ...p, lines: [...p.lines, { id: newId(), rawMaterialId: "", unitId: undefined, quantity: 1 }] }));

  const updatePoLine = (lineId: string, patch: Partial<PurchaseOrderLine>) =>
    setPoDraft((p) => ({ ...p, lines: p.lines.map((l) => (l.id === lineId ? { ...l, ...patch } : l)) }));

  const removePoLine = (lineId: string) => setPoDraft((p) => ({ ...p, lines: p.lines.filter((l) => l.id !== lineId) }));

  const deletePo = (poId: string) => {
    deletePurchaseOrder(poId, actor);
  };

  const submitPo = () => {
    setPoError(null);
    const id = toRequiredString(poDraft.id);
    const supplierId = toRequiredString(poDraft.supplierId);
    if (!id) return setPoError("PO ID is required.");
    if (!supplierId) return setPoError("Supplier is required.");
    const lines = poDraft.lines
      .map((l) => ({ ...l, rawMaterialId: toRequiredString(l.rawMaterialId), quantity: toNumber(l.quantity), unitId: toOptionalString(l.unitId) }))
      .filter((l) => l.rawMaterialId && Number.isFinite(l.quantity) && l.quantity > 0);
    if (!lines.length) return setPoError("Add at least one valid line (material + qty).");

    const order: PurchaseOrder = {
      id,
      supplierId,
      orderedAt: toRequiredString(poDraft.orderedAt) || new Date().toISOString(),
      expectedAt: toOptionalString(poDraft.expectedAt),
      status: poDraft.status,
      notes: toOptionalString(poDraft.notes),
      lines,
    };
    upsertPurchaseOrder(order, actor);
    setPoDialogOpen(false);
  };

  const startReceive = (poId: string) => {
    setReceiveError(null);
    const po = purchaseOrders.find((o) => o.id === poId);
    if (!po) {
      setReceiveError("Purchase order not found.");
      setReceiveDialogOpen(true);
      return;
    }
    setReceiveDraft({
      purchaseOrderId: po.id,
      receivedAt: new Date().toISOString().slice(0, 10),
      lines: po.lines.map<GoodsReceiptLine>((l) => ({
        id: newId(),
        rawMaterialId: l.rawMaterialId,
        unitId: l.unitId,
        quantity: l.quantity,
        batchNo: "",
        expiryDate: "",
      })),
    });
    setReceiveDialogOpen(true);
  };

  const addReceiveLine = () =>
    setReceiveDraft((p) => ({
      ...p,
      lines: [...p.lines, { id: newId(), rawMaterialId: "", unitId: undefined, quantity: 1, batchNo: "", expiryDate: "" }],
    }));

  const updateReceiveLine = (lineId: string, patch: Partial<GoodsReceiptLine>) =>
    setReceiveDraft((p) => ({ ...p, lines: p.lines.map((l) => (l.id === lineId ? { ...l, ...patch } : l)) }));

  const removeReceiveLine = (lineId: string) =>
    setReceiveDraft((p) => ({ ...p, lines: p.lines.length <= 1 ? p.lines : p.lines.filter((l) => l.id !== lineId) }));

  const submitReceive = () => {
    setReceiveError(null);
    const receivedAtRaw = toOptionalString(receiveDraft.receivedAt);
    const receivedAt = receivedAtRaw ? new Date(receivedAtRaw).toISOString() : undefined;
    const error = receivePurchaseOrder(
      {
        purchaseOrderId: toRequiredString(receiveDraft.purchaseOrderId),
        receivedAt,
        lines: receiveDraft.lines.map((l) => ({
          rawMaterialId: toRequiredString(l.rawMaterialId),
          unitId: toOptionalString(l.unitId),
          quantity: toNumber(l.quantity),
          batchNo: toOptionalString(l.batchNo),
          expiryDate: toOptionalString(l.expiryDate),
        })),
      },
      actor,
    );
    if (error) return setReceiveError(error);
    setReceiveDialogOpen(false);
  };

  const startAdjustStock = () => {
    setAdjustError(null);
    setAdjustDraft({
      rawMaterialId: "",
      unitId: undefined,
      quantityDelta: 0,
      at: new Date().toISOString().slice(0, 10),
      batchNo: "",
      expiryDate: "",
    });
    setAdjustDialogOpen(true);
  };

  const startAdjustFor = (rawMaterialId: string, unitId?: string) => {
    setAdjustError(null);
    setAdjustDraft({
      rawMaterialId,
      unitId,
      quantityDelta: 0,
      at: new Date().toISOString().slice(0, 10),
      batchNo: "",
      expiryDate: "",
    });
    setAdjustDialogOpen(true);
  };

  const submitAdjustStock = () => {
    setAdjustError(null);
    const atRaw = toOptionalString(adjustDraft.at);
    const at = atRaw ? new Date(atRaw).toISOString() : undefined;
    const err = adjustStock(
      {
        rawMaterialId: toRequiredString(adjustDraft.rawMaterialId),
        unitId: toOptionalString(adjustDraft.unitId),
        quantityDelta: toNumber(adjustDraft.quantityDelta),
        at,
        batchNo: toOptionalString(adjustDraft.batchNo),
        expiryDate: toOptionalString(adjustDraft.expiryDate),
      },
      actor,
    );
    if (err) return setAdjustError(err);
    setAdjustDialogOpen(false);
  };

  const moduleDescription = (m: AppModule): string => {
    if (m === "inventory") return "Stock control, purchasing, and master data.";
    if (m === "production") return "Work orders, outputs, and production tracking.";
    if (m === "sales") return "Sales orders, delivery, and invoicing.";
    if (m === "finance") return "Financial views and reporting.";
    return "Staff access and administration.";
  };

  const goToModules = () => {
    setActiveModule(null);
    setSearch("");
  };

  const actionNode =
    tab === "procurement" ? (
      <div className="flex items-center gap-2">
        <Input
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search PO or supplier..."
          className="w-[260px] bg-[hsl(220,20%,12%)]"
        />
        <Button onClick={startNewPo} disabled={!canCreatePo}>
          New PO
        </Button>
      </div>
    ) : tab === "inventory" ? (
      <div className="flex items-center gap-2">
        <Input
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search material..."
          className="w-[260px] bg-[hsl(220,20%,12%)]"
        />
        <Button onClick={startAdjustStock} disabled={!canAdjustStock}>
          Adjust Stock
        </Button>
      </div>
    ) : tab === "attendance" ? (
      <div className="flex items-center gap-2">
        <Input
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search staff or date..."
          className="w-[260px] bg-[hsl(220,20%,12%)]"
        />
      </div>
    ) : tab !== "audit" && isMasterDataTab(tab) ? (
      <div className="flex items-center gap-2">
        <Input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search..." className="w-[260px] bg-[hsl(220,20%,12%)]" />
        <Button onClick={() => startImport(tab)}>Import CSV</Button>
        {tab === "staff" || tab === "suppliers" || tab === "units" || tab === "products" || tab === "rawMaterials" || tab === "bom" ? (
          <Button variant="outline" onClick={() => exportCsv(tab)}>
            Export CSV
          </Button>
        ) : null}
        <Button onClick={() => startCreate(tab)}>Add</Button>
      </div>
    ) : tab === "audit" ? (
      <div className="flex items-center gap-2">
        <Button variant="outline" onClick={clearAudit}>
          Clear Audit
        </Button>
      </div>
    ) : null;

  return (
    <div className="min-h-screen bg-[hsl(220,20%,8%)] text-foreground font-sans">
      <div className="sticky top-0 z-10 border-b border-[hsl(220,20%,18%)] bg-[hsl(220,20%,8%)]/80 backdrop-blur">
        <div className="mx-auto flex max-w-6xl items-center justify-between px-6 py-4">
          <div>
            <button
              type="button"
              onClick={activeModule ? goToModules : undefined}
              className={activeModule ? "text-xl font-bold text-[hsl(45,93%,47%)] transition hover:text-[hsl(45,93%,55%)]" : "text-xl font-bold text-[hsl(45,93%,47%)]"}
            >
              SHAB Dashboard
            </button>
            <div className="text-sm text-muted-foreground">
              {user.fullName} ({user.role})
            </div>
          </div>
          <Button variant="outline" onClick={logout}>
            Logout
          </Button>
        </div>
      </div>

      <div className="mx-auto max-w-6xl p-6">
        {activeModule === null ? (
          <>
            <div className="mb-6">
              <div className="text-3xl font-bold text-[hsl(45,93%,47%)]">Modules</div>
              <div className="text-muted-foreground">Select a module to open the dashboard.</div>
            </div>

            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
              <button
                type="button"
                onClick={() => openModule("inventory")}
                className="rounded-2xl border border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)] p-5 text-left transition hover:border-[hsl(45,93%,47%)]/40 hover:bg-[hsl(220,20%,11%)]"
              >
                <div className="mb-4 inline-flex h-10 w-10 items-center justify-center rounded-xl bg-[hsl(45,93%,47%)]/15 text-[hsl(45,93%,47%)]">
                  <Warehouse className="h-5 w-5" />
                </div>
                <div className="text-lg font-semibold text-[hsl(0,0%,98%)]">Procurement</div>
                <div className="mt-1 text-sm text-[hsl(220,10%,60%)]">Purchasing, stock, materials, and master data.</div>
              </button>

              <button
                type="button"
                onClick={() => openModule("production")}
                className="rounded-2xl border border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)] p-5 text-left transition hover:border-[hsl(45,93%,47%)]/40 hover:bg-[hsl(220,20%,11%)]"
              >
                <div className="mb-4 inline-flex h-10 w-10 items-center justify-center rounded-xl bg-[hsl(45,93%,47%)]/15 text-[hsl(45,93%,47%)]">
                  <Factory className="h-5 w-5" />
                </div>
                <div className="text-lg font-semibold text-[hsl(0,0%,98%)]">Production</div>
                <div className="mt-1 text-sm text-[hsl(220,10%,60%)]">Plan and track work orders and outputs.</div>
              </button>

              <button
                type="button"
                onClick={() => openModule("sales")}
                className="rounded-2xl border border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)] p-5 text-left transition hover:border-[hsl(45,93%,47%)]/40 hover:bg-[hsl(220,20%,11%)]"
              >
                <div className="mb-4 inline-flex h-10 w-10 items-center justify-center rounded-xl bg-[hsl(45,93%,47%)]/15 text-[hsl(45,93%,47%)]">
                  <ShoppingCart className="h-5 w-5" />
                </div>
                <div className="text-lg font-semibold text-[hsl(0,0%,98%)]">Sales</div>
                <div className="mt-1 text-sm text-[hsl(220,10%,60%)]">Orders, delivery notes, and invoices.</div>
              </button>

              <button
                type="button"
                onClick={() => openModule("finance")}
                className="rounded-2xl border border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)] p-5 text-left transition hover:border-[hsl(45,93%,47%)]/40 hover:bg-[hsl(220,20%,11%)]"
              >
                <div className="mb-4 inline-flex h-10 w-10 items-center justify-center rounded-xl bg-[hsl(45,93%,47%)]/15 text-[hsl(45,93%,47%)]">
                  <Wallet className="h-5 w-5" />
                </div>
                <div className="text-lg font-semibold text-[hsl(0,0%,98%)]">Finance</div>
                <div className="mt-1 text-sm text-[hsl(220,10%,60%)]">Financial views and reporting.</div>
              </button>

              <button
                type="button"
                onClick={() => openModule("hr")}
                className="rounded-2xl border border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)] p-5 text-left transition hover:border-[hsl(45,93%,47%)]/40 hover:bg-[hsl(220,20%,11%)]"
              >
                <div className="mb-4 inline-flex h-10 w-10 items-center justify-center rounded-xl bg-[hsl(45,93%,47%)]/15 text-[hsl(45,93%,47%)]">
                  <Users className="h-5 w-5" />
                </div>
                <div className="text-lg font-semibold text-[hsl(0,0%,98%)]">Human Resource</div>
                <div className="mt-1 text-sm text-[hsl(220,10%,60%)]">User access, roles, and staff management.</div>
              </button>

              <div className="rounded-2xl border border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)] p-5 text-left">
                <div className="mb-4 inline-flex h-10 w-10 items-center justify-center rounded-xl bg-[hsl(45,93%,47%)]/15 text-[hsl(45,93%,47%)]">
                  <span className="text-sm font-semibold">A</span>
                </div>
                <div className="text-lg font-semibold text-[hsl(0,0%,98%)]">Analytics</div>
                <div className="mt-1 text-sm text-[hsl(220,10%,60%)]">Dashboards and insights.</div>
              </div>
            </div>
          </>
        ) : (
          <>
            <div className="mb-6 flex items-end justify-between gap-4">
              <div>
                <div className="text-3xl font-bold text-[hsl(45,93%,47%)]">{labelForModule(activeModule)}</div>
                <div className="text-muted-foreground">{moduleDescription(activeModule)}</div>
              </div>
              <div className="flex items-center gap-2">
                {actionNode}
              </div>
            </div>

            {tab !== "audit" && isMasterDataTab(tab) ? (
              <>
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".csv,text/csv"
                  className="hidden"
                  onChange={(e) => {
                    const file = e.target.files?.[0];
                    e.target.value = "";
                    if (file) void handleImportFile(file);
                  }}
                />
                {importError ? (
                  <div className="mb-4 rounded-md border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{importError}</div>
                ) : null}
              </>
            ) : null}

            <Tabs value={tab} onValueChange={(v) => setTab(v as EntityTabKey)}>
              <TabsList className="mb-4 bg-[hsl(220,20%,12%)]">
                {activeModule === "inventory" ? (
                  <>
                    <TabsTrigger value="procurement">Procurement</TabsTrigger>
                    <TabsTrigger value="inventory">Inventory</TabsTrigger>
                    <TabsTrigger value="suppliers">Suppliers</TabsTrigger>
                    <TabsTrigger value="rawMaterials">Raw Materials</TabsTrigger>
                    <TabsTrigger value="units">Units</TabsTrigger>
                    <TabsTrigger value="bom">BOM</TabsTrigger>
                    <TabsTrigger value="products">Products</TabsTrigger>
                  </>
                ) : null}
                {activeModule === "production" ? (
                  <>
                    <TabsTrigger value="production">Production</TabsTrigger>
                    <TabsTrigger value="finishedGoods">Finished Goods</TabsTrigger>
                  </>
                ) : null}
                {activeModule === "sales" ? (
                  <>
                    <TabsTrigger value="sales">Sales</TabsTrigger>
                    <TabsTrigger value="customers">Customers</TabsTrigger>
                  </>
                ) : null}
                {activeModule === "finance" ? (
                  <>
                    <TabsTrigger value="finance">Finance</TabsTrigger>
                    <TabsTrigger value="journal">Journal</TabsTrigger>
                  </>
                ) : null}
                {activeModule === "hr" ? (
                  <>
                    <TabsTrigger value="staff">Staff</TabsTrigger>
                    <TabsTrigger value="attendance">Attendance</TabsTrigger>
                  </>
                ) : null}
                <TabsTrigger value="audit">Audit</TabsTrigger>
              </TabsList>

          <TabsContent value="procurement">
            <Card className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
              <CardHeader>
                <CardTitle className="text-[hsl(0,0%,98%)]">Purchase Orders</CardTitle>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>PO</TableHead>
                      <TableHead>Supplier</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Ordered</TableHead>
                      <TableHead className="text-right">Lines</TableHead>
                      <TableHead className="text-right">Receipts</TableHead>
                      <TableHead className="text-right">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {procurementRows.length ? (
                      procurementRows.map((r) => {
                        const po = poById.get(r.id);
                        const canReceiveThisPo = canReceivePo && po?.status !== "received" && po?.status !== "cancelled";
                        return (
                          <TableRow key={r.id}>
                            <TableCell className="font-mono">{r.id}</TableCell>
                            <TableCell>{r.supplier}</TableCell>
                            <TableCell className="font-mono text-xs">{formatStatusText(r.status)}</TableCell>
                            <TableCell className="font-mono text-xs">{r.orderedAt}</TableCell>
                            <TableCell className="text-right font-mono">{r.lineCount}</TableCell>
                            <TableCell className="text-right font-mono">{r.receiptCount}</TableCell>
                            <TableCell className="text-right">
                              <div className="flex justify-end gap-2">
                                <Button variant="outline" size="sm" onClick={() => po && startEditPo(po)} disabled={!po}>
                                  Edit
                                </Button>
                                <Button variant="outline" size="sm" onClick={() => startReceive(r.id)} disabled={!canReceiveThisPo}>
                                  Receive
                                </Button>
                                <Button variant="destructive" size="sm" onClick={() => deletePo(r.id)}>
                                  Delete
                                </Button>
                              </div>
                            </TableCell>
                          </TableRow>
                        );
                      })
                    ) : (
                      <TableRow>
                        <TableCell colSpan={7} className="text-center text-muted-foreground">
                          No purchase orders
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="inventory">
            <Card className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
              <CardHeader>
                <CardTitle className="text-[hsl(0,0%,98%)]">Stock Summary</CardTitle>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>ID</TableHead>
                      <TableHead>Name</TableHead>
                      <TableHead>Unit</TableHead>
                      <TableHead className="text-right">On Hand</TableHead>
                      <TableHead className="text-right">Lots</TableHead>
                      <TableHead>Earliest Expiry</TableHead>
                      <TableHead className="text-right">Reorder</TableHead>
                      <TableHead className="text-right">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {inventoryRows.length ? (
                      inventoryRows.map((r) => (
                        <TableRow key={r.id} className={r.isLow ? "bg-red-500/5" : undefined}>
                          <TableCell className="font-mono">{r.id}</TableCell>
                          <TableCell>{r.name}</TableCell>
                          <TableCell className="text-xs">{r.unit}</TableCell>
                          <TableCell className="text-right font-mono">{r.onHand}</TableCell>
                          <TableCell className="text-right font-mono">{r.lotCount}</TableCell>
                          <TableCell className="font-mono text-xs">{r.earliestExpiry ?? "-"}</TableCell>
                          <TableCell className="text-right font-mono">{r.reorderLevel ?? "-"}</TableCell>
                          <TableCell className="text-right">
                            <Button variant="outline" size="sm" onClick={() => startAdjustFor(r.id, materialById.get(r.id)?.unit)} disabled={!canAdjustStock}>
                              Adjust
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))
                    ) : (
                      <TableRow>
                        <TableCell colSpan={8} className="text-center text-muted-foreground">
                          No raw materials
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="production">
            <Card className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
              <CardHeader>
                <CardTitle className="text-[hsl(0,0%,98%)]">Work Orders</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="mb-3 flex gap-2">
                  <Button variant="outline" size="sm" onClick={() => setWoDialogOpen(true)}>
                    New WO
                  </Button>
                </div>
                {productionError ? (
                  <div className="mb-3 rounded-md border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{productionError}</div>
                ) : null}
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>WO</TableHead>
                      <TableHead>Product</TableHead>
                      <TableHead className="text-right">Qty</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead className="text-right">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {woRows.length ? (
                      woRows.map((r) => {
                        const wo = workOrders.find((w) => w.id === r.id);
                        const canStart = wo && wo.status === "planned";
                        const canIssue = wo && wo.status === "in_progress";
                        const canComplete = wo && (wo.status === "in_progress" || wo.status === "planned");
                        return (
                          <TableRow key={r.id}>
                            <TableCell className="font-mono">{r.id}</TableCell>
                            <TableCell>{r.product}</TableCell>
                            <TableCell className="text-right font-mono">{r.quantity}</TableCell>
                            <TableCell className="font-mono text-xs">{formatStatusText(r.status)}</TableCell>
                            <TableCell className="text-right">
                              <div className="flex justify-end gap-2">
                                <Button
                                  variant="outline"
                                  size="sm"
                                  onClick={() => {
                                    if (!wo || !canStart) return;
                                    setProductionError(null);
                                    startWorkOrder(wo.id, actor);
                                  }}
                                  disabled={!canStart}
                                >
                                  Start
                                </Button>
                                <Button
                                  variant="outline"
                                  size="sm"
                                  onClick={() => {
                                    if (!wo || !canIssue) return;
                                    setProductionError(null);
                                    const err = issueWorkOrderMaterials(wo.id, actor);
                                    if (err) setProductionError(err);
                                  }}
                                  disabled={!canIssue}
                                >
                                  Issue
                                </Button>
                                <Button
                                  variant="outline"
                                  size="sm"
                                  onClick={() => {
                                    if (!wo || !canComplete) return;
                                    setProductionError(null);
                                    setCompleteError(null);
                                    setCompleteDraft({ workOrderId: wo.id, quantity: wo.quantity, batchNo: "", expiryDate: "" });
                                    setCompleteDialogOpen(true);
                                  }}
                                  disabled={!canComplete}
                                >
                                  Complete
                                </Button>
                              </div>
                            </TableCell>
                          </TableRow>
                        );
                      })
                    ) : (
                      <TableRow>
                        <TableCell colSpan={5} className="text-center text-muted-foreground">
                          No work orders
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="finishedGoods">
            <Card className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
              <CardHeader>
                <CardTitle className="text-[hsl(0,0%,98%)]">Finished Goods</CardTitle>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Product</TableHead>
                      <TableHead className="text-right">On Hand</TableHead>
                      <TableHead className="text-right">Lots</TableHead>
                      <TableHead>Earliest Expiry</TableHead>
                      <TableHead>Last Movement</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {finishedGoodsRows.length ? (
                      finishedGoodsRows.map((r) => (
                        <TableRow key={r.id}>
                          <TableCell>
                            <div className="font-mono text-xs">{r.id}</div>
                            <div>{r.name}</div>
                          </TableCell>
                          <TableCell className="text-right font-mono">{r.onHand}</TableCell>
                          <TableCell className="text-right font-mono">{r.lots}</TableCell>
                          <TableCell className="font-mono text-xs">{r.earliestExpiry ?? "-"}</TableCell>
                          <TableCell className="font-mono text-xs">{r.lastMoveAt ?? "-"}</TableCell>
                        </TableRow>
                      ))
                    ) : (
                      <TableRow>
                        <TableCell colSpan={5} className="text-center text-muted-foreground">
                          No products
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="sales">
            <Card className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
              <CardHeader>
                <CardTitle className="text-[hsl(0,0%,98%)]">Sales Orders</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="mb-3 flex gap-2">
                  <Button variant="outline" size="sm" onClick={() => setSalesDialogOpen(true)}>
                    New Order
                  </Button>
                </div>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>SO</TableHead>
                      <TableHead>Customer</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Ordered</TableHead>
                      <TableHead className="text-right">Lines</TableHead>
                      <TableHead className="text-right">Deliveries</TableHead>
                      <TableHead className="text-right">Invoices</TableHead>
                      <TableHead className="text-right">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {salesRows.length ? (
                      salesRows.map((r) => {
                        const so = salesOrders.find((s) => s.id === r.id);
                        const canShip = so && so.status !== "cancelled";
                        return (
                          <TableRow key={r.id}>
                            <TableCell className="font-mono">{r.id}</TableCell>
                            <TableCell>{r.customer}</TableCell>
                            <TableCell className="font-mono text-xs">{formatStatusText(r.status)}</TableCell>
                            <TableCell className="font-mono text-xs">{r.orderedAt}</TableCell>
                            <TableCell className="text-right font-mono">{r.lineCount}</TableCell>
                            <TableCell className="text-right font-mono">{r.deliveries}</TableCell>
                            <TableCell className="text-right font-mono">{r.invoices}</TableCell>
                            <TableCell className="text-right">
                              <div className="flex justify-end gap-2">
                                <Button
                                  variant="outline"
                                  size="sm"
                                  onClick={() => {
                                    if (!so) return;
                                    setSalesError(null);
                                    setSalesDraft(so);
                                    setSalesDialogOpen(true);
                                  }}
                                  disabled={!so}
                                >
                                  Edit
                                </Button>
                                <Button
                                  variant="outline"
                                  size="sm"
                                  onClick={() => {
                                    if (!so || !canShip) return;
                                    setShipError(null);
                                    setShipDraft({ salesOrderId: so.id, shippedAt: new Date().toISOString().slice(0, 10), lines: so.lines });
                                    setShipDialogOpen(true);
                                  }}
                                  disabled={!canShip}
                                >
                                  Fulfill
                                </Button>
                                <Button variant="outline" size="sm" onClick={() => so && generateInvoice(so.id, new Date().toISOString(), actor)} disabled={!so}>
                                  Invoice
                                </Button>
                                <Button variant="destructive" size="sm" onClick={() => so && deleteSalesOrder(so.id, actor)} disabled={!so}>
                                  Delete
                                </Button>
                              </div>
                            </TableCell>
                          </TableRow>
                        );
                      })
                    ) : (
                      <TableRow>
                        <TableCell colSpan={8} className="text-center text-muted-foreground">
                          No sales orders
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="finance">
            <Card className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
              <CardHeader>
                <CardTitle className="text-[hsl(0,0%,98%)]">Finance</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid gap-4 md:grid-cols-3">
                  <Card className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,9%)]">
                    <CardHeader>
                      <CardTitle className="text-sm text-[hsl(0,0%,98%)]">Cash (Journal)</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="font-mono text-lg">{formatMoney(cashBalance)}</div>
                      <div className="text-xs text-muted-foreground">From journal lines on account 1000</div>
                    </CardContent>
                  </Card>
                  <Card className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,9%)]">
                    <CardHeader>
                      <CardTitle className="text-sm text-[hsl(0,0%,98%)]">Accounts Receivable</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="font-mono text-lg">{formatMoney(arOutstanding)}</div>
                      <div className="text-xs text-muted-foreground">{arRows.length} invoices</div>
                    </CardContent>
                  </Card>
                  <Card className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,9%)]">
                    <CardHeader>
                      <CardTitle className="text-sm text-[hsl(0,0%,98%)]">Accounts Payable</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="font-mono text-lg">{formatMoney(apOutstanding)}</div>
                      <div className="text-xs text-muted-foreground">{apRows.length} bills</div>
                    </CardContent>
                  </Card>
                </div>

                <div className="mt-6 grid gap-6">
                  <div>
                    <div className="mb-2 text-sm font-semibold text-[hsl(0,0%,98%)]">Accounts Receivable</div>
                    <Table className="w-full table-fixed">
                      <TableHeader>
                        <TableRow>
                          <TableHead className="w-[12%]">Invoice</TableHead>
                          <TableHead className="w-[12%]">SO</TableHead>
                          <TableHead className="w-[20%]">Customer</TableHead>
                          <TableHead className="w-[12%]">Status</TableHead>
                          <TableHead className="w-[12%]">Invoiced</TableHead>
                          <TableHead className="w-[10%] text-right">Total</TableHead>
                          <TableHead className="w-[11%] text-right">Received</TableHead>
                          <TableHead className="w-[11%] text-right">Balance</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {arRows.length ? (
                          arRows.slice(0, 20).map((r) => (
                            <TableRow key={r.id}>
                              <TableCell className="w-[12%] font-mono">{r.id}</TableCell>
                              <TableCell className="w-[12%] font-mono text-xs">{r.salesOrderId}</TableCell>
                              <TableCell className="w-[20%] max-w-0 truncate text-xs">{r.customer}</TableCell>
                              <TableCell className="w-[12%] font-mono text-xs">{formatStatusText(r.status)}</TableCell>
                              <TableCell className="w-[12%]">
                                <div className="leading-tight">
                                  <div className="font-mono text-xs">{formatDateTimeParts(r.invoicedAt).date}</div>
                                  <div className="font-mono text-xs text-muted-foreground">{formatDateTimeParts(r.invoicedAt).time}</div>
                                </div>
                              </TableCell>
                              <TableCell className="w-[10%] text-right font-mono">{formatMoney(r.totalAmount)}</TableCell>
                              <TableCell className="w-[11%] text-right font-mono">{formatMoney(r.received)}</TableCell>
                              <TableCell className="w-[11%] text-right font-mono">{formatMoney(r.balance)}</TableCell>
                            </TableRow>
                          ))
                        ) : (
                          <TableRow>
                            <TableCell colSpan={8} className="text-center text-muted-foreground">
                              No receivables
                            </TableCell>
                          </TableRow>
                        )}
                      </TableBody>
                    </Table>
                  </div>

                  <div>
                    <div className="mb-2 text-sm font-semibold text-[hsl(0,0%,98%)]">Accounts Payable</div>
                    <Table className="w-full table-fixed">
                      <TableHeader>
                        <TableRow>
                          <TableHead className="w-[12%]">Bill</TableHead>
                          <TableHead className="w-[12%]">PO</TableHead>
                          <TableHead className="w-[20%]">Supplier</TableHead>
                          <TableHead className="w-[12%]">Status</TableHead>
                          <TableHead className="w-[12%]">Billed</TableHead>
                          <TableHead className="w-[10%] text-right">Total</TableHead>
                          <TableHead className="w-[11%] text-right">Paid</TableHead>
                          <TableHead className="w-[11%] text-right">Balance</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {apRows.length ? (
                          apRows.slice(0, 20).map((r) => (
                            <TableRow key={r.id}>
                              <TableCell className="w-[12%] font-mono">{r.id}</TableCell>
                              <TableCell className="w-[12%] font-mono text-xs">{r.purchaseOrderId}</TableCell>
                              <TableCell className="w-[20%] max-w-0 truncate text-xs">{r.supplier}</TableCell>
                              <TableCell className="w-[12%] font-mono text-xs">{formatStatusText(r.status)}</TableCell>
                              <TableCell className="w-[12%]">
                                <div className="leading-tight">
                                  <div className="font-mono text-xs">{formatDateTimeParts(r.billedAt).date}</div>
                                  <div className="font-mono text-xs text-muted-foreground">{formatDateTimeParts(r.billedAt).time}</div>
                                </div>
                              </TableCell>
                              <TableCell className="w-[10%] text-right font-mono">{formatMoney(r.totalAmount)}</TableCell>
                              <TableCell className="w-[11%] text-right font-mono">{formatMoney(r.paid)}</TableCell>
                              <TableCell className="w-[11%] text-right font-mono">{formatMoney(r.balance)}</TableCell>
                            </TableRow>
                          ))
                        ) : (
                          <TableRow>
                            <TableCell colSpan={8} className="text-center text-muted-foreground">
                              No payables
                            </TableCell>
                          </TableRow>
                        )}
                      </TableBody>
                    </Table>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>
          <TabsContent value="journal">
            <Card className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
              <CardHeader>
                <CardTitle className="text-[hsl(0,0%,98%)]">Journal</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="mb-2 text-sm font-semibold text-[hsl(0,0%,98%)]">Journal Entries</div>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>JE</TableHead>
                      <TableHead>Posted</TableHead>
                      <TableHead>Memo</TableHead>
                      <TableHead>Source</TableHead>
                      <TableHead className="text-right">Debit</TableHead>
                      <TableHead className="text-right">Credit</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {journalRows.length ? (
                      journalRows.slice(0, 20).map((r) => (
                        <TableRow key={r.id}>
                          <TableCell className="font-mono">{r.id}</TableCell>
                          <TableCell className="font-mono text-xs">{r.postedAt}</TableCell>
                          <TableCell className="text-xs">{r.memo ?? "-"}</TableCell>
                          <TableCell className="font-mono text-xs">{r.source}</TableCell>
                          <TableCell className="text-right font-mono">{formatMoney(r.debit)}</TableCell>
                          <TableCell className="text-right font-mono">{formatMoney(r.credit)}</TableCell>
                        </TableRow>
                      ))
                    ) : (
                      <TableRow>
                        <TableCell colSpan={6} className="text-center text-muted-foreground">
                          No journal entries
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>
          <TabsContent value="attendance">
            <Card className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
              <CardHeader>
                <CardTitle className="text-[hsl(0,0%,98%)]">Attendance</CardTitle>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("date")}>
                        Date{sortIndicator("date")}
                      </TableHead>
                      <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("fullName")}>
                        Staff{sortIndicator("fullName")}
                      </TableHead>
                      <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("department")}>
                        Department{sortIndicator("department")}
                      </TableHead>
                      <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("clockIn")}>
                        Clock In{sortIndicator("clockIn")}
                      </TableHead>
                      <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("clockOut")}>
                        Clock Out{sortIndicator("clockOut")}
                      </TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {tab === "attendance" && (sortedList as AttendanceRecord[]).length ? (
                      (sortedList as AttendanceRecord[]).map((r) => {
                        const staffRow = staffById.get(r.staffId);
                        return (
                          <TableRow key={r.id}>
                            <TableCell className="font-mono text-xs">{r.date}</TableCell>
                            <TableCell>
                              <div className="grid gap-1">
                                <div>{staffRow?.fullName ?? r.staffId}</div>
                                <div className="font-mono text-xs text-muted-foreground">{r.staffId}</div>
                              </div>
                            </TableCell>
                            <TableCell className="text-xs">{staffRow?.department ?? "-"}</TableCell>
                            <TableCell className="font-mono text-xs">{r.clockIn}</TableCell>
                            <TableCell className="font-mono text-xs">{r.clockOut ?? "-"}</TableCell>
                          </TableRow>
                        );
                      })
                    ) : (
                      <TableRow>
                        <TableCell colSpan={5} className="text-center text-muted-foreground">
                          No attendance records
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>
          {(["staff", "customers", "suppliers", "products", "rawMaterials", "units", "bom"] as const).map((key) => (
            <TabsContent key={key} value={key}>
              <Card className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
                <CardHeader>
                  <CardTitle className="text-[hsl(0,0%,98%)]">{labelForEntityKey(key)}</CardTitle>
                </CardHeader>
                <CardContent>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        {key === "staff" ? (
                          <>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("id")}>
                              User ID{sortIndicator("id")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("fullName")}>
                              Full Name{sortIndicator("fullName")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("roles")}>
                              Roles{sortIndicator("roles")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("department")}>
                              Department{sortIndicator("department")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("username")}>
                              Username / Password{sortIndicator("username")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("status")}>
                              Status{sortIndicator("status")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("dateJoined")}>
                              Date Joined{sortIndicator("dateJoined")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("lastLogin")}>
                              Last Login{sortIndicator("lastLogin")}
                            </TableHead>
                          </>
                        ) : key === "customers" ? (
                          <>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("id")}>
                              ID{sortIndicator("id")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("name")}>
                              Name{sortIndicator("name")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("phone")}>
                              Phone{sortIndicator("phone")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("email")}>
                              Email{sortIndicator("email")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("address")}>
                              Address{sortIndicator("address")}
                            </TableHead>
                          </>
                        ) : key === "suppliers" ? (
                          <>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("id")}>
                              Supplier ID{sortIndicator("id")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("name")}>
                              Supplier Name{sortIndicator("name")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("country")}>
                              Country{sortIndicator("country")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("contactPerson")}>
                              Contact Person{sortIndicator("contactPerson")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("email")}>
                              Email{sortIndicator("email")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("phone")}>
                              Phone{sortIndicator("phone")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none text-right" onClick={() => toggleSort("leadTimeDays")}>
                              Lead Time (Days){sortIndicator("leadTimeDays")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("status")}>
                              Status{sortIndicator("status")}
                            </TableHead>
                          </>
                        ) : key === "units" ? (
                          <>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("id")}>
                              Unit Code{sortIndicator("id")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("name")}>
                              Unit Name{sortIndicator("name")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("unitType")}>
                              Type{sortIndicator("unitType")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("conversionBase")}>
                              Conversion Base{sortIndicator("conversionBase")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none text-right" onClick={() => toggleSort("conversionRate")}>
                              Conversion Rate{sortIndicator("conversionRate")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("symbol")}>
                              Symbol{sortIndicator("symbol")}
                            </TableHead>
                          </>
                        ) : key === "products" ? (
                          <>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("id")}>
                              SKU{sortIndicator("id")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("name")}>
                              Product Name{sortIndicator("name")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("variant")}>
                              Variant{sortIndicator("variant")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("category")}>
                              Category{sortIndicator("category")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none text-right" onClick={() => toggleSort("size")}>
                              Size{sortIndicator("size")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("unit")}>
                              Unit{sortIndicator("unit")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("version")}>
                              Version{sortIndicator("version")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("status")}>
                              Status{sortIndicator("status")}
                            </TableHead>
                          </>
                        ) : key === "rawMaterials" ? (
                          <>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("id")}>
                              Material Code{sortIndicator("id")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("name")}>
                              Material Name{sortIndicator("name")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("inciName")}>
                              INCI Name{sortIndicator("inciName")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("specGrade")}>
                              Spec Grade{sortIndicator("specGrade")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("unit")}>
                              Unit{sortIndicator("unit")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none text-right" onClick={() => toggleSort("shelfLifeMonths")}>
                              Shelf Life (Months){sortIndicator("shelfLifeMonths")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("storageCondition")}>
                              Storage Condition{sortIndicator("storageCondition")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("status")}>
                              Status{sortIndicator("status")}
                            </TableHead>
                          </>
                        ) : (
                          <>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("id")}>
                              BOM ID{sortIndicator("id")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("productId")}>
                              Product SKU{sortIndicator("productId")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("rawMaterialId")}>
                              Material Code{sortIndicator("rawMaterialId")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("rawMaterialName")}>
                              Material Name{sortIndicator("rawMaterialName")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none text-right" onClick={() => toggleSort("quantity")}>
                              Quantity{sortIndicator("quantity")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("unit")}>
                              Unit{sortIndicator("unit")}
                            </TableHead>
                            <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("stage")}>
                              Stage{sortIndicator("stage")}
                            </TableHead>
                          </>
                        )}
                        <TableHead className="text-right">Actions</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {sortedList.length ? (
                        sortedList.map((r) => {
                          const id = (r as { id: string }).id;
                          return (
                            <TableRow key={id}>
                              {key === "staff" ? (
                                (() => {
                                  const row = r as Staff;
                                  return (
                                    <>
                                      <TableCell className="font-mono">{row.id}</TableCell>
                                      <TableCell>
                                        <div className="grid gap-1">
                                          <div>{row.fullName}</div>
                                          <div className="font-mono text-xs text-muted-foreground">{row.email ?? "-"}</div>
                                        </div>
                                      </TableCell>
                                      <TableCell className="font-mono text-xs">
                                        {(row.roles?.length ? row.roles : [row.role]).map(labelForRole).join(", ")}
                                      </TableCell>
                                      <TableCell className="text-xs">{row.department ?? "-"}</TableCell>
                                      <TableCell className="font-mono text-xs">
                                        <div className="grid gap-1">
                                          <div>{row.username}</div>
                                          <div className="text-muted-foreground">{row.passwordHash}</div>
                                        </div>
                                      </TableCell>
                                      <TableCell className="font-mono text-xs">{formatStatusText(row.status)}</TableCell>
                                      <TableCell className="font-mono text-xs">{row.dateJoined ? formatCsvDateTime(row.dateJoined) : "-"}</TableCell>
                                      <TableCell className="font-mono text-xs">{row.lastLogin ? formatCsvDateTime(row.lastLogin) : "-"}</TableCell>
                                    </>
                                  );
                                })()
                              ) : key === "customers" ? (
                                (() => {
                                  const row = r as Customer;
                                  return (
                                    <>
                                      <TableCell className="font-mono">{row.id}</TableCell>
                                      <TableCell>{row.name}</TableCell>
                                      <TableCell className="font-mono text-xs">{row.phone ?? "-"}</TableCell>
                                      <TableCell className="font-mono text-xs">{row.email ?? "-"}</TableCell>
                                      <TableCell className="text-xs">{row.address ?? "-"}</TableCell>
                                    </>
                                  );
                                })()
                              ) : key === "suppliers" ? (
                                (() => {
                                  const row = r as Supplier;
                                  return (
                                    <>
                                      <TableCell className="font-mono">{row.id}</TableCell>
                                      <TableCell>{row.name}</TableCell>
                                      <TableCell className="text-xs">{row.country ?? "-"}</TableCell>
                                      <TableCell className="text-xs">{row.contactPerson ?? "-"}</TableCell>
                                      <TableCell className="font-mono text-xs">{row.email ?? "-"}</TableCell>
                                      <TableCell className="font-mono text-xs">{row.phone ?? "-"}</TableCell>
                                      <TableCell className="text-right font-mono">{row.leadTimeDays ?? "-"}</TableCell>
                                      <TableCell className="font-mono text-xs">{row.status ?? "-"}</TableCell>
                                    </>
                                  );
                                })()
                              ) : key === "units" ? (
                                (() => {
                                  const row = r as MeasurementUnit;
                                  return (
                                    <>
                                      <TableCell className="font-mono">{row.id}</TableCell>
                                      <TableCell>{row.name}</TableCell>
                                      <TableCell className="text-xs">{row.unitType ?? "-"}</TableCell>
                                      <TableCell className="text-xs">{row.conversionBase ?? "-"}</TableCell>
                                      <TableCell className="text-right font-mono">{row.conversionRate ?? "-"}</TableCell>
                                      <TableCell className="font-mono text-xs">{row.symbol ?? "-"}</TableCell>
                                    </>
                                  );
                                })()
                              ) : key === "products" ? (
                                (() => {
                                  const row = r as Product;
                                  return (
                                    <>
                                      <TableCell className="font-mono">{row.id}</TableCell>
                                      <TableCell>{row.name}</TableCell>
                                      <TableCell className="text-xs">{row.variant ?? "-"}</TableCell>
                                      <TableCell className="text-xs">{row.category ?? "-"}</TableCell>
                                      <TableCell className="text-right font-mono">{row.size ?? "-"}</TableCell>
                                      <TableCell className="font-mono text-xs">{row.unit ?? "-"}</TableCell>
                                      <TableCell className="font-mono text-xs">{row.version ?? "-"}</TableCell>
                                      <TableCell className="font-mono text-xs">{row.status ?? "-"}</TableCell>
                                    </>
                                  );
                                })()
                              ) : key === "rawMaterials" ? (
                                (() => {
                                  const row = r as RawMaterial;
                                  return (
                                    <>
                                      <TableCell className="font-mono">{row.id}</TableCell>
                                      <TableCell>{row.name}</TableCell>
                                      <TableCell className="text-xs">{row.inciName ?? "-"}</TableCell>
                                      <TableCell className="text-xs">{row.specGrade ?? "-"}</TableCell>
                                      <TableCell className="font-mono text-xs">{row.unit ?? "-"}</TableCell>
                                      <TableCell className="text-right font-mono">{row.shelfLifeMonths ?? "-"}</TableCell>
                                      <TableCell className="text-xs">{row.storageCondition ?? "-"}</TableCell>
                                      <TableCell className="font-mono text-xs">{row.status ?? "-"}</TableCell>
                                    </>
                                  );
                                })()
                              ) : (
                                (() => {
                                  const row = r as BillOfMaterialLine;
                                  return (
                                    <>
                                      <TableCell className="font-mono">{row.id}</TableCell>
                                      <TableCell className="font-mono text-xs">{row.productId}</TableCell>
                                      <TableCell className="font-mono text-xs">{row.rawMaterialId}</TableCell>
                                      <TableCell className="text-xs">{row.rawMaterialName ?? "-"}</TableCell>
                                      <TableCell className="text-right font-mono">{row.quantity}</TableCell>
                                      <TableCell className="font-mono text-xs">{row.unit ?? "-"}</TableCell>
                                      <TableCell className="text-xs">{row.stage ?? "-"}</TableCell>
                                    </>
                                  );
                                })()
                              )}
                              <TableCell className="text-right">
                                <div className="flex justify-end gap-2">
                                  <Button variant="outline" size="sm" onClick={() => startEdit(key, r)}>
                                    Edit
                                  </Button>
                                  <Button variant="destructive" size="sm" onClick={() => deleteById(key, id)}>
                                    Delete
                                  </Button>
                                </div>
                              </TableCell>
                            </TableRow>
                          );
                        })
                      ) : (
                        <TableRow>
                          <TableCell colSpan={12} className="text-center text-muted-foreground">
                            No records
                          </TableCell>
                        </TableRow>
                      )}
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>
            </TabsContent>
          ))}

          <TabsContent value="audit">
            <Card className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
              <CardHeader>
                <CardTitle className="text-[hsl(0,0%,98%)]">Audit Log</CardTitle>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>At</TableHead>
                      <TableHead>Actor</TableHead>
                      <TableHead>Action</TableHead>
                      <TableHead>Entity</TableHead>
                      <TableHead>Entity ID</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {audit.length ? (
                      audit.slice(0, 200).map((a) => (
                        <TableRow key={a.id}>
                          <TableCell className="font-mono text-xs">{a.at}</TableCell>
                          <TableCell>{a.actor ? `${a.actor.name} (${a.actor.role})` : "-"}</TableCell>
                          <TableCell className="font-mono text-xs">{a.action}</TableCell>
                          <TableCell className="font-mono text-xs">{a.entity}</TableCell>
                          <TableCell className="font-mono text-xs">{a.entityId ?? "-"}</TableCell>
                        </TableRow>
                      ))
                    ) : (
                      <TableRow>
                        <TableCell colSpan={5} className="text-center text-muted-foreground">
                          No audit events
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
          </>
        )}
      </div>

      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="max-h-[85vh] overflow-y-auto border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
          <DialogHeader>
            <DialogTitle className="text-[hsl(0,0%,98%)]">{editingKey ? `Edit ${editingKey}` : "Edit"}</DialogTitle>
          </DialogHeader>

          {editingKey === "staff" ? (
            <div className="grid gap-3">
              <div className="grid gap-2">
                <Label htmlFor="st_id">User ID</Label>
                <Input
                  id="st_id"
                  value={staffDraft.id}
                  onChange={(e) => setStaffDraft((p) => ({ ...p, id: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="st_fullname">Full Name</Label>
                <Input
                  id="st_fullname"
                  value={staffDraft.fullName}
                  onChange={(e) => setStaffDraft((p) => ({ ...p, fullName: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="st_email">Email</Label>
                <Input
                  id="st_email"
                  value={staffDraft.email ?? ""}
                  onChange={(e) => setStaffDraft((p) => ({ ...p, email: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label>Roles</Label>
                <div className="grid grid-cols-2 gap-2">
                  {(["operator", "supervisor", "production_manager", "procurement_manager", "finance_manager", "sales_manager", "superadmin"] as const).map((r) => {
                    const currentRoles = staffDraft.roles?.length ? staffDraft.roles : [staffDraft.role];
                    const checked = currentRoles.includes(r);
                    return (
                      <label key={r} className="flex cursor-pointer items-center gap-2 rounded-md border border-[hsl(220,20%,18%)] bg-[hsl(220,20%,12%)] px-3 py-2">
                        <Checkbox
                          checked={checked}
                          onCheckedChange={(next) => {
                            const shouldEnable = next === true;
                            setStaffDraft((p) => {
                              const existing = p.roles?.length ? p.roles : [p.role];
                              const nextRoles = shouldEnable ? Array.from(new Set([...existing, r])) : existing.filter((x) => x !== r);
                              const normalizedRoles = nextRoles.length ? nextRoles : (["operator"] as UserRole[]);
                              const role = normalizedRoles[0] ?? "operator";
                              const nextModules = Array.from(new Set([...(p.modules ?? []), ...defaultModulesForRoles(normalizedRoles)]));
                              return { ...p, roles: normalizedRoles, role, modules: nextModules };
                            });
                          }}
                        />
                        <span className="text-sm">{labelForRole(r)}</span>
                      </label>
                    );
                  })}
                </div>
              </div>
              <div className="grid gap-2">
                <Label>Department</Label>
                <Select
                  value={staffDraft.department && staffDraft.department.trim() ? staffDraft.department : "__none__"}
                  onValueChange={(v) => setStaffDraft((p) => ({ ...p, department: v === "__none__" ? "" : v }))}
                >
                  <SelectTrigger className="bg-[hsl(220,20%,12%)]">
                    <SelectValue placeholder="Select department" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="__none__">-</SelectItem>
                    <SelectItem value="Finance">Finance</SelectItem>
                    <SelectItem value="HR">Human Resource</SelectItem>
                    <SelectItem value="Management">Management</SelectItem>
                    <SelectItem value="Operation">Operation</SelectItem>
                    <SelectItem value="Procurement">Procurement</SelectItem>
                    <SelectItem value="Production">Production</SelectItem>
                    <SelectItem value="Quality">Quality</SelectItem>
                    <SelectItem value="Sales">Sales</SelectItem>
                    <SelectItem value="Warehouse">Warehouse</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="grid gap-2">
                <Label>Modules</Label>
                <div className="grid grid-cols-2 gap-2">
                  {ALL_MODULES.map((m) => {
                    const checked = staffDraft.modules.includes(m);
                    return (
                      <label key={m} className="flex cursor-pointer items-center gap-2 rounded-md border border-[hsl(220,20%,18%)] bg-[hsl(220,20%,12%)] px-3 py-2">
                        <Checkbox
                          checked={checked}
                          onCheckedChange={(next) => {
                            const shouldEnable = next === true;
                            setStaffDraft((p) => {
                              const current = p.modules ?? [];
                              const nextModules = shouldEnable ? Array.from(new Set([...current, m])) : current.filter((x) => x !== m);
                              return { ...p, modules: nextModules };
                            });
                          }}
                        />
                        <span className="text-sm">{m === "inventory" ? "Procurement" : labelForModule(m)}</span>
                      </label>
                    );
                  })}
                </div>
              </div>
              <div className="grid gap-2">
                <Label htmlFor="st_username">Username</Label>
                <Input
                  id="st_username"
                  value={staffDraft.username}
                  onChange={(e) => setStaffDraft((p) => ({ ...p, username: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="st_password_hash">Password</Label>
                <Input
                  id="st_password_hash"
                  value={staffDraft.passwordHash}
                  onChange={(e) => setStaffDraft((p) => ({ ...p, passwordHash: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label>Status</Label>
                <Select
                  value={staffDraft.status}
                  onValueChange={(v) => setStaffDraft((p) => ({ ...p, status: v as StaffStatus }))}
                >
                  <SelectTrigger className="bg-[hsl(220,20%,12%)]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="active">active</SelectItem>
                    <SelectItem value="inactive">inactive</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="grid gap-2">
                <Label htmlFor="st_date_joined">Date Joined</Label>
                <Input
                  id="st_date_joined"
                  value={staffDraft.dateJoined ?? ""}
                  onChange={(e) => setStaffDraft((p) => ({ ...p, dateJoined: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="st_last_login">Last Login</Label>
                <Input
                  id="st_last_login"
                  value={staffDraft.lastLogin ?? ""}
                  onChange={(e) => setStaffDraft((p) => ({ ...p, lastLogin: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
            </div>
          ) : null}

          {editingKey === "customers" ? (
            <div className="grid gap-3">
              <div className="grid gap-2">
                <Label htmlFor="c_id">ID</Label>
                <Input
                  id="c_id"
                  value={customerDraft.id}
                  onChange={(e) => setCustomerDraft((p) => ({ ...p, id: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="c_name">Name</Label>
                <Input
                  id="c_name"
                  value={customerDraft.name}
                  onChange={(e) => setCustomerDraft((p) => ({ ...p, name: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="c_phone">Phone</Label>
                <Input
                  id="c_phone"
                  value={customerDraft.phone ?? ""}
                  onChange={(e) => setCustomerDraft((p) => ({ ...p, phone: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="c_email">Email</Label>
                <Input
                  id="c_email"
                  value={customerDraft.email ?? ""}
                  onChange={(e) => setCustomerDraft((p) => ({ ...p, email: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="c_address">Address</Label>
                <Input
                  id="c_address"
                  value={customerDraft.address ?? ""}
                  onChange={(e) => setCustomerDraft((p) => ({ ...p, address: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
            </div>
          ) : null}

          {editingKey === "suppliers" ? (
            <div className="grid gap-3">
              <div className="grid gap-2">
                <Label htmlFor="s_id">ID</Label>
                <Input
                  id="s_id"
                  value={supplierDraft.id}
                  onChange={(e) => setSupplierDraft((p) => ({ ...p, id: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="s_name">Name</Label>
                <Input
                  id="s_name"
                  value={supplierDraft.name}
                  onChange={(e) => setSupplierDraft((p) => ({ ...p, name: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="s_country">Country</Label>
                <Input
                  id="s_country"
                  value={supplierDraft.country ?? ""}
                  onChange={(e) => setSupplierDraft((p) => ({ ...p, country: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="s_contact">Contact Person</Label>
                <Input
                  id="s_contact"
                  value={supplierDraft.contactPerson ?? ""}
                  onChange={(e) => setSupplierDraft((p) => ({ ...p, contactPerson: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="s_phone">Phone</Label>
                <Input
                  id="s_phone"
                  value={supplierDraft.phone ?? ""}
                  onChange={(e) => setSupplierDraft((p) => ({ ...p, phone: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="s_email">Email</Label>
                <Input
                  id="s_email"
                  value={supplierDraft.email ?? ""}
                  onChange={(e) => setSupplierDraft((p) => ({ ...p, email: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="s_lead">Lead Time (Days)</Label>
                <Input
                  id="s_lead"
                  type="number"
                  value={supplierDraft.leadTimeDays ?? ""}
                  onChange={(e) =>
                    setSupplierDraft((p) => ({
                      ...p,
                      leadTimeDays: e.target.value === "" ? undefined : Number(e.target.value),
                    }))
                  }
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="s_status">Status</Label>
                <Input
                  id="s_status"
                  value={supplierDraft.status ?? ""}
                  onChange={(e) => setSupplierDraft((p) => ({ ...p, status: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
            </div>
          ) : null}

          {editingKey === "units" ? (
            <div className="grid gap-3">
              <div className="grid gap-2">
                <Label htmlFor="u_id">ID</Label>
                <Input
                  id="u_id"
                  value={unitDraft.id}
                  onChange={(e) => setUnitDraft((p) => ({ ...p, id: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="u_name">Name</Label>
                <Input
                  id="u_name"
                  value={unitDraft.name}
                  onChange={(e) => setUnitDraft((p) => ({ ...p, name: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="u_type">Type</Label>
                <Input
                  id="u_type"
                  value={unitDraft.unitType ?? ""}
                  onChange={(e) => setUnitDraft((p) => ({ ...p, unitType: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="u_base">Conversion Base</Label>
                <Input
                  id="u_base"
                  value={unitDraft.conversionBase ?? ""}
                  onChange={(e) => setUnitDraft((p) => ({ ...p, conversionBase: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="u_rate">Conversion Rate</Label>
                <Input
                  id="u_rate"
                  type="number"
                  value={unitDraft.conversionRate ?? ""}
                  onChange={(e) =>
                    setUnitDraft((p) => ({
                      ...p,
                      conversionRate: e.target.value === "" ? undefined : Number(e.target.value),
                    }))
                  }
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
            </div>
          ) : null}

          {editingKey === "products" ? (
            <div className="grid gap-3">
              <div className="grid gap-2">
                <Label htmlFor="p_id">ID</Label>
                <Input
                  id="p_id"
                  value={productDraft.id}
                  onChange={(e) => setProductDraft((p) => ({ ...p, id: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="p_name">Name</Label>
                <Input
                  id="p_name"
                  value={productDraft.name}
                  onChange={(e) => setProductDraft((p) => ({ ...p, name: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="p_variant">Variant</Label>
                <Input
                  id="p_variant"
                  value={productDraft.variant ?? ""}
                  onChange={(e) => setProductDraft((p) => ({ ...p, variant: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="p_category">Category</Label>
                <Input
                  id="p_category"
                  value={productDraft.category ?? ""}
                  onChange={(e) => setProductDraft((p) => ({ ...p, category: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="p_size">Size</Label>
                <Input
                  id="p_size"
                  type="number"
                  value={productDraft.size ?? ""}
                  onChange={(e) =>
                    setProductDraft((p) => ({
                      ...p,
                      size: e.target.value === "" ? undefined : Number(e.target.value),
                    }))
                  }
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="p_unit">Unit</Label>
                <Input
                  id="p_unit"
                  value={productDraft.unit ?? ""}
                  onChange={(e) => setProductDraft((p) => ({ ...p, unit: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="p_version">Version</Label>
                <Input
                  id="p_version"
                  value={productDraft.version ?? ""}
                  onChange={(e) => setProductDraft((p) => ({ ...p, version: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="p_status">Status</Label>
                <Input
                  id="p_status"
                  value={productDraft.status ?? ""}
                  onChange={(e) => setProductDraft((p) => ({ ...p, status: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
            </div>
          ) : null}

          {editingKey === "rawMaterials" ? (
            <div className="grid gap-3">
              <div className="grid gap-2">
                <Label htmlFor="rm_id">ID</Label>
                <Input
                  id="rm_id"
                  value={rawMaterialDraft.id}
                  onChange={(e) => setRawMaterialDraft((p) => ({ ...p, id: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="rm_name">Name</Label>
                <Input
                  id="rm_name"
                  value={rawMaterialDraft.name}
                  onChange={(e) => setRawMaterialDraft((p) => ({ ...p, name: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="rm_inci">INCI Name</Label>
                <Input
                  id="rm_inci"
                  value={rawMaterialDraft.inciName ?? ""}
                  onChange={(e) => setRawMaterialDraft((p) => ({ ...p, inciName: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="rm_grade">Spec Grade</Label>
                <Input
                  id="rm_grade"
                  value={rawMaterialDraft.specGrade ?? ""}
                  onChange={(e) => setRawMaterialDraft((p) => ({ ...p, specGrade: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="rm_unit">Unit</Label>
                <Input
                  id="rm_unit"
                  value={rawMaterialDraft.unit ?? ""}
                  onChange={(e) => setRawMaterialDraft((p) => ({ ...p, unit: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="rm_shelf">Shelf Life (Months)</Label>
                <Input
                  id="rm_shelf"
                  type="number"
                  value={rawMaterialDraft.shelfLifeMonths ?? ""}
                  onChange={(e) =>
                    setRawMaterialDraft((p) => ({
                      ...p,
                      shelfLifeMonths: e.target.value === "" ? undefined : Number(e.target.value),
                    }))
                  }
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="rm_storage">Storage Condition</Label>
                <Input
                  id="rm_storage"
                  value={rawMaterialDraft.storageCondition ?? ""}
                  onChange={(e) => setRawMaterialDraft((p) => ({ ...p, storageCondition: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="rm_status">Status</Label>
                <Input
                  id="rm_status"
                  value={rawMaterialDraft.status ?? ""}
                  onChange={(e) => setRawMaterialDraft((p) => ({ ...p, status: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
            </div>
          ) : null}

          {editingKey === "bom" ? (
            <div className="grid gap-3">
              <div className="grid gap-2">
                <Label htmlFor="b_id">ID</Label>
                <Input
                  id="b_id"
                  value={bomDraft.id}
                  onChange={(e) => setBomDraft((p) => ({ ...p, id: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label>Product</Label>
                <Select value={bomDraft.productId} onValueChange={(v) => setBomDraft((p) => ({ ...p, productId: v }))}>
                  <SelectTrigger className="bg-[hsl(220,20%,12%)]">
                    <SelectValue placeholder="Select product" />
                  </SelectTrigger>
                  <SelectContent>
                    {products.map((p) => (
                      <SelectItem key={p.id} value={p.id}>
                        {p.id} - {p.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="grid gap-2">
                <Label>Raw Material</Label>
                <Select
                  value={bomDraft.rawMaterialId}
                  onValueChange={(v) =>
                    setBomDraft((p) => ({
                      ...p,
                      rawMaterialId: v,
                      rawMaterialName: rawMaterials.find((m) => m.id === v)?.name ?? p.rawMaterialName,
                    }))
                  }
                >
                  <SelectTrigger className="bg-[hsl(220,20%,12%)]">
                    <SelectValue placeholder="Select raw material" />
                  </SelectTrigger>
                  <SelectContent>
                    {rawMaterials.map((m) => (
                      <SelectItem key={m.id} value={m.id}>
                        {m.id} - {m.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="grid gap-2">
                <Label htmlFor="b_unit">Unit</Label>
                <Input
                  id="b_unit"
                  value={bomDraft.unit ?? ""}
                  onChange={(e) => setBomDraft((p) => ({ ...p, unit: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="b_qty">Quantity</Label>
                <Input
                  id="b_qty"
                  type="number"
                  value={String(bomDraft.quantity)}
                  onChange={(e) => {
                    const next = toNumber(e.target.value);
                    setBomDraft((p) => ({ ...p, quantity: next }));
                  }}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="b_stage">Stage</Label>
                <Input
                  id="b_stage"
                  value={bomDraft.stage ?? ""}
                  onChange={(e) => setBomDraft((p) => ({ ...p, stage: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
            </div>
          ) : null}

          {formError ? <div className="mt-2 rounded-md border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{formError}</div> : null}

          <DialogFooter>
            <Button variant="outline" onClick={() => setDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={submit} disabled={!editingKey}>
              Save
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={woDialogOpen} onOpenChange={setWoDialogOpen}>
        <DialogContent className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
          <DialogHeader>
            <DialogTitle className="text-[hsl(0,0%,98%)]">{woDraft.id ? `Work Order ${woDraft.id}` : "Work Order"}</DialogTitle>
          </DialogHeader>
          <div className="grid gap-3">
            <div className="grid gap-2">
              <Label htmlFor="wo_id">WO ID</Label>
              <Input id="wo_id" value={woDraft.id} onChange={(e) => setWoDraft((p) => ({ ...p, id: e.target.value }))} className="bg-[hsl(220,20%,12%)]" />
            </div>
            <div className="grid gap-2">
              <Label>Product</Label>
              <Select value={woDraft.productId} onValueChange={(v) => setWoDraft((p) => ({ ...p, productId: v }))}>
                <SelectTrigger className="bg-[hsl(220,20%,12%)]">
                  <SelectValue placeholder="Select product" />
                </SelectTrigger>
                <SelectContent>
                  {products.map((p) => (
                    <SelectItem key={p.id} value={p.id}>
                      {p.id} - {p.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="wo_qty">Quantity</Label>
              <Input id="wo_qty" type="number" value={String(woDraft.quantity)} onChange={(e) => setWoDraft((p) => ({ ...p, quantity: Number(e.target.value) }))} className="bg-[hsl(220,20%,12%)]" />
            </div>
          </div>
          {woError ? <div className="mt-2 rounded-md border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{woError}</div> : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setWoDialogOpen(false)}>
              Cancel
            </Button>
            <Button
              onClick={() => {
                setWoError(null);
                if (!woDraft.id || !woDraft.productId || !Number.isFinite(woDraft.quantity) || woDraft.quantity <= 0) {
                  setWoError("Invalid work order.");
                  return;
                }
                upsertWorkOrder(woDraft, actor);
                setWoDialogOpen(false);
              }}
            >
              Save
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={completeDialogOpen} onOpenChange={setCompleteDialogOpen}>
        <DialogContent className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
          <DialogHeader>
            <DialogTitle className="text-[hsl(0,0%,98%)]">Complete WO</DialogTitle>
          </DialogHeader>
          <div className="grid gap-3">
            <div className="grid gap-2">
              <Label htmlFor="co_qty">Output Quantity</Label>
              <Input id="co_qty" type="number" value={String(completeDraft.quantity)} onChange={(e) => setCompleteDraft((p) => ({ ...p, quantity: Number(e.target.value) }))} className="bg-[hsl(220,20%,12%)]" />
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div className="grid gap-2">
                <Label htmlFor="co_batch">Batch No</Label>
                <Input id="co_batch" value={completeDraft.batchNo ?? ""} onChange={(e) => setCompleteDraft((p) => ({ ...p, batchNo: e.target.value }))} className="bg-[hsl(220,20%,12%)]" />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="co_exp">Expiry</Label>
                <Input id="co_exp" type="date" value={completeDraft.expiryDate ?? ""} onChange={(e) => setCompleteDraft((p) => ({ ...p, expiryDate: e.target.value }))} className="bg-[hsl(220,20%,12%)]" />
              </div>
            </div>
          </div>
          {completeError ? <div className="mt-2 rounded-md border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{completeError}</div> : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setCompleteDialogOpen(false)}>
              Cancel
            </Button>
            <Button
              onClick={() => {
                setCompleteError(null);
                if (!completeDraft.workOrderId || !Number.isFinite(completeDraft.quantity) || completeDraft.quantity <= 0) {
                  setCompleteError("Invalid output.");
                  return;
                }
                const error = completeWorkOrder(completeDraft.workOrderId, completeDraft.quantity, completeDraft.batchNo, completeDraft.expiryDate, actor);
                if (error) {
                  setCompleteError(error);
                  return;
                }
                setCompleteDialogOpen(false);
              }}
            >
              Save
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={salesDialogOpen} onOpenChange={setSalesDialogOpen}>
        <DialogContent className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
          <DialogHeader>
            <DialogTitle className="text-[hsl(0,0%,98%)]">{salesDraft.id ? `Sales Order ${salesDraft.id}` : "Sales Order"}</DialogTitle>
          </DialogHeader>
          <div className="grid gap-3">
            <div className="grid gap-2">
              <Label htmlFor="so_id">SO ID</Label>
              <Input id="so_id" value={salesDraft.id} onChange={(e) => setSalesDraft((p) => ({ ...p, id: e.target.value }))} className="bg-[hsl(220,20%,12%)]" />
            </div>
            <div className="grid gap-2">
              <Label>Customer</Label>
              <Select value={salesDraft.customerId} onValueChange={(v) => setSalesDraft((p) => ({ ...p, customerId: v }))}>
                <SelectTrigger className="bg-[hsl(220,20%,12%)]">
                  <SelectValue placeholder="Select customer" />
                </SelectTrigger>
                <SelectContent>
                  {customers.map((c) => (
                    <SelectItem key={c.id} value={c.id}>
                      {c.id} - {c.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="rounded-md border border-[hsl(220,20%,18%)] p-3">
              <div className="mb-2 flex items-center justify-between">
                <div className="font-medium text-[hsl(0,0%,98%)]">Lines</div>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() =>
                    setSalesDraft((p) => ({
                      ...p,
                      lines: [...p.lines, { id: newId(), productId: "", unitId: undefined, quantity: 1 }],
                    }))
                  }
                >
                  Add Line
                </Button>
              </div>
              <div className="grid gap-3">
                {salesDraft.lines.map((line) => (
                  <div key={line.id} className="grid grid-cols-12 gap-2">
                    <div className="col-span-6">
                      <Select
                        value={line.productId}
                        onValueChange={(v) =>
                          setSalesDraft((p) => ({ ...p, lines: p.lines.map((l) => (l.id === line.id ? { ...l, productId: v } : l)) }))
                        }
                      >
                        <SelectTrigger className="bg-[hsl(220,20%,12%)]">
                          <SelectValue placeholder="Product" />
                        </SelectTrigger>
                        <SelectContent>
                          {products.map((m) => (
                            <SelectItem key={m.id} value={m.id}>
                              {m.id} - {m.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="col-span-3">
                      <Input
                        type="number"
                        value={String(line.quantity)}
                        onChange={(e) =>
                          setSalesDraft((p) => ({ ...p, lines: p.lines.map((l) => (l.id === line.id ? { ...l, quantity: Number(e.target.value) } : l)) }))
                        }
                        className="bg-[hsl(220,20%,12%)]"
                      />
                    </div>
                    <div className="col-span-3 flex justify-end">
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => setSalesDraft((p) => ({ ...p, lines: p.lines.filter((l) => l.id !== line.id) }))}
                        disabled={salesDraft.lines.length <= 1}
                      >
                        Remove
                      </Button>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
          {salesError ? <div className="mt-2 rounded-md border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{salesError}</div> : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setSalesDialogOpen(false)}>
              Cancel
            </Button>
            <Button
              onClick={() => {
                setSalesError(null);
                if (!salesDraft.id || !salesDraft.customerId || !salesDraft.lines.length || salesDraft.lines.some((l) => !l.productId || l.quantity <= 0)) {
                  setSalesError("Invalid sales order.");
                  return;
                }
                upsertSalesOrder(salesDraft, actor);
                setSalesDialogOpen(false);
              }}
            >
              Save
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={shipDialogOpen} onOpenChange={setShipDialogOpen}>
        <DialogContent className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
          <DialogHeader>
            <DialogTitle className="text-[hsl(0,0%,98%)]">Fulfill SO</DialogTitle>
          </DialogHeader>
          <div className="grid gap-3">
            <div className="grid gap-2">
              <Label htmlFor="shipat">Shipped Date</Label>
              <Input id="shipat" type="date" value={shipDraft.shippedAt ?? ""} onChange={(e) => setShipDraft((p) => ({ ...p, shippedAt: e.target.value }))} className="bg-[hsl(220,20%,12%)]" />
            </div>
          </div>
          {shipError ? <div className="mt-2 rounded-md border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{shipError}</div> : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setShipDialogOpen(false)}>
              Cancel
            </Button>
            <Button
              onClick={() => {
                setShipError(null);
                if (!shipDraft.salesOrderId || !shipDraft.lines.length) {
                  setShipError("Invalid fulfillment.");
                  return;
                }
                const err = fulfillSalesOrder({ salesOrderId: shipDraft.salesOrderId, shippedAt: shipDraft.shippedAt, lines: shipDraft.lines }, actor);
                if (err) {
                  setShipError(err);
                  return;
                }
                setShipDialogOpen(false);
              }}
            >
              Fulfill
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <Dialog open={poDialogOpen} onOpenChange={setPoDialogOpen}>
        <DialogContent className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
          <DialogHeader>
            <DialogTitle className="text-[hsl(0,0%,98%)]">{poDraft.id ? `Purchase Order ${poDraft.id}` : "Purchase Order"}</DialogTitle>
          </DialogHeader>

          <div className="grid gap-3">
            <div className="grid gap-2">
              <Label htmlFor="po_id">PO ID</Label>
              <Input id="po_id" value={poDraft.id} onChange={(e) => setPoDraft((p) => ({ ...p, id: e.target.value }))} className="bg-[hsl(220,20%,12%)]" />
            </div>

            <div className="grid gap-2">
              <Label>Supplier</Label>
              <Select value={poDraft.supplierId} onValueChange={(v) => setPoDraft((p) => ({ ...p, supplierId: v }))}>
                <SelectTrigger className="bg-[hsl(220,20%,12%)]">
                  <SelectValue placeholder="Select supplier" />
                </SelectTrigger>
                <SelectContent>
                  {suppliers.map((s) => (
                    <SelectItem key={s.id} value={s.id}>
                      {s.id} - {s.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div className="grid gap-2">
                <Label htmlFor="po_ordered">Ordered Date</Label>
                <Input
                  id="po_ordered"
                  type="date"
                  value={poDraft.orderedAt.slice(0, 10)}
                  onChange={(e) => setPoDraft((p) => ({ ...p, orderedAt: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="po_expected">Expected Date</Label>
                <Input
                  id="po_expected"
                  type="date"
                  value={poDraft.expectedAt ?? ""}
                  onChange={(e) => setPoDraft((p) => ({ ...p, expectedAt: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div className="grid gap-2">
                <Label>Status</Label>
                <Select value={poDraft.status} onValueChange={(v) => setPoDraft((p) => ({ ...p, status: v as PurchaseOrder["status"] }))}>
                  <SelectTrigger className="bg-[hsl(220,20%,12%)]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="draft">draft</SelectItem>
                    <SelectItem value="sent">sent</SelectItem>
                    <SelectItem value="received">received</SelectItem>
                    <SelectItem value="cancelled">cancelled</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="grid gap-2">
                <Label htmlFor="po_notes">Notes</Label>
                <Input id="po_notes" value={poDraft.notes ?? ""} onChange={(e) => setPoDraft((p) => ({ ...p, notes: e.target.value }))} className="bg-[hsl(220,20%,12%)]" />
              </div>
            </div>

            <div className="rounded-md border border-[hsl(220,20%,18%)] p-3">
              <div className="mb-2 flex items-center justify-between">
                <div className="font-medium text-[hsl(0,0%,98%)]">Lines</div>
                <Button variant="outline" size="sm" onClick={addPoLine} disabled={!canCreatePo}>
                  Add Line
                </Button>
              </div>
              <div className="grid gap-3">
                {poDraft.lines.map((line) => (
                  <div key={line.id} className="grid grid-cols-12 gap-2">
                    <div className="col-span-5">
                      <Select value={line.rawMaterialId} onValueChange={(v) => updatePoLine(line.id, { rawMaterialId: v })}>
                        <SelectTrigger className="bg-[hsl(220,20%,12%)]">
                          <SelectValue placeholder="Raw material" />
                        </SelectTrigger>
                        <SelectContent>
                          {rawMaterials.map((m) => (
                            <SelectItem key={m.id} value={m.id}>
                              {m.id} - {m.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="col-span-2">
                      <Select value={line.unitId ?? ""} onValueChange={(v) => updatePoLine(line.id, { unitId: v })}>
                        <SelectTrigger className="bg-[hsl(220,20%,12%)]">
                          <SelectValue placeholder="Unit" />
                        </SelectTrigger>
                        <SelectContent>
                          {units.map((u) => (
                            <SelectItem key={u.id} value={u.id}>
                              {u.id}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="col-span-2">
                      <Input type="number" value={String(line.quantity)} onChange={(e) => updatePoLine(line.id, { quantity: Number(e.target.value) })} className="bg-[hsl(220,20%,12%)]" />
                    </div>
                    <div className="col-span-2">
                      <Input
                        type="number"
                        value={line.unitPrice ?? ""}
                        onChange={(e) => updatePoLine(line.id, { unitPrice: e.target.value === "" ? undefined : Number(e.target.value) })}
                        className="bg-[hsl(220,20%,12%)]"
                      />
                    </div>
                    <div className="col-span-1 flex justify-end">
                      <Button variant="outline" size="sm" onClick={() => removePoLine(line.id)} disabled={poDraft.lines.length <= 1 || !canCreatePo}>
                        Remove
                      </Button>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {poError ? <div className="mt-2 rounded-md border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{poError}</div> : null}

          <DialogFooter>
            <Button variant="outline" onClick={() => setPoDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={submitPo} disabled={!canCreatePo}>
              Save
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={receiveDialogOpen} onOpenChange={setReceiveDialogOpen}>
        <DialogContent className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
          <DialogHeader>
            <DialogTitle className="text-[hsl(0,0%,98%)]">Receive PO</DialogTitle>
          </DialogHeader>

          <div className="grid gap-3">
            <div className="grid gap-2">
              <Label>Purchase Order</Label>
              <Input value={receiveDraft.purchaseOrderId} readOnly className="bg-[hsl(220,20%,12%)]" />
            </div>

            <div className="grid gap-2">
              <Label htmlFor="gr_date">Received Date</Label>
              <Input
                id="gr_date"
                type="date"
                value={receiveDraft.receivedAt?.slice(0, 10) ?? ""}
                onChange={(e) => setReceiveDraft((p) => ({ ...p, receivedAt: e.target.value }))}
                className="bg-[hsl(220,20%,12%)]"
              />
            </div>

            <div className="rounded-md border border-[hsl(220,20%,18%)] p-3">
              <div className="mb-2 flex items-center justify-between">
                <div className="font-medium text-[hsl(0,0%,98%)]">Receipt Lines</div>
                <Button variant="outline" size="sm" onClick={addReceiveLine} disabled={!canReceivePo}>
                  Add Line
                </Button>
              </div>
              <div className="grid gap-3">
                {receiveDraft.lines.map((line) => (
                  <div key={line.id} className="grid grid-cols-12 gap-2">
                    <div className="col-span-4">
                      <Select value={line.rawMaterialId} onValueChange={(v) => updateReceiveLine(line.id, { rawMaterialId: v })}>
                        <SelectTrigger className="bg-[hsl(220,20%,12%)]">
                          <SelectValue placeholder="Raw material" />
                        </SelectTrigger>
                        <SelectContent>
                          {rawMaterials.map((m) => (
                            <SelectItem key={m.id} value={m.id}>
                              {m.id} - {m.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="col-span-2">
                      <Select value={line.unitId ?? ""} onValueChange={(v) => updateReceiveLine(line.id, { unitId: v })}>
                        <SelectTrigger className="bg-[hsl(220,20%,12%)]">
                          <SelectValue placeholder="Unit" />
                        </SelectTrigger>
                        <SelectContent>
                          {units.map((u) => (
                            <SelectItem key={u.id} value={u.id}>
                              {u.id}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="col-span-2">
                      <Input type="number" value={String(line.quantity)} onChange={(e) => updateReceiveLine(line.id, { quantity: Number(e.target.value) })} className="bg-[hsl(220,20%,12%)]" />
                    </div>
                    <div className="col-span-2">
                      <Input value={line.batchNo ?? ""} onChange={(e) => updateReceiveLine(line.id, { batchNo: e.target.value })} placeholder="Batch" className="bg-[hsl(220,20%,12%)]" />
                    </div>
                    <div className="col-span-2">
                      <Input type="date" value={line.expiryDate?.slice(0, 10) ?? ""} onChange={(e) => updateReceiveLine(line.id, { expiryDate: e.target.value })} className="bg-[hsl(220,20%,12%)]" />
                    </div>
                    <div className="col-span-12 flex justify-end">
                      <Button variant="outline" size="sm" onClick={() => removeReceiveLine(line.id)} disabled={!canReceivePo}>
                        Remove
                      </Button>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {receiveError ? <div className="mt-2 rounded-md border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{receiveError}</div> : null}

          <DialogFooter>
            <Button variant="outline" onClick={() => setReceiveDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={submitReceive} disabled={!canReceivePo}>
              Receive
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={adjustDialogOpen} onOpenChange={setAdjustDialogOpen}>
        <DialogContent className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
          <DialogHeader>
            <DialogTitle className="text-[hsl(0,0%,98%)]">Adjust Stock</DialogTitle>
          </DialogHeader>

          <div className="grid gap-3">
            <div className="grid gap-2">
              <Label>Raw Material</Label>
              <Select value={adjustDraft.rawMaterialId} onValueChange={(v) => setAdjustDraft((p) => ({ ...p, rawMaterialId: v }))}>
                <SelectTrigger className="bg-[hsl(220,20%,12%)]">
                  <SelectValue placeholder="Select material" />
                </SelectTrigger>
                <SelectContent>
                  {rawMaterials.map((m) => (
                    <SelectItem key={m.id} value={m.id}>
                      {m.id} - {m.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div className="grid gap-2">
                <Label>Unit</Label>
                <Select value={adjustDraft.unitId ?? ""} onValueChange={(v) => setAdjustDraft((p) => ({ ...p, unitId: v }))}>
                  <SelectTrigger className="bg-[hsl(220,20%,12%)]">
                    <SelectValue placeholder="Unit" />
                  </SelectTrigger>
                  <SelectContent>
                    {units.map((u) => (
                      <SelectItem key={u.id} value={u.id}>
                        {u.id} - {u.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="grid gap-2">
                <Label htmlFor="adj_qty">Quantity Delta</Label>
                <Input id="adj_qty" type="number" value={String(adjustDraft.quantityDelta)} onChange={(e) => setAdjustDraft((p) => ({ ...p, quantityDelta: Number(e.target.value) }))} className="bg-[hsl(220,20%,12%)]" />
              </div>
            </div>

            <div className="grid grid-cols-3 gap-3">
              <div className="grid gap-2">
                <Label htmlFor="adj_at">Date</Label>
                <Input id="adj_at" type="date" value={adjustDraft.at?.slice(0, 10) ?? ""} onChange={(e) => setAdjustDraft((p) => ({ ...p, at: e.target.value }))} className="bg-[hsl(220,20%,12%)]" />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="adj_batch">Batch No</Label>
                <Input id="adj_batch" value={adjustDraft.batchNo ?? ""} onChange={(e) => setAdjustDraft((p) => ({ ...p, batchNo: e.target.value }))} className="bg-[hsl(220,20%,12%)]" />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="adj_exp">Expiry</Label>
                <Input id="adj_exp" type="date" value={adjustDraft.expiryDate?.slice(0, 10) ?? ""} onChange={(e) => setAdjustDraft((p) => ({ ...p, expiryDate: e.target.value }))} className="bg-[hsl(220,20%,12%)]" />
              </div>
            </div>
          </div>

          {adjustError ? <div className="mt-2 rounded-md border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{adjustError}</div> : null}

          <DialogFooter>
            <Button variant="outline" onClick={() => setAdjustDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={submitAdjustStock} disabled={!canAdjustStock}>
              Save
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

