import { useEffect, useMemo, useRef, useState } from "react";
import Papa from "papaparse";
import { z } from "zod";
import { Navigate } from "react-router-dom";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useAuth } from "@/contexts/useAuth";
import { useData } from "@/contexts/useData";
import type {
  AdjustStockInput,
  AuditActor,
  BillOfMaterialLine,
  Customer,
  FulfillSalesOrderInput,
  GoodsReceiptLine,
  MasterDataKey,
  MeasurementUnit,
  Product,
  PurchaseOrder,
  PurchaseOrderLine,
  RawMaterial,
  SalesOrder,
  Staff,
  Supplier,
  UserRole,
  WorkOrder,
} from "@/contexts/master-data-types";
import { defaultModulesForRoles } from "@/contexts/master-data-types";

const normalizeKey = (value: string): string => value.toLowerCase().replace(/[^a-z0-9]/g, "");

const labelForEntityKey = (key: EditableEntityKey): string => {
  if (key === "rawMaterials") return "Raw Materials";
  if (key === "bom") return "BOM";
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

const minutesBetween = (start: string | undefined, end: string | undefined): number => {
  if (!start || !end) return 0;
  const startMs = new Date(start).getTime();
  const endMs = new Date(end).getTime();
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) return 0;
  return Math.round((endMs - startMs) / 60000);
};

const formatMinutes = (minutes: number): string => {
  const safe = Number.isFinite(minutes) ? Math.max(0, Math.round(minutes)) : 0;
  const h = Math.floor(safe / 60);
  const m = safe % 60;
  return `${h}:${String(m).padStart(2, "0")}`;
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
  email: z.string().optional(),
  phone: z.string().optional(),
  leadTimeDays: z.number().optional(),
  status: z.string().optional(),
});

const unitSchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  symbol: z.string().optional(),
  unitType: z.string().optional(),
  conversionBase: z.string().optional(),
  conversionRate: z.number().optional(),
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
  shelfLifeMonths: z.number().optional(),
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

const canEditEntity = (roles: UserRole[], entity: MasterDataKey): boolean => {
  if (roles.includes("superadmin")) return true;
  if (entity === "staff") return false;
  if (roles.includes("procurement_manager") || roles.includes("procurement")) return entity === "suppliers" || entity === "rawMaterials" || entity === "units" || entity === "bom";
  if (roles.includes("sales_manager") || roles.includes("sales")) return entity === "customers" || entity === "products" || entity === "units";
  if (roles.includes("production_manager") || roles.includes("manager") || roles.includes("operator") || roles.includes("operations") || roles.includes("staff")) {
    return entity === "products" || entity === "rawMaterials" || entity === "units" || entity === "bom";
  }
  return false;
};

const buildActor = (user: { id: string; fullName: string; role: UserRole } | null): AuditActor | undefined => {
  if (!user) return undefined;
  return { id: user.id, name: user.fullName, role: user.role };
};

type EditableEntityKey = Exclude<MasterDataKey, "staff">;
type EntityTabKey = "procurement" | "inventory" | "production" | "sales" | "finance" | "journal" | "hr" | "audit" | EditableEntityKey;
type ReceiveDraftState = { purchaseOrderId: string; receivedAt: string; lines: GoodsReceiptLine[] };

export default function ManagerDashboard() {
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
    workOrders,
    salesOrders,
    deliveryNotes,
    invoices,
    financeAccounts,
    financeJournalEntries,
    financeJournalLines,
    apBills,
    apPayments,
    arReceipts,
    attendanceRecords,
    attendanceIntervals,
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
    deleteSalesOrder: _deleteSalesOrder,
    fulfillSalesOrder,
    generateInvoice,
    clearAudit,
  } = useData();

  const roles = (user?.roles?.length ? user.roles : [user?.role ?? "operator"]) as UserRole[];
  const modules = user?.modules ?? defaultModulesForRoles(roles);
  const canSeeInventory = modules.includes("inventory");
  const canSeeProduction = modules.includes("production");
  const canSeeSales = modules.includes("sales");
  const canSeeFinance = modules.includes("finance");
  const canSeeHr = modules.includes("hr");
  const actor = buildActor(user);

  const [tab, setTab] = useState<EntityTabKey>("procurement");
  const [search, setSearch] = useState("");
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingKey, setEditingKey] = useState<EditableEntityKey | null>(null);

  const [hrDate, setHrDate] = useState(() => new Date().toISOString().slice(0, 10));
  const [hrStaffId, setHrStaffId] = useState<string>("all");

  const [customerDraft, setCustomerDraft] = useState<Customer>({ id: "", name: "" });
  const [supplierDraft, setSupplierDraft] = useState<Supplier>({ id: "", name: "" });
  const [unitDraft, setUnitDraft] = useState<MeasurementUnit>({ id: "", name: "" });
  const [productDraft, setProductDraft] = useState<Product>({ id: "", name: "" });
  const [rawMaterialDraft, setRawMaterialDraft] = useState<RawMaterial>({ id: "", name: "" });
  const [bomDraft, setBomDraft] = useState<BillOfMaterialLine>({
    id: "",
    productId: "",
    rawMaterialId: "",
    rawMaterialName: undefined,
    quantity: 1,
    unit: undefined,
    stage: undefined,
  });
  const [formError, setFormError] = useState<string | null>(null);

  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const [importKey, setImportKey] = useState<EditableEntityKey | null>(null);
  const [importError, setImportError] = useState<string | null>(null);

  const staffById = useMemo(() => {
    const map = new Map<string, Staff>();
    for (const s of staff) map.set(s.id, s);
    return map;
  }, [staff]);

  const hrRows = useMemo(() => {
    const staffList = hrStaffId === "all" ? staff : staff.filter((s) => s.id === hrStaffId);

    const rows = staffList.map((s) => {
      const intervalsForDay = attendanceIntervals
        .filter((i) => i.staffId === s.id && i.date === hrDate)
        .slice()
        .sort((a, b) => new Date(a.startAt).getTime() - new Date(b.startAt).getTime());

      const workIntervals = intervalsForDay.filter((i) => i.kind === "work");
      const breakIntervals = intervalsForDay.filter((i) => i.kind === "break");

      const record = attendanceRecords.find((r) => r.staffId === s.id && r.date === hrDate);

      const clockIn = workIntervals[0]?.startAt ?? record?.clockIn ?? undefined;
      const workEndAt = workIntervals.filter((w) => w.endAt).at(-1)?.endAt ?? undefined;
      const clockOut = workEndAt ?? record?.clockOut ?? undefined;

      const workMinutes = workIntervals.reduce((sum, w) => sum + minutesBetween(w.startAt, w.endAt), 0);
      const breakMinutes = breakIntervals.reduce((sum, b) => sum + minutesBetween(b.startAt, b.endAt), 0);

      const status = !clockIn ? "Absent" : !clockOut ? "In progress" : "Complete";

      return {
        staffId: s.id,
        staffName: s.fullName,
        clockIn,
        clockOut,
        workIntervals,
        breakIntervals,
        workMinutes,
        breakMinutes,
        status,
      };
    });

    return rows.sort((a, b) => a.staffId.localeCompare(b.staffId));
  }, [attendanceIntervals, attendanceRecords, hrDate, hrStaffId, staff]);

  const newId = (): string => {
    try {
      return crypto.randomUUID();
    } catch {
      return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    }
  };

  const allowedTabs = useMemo(() => {
    const tabs: EntityTabKey[] = [];
    if (canSeeInventory) tabs.push("procurement", "inventory", "suppliers", "rawMaterials", "units", "bom", "products");
    if (canSeeProduction) tabs.push("production");
    if (canSeeSales) tabs.push("sales", "customers");
    if (canSeeFinance) tabs.push("finance");
    if (canSeeHr) tabs.push("hr");
    tabs.push("audit");
    return Array.from(new Set(tabs));
  }, [canSeeFinance, canSeeHr, canSeeInventory, canSeeProduction, canSeeSales]);

  useEffect(() => {
    if (!allowedTabs.length) return;
    if (!allowedTabs.includes(tab)) setTab(allowedTabs[0]);
  }, [allowedTabs, tab]);

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
    createdAt: new Date().toISOString(),
  });
  const [woError, setWoError] = useState<string | null>(null);

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
    orderedAt: new Date().toISOString(),
    status: "draft",
    notes: "",
    lines: [],
  });
  const [salesError, setSalesError] = useState<string | null>(null);

  const [shipDialogOpen, setShipDialogOpen] = useState(false);
  const [shipDraft, setShipDraft] = useState<FulfillSalesOrderInput>({
    salesOrderId: "",
    shippedAt: new Date().toISOString().slice(0, 10),
    lines: [],
  });
  const [shipError, setShipError] = useState<string | null>(null);

  const activeList = useMemo(() => {
    if (tab === "customers") return customers;
    if (tab === "suppliers") return suppliers;
    if (tab === "products") return products;
    if (tab === "rawMaterials") return rawMaterials;
    if (tab === "units") return units;
    if (tab === "bom") return bom;
    return [];
  }, [tab, customers, suppliers, products, rawMaterials, units, bom]);

  const filteredList = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return activeList;
    return activeList.filter((r) => {
      const id = String((r as { id: string }).id ?? "").toLowerCase();
      const name = String((r as { name?: string }).name ?? "").toLowerCase();
      return id.includes(q) || name.includes(q);
    });
  }, [activeList, search]);

  const canCreatePo = roles.includes("procurement_manager") || roles.includes("procurement") || roles.includes("production_manager") || roles.includes("manager");
  const canReceivePo =
    roles.includes("procurement_manager") ||
    roles.includes("procurement") ||
    roles.includes("production_manager") ||
    roles.includes("manager") ||
    roles.includes("operator") ||
    roles.includes("operations");
  const canAdjustStock = roles.includes("production_manager") || roles.includes("manager") || roles.includes("operator") || roles.includes("operations");

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

  const salesRows = useMemo(() => {
    const q = search.trim().toLowerCase();
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
    if (!q) return rows;
    return rows.filter((r) => r.id.toLowerCase().includes(q) || r.customer.toLowerCase().includes(q));
  }, [salesOrders, deliveryNotes, invoices, search, customerById]);
  const materialById = useMemo(() => new Map(rawMaterials.map((m) => [m.id, m])), [rawMaterials]);
  const poById = useMemo(() => new Map(purchaseOrders.map((po) => [po.id, po])), [purchaseOrders]);

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
  if (roles.includes("superadmin")) return <Navigate to="/superadmin" replace />;

  const startCreate = (key: EditableEntityKey) => {
    setFormError(null);
    setEditingKey(key);
    if (key === "customers") setCustomerDraft({ id: "", name: "" });
    if (key === "suppliers") setSupplierDraft({ id: "", name: "" });
    if (key === "units") setUnitDraft({ id: "", name: "" });
    if (key === "products") setProductDraft({ id: "", name: "" });
    if (key === "rawMaterials") setRawMaterialDraft({ id: "", name: "" });
    if (key === "bom") setBomDraft({ id: "", productId: "", rawMaterialId: "", rawMaterialName: undefined, quantity: 1, unit: undefined, stage: undefined });
    setDialogOpen(true);
  };

  const startEdit = (key: EditableEntityKey, record: unknown) => {
    setFormError(null);
    setEditingKey(key);
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
        const parsed = supplierSchema.parse({
          id: toRequiredString(supplierDraft.id),
          name: toRequiredString(supplierDraft.name),
          country: toOptionalString(supplierDraft.country),
          contactPerson: toOptionalString(supplierDraft.contactPerson),
          email: toOptionalString(supplierDraft.email),
          phone: toOptionalString(supplierDraft.phone),
          leadTimeDays:
            supplierDraft.leadTimeDays === undefined || supplierDraft.leadTimeDays === null ? undefined : toNumber(supplierDraft.leadTimeDays),
          status: toOptionalString(supplierDraft.status),
        });
        upsertRecord("suppliers", parsed as Supplier, actor);
      }
      if (editingKey === "units") {
        const parsed = unitSchema.parse({
          id: toRequiredString(unitDraft.id),
          name: toRequiredString(unitDraft.name),
          unitType: toOptionalString(unitDraft.unitType),
          conversionBase: toOptionalString(unitDraft.conversionBase),
          conversionRate:
            unitDraft.conversionRate === undefined || unitDraft.conversionRate === null ? undefined : toNumber(unitDraft.conversionRate),
        });
        upsertRecord("units", parsed as MeasurementUnit, actor);
      }
      if (editingKey === "products") {
        const parsed = productSchema.parse({
          id: toRequiredString(productDraft.id),
          name: toRequiredString(productDraft.name),
          variant: toOptionalString(productDraft.variant),
          category: toOptionalString(productDraft.category),
          size: productDraft.size === undefined || productDraft.size === null ? undefined : toNumber(productDraft.size),
          unit: toOptionalString(productDraft.unit),
          version: toOptionalString(productDraft.version),
          status: toOptionalString(productDraft.status),
        });
        upsertRecord("products", parsed as Product, actor);
      }
      if (editingKey === "rawMaterials") {
        const parsed = rawMaterialSchema.parse({
          id: toRequiredString(rawMaterialDraft.id),
          name: toRequiredString(rawMaterialDraft.name),
          inciName: toOptionalString(rawMaterialDraft.inciName),
          specGrade: toOptionalString(rawMaterialDraft.specGrade),
          unit: toOptionalString(rawMaterialDraft.unit),
          shelfLifeMonths:
            rawMaterialDraft.shelfLifeMonths === undefined || rawMaterialDraft.shelfLifeMonths === null
              ? undefined
              : toNumber(rawMaterialDraft.shelfLifeMonths),
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
    key === "customers" || key === "suppliers" || key === "products" || key === "rawMaterials" || key === "units" || key === "bom";

  const deleteById = (entity: EditableEntityKey, id: string) => {
    deleteRecord(entity, id, actor);
  };

  const makePoId = (): string => {
    const date = new Date().toISOString().slice(0, 10).replace(/-/g, "");
    const rand = Math.random().toString(16).slice(2, 6).toUpperCase();
    return `PO-${date}-${rand}`;
  };

  const makeWoId = (): string => {
    const date = new Date().toISOString().slice(0, 10).replace(/-/g, "");
    const rand = Math.random().toString(16).slice(2, 6).toUpperCase();
    return `WO-${date}-${rand}`;
  };

  const startNewWo = () => {
    setWoError(null);
    setWoDraft({
      id: makeWoId(),
      productId: "",
      quantity: 1,
      status: "planned",
      createdAt: new Date().toISOString(),
    });
    setWoDialogOpen(true);
  };

  const startEditWo = (wo: WorkOrder) => {
    setWoError(null);
    setWoDraft(wo);
    setWoDialogOpen(true);
  };

  const submitWo = () => {
    setWoError(null);
    const id = toRequiredString(woDraft.id);
    const productId = toRequiredString(woDraft.productId);
    const quantity = Number(woDraft.quantity);
    if (!id) return setWoError("WO ID is required.");
    if (!productId) return setWoError("Product is required.");
    if (!Number.isFinite(quantity) || quantity <= 0) return setWoError("Quantity must be > 0.");

    upsertWorkOrder(
      {
        ...woDraft,
        id,
        productId,
        quantity,
        createdAt: toRequiredString(woDraft.createdAt) || new Date().toISOString(),
      },
      actor,
    );
    setWoDialogOpen(false);
  };

  const makeSoId = (): string => {
    const date = new Date().toISOString().slice(0, 10).replace(/-/g, "");
    const rand = Math.random().toString(16).slice(2, 6).toUpperCase();
    return `SO-${date}-${rand}`;
  };

  const startNewSo = () => {
    setSalesError(null);
    setSalesDraft({
      id: makeSoId(),
      customerId: "",
      orderedAt: new Date().toISOString(),
      status: "draft",
      notes: "",
      lines: [{ id: newId(), productId: "", unitId: undefined, quantity: 1, unitPrice: undefined }],
    });
    setSalesDialogOpen(true);
  };

  const startEditSo = (so: SalesOrder) => {
    setSalesError(null);
    setSalesDraft({ ...so, notes: so.notes ?? "", lines: so.lines.length ? so.lines : [{ id: newId(), productId: "", unitId: undefined, quantity: 1 }] });
    setSalesDialogOpen(true);
  };

  const addSalesLine = () =>
    setSalesDraft((p) => ({ ...p, lines: [...(p.lines.length ? p.lines : []), { id: newId(), productId: "", unitId: undefined, quantity: 1, unitPrice: undefined }] }));

  const updateSalesLine = (lineId: string, patch: Partial<SalesOrder["lines"][number]>) =>
    setSalesDraft((p) => ({ ...p, lines: p.lines.map((l) => (l.id === lineId ? { ...l, ...patch } : l)) }));

  const removeSalesLine = (lineId: string) => setSalesDraft((p) => ({ ...p, lines: p.lines.filter((l) => l.id !== lineId) }));

  const submitSo = () => {
    setSalesError(null);
    const id = toRequiredString(salesDraft.id);
    const customerId = toRequiredString(salesDraft.customerId);
    if (!id || !customerId) return setSalesError("Sales order ID and customer are required.");

    const lines = (salesDraft.lines ?? [])
      .map((l) => ({ ...l, productId: toRequiredString(l.productId), quantity: Number(l.quantity) }))
      .filter((l) => l.productId && Number.isFinite(l.quantity) && l.quantity > 0);
    if (!lines.length) return setSalesError("Add at least one valid line (product + qty).");

    upsertSalesOrder(
      {
        ...salesDraft,
        id,
        customerId,
        orderedAt: toRequiredString(salesDraft.orderedAt) || new Date().toISOString(),
        notes: toOptionalString(salesDraft.notes),
        lines,
      },
      actor,
    );
    setSalesDialogOpen(false);
  };

  const startShip = (so: SalesOrder) => {
    setShipError(null);
    setShipDraft({
      salesOrderId: so.id,
      shippedAt: new Date().toISOString().slice(0, 10),
      lines: so.lines.map((l) => ({ productId: l.productId, unitId: l.unitId, quantity: l.quantity })),
    });
    setShipDialogOpen(true);
  };

  const submitShip = () => {
    setShipError(null);
    const shippedAtRaw = toOptionalString(shipDraft.shippedAt);
    const shippedAt = shippedAtRaw ? new Date(shippedAtRaw).toISOString() : undefined;
    const lines = (shipDraft.lines ?? []).filter((l) => l.productId && Number.isFinite(l.quantity) && l.quantity > 0);
    if (!shipDraft.salesOrderId || !lines.length) return setShipError("Invalid fulfillment.");

    const err = fulfillSalesOrder({ salesOrderId: shipDraft.salesOrderId, shippedAt, lines }, actor);
    if (err) return setShipError(err);
    setShipDialogOpen(false);
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

  const addReceiveLine = () =>
    setReceiveDraft((p) => ({
      ...p,
      lines: [...p.lines, { id: newId(), rawMaterialId: "", unitId: undefined, quantity: 1, batchNo: "", expiryDate: "" }],
    }));

  const updateReceiveLine = (lineId: string, patch: Partial<GoodsReceiptLine>) =>
    setReceiveDraft((p) => ({ ...p, lines: p.lines.map((l) => (l.id === lineId ? { ...l, ...patch } : l)) }));

  const removeReceiveLine = (lineId: string) =>
    setReceiveDraft((p) => ({ ...p, lines: p.lines.length <= 1 ? p.lines : p.lines.filter((l) => l.id !== lineId) }));

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

  return (
    <div className="min-h-screen bg-[hsl(220,20%,8%)] text-foreground font-sans">
      <div className="sticky top-0 z-10 border-b border-[hsl(220,20%,18%)] bg-[hsl(220,20%,8%)]/80 backdrop-blur">
        <div className="mx-auto flex max-w-6xl items-center justify-between px-6 py-4">
          <div>
            <div className="text-xl font-bold text-[hsl(45,93%,47%)]">SHAB Dashboard</div>
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
        <div className="mb-6 flex items-end justify-between gap-4">
          <div>
            <div className="text-3xl font-bold text-[hsl(45,93%,47%)]">
              {tab === "procurement" ? "Procurement" : tab === "inventory" ? "Inventory" : tab === "audit" ? "Audit" : "Master Data"}
            </div>
            <div className="text-muted-foreground">
              {tab === "procurement"
                ? "Create purchase orders and receive stock."
                : tab === "inventory"
                  ? "Track stock on hand and low-stock levels."
                  : tab === "audit"
                    ? "Change history across the system."
                    : "Manage suppliers, raw materials, units, BOM, products, and customers."}
            </div>
          </div>
          {tab === "procurement" ? (
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
          ) : tab !== "audit" && isMasterDataTab(tab) ? (
            <div className="flex items-center gap-2">
              <Input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search by ID or name..."
                className="w-[260px] bg-[hsl(220,20%,12%)]"
              />
              <Button onClick={() => startImport(tab)} disabled={!canEditEntity(roles, tab)}>
                Import CSV
              </Button>
              {tab === "suppliers" || tab === "units" || tab === "products" || tab === "rawMaterials" || tab === "bom" ? (
                <Button variant="outline" onClick={() => exportCsv(tab)}>
                  Export CSV
                </Button>
              ) : null}
              <Button onClick={() => startCreate(tab)} disabled={!canEditEntity(roles, tab)}>
                Add
              </Button>
            </div>
          ) : (
            <div className="flex items-center gap-2">
              <Button variant="outline" onClick={clearAudit}>
                Clear Audit
              </Button>
            </div>
          )}
        </div>

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
        {importError ? <div className="mb-4 rounded-md border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{importError}</div> : null}

        <Tabs value={tab} onValueChange={(v) => setTab(v as EntityTabKey)}>
          <TabsList className="mb-4 bg-[hsl(220,20%,12%)]">
            {canSeeInventory ? (
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
            {canSeeProduction ? <TabsTrigger value="production">Production</TabsTrigger> : null}
            {canSeeSales ? (
              <>
                <TabsTrigger value="sales">Sales</TabsTrigger>
                <TabsTrigger value="customers">Customers</TabsTrigger>
              </>
            ) : null}
            {canSeeFinance ? (
              <>
                <TabsTrigger value="finance">Finance</TabsTrigger>
                <TabsTrigger value="journal">Journal</TabsTrigger>
              </>
            ) : null}
            {canSeeHr ? <TabsTrigger value="hr">Human Resource</TabsTrigger> : null}
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
                        const canEditPo = canCreatePo && (po?.status === "draft" || po?.status === "sent");
                        const canDeletePo = canCreatePo && (po?.status === "draft" || po?.status === "sent");
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
                                <Button variant="outline" size="sm" onClick={() => po && startEditPo(po)} disabled={!canEditPo || !po}>
                                  Edit
                                </Button>
                                <Button variant="outline" size="sm" onClick={() => startReceive(r.id)} disabled={!canReceiveThisPo}>
                                  Receive
                                </Button>
                                <Button variant="destructive" size="sm" onClick={() => deletePo(r.id)} disabled={!canDeletePo}>
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
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => startAdjustFor(r.id, materialById.get(r.id)?.unit)}
                              disabled={!canAdjustStock}
                            >
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
                  <Button variant="outline" size="sm" onClick={startNewWo}>
                    New WO
                  </Button>
                </div>
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
                                <Button variant="outline" size="sm" onClick={() => wo && startEditWo(wo)} disabled={!wo}>
                                  Edit
                                </Button>
                                <Button variant="outline" size="sm" onClick={() => wo && startWorkOrder(wo.id, actor)} disabled={!canStart}>
                                  Start
                                </Button>
                                <Button
                                  variant="outline"
                                  size="sm"
                                  onClick={() => {
                                    if (!wo || !canIssue) return;
                                    const err = issueWorkOrderMaterials(wo.id, actor);
                                    if (err) setCompleteError(err);
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

          <TabsContent value="sales">
            <Card className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
              <CardHeader>
                <CardTitle className="text-[hsl(0,0%,98%)]">Sales Orders</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="mb-3 flex gap-2">
                  <Button variant="outline" size="sm" onClick={startNewSo}>
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
                                <Button variant="outline" size="sm" onClick={() => so && startEditSo(so)} disabled={!so}>
                                  Edit
                                </Button>
                                <Button variant="outline" size="sm" onClick={() => so && startShip(so)} disabled={!canShip}>
                                  Fulfill
                                </Button>
                                <Button variant="outline" size="sm" onClick={() => so && generateInvoice(so.id, new Date().toISOString(), actor)} disabled={!so}>
                                  Invoice
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

          <TabsContent value="hr">
            <Card className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
              <CardHeader>
                <CardTitle className="text-[hsl(0,0%,98%)]">Human Resource</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="flex flex-col gap-4">
                  <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
                    <div className="space-y-2">
                      <Label>Date</Label>
                      <Input type="date" value={hrDate} onChange={(e) => setHrDate(e.target.value)} />
                    </div>
                    <div className="space-y-2">
                      <Label>Staff</Label>
                      <Select value={hrStaffId} onValueChange={setHrStaffId}>
                        <SelectTrigger>
                          <SelectValue placeholder="All staff" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="all">All staff</SelectItem>
                          {staff
                            .slice()
                            .sort((a, b) => a.id.localeCompare(b.id))
                            .map((s) => (
                              <SelectItem key={s.id} value={s.id}>
                                {s.id} — {s.fullName}
                              </SelectItem>
                            ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label>Summary</Label>
                      <div className="text-sm text-muted-foreground">
                        {hrRows.length} staff · {hrDate}
                      </div>
                    </div>
                  </div>

                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Staff ID</TableHead>
                        <TableHead>Name</TableHead>
                        <TableHead>Clock In</TableHead>
                        <TableHead>Work</TableHead>
                        <TableHead>Lunch / Break</TableHead>
                        <TableHead>Clock Out</TableHead>
                        <TableHead className="text-right">Work (h:mm)</TableHead>
                        <TableHead className="text-right">Break (h:mm)</TableHead>
                        <TableHead>Status</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {hrRows.length ? (
                        hrRows.map((r) => (
                          <TableRow key={r.staffId}>
                            <TableCell className="font-mono">{r.staffId}</TableCell>
                            <TableCell className="text-xs">{staffById.get(r.staffId)?.fullName ?? r.staffName}</TableCell>
                            <TableCell className="font-mono text-xs">{formatDateTimeParts(r.clockIn).time}</TableCell>
                            <TableCell className="text-xs">
                              {r.workIntervals.length ? (
                                <div className="flex flex-col gap-1">
                                  {r.workIntervals.map((i) => (
                                    <div key={i.id} className="font-mono">
                                      {formatDateTimeParts(i.startAt).time} - {formatDateTimeParts(i.endAt).time}
                                    </div>
                                  ))}
                                </div>
                              ) : (
                                <span className="text-muted-foreground">-</span>
                              )}
                            </TableCell>
                            <TableCell className="text-xs">
                              {r.breakIntervals.length ? (
                                <div className="flex flex-col gap-1">
                                  {r.breakIntervals.map((i) => (
                                    <div key={i.id} className="font-mono">
                                      {formatDateTimeParts(i.startAt).time} - {formatDateTimeParts(i.endAt).time}
                                    </div>
                                  ))}
                                </div>
                              ) : (
                                <span className="text-muted-foreground">-</span>
                              )}
                            </TableCell>
                            <TableCell className="font-mono text-xs">{formatDateTimeParts(r.clockOut).time}</TableCell>
                            <TableCell className="text-right font-mono">{formatMinutes(r.workMinutes)}</TableCell>
                            <TableCell className="text-right font-mono">{formatMinutes(r.breakMinutes)}</TableCell>
                            <TableCell className="text-xs">{r.status}</TableCell>
                          </TableRow>
                        ))
                      ) : (
                        <TableRow>
                          <TableCell colSpan={9} className="text-center text-muted-foreground">
                            No attendance data
                          </TableCell>
                        </TableRow>
                      )}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>
          </TabsContent>
          {(["customers", "suppliers", "products", "rawMaterials", "units", "bom"] as const).map((key) => (
            <TabsContent key={key} value={key}>
              <Card className="border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
                <CardHeader>
                  <CardTitle className="text-[hsl(0,0%,98%)]">{labelForEntityKey(key)}</CardTitle>
                </CardHeader>
                <CardContent>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        {key === "customers" ? (
                          <>
                            <TableHead>ID</TableHead>
                            <TableHead>Name</TableHead>
                            <TableHead>Phone</TableHead>
                            <TableHead>Email</TableHead>
                            <TableHead>Address</TableHead>
                          </>
                        ) : key === "suppliers" ? (
                          <>
                            <TableHead>Supplier ID</TableHead>
                            <TableHead>Supplier Name</TableHead>
                            <TableHead>Country</TableHead>
                            <TableHead>Contact Person</TableHead>
                            <TableHead>Email</TableHead>
                            <TableHead>Phone</TableHead>
                            <TableHead className="text-right">Lead Time (Days)</TableHead>
                            <TableHead>Status</TableHead>
                          </>
                        ) : key === "units" ? (
                          <>
                            <TableHead>Unit Code</TableHead>
                            <TableHead>Unit Name</TableHead>
                            <TableHead>Type</TableHead>
                            <TableHead>Conversion Base</TableHead>
                            <TableHead className="text-right">Conversion Rate</TableHead>
                            <TableHead>Symbol</TableHead>
                          </>
                        ) : key === "products" ? (
                          <>
                            <TableHead>SKU</TableHead>
                            <TableHead>Product Name</TableHead>
                            <TableHead>Variant</TableHead>
                            <TableHead>Category</TableHead>
                            <TableHead className="text-right">Size</TableHead>
                            <TableHead>Unit</TableHead>
                            <TableHead>Version</TableHead>
                            <TableHead>Status</TableHead>
                          </>
                        ) : key === "rawMaterials" ? (
                          <>
                            <TableHead>Material Code</TableHead>
                            <TableHead>Material Name</TableHead>
                            <TableHead>INCI Name</TableHead>
                            <TableHead>Spec Grade</TableHead>
                            <TableHead>Unit</TableHead>
                            <TableHead className="text-right">Shelf Life (Months)</TableHead>
                            <TableHead>Storage Condition</TableHead>
                            <TableHead>Status</TableHead>
                          </>
                        ) : (
                          <>
                            <TableHead>BOM ID</TableHead>
                            <TableHead>Product SKU</TableHead>
                            <TableHead>Material Code</TableHead>
                            <TableHead>Material Name</TableHead>
                            <TableHead className="text-right">Quantity</TableHead>
                            <TableHead>Unit</TableHead>
                            <TableHead>Stage</TableHead>
                          </>
                        )}
                        <TableHead className="text-right">Actions</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {filteredList.length ? (
                        filteredList.map((r) => {
                          const id = (r as { id: string }).id;
                          return (
                            <TableRow key={id}>
                              {key === "customers" ? (
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
                                  <Button variant="outline" size="sm" onClick={() => startEdit(key, r)} disabled={!canEditEntity(roles, key)}>
                                    Edit
                                  </Button>
                                  <Button
                                    variant="destructive"
                                    size="sm"
                                    onClick={() => deleteById(key, id)}
                                    disabled={!canEditEntity(roles, key)}
                                  >
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
      </div>

      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="max-h-[85vh] overflow-y-auto border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
          <DialogHeader>
            <DialogTitle className="text-[hsl(0,0%,98%)]">
              {editingKey ? `${editingKey} ${((() => {
                const id =
                  editingKey === "customers"
                    ? customerDraft.id
                    : editingKey === "suppliers"
                      ? supplierDraft.id
                      : editingKey === "units"
                        ? unitDraft.id
                        : editingKey === "products"
                          ? productDraft.id
                          : editingKey === "rawMaterials"
                            ? rawMaterialDraft.id
                            : bomDraft.id;
                return id ? "edit" : "create";
              })())}` : "Edit"}
            </DialogTitle>
          </DialogHeader>

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
                <Label htmlFor="s_email">Email</Label>
                <Input
                  id="s_email"
                  value={supplierDraft.email ?? ""}
                  onChange={(e) => setSupplierDraft((p) => ({ ...p, email: e.target.value }))}
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
            <Button onClick={submit} disabled={!editingKey || !canEditEntity(roles, editingKey)}>
              Save
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={woDialogOpen} onOpenChange={setWoDialogOpen}>
        <DialogContent className="max-h-[85vh] overflow-y-auto border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
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
              <Input
                id="wo_qty"
                type="number"
                value={String(woDraft.quantity)}
                onChange={(e) => setWoDraft((p) => ({ ...p, quantity: Number(e.target.value) }))}
                className="bg-[hsl(220,20%,12%)]"
              />
            </div>
          </div>
          {woError ? <div className="mt-2 rounded-md border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{woError}</div> : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setWoDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={submitWo}>Save</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={completeDialogOpen} onOpenChange={setCompleteDialogOpen}>
        <DialogContent className="max-h-[85vh] overflow-y-auto border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
          <DialogHeader>
            <DialogTitle className="text-[hsl(0,0%,98%)]">Complete Work Order</DialogTitle>
          </DialogHeader>
          <div className="grid gap-3">
            <div className="grid gap-2">
              <Label htmlFor="wo_out_qty">Output Quantity</Label>
              <Input
                id="wo_out_qty"
                type="number"
                value={String(completeDraft.quantity)}
                onChange={(e) => setCompleteDraft((p) => ({ ...p, quantity: Number(e.target.value) }))}
                className="bg-[hsl(220,20%,12%)]"
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="wo_batch">Batch No</Label>
              <Input id="wo_batch" value={completeDraft.batchNo ?? ""} onChange={(e) => setCompleteDraft((p) => ({ ...p, batchNo: e.target.value }))} className="bg-[hsl(220,20%,12%)]" />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="wo_expiry">Expiry Date</Label>
              <Input id="wo_expiry" type="date" value={(completeDraft.expiryDate ?? "").slice(0, 10)} onChange={(e) => setCompleteDraft((p) => ({ ...p, expiryDate: e.target.value }))} className="bg-[hsl(220,20%,12%)]" />
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
                const qty = Number(completeDraft.quantity);
                if (!Number.isFinite(qty) || qty <= 0) {
                  setCompleteError("Quantity must be > 0.");
                  return;
                }
                const err = completeWorkOrder(completeDraft.workOrderId, qty, completeDraft.batchNo, completeDraft.expiryDate, actor);
                if (err) {
                  setCompleteError(err);
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
        <DialogContent className="max-h-[85vh] overflow-y-auto border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
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
                <Button variant="outline" size="sm" onClick={addSalesLine}>
                  Add Line
                </Button>
              </div>
              <div className="grid gap-3">
                {salesDraft.lines.map((line) => (
                  <div key={line.id} className="grid grid-cols-12 gap-2">
                    <div className="col-span-6">
                      <Select value={line.productId} onValueChange={(v) => updateSalesLine(line.id, { productId: v })}>
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
                      <Input type="number" value={String(line.quantity)} onChange={(e) => updateSalesLine(line.id, { quantity: Number(e.target.value) })} className="bg-[hsl(220,20%,12%)]" />
                    </div>
                    <div className="col-span-3 flex justify-end">
                      <Button variant="outline" size="sm" onClick={() => removeSalesLine(line.id)} disabled={salesDraft.lines.length <= 1}>
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
                submitSo();
              }}
            >
              Save
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={shipDialogOpen} onOpenChange={setShipDialogOpen}>
        <DialogContent className="max-h-[85vh] overflow-y-auto border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
          <DialogHeader>
            <DialogTitle className="text-[hsl(0,0%,98%)]">Fulfill Sales Order</DialogTitle>
          </DialogHeader>
          <div className="grid gap-3">
            <div className="grid gap-2">
              <Label htmlFor="ship_date">Ship Date</Label>
              <Input id="ship_date" type="date" value={(shipDraft.shippedAt ?? "").slice(0, 10)} onChange={(e) => setShipDraft((p) => ({ ...p, shippedAt: e.target.value }))} className="bg-[hsl(220,20%,12%)]" />
            </div>
            <div className="rounded-md border border-[hsl(220,20%,18%)] p-3">
              <div className="mb-2 font-medium text-[hsl(0,0%,98%)]">Lines</div>
              <div className="grid gap-3">
                {shipDraft.lines.map((line, idx) => (
                  <div key={`${line.productId}-${idx}`} className="grid grid-cols-12 gap-2">
                    <div className="col-span-6">
                      <Select
                        value={line.productId}
                        onValueChange={(v) => setShipDraft((p) => ({ ...p, lines: p.lines.map((l, i) => (i === idx ? { ...l, productId: v } : l)) }))}>
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
                        onChange={(e) => setShipDraft((p) => ({ ...p, lines: p.lines.map((l, i) => (i === idx ? { ...l, quantity: Number(e.target.value) } : l)) }))}
                        className="bg-[hsl(220,20%,12%)]"
                      />
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
          {shipError ? <div className="mt-2 rounded-md border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{shipError}</div> : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setShipDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={submitShip}>Fulfill</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <Dialog open={poDialogOpen} onOpenChange={setPoDialogOpen}>
        <DialogContent className="max-h-[85vh] overflow-y-auto border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
          <DialogHeader>
            <DialogTitle className="text-[hsl(0,0%,98%)]">{poDraft.id ? `Purchase Order ${poDraft.id}` : "Purchase Order"}</DialogTitle>
          </DialogHeader>

          <div className="grid gap-3">
            <div className="grid gap-2">
              <Label htmlFor="po_id">PO ID</Label>
              <Input
                id="po_id"
                value={poDraft.id}
                onChange={(e) => setPoDraft((p) => ({ ...p, id: e.target.value }))}
                className="bg-[hsl(220,20%,12%)]"
              />
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
                <Input
                  id="po_notes"
                  value={poDraft.notes ?? ""}
                  onChange={(e) => setPoDraft((p) => ({ ...p, notes: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
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
                      <Input
                        type="number"
                        value={String(line.quantity)}
                        onChange={(e) => updatePoLine(line.id, { quantity: Number(e.target.value) })}
                        className="bg-[hsl(220,20%,12%)]"
                      />
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
        <DialogContent className="max-h-[85vh] overflow-y-auto border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
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
                      <Input
                        type="number"
                        value={String(line.quantity)}
                        onChange={(e) => updateReceiveLine(line.id, { quantity: Number(e.target.value) })}
                        className="bg-[hsl(220,20%,12%)]"
                      />
                    </div>
                    <div className="col-span-2">
                      <Input
                        value={line.batchNo ?? ""}
                        onChange={(e) => updateReceiveLine(line.id, { batchNo: e.target.value })}
                        placeholder="Batch"
                        className="bg-[hsl(220,20%,12%)]"
                      />
                    </div>
                    <div className="col-span-2">
                      <Input
                        type="date"
                        value={line.expiryDate?.slice(0, 10) ?? ""}
                        onChange={(e) => updateReceiveLine(line.id, { expiryDate: e.target.value })}
                        className="bg-[hsl(220,20%,12%)]"
                      />
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

          {receiveError ? (
            <div className="mt-2 rounded-md border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{receiveError}</div>
          ) : null}

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
        <DialogContent className="max-h-[85vh] overflow-y-auto border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)]">
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
                <Input
                  id="adj_qty"
                  type="number"
                  value={String(adjustDraft.quantityDelta)}
                  onChange={(e) => setAdjustDraft((p) => ({ ...p, quantityDelta: Number(e.target.value) }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
            </div>

            <div className="grid grid-cols-3 gap-3">
              <div className="grid gap-2">
                <Label htmlFor="adj_at">Date</Label>
                <Input
                  id="adj_at"
                  type="date"
                  value={adjustDraft.at?.slice(0, 10) ?? ""}
                  onChange={(e) => setAdjustDraft((p) => ({ ...p, at: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="adj_batch">Batch No</Label>
                <Input
                  id="adj_batch"
                  value={adjustDraft.batchNo ?? ""}
                  onChange={(e) => setAdjustDraft((p) => ({ ...p, batchNo: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="adj_exp">Expiry</Label>
                <Input
                  id="adj_exp"
                  type="date"
                  value={adjustDraft.expiryDate?.slice(0, 10) ?? ""}
                  onChange={(e) => setAdjustDraft((p) => ({ ...p, expiryDate: e.target.value }))}
                  className="bg-[hsl(220,20%,12%)]"
                />
              </div>
            </div>
          </div>

          {adjustError ? (
            <div className="mt-2 rounded-md border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">{adjustError}</div>
          ) : null}

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
