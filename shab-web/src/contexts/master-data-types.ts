export type UserRole =
  | "superadmin"
  | "supervisor"
  | "operator"
  | "production_manager"
  | "procurement_manager"
  | "finance_manager"
  | "sales_manager"
  | "manager"
  | "staff"
  | "operations"
  | "procurement"
  | "sales";

export type AppModule = "inventory" | "production" | "sales" | "finance" | "hr";

export const ALL_MODULES: AppModule[] = ["inventory", "production", "sales", "finance", "hr"];

export const defaultModulesForRole = (role: UserRole): AppModule[] => {
  if (role === "superadmin") return ALL_MODULES;
  if (role === "production_manager") return ["inventory", "production"];
  if (role === "procurement_manager") return ["inventory"];
  if (role === "sales_manager") return ["sales"];
  if (role === "finance_manager") return ["finance"];
  if (role === "manager") return ["inventory", "production"];
  if (role === "supervisor") return ["inventory"];
  if (role === "operator") return ["inventory", "production"];
  if (role === "staff") return ["inventory"];
  if (role === "operations") return ["inventory", "production"];
  if (role === "procurement") return ["inventory"];
  if (role === "sales") return ["sales"];
  return [];
};

export const defaultModulesForRoles = (roles: UserRole[]): AppModule[] => {
  const modules = new Set<AppModule>();
  for (const role of roles) {
    for (const m of defaultModulesForRole(role)) modules.add(m);
  }
  return Array.from(modules);
};

export type StaffStatus = "active" | "inactive";

export interface Staff {
  id: string;
  fullName: string;
  email?: string;
  role: UserRole;
  roles?: UserRole[];
  department?: string;
  username: string;
  passwordHash: string;
  status: StaffStatus;
  dateJoined?: string;
  lastLogin?: string;
  modules: AppModule[];
}

export interface AttendanceRecord {
  id: string;
  staffId: string;
  date: string;
  clockIn: string;
  clockOut?: string;
}

export type AttendanceIntervalKind = "work" | "break";

export interface AttendanceEvent {
  id: string;
  staffId: string;
  deviceId: string;
  occurredAt: string;
  eventDate: string;
}

export interface AttendanceInterval {
  id: string;
  staffId: string;
  date: string;
  kind: AttendanceIntervalKind;
  startAt: string;
  endAt?: string;
}

export interface Customer {
  id: string;
  name: string;
  phone?: string;
  email?: string;
  address?: string;
}

export interface Supplier {
  id: string;
  name: string;
  country?: string;
  contactPerson?: string;
  phone?: string;
  email?: string;
  leadTimeDays?: number;
  status?: string;
}

export interface MeasurementUnit {
  id: string;
  name: string;
  unitType?: string;
  conversionBase?: string;
  conversionRate?: number;
  symbol?: string;
}

export interface Product {
  id: string;
  name: string;
  variant?: string;
  category?: string;
  size?: number;
  unit?: string;
  version?: string;
  status?: string;
}

export interface RawMaterial {
  id: string;
  name: string;
  inciName?: string;
  specGrade?: string;
  unit?: string;
  shelfLifeMonths?: number;
  storageCondition?: string;
  status?: string;
}

export interface BillOfMaterialLine {
  id: string;
  productId: string;
  rawMaterialId: string;
  rawMaterialName?: string;
  quantity: number;
  unit?: string;
  stage?: string;
}

export type MasterDataKey = "staff" | "customers" | "suppliers" | "products" | "rawMaterials" | "units" | "bom";

export interface MasterDataState {
  staff: Staff[];
  customers: Customer[];
  suppliers: Supplier[];
  products: Product[];
  rawMaterials: RawMaterial[];
  units: MeasurementUnit[];
  bom: BillOfMaterialLine[];
}

export type PurchaseOrderStatus = "draft" | "sent" | "received" | "cancelled";

export interface PurchaseOrderLine {
  id: string;
  rawMaterialId: string;
  unitId?: string;
  quantity: number;
  unitPrice?: number;
}

export interface PurchaseOrder {
  id: string;
  supplierId: string;
  orderedAt: string;
  expectedAt?: string;
  status: PurchaseOrderStatus;
  notes?: string;
  lines: PurchaseOrderLine[];
}

export interface GoodsReceiptLine {
  id: string;
  rawMaterialId: string;
  unitId?: string;
  quantity: number;
  batchNo?: string;
  expiryDate?: string;
}

export interface GoodsReceipt {
  id: string;
  purchaseOrderId: string;
  receivedAt: string;
  lines: GoodsReceiptLine[];
}

export interface StockLot {
  id: string;
  rawMaterialId: string;
  unitId?: string;
  receivedAt: string;
  batchNo?: string;
  expiryDate?: string;
  quantityOnHand: number;
  sourceReceiptId?: string;
}

export type StockMovementType = "receive" | "issue" | "adjust";

export interface StockMovement {
  id: string;
  at: string;
  type: StockMovementType;
  rawMaterialId: string;
  lotId?: string;
  quantityDelta: number;
  referenceId?: string;
}

export interface ProcurementState {
  purchaseOrders: PurchaseOrder[];
  goodsReceipts: GoodsReceipt[];
  stockLots: StockLot[];
  stockMovements: StockMovement[];
}

export type WorkOrderStatus = "planned" | "in_progress" | "completed" | "cancelled";

export interface WorkOrder {
  id: string;
  productId: string;
  quantity: number;
  status: WorkOrderStatus;
  createdAt: string;
  startedAt?: string;
  completedAt?: string;
}

export interface ProductLot {
  id: string;
  productId: string;
  unitId?: string;
  producedAt: string;
  batchNo?: string;
  expiryDate?: string;
  quantityOnHand: number;
  sourceWorkOrderId?: string;
}

export type ProductMovementType = "produce" | "adjust" | "ship";

export interface ProductMovement {
  id: string;
  at: string;
  type: ProductMovementType;
  productId: string;
  lotId?: string;
  quantityDelta: number;
  referenceId?: string;
}

export interface ProductionState {
  workOrders: WorkOrder[];
  productLots: ProductLot[];
  productMovements: ProductMovement[];
}

export type SalesOrderStatus = "draft" | "confirmed" | "fulfilled" | "cancelled";

export interface SalesOrderLine {
  id: string;
  productId: string;
  unitId?: string;
  quantity: number;
  unitPrice?: number;
}

export interface SalesOrder {
  id: string;
  customerId: string;
  orderedAt: string;
  status: SalesOrderStatus;
  notes?: string;
  lines: SalesOrderLine[];
}

export interface DeliveryNoteLine {
  id: string;
  productId: string;
  unitId?: string;
  quantity: number;
}

export interface DeliveryNote {
  id: string;
  salesOrderId: string;
  shippedAt: string;
  lines: DeliveryNoteLine[];
}

export interface Invoice {
  id: string;
  salesOrderId: string;
  invoicedAt: string;
  totalAmount?: number;
}

export interface SalesState {
  salesOrders: SalesOrder[];
  deliveryNotes: DeliveryNote[];
  invoices: Invoice[];
}

export interface FinanceAccount {
  id: string;
  code: string;
  name: string;
  type: string;
  isActive?: boolean;
}

export interface FinanceJournalEntry {
  id: string;
  postedAt: string;
  memo?: string;
  sourceTable?: string;
  sourceId?: string;
}

export interface FinanceJournalLine {
  id: string;
  journalEntryId: string;
  accountId: string;
  description?: string;
  debit: number;
  credit: number;
}

export interface ApBill {
  id: string;
  supplierId?: string;
  purchaseOrderId?: string;
  billedAt: string;
  dueAt?: string;
  status: string;
  totalAmount: number;
  notes?: string;
}

export interface ApPayment {
  id: string;
  billId?: string;
  paidAt: string;
  method?: string;
  amount: number;
  reference?: string;
}

export interface ArReceipt {
  id: string;
  customerId?: string;
  invoiceId?: string;
  receivedAt: string;
  method?: string;
  amount: number;
  reference?: string;
}

export interface FinanceState {
  financeAccounts: FinanceAccount[];
  financeJournalEntries: FinanceJournalEntry[];
  financeJournalLines: FinanceJournalLine[];
  apBills: ApBill[];
  apPayments: ApPayment[];
  arReceipts: ArReceipt[];
}

export type AppEntityKey =
  | MasterDataKey
  | "purchaseOrders"
  | "goodsReceipts"
  | "stockLots"
  | "stockMovements"
  | "workOrders"
  | "productLots"
  | "productMovements"
  | "salesOrders"
  | "deliveryNotes"
  | "invoices"
  | "financeAccounts"
  | "financeJournalEntries"
  | "financeJournalLines"
  | "apBills"
  | "apPayments"
  | "arReceipts";

export type AuditAction = "import" | "create" | "update" | "delete" | "reset";

export interface AuditActor {
  id: string;
  name: string;
  role: UserRole;
}

export interface AuditEvent {
  id: string;
  at: string;
  actor?: AuditActor;
  action: AuditAction;
  entity: AppEntityKey;
  entityId?: string;
  before?: unknown;
  after?: unknown;
  meta?: Record<string, unknown>;
}

export interface ReceivePurchaseOrderInput {
  purchaseOrderId: string;
  receivedAt?: string;
  lines: Array<{
    rawMaterialId: string;
    unitId?: string;
    quantity: number;
    batchNo?: string;
    expiryDate?: string;
  }>;
}

export interface AdjustStockInput {
  rawMaterialId: string;
  unitId?: string;
  quantityDelta: number;
  at?: string;
  batchNo?: string;
  expiryDate?: string;
}

export interface FulfillSalesOrderInput {
  salesOrderId: string;
  shippedAt?: string;
  lines: Array<{
    productId: string;
    unitId?: string;
    quantity: number;
  }>;
}

export interface AttendanceState {
  attendanceRecords: AttendanceRecord[];
  attendanceEvents: AttendanceEvent[];
  attendanceIntervals: AttendanceInterval[];
}

export interface DataContextType extends MasterDataState, ProcurementState, ProductionState, SalesState, FinanceState, AttendanceState {
  isLoading: boolean;
  audit: AuditEvent[];
  importRecords: <T extends MasterDataKey>(key: T, records: MasterDataState[T], actor?: AuditActor) => void;
  upsertRecord: <T extends MasterDataKey>(key: T, record: MasterDataState[T][number], actor?: AuditActor) => void;
  deleteRecord: (key: MasterDataKey, id: string, actor?: AuditActor) => void;
  upsertPurchaseOrder: (order: PurchaseOrder, actor?: AuditActor) => void;
  deletePurchaseOrder: (id: string, actor?: AuditActor) => void;
  receivePurchaseOrder: (input: ReceivePurchaseOrderInput, actor?: AuditActor) => string | null;
  adjustStock: (input: AdjustStockInput, actor?: AuditActor) => string | null;
  upsertWorkOrder: (order: WorkOrder, actor?: AuditActor) => void;
  startWorkOrder: (id: string, actor?: AuditActor) => void;
  issueWorkOrderMaterials: (id: string, actor?: AuditActor) => string | null;
  completeWorkOrder: (id: string, outputQuantity: number, batchNo?: string, expiryDate?: string, actor?: AuditActor) => string | null;
  upsertSalesOrder: (order: SalesOrder, actor?: AuditActor) => void;
  deleteSalesOrder: (id: string, actor?: AuditActor) => void;
  fulfillSalesOrder: (input: FulfillSalesOrderInput, actor?: AuditActor) => string | null;
  generateInvoice: (salesOrderId: string, invoicedAt?: string, actor?: AuditActor) => void;
  clearAudit: () => void;
  resetAll: () => void;
}
