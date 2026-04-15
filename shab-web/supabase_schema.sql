create extension if not exists "uuid-ossp";

create table if not exists customers (
  id text primary key,
  company_name text,
  brand_name text,
  country text,
  contact_person text,
  email text,
  phone text,
  type text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists suppliers (
  id text primary key,
  name text,
  country text,
  contact_person text,
  email text,
  phone text,
  lead_time_days integer,
  status text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists staff (
  id text primary key,
  full_name text,
  email text,
  role text,
  department text,
  username text,
  password_hash text,
  status text,
  date_joined timestamptz,
  last_login timestamptz,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists attendance_events (
  id text primary key default uuid_generate_v4()::text,
  staff_id text not null references staff(id) on delete cascade,
  device_id text not null,
  occurred_at timestamptz not null,
  event_date date not null,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create unique index if not exists attendance_events_unique_punch on attendance_events (staff_id, occurred_at, device_id);
create index if not exists attendance_events_staff_date_time_idx on attendance_events (staff_id, event_date, occurred_at);

create table if not exists attendance_intervals (
  id text primary key default uuid_generate_v4()::text,
  staff_id text not null references staff(id) on delete cascade,
  event_date date not null,
  kind text not null check (kind in ('work', 'break')),
  start_at timestamptz not null,
  end_at timestamptz,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create index if not exists attendance_intervals_staff_date_idx on attendance_intervals (staff_id, event_date);

create table if not exists attendance_records (
  id text primary key,
  staff_id text not null references staff(id) on delete cascade,
  date date not null,
  clock_in timestamptz not null,
  clock_out timestamptz,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create index if not exists attendance_records_staff_date_idx on attendance_records (staff_id, date);

create or replace function shab_set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create or replace function shab_attendance_events_fill_event_date()
returns trigger
language plpgsql
as $$
begin
  if new.event_date is null then
    new.event_date = new.occurred_at::date;
  end if;
  return new;
end;
$$;

drop trigger if exists attendance_events_fill_event_date on attendance_events;
create trigger attendance_events_fill_event_date
before insert or update on attendance_events
for each row
execute function shab_attendance_events_fill_event_date();

drop trigger if exists attendance_events_set_updated_at on attendance_events;
create trigger attendance_events_set_updated_at
before update on attendance_events
for each row
execute function shab_set_updated_at();

drop trigger if exists attendance_intervals_set_updated_at on attendance_intervals;
create trigger attendance_intervals_set_updated_at
before update on attendance_intervals
for each row
execute function shab_set_updated_at();

drop trigger if exists attendance_records_set_updated_at on attendance_records;
create trigger attendance_records_set_updated_at
before update on attendance_records
for each row
execute function shab_set_updated_at();

create or replace function shab_rebuild_attendance_day(p_staff_id text, p_event_date date)
returns void
language plpgsql
as $$
declare
  total_count integer;
  day_id text;
  first_punch timestamptz;
  last_punch timestamptz;
begin
  delete from attendance_intervals where staff_id = p_staff_id and event_date = p_event_date;

  select count(*), min(occurred_at), max(occurred_at)
  into total_count, first_punch, last_punch
  from attendance_events
  where staff_id = p_staff_id and event_date = p_event_date;

  if total_count is null or total_count = 0 then
    delete from attendance_records where staff_id = p_staff_id and date = p_event_date;
    return;
  end if;

  with ordered as (
    select
      occurred_at,
      row_number() over (order by occurred_at) as rn
    from attendance_events
    where staff_id = p_staff_id and event_date = p_event_date
  ),
  closed_intervals as (
    select
      a.rn as rn,
      a.occurred_at as start_at,
      b.occurred_at as end_at
    from ordered a
    join ordered b on b.rn = a.rn + 1
  )
  insert into attendance_intervals (staff_id, event_date, kind, start_at, end_at)
  select
    p_staff_id,
    p_event_date,
    case when mod(rn, 2) = 1 then 'work' else 'break' end,
    start_at,
    end_at
  from closed_intervals
  order by rn;

  if mod(total_count, 2) = 1 then
    insert into attendance_intervals (staff_id, event_date, kind, start_at, end_at)
    values (p_staff_id, p_event_date, 'work', last_punch, null);
  end if;

  day_id := p_staff_id || '-' || to_char(p_event_date, 'YYYY-MM-DD');

  insert into attendance_records (id, staff_id, date, clock_in, clock_out, updated_at)
  values (
    day_id,
    p_staff_id,
    p_event_date,
    first_punch,
    case when mod(total_count, 2) = 0 then last_punch else null end,
    now()
  )
  on conflict (id) do update set
    staff_id = excluded.staff_id,
    date = excluded.date,
    clock_in = excluded.clock_in,
    clock_out = excluded.clock_out,
    updated_at = now();
end;
$$;

create or replace function shab_attendance_events_after_change()
returns trigger
language plpgsql
as $$
begin
  if tg_op = 'DELETE' then
    perform shab_rebuild_attendance_day(old.staff_id, old.event_date);
    return old;
  end if;

  if tg_op = 'UPDATE' then
    if old.staff_id is distinct from new.staff_id or old.event_date is distinct from new.event_date then
      perform shab_rebuild_attendance_day(old.staff_id, old.event_date);
    end if;
  end if;

  perform shab_rebuild_attendance_day(new.staff_id, new.event_date);
  return new;
end;
$$;

drop trigger if exists attendance_events_after_change on attendance_events;
create trigger attendance_events_after_change
after insert or update or delete on attendance_events
for each row
execute function shab_attendance_events_after_change();

create table if not exists units (
  id text primary key,
  name text,
  unit_type text,
  conversion_base text,
  conversion_rate numeric,
  symbol text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists products (
  id text primary key,
  name text,
  variant text,
  category text,
  size numeric,
  unit text,
  version text,
  status text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists raw_materials (
  id text primary key,
  name text,
  inci_name text,
  spec_grade text,
  unit text,
  shelf_life_months integer,
  storage_condition text,
  status text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists bom (
  id text primary key,
  product_id text,
  raw_material_id text,
  raw_material_name text,
  quantity numeric,
  unit text,
  stage text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

alter table customers add column if not exists name text;
alter table customers add column if not exists phone text;
alter table customers add column if not exists address text;

create table if not exists purchase_orders (
  id text primary key,
  supplier_id text references suppliers(id),
  ordered_at timestamptz not null,
  expected_at timestamptz,
  status text,
  notes text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists purchase_order_lines (
  id text primary key,
  purchase_order_id text not null references purchase_orders(id) on delete cascade,
  raw_material_id text not null references raw_materials(id),
  unit_id text references units(id),
  quantity numeric not null,
  unit_price numeric,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists goods_receipts (
  id text primary key,
  purchase_order_id text references purchase_orders(id) on delete set null,
  received_at timestamptz not null,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists goods_receipt_lines (
  id text primary key,
  goods_receipt_id text not null references goods_receipts(id) on delete cascade,
  raw_material_id text not null references raw_materials(id),
  unit_id text references units(id),
  quantity numeric not null,
  batch_no text,
  expiry_date date,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists stock_lots (
  id text primary key,
  raw_material_id text not null references raw_materials(id),
  unit_id text references units(id),
  received_at timestamptz not null,
  batch_no text,
  expiry_date date,
  quantity_on_hand numeric not null,
  source_receipt_id text references goods_receipts(id) on delete set null,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists stock_movements (
  id text primary key,
  at timestamptz not null,
  type text not null,
  raw_material_id text not null references raw_materials(id),
  lot_id text references stock_lots(id) on delete set null,
  quantity_delta numeric not null,
  reference_id text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists work_orders (
  id text primary key,
  product_id text not null references products(id),
  quantity numeric not null,
  status text not null,
  created_at timestamptz not null,
  started_at timestamptz,
  completed_at timestamptz,
  updated_at timestamptz default now()
);

create table if not exists product_lots (
  id text primary key,
  product_id text not null references products(id),
  unit_id text references units(id),
  produced_at timestamptz not null,
  batch_no text,
  expiry_date date,
  quantity_on_hand numeric not null,
  source_work_order_id text references work_orders(id) on delete set null,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists product_movements (
  id text primary key,
  at timestamptz not null,
  type text not null,
  product_id text not null references products(id),
  lot_id text references product_lots(id) on delete set null,
  quantity_delta numeric not null,
  reference_id text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists sales_orders (
  id text primary key,
  customer_id text references customers(id),
  ordered_at timestamptz not null,
  status text not null,
  notes text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists sales_order_lines (
  id text primary key,
  sales_order_id text not null references sales_orders(id) on delete cascade,
  product_id text not null references products(id),
  unit_id text references units(id),
  quantity numeric not null,
  unit_price numeric,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists delivery_notes (
  id text primary key,
  sales_order_id text references sales_orders(id) on delete set null,
  shipped_at timestamptz not null,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists delivery_note_lines (
  id text primary key,
  delivery_note_id text not null references delivery_notes(id) on delete cascade,
  product_id text not null references products(id),
  unit_id text references units(id),
  quantity numeric not null,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists invoices (
  id text primary key,
  sales_order_id text references sales_orders(id) on delete set null,
  invoiced_at timestamptz not null,
  total_amount numeric,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

alter table customers enable row level security;
alter table suppliers enable row level security;
alter table staff enable row level security;
alter table units enable row level security;
alter table products enable row level security;
alter table raw_materials enable row level security;
alter table bom enable row level security;
alter table purchase_orders enable row level security;
alter table purchase_order_lines enable row level security;
alter table goods_receipts enable row level security;
alter table goods_receipt_lines enable row level security;
alter table stock_lots enable row level security;
alter table stock_movements enable row level security;
alter table work_orders enable row level security;
alter table product_lots enable row level security;
alter table product_movements enable row level security;
alter table sales_orders enable row level security;
alter table sales_order_lines enable row level security;
alter table delivery_notes enable row level security;
alter table delivery_note_lines enable row level security;
alter table invoices enable row level security;

drop policy if exists "Enable all for customers" on customers;
create policy "Enable all for customers" on customers for all using (true) with check (true);
drop policy if exists "Enable all for suppliers" on suppliers;
create policy "Enable all for suppliers" on suppliers for all using (true) with check (true);
drop policy if exists "Enable all for staff" on staff;
create policy "Enable all for staff" on staff for all using (true) with check (true);
drop policy if exists "Enable all for units" on units;
create policy "Enable all for units" on units for all using (true) with check (true);
drop policy if exists "Enable all for products" on products;
create policy "Enable all for products" on products for all using (true) with check (true);
drop policy if exists "Enable all for raw_materials" on raw_materials;
create policy "Enable all for raw_materials" on raw_materials for all using (true) with check (true);
drop policy if exists "Enable all for bom" on bom;
create policy "Enable all for bom" on bom for all using (true) with check (true);
drop policy if exists "Enable all for purchase_orders" on purchase_orders;
create policy "Enable all for purchase_orders" on purchase_orders for all using (true) with check (true);
drop policy if exists "Enable all for purchase_order_lines" on purchase_order_lines;
create policy "Enable all for purchase_order_lines" on purchase_order_lines for all using (true) with check (true);
drop policy if exists "Enable all for goods_receipts" on goods_receipts;
create policy "Enable all for goods_receipts" on goods_receipts for all using (true) with check (true);
drop policy if exists "Enable all for goods_receipt_lines" on goods_receipt_lines;
create policy "Enable all for goods_receipt_lines" on goods_receipt_lines for all using (true) with check (true);
drop policy if exists "Enable all for stock_lots" on stock_lots;
create policy "Enable all for stock_lots" on stock_lots for all using (true) with check (true);
drop policy if exists "Enable all for stock_movements" on stock_movements;
create policy "Enable all for stock_movements" on stock_movements for all using (true) with check (true);
drop policy if exists "Enable all for work_orders" on work_orders;
create policy "Enable all for work_orders" on work_orders for all using (true) with check (true);
drop policy if exists "Enable all for product_lots" on product_lots;
create policy "Enable all for product_lots" on product_lots for all using (true) with check (true);
drop policy if exists "Enable all for product_movements" on product_movements;
create policy "Enable all for product_movements" on product_movements for all using (true) with check (true);
drop policy if exists "Enable all for sales_orders" on sales_orders;
create policy "Enable all for sales_orders" on sales_orders for all using (true) with check (true);
drop policy if exists "Enable all for sales_order_lines" on sales_order_lines;
create policy "Enable all for sales_order_lines" on sales_order_lines for all using (true) with check (true);
drop policy if exists "Enable all for delivery_notes" on delivery_notes;
create policy "Enable all for delivery_notes" on delivery_notes for all using (true) with check (true);
drop policy if exists "Enable all for delivery_note_lines" on delivery_note_lines;
create policy "Enable all for delivery_note_lines" on delivery_note_lines for all using (true) with check (true);
drop policy if exists "Enable all for invoices" on invoices;
create policy "Enable all for invoices" on invoices for all using (true) with check (true);

insert into staff (id, full_name, email, role, department, username, password_hash, status, date_joined, last_login)
values
  ('SA0001', 'Superadmin', 'superadmin@company.com', 'superadmin', 'Management', 'superadmin', 'abcd1234', 'active', '2026-02-01T09:00:00Z', null),
  ('MG0001', 'Manager', 'manager@company.com', 'manager', 'Management', 'manager', 'abcd1234', 'active', '2026-02-01T09:00:00Z', null),
  ('OP0001', 'Operations', 'operations@company.com', 'operations', 'Operations', 'operations', 'abcd1234', 'active', '2026-02-01T09:00:00Z', null),
  ('PR0001', 'Procurement', 'procurement@company.com', 'procurement', 'Procurement', 'procurement', 'abcd1234', 'active', '2026-02-01T09:00:00Z', null),
  ('SL0001', 'Sales', 'sales@company.com', 'sales', 'Sales', 'sales', 'abcd1234', 'active', '2026-02-01T09:00:00Z', null)
on conflict (id) do update set
  full_name = excluded.full_name,
  email = excluded.email,
  role = excluded.role,
  department = excluded.department,
  username = excluded.username,
  password_hash = excluded.password_hash,
  status = excluded.status,
  date_joined = excluded.date_joined,
  last_login = excluded.last_login,
  updated_at = now();

insert into suppliers (id, name, country, contact_person, email, phone, lead_time_days, status)
values ('SUP-DEMO-001', 'Demo Supplier', 'MY', 'Demo Contact', 'supplier@example.com', '+60110000000', 7, 'active')
on conflict (id) do update set
  name = excluded.name,
  country = excluded.country,
  contact_person = excluded.contact_person,
  email = excluded.email,
  phone = excluded.phone,
  lead_time_days = excluded.lead_time_days,
  status = excluded.status,
  updated_at = now();

insert into customers (id, name, phone, email, address)
values ('CUST-DEMO-001', 'Demo Customer', '+60112223333', 'customer@example.com', 'Kuala Lumpur')
on conflict (id) do update set
  name = excluded.name,
  phone = excluded.phone,
  email = excluded.email,
  address = excluded.address,
  updated_at = now();

insert into units (id, name, unit_type, conversion_base, conversion_rate, symbol)
values
  ('kg', 'Kilogram', 'weight', 'kg', 1, 'kg'),
  ('pcs', 'Pieces', 'count', 'pcs', 1, 'pcs')
on conflict (id) do update set
  name = excluded.name,
  unit_type = excluded.unit_type,
  conversion_base = excluded.conversion_base,
  conversion_rate = excluded.conversion_rate,
  symbol = excluded.symbol,
  updated_at = now();

insert into raw_materials (id, name, inci_name, spec_grade, unit, shelf_life_months, storage_condition, status)
values
  ('RM-DEMO-ETHANOL', 'Ethanol', 'Alcohol', 'USP', 'kg', 24, 'Cool dry place', 'active'),
  ('RM-DEMO-GLYCERIN', 'Glycerin', 'Glycerin', 'USP', 'kg', 36, 'Cool dry place', 'active')
on conflict (id) do update set
  name = excluded.name,
  inci_name = excluded.inci_name,
  spec_grade = excluded.spec_grade,
  unit = excluded.unit,
  shelf_life_months = excluded.shelf_life_months,
  storage_condition = excluded.storage_condition,
  status = excluded.status,
  updated_at = now();

insert into products (id, name, variant, category, size, unit, version, status)
values ('PRD-DEMO-SHAB-001', 'SHAB Demo Product', 'Default', 'Demo', 1, 'pcs', 'v1', 'active')
on conflict (id) do update set
  name = excluded.name,
  variant = excluded.variant,
  category = excluded.category,
  size = excluded.size,
  unit = excluded.unit,
  version = excluded.version,
  status = excluded.status,
  updated_at = now();

insert into bom (id, product_id, raw_material_id, raw_material_name, quantity, unit, stage)
values
  ('BOM-DEMO-001', 'PRD-DEMO-SHAB-001', 'RM-DEMO-ETHANOL', 'Ethanol', 0.6, 'kg', 'mix'),
  ('BOM-DEMO-002', 'PRD-DEMO-SHAB-001', 'RM-DEMO-GLYCERIN', 'Glycerin', 0.2, 'kg', 'mix')
on conflict (id) do update set
  product_id = excluded.product_id,
  raw_material_id = excluded.raw_material_id,
  raw_material_name = excluded.raw_material_name,
  quantity = excluded.quantity,
  unit = excluded.unit,
  stage = excluded.stage,
  updated_at = now();

insert into purchase_orders (id, supplier_id, ordered_at, expected_at, status, notes)
values ('PO-DEMO-1001', 'SUP-DEMO-001', '2026-03-01T09:00:00Z', '2026-03-05T09:00:00Z', 'received', 'Demo PO for seeding')
on conflict (id) do update set
  supplier_id = excluded.supplier_id,
  ordered_at = excluded.ordered_at,
  expected_at = excluded.expected_at,
  status = excluded.status,
  notes = excluded.notes,
  updated_at = now();

delete from purchase_order_lines where purchase_order_id = 'PO-DEMO-1001';
insert into purchase_order_lines (id, purchase_order_id, raw_material_id, unit_id, quantity, unit_price)
values
  ('POL-DEMO-1001-1', 'PO-DEMO-1001', 'RM-DEMO-ETHANOL', 'kg', 100, 10),
  ('POL-DEMO-1001-2', 'PO-DEMO-1001', 'RM-DEMO-GLYCERIN', 'kg', 50, 8);

insert into goods_receipts (id, purchase_order_id, received_at)
values ('GR-DEMO-2001', 'PO-DEMO-1001', '2026-03-02T10:00:00Z')
on conflict (id) do update set
  purchase_order_id = excluded.purchase_order_id,
  received_at = excluded.received_at,
  updated_at = now();

delete from goods_receipt_lines where goods_receipt_id = 'GR-DEMO-2001';
insert into goods_receipt_lines (id, goods_receipt_id, raw_material_id, unit_id, quantity, batch_no, expiry_date)
values
  ('GRL-DEMO-2001-1', 'GR-DEMO-2001', 'RM-DEMO-ETHANOL', 'kg', 100, 'ETH-01', '2028-03-01'),
  ('GRL-DEMO-2001-2', 'GR-DEMO-2001', 'RM-DEMO-GLYCERIN', 'kg', 50, 'GLY-01', '2029-03-01');

insert into stock_lots (id, raw_material_id, unit_id, received_at, batch_no, expiry_date, quantity_on_hand, source_receipt_id)
values
  ('LOT-RM-ETH-01', 'RM-DEMO-ETHANOL', 'kg', '2026-03-02T10:00:00Z', 'ETH-01', '2028-03-01', 40, 'GR-DEMO-2001'),
  ('LOT-RM-GLY-01', 'RM-DEMO-GLYCERIN', 'kg', '2026-03-02T10:00:00Z', 'GLY-01', '2029-03-01', 30, 'GR-DEMO-2001')
on conflict (id) do update set
  raw_material_id = excluded.raw_material_id,
  unit_id = excluded.unit_id,
  received_at = excluded.received_at,
  batch_no = excluded.batch_no,
  expiry_date = excluded.expiry_date,
  quantity_on_hand = excluded.quantity_on_hand,
  source_receipt_id = excluded.source_receipt_id,
  updated_at = now();

insert into stock_movements (id, at, type, raw_material_id, lot_id, quantity_delta, reference_id)
values
  ('SM-DEMO-RCV-ETH', '2026-03-02T10:00:00Z', 'receive', 'RM-DEMO-ETHANOL', 'LOT-RM-ETH-01', 100, 'GR-DEMO-2001'),
  ('SM-DEMO-RCV-GLY', '2026-03-02T10:00:00Z', 'receive', 'RM-DEMO-GLYCERIN', 'LOT-RM-GLY-01', 50, 'GR-DEMO-2001'),
  ('SM-DEMO-ISS-ETH', '2026-03-03T09:00:00Z', 'issue', 'RM-DEMO-ETHANOL', null, -60, 'WO-DEMO-3001'),
  ('SM-DEMO-ISS-GLY', '2026-03-03T09:00:00Z', 'issue', 'RM-DEMO-GLYCERIN', null, -20, 'WO-DEMO-3001')
on conflict (id) do update set
  at = excluded.at,
  type = excluded.type,
  raw_material_id = excluded.raw_material_id,
  lot_id = excluded.lot_id,
  quantity_delta = excluded.quantity_delta,
  reference_id = excluded.reference_id,
  updated_at = now();

insert into work_orders (id, product_id, quantity, status, created_at, started_at, completed_at)
values ('WO-DEMO-3001', 'PRD-DEMO-SHAB-001', 100, 'completed', '2026-03-03T08:00:00Z', '2026-03-03T08:30:00Z', '2026-03-03T12:00:00Z')
on conflict (id) do update set
  product_id = excluded.product_id,
  quantity = excluded.quantity,
  status = excluded.status,
  created_at = excluded.created_at,
  started_at = excluded.started_at,
  completed_at = excluded.completed_at,
  updated_at = now();

insert into product_lots (id, product_id, unit_id, produced_at, batch_no, expiry_date, quantity_on_hand, source_work_order_id)
values ('LOT-FG-01', 'PRD-DEMO-SHAB-001', 'pcs', '2026-03-03T12:00:00Z', 'FG-01', '2027-03-01', 70, 'WO-DEMO-3001')
on conflict (id) do update set
  product_id = excluded.product_id,
  unit_id = excluded.unit_id,
  produced_at = excluded.produced_at,
  batch_no = excluded.batch_no,
  expiry_date = excluded.expiry_date,
  quantity_on_hand = excluded.quantity_on_hand,
  source_work_order_id = excluded.source_work_order_id,
  updated_at = now();

insert into product_movements (id, at, type, product_id, lot_id, quantity_delta, reference_id)
values
  ('PM-DEMO-PROD-01', '2026-03-03T12:00:00Z', 'produce', 'PRD-DEMO-SHAB-001', 'LOT-FG-01', 100, 'WO-DEMO-3001'),
  ('PM-DEMO-SHIP-01', '2026-03-04T11:00:00Z', 'ship', 'PRD-DEMO-SHAB-001', null, -30, 'DN-DEMO-6001')
on conflict (id) do update set
  at = excluded.at,
  type = excluded.type,
  product_id = excluded.product_id,
  lot_id = excluded.lot_id,
  quantity_delta = excluded.quantity_delta,
  reference_id = excluded.reference_id,
  updated_at = now();

insert into sales_orders (id, customer_id, ordered_at, status, notes)
values ('SO-DEMO-5001', 'CUST-DEMO-001', '2026-03-04T09:00:00Z', 'fulfilled', 'Demo SO for seeding')
on conflict (id) do update set
  customer_id = excluded.customer_id,
  ordered_at = excluded.ordered_at,
  status = excluded.status,
  notes = excluded.notes,
  updated_at = now();

delete from sales_order_lines where sales_order_id = 'SO-DEMO-5001';
insert into sales_order_lines (id, sales_order_id, product_id, unit_id, quantity, unit_price)
values ('SOL-DEMO-5001-1', 'SO-DEMO-5001', 'PRD-DEMO-SHAB-001', 'pcs', 30, 25);

insert into delivery_notes (id, sales_order_id, shipped_at)
values ('DN-DEMO-6001', 'SO-DEMO-5001', '2026-03-04T11:00:00Z')
on conflict (id) do update set
  sales_order_id = excluded.sales_order_id,
  shipped_at = excluded.shipped_at,
  updated_at = now();

delete from delivery_note_lines where delivery_note_id = 'DN-DEMO-6001';
insert into delivery_note_lines (id, delivery_note_id, product_id, unit_id, quantity)
values ('DNL-DEMO-6001-1', 'DN-DEMO-6001', 'PRD-DEMO-SHAB-001', 'pcs', 30);

insert into invoices (id, sales_order_id, invoiced_at, total_amount)
values ('INV-DEMO-7001', 'SO-DEMO-5001', '2026-03-04T12:00:00Z', 750)
on conflict (id) do update set
  sales_order_id = excluded.sales_order_id,
  invoiced_at = excluded.invoiced_at,
  total_amount = excluded.total_amount,
  updated_at = now();
