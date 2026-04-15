create extension if not exists "uuid-ossp";

create table if not exists public.attendance_events (
  id text primary key default uuid_generate_v4()::text,
  staff_id text not null references public.staff(id) on delete cascade,
  device_id text not null,
  occurred_at timestamptz not null,
  event_date date not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists attendance_events_unique_punch on public.attendance_events (staff_id, occurred_at, device_id);
create index if not exists attendance_events_staff_date_time_idx on public.attendance_events (staff_id, event_date, occurred_at);

create table if not exists public.attendance_intervals (
  id text primary key default uuid_generate_v4()::text,
  staff_id text not null references public.staff(id) on delete cascade,
  event_date date not null,
  kind text not null check (kind in ('work', 'break')),
  start_at timestamptz not null,
  end_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists attendance_intervals_staff_date_idx on public.attendance_intervals (staff_id, event_date);

create table if not exists public.attendance_records (
  id text primary key,
  staff_id text not null references public.staff(id) on delete cascade,
  date date not null,
  clock_in timestamptz not null,
  clock_out timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists attendance_records_staff_date_key on public.attendance_records (staff_id, date);
create index if not exists attendance_records_staff_date_idx on public.attendance_records (staff_id, date);

create or replace function public.shab_set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create or replace function public.shab_attendance_events_fill_event_date()
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

drop trigger if exists attendance_events_fill_event_date on public.attendance_events;
create trigger attendance_events_fill_event_date
before insert or update on public.attendance_events
for each row
execute function public.shab_attendance_events_fill_event_date();

drop trigger if exists attendance_events_set_updated_at on public.attendance_events;
create trigger attendance_events_set_updated_at
before update on public.attendance_events
for each row
execute function public.shab_set_updated_at();

drop trigger if exists attendance_intervals_set_updated_at on public.attendance_intervals;
create trigger attendance_intervals_set_updated_at
before update on public.attendance_intervals
for each row
execute function public.shab_set_updated_at();

drop trigger if exists attendance_records_set_updated_at on public.attendance_records;
create trigger attendance_records_set_updated_at
before update on public.attendance_records
for each row
execute function public.shab_set_updated_at();

create or replace function public.shab_rebuild_attendance_day(p_staff_id text, p_event_date date)
returns void
language plpgsql
as $$
declare
  total_count integer;
  day_id text;
  first_punch timestamptz;
  last_punch timestamptz;
begin
  delete from public.attendance_intervals where staff_id = p_staff_id and event_date = p_event_date;

  select count(*), min(occurred_at), max(occurred_at)
  into total_count, first_punch, last_punch
  from public.attendance_events
  where staff_id = p_staff_id and event_date = p_event_date;

  if total_count is null or total_count = 0 then
    delete from public.attendance_records where staff_id = p_staff_id and date = p_event_date;
    return;
  end if;

  with ordered as (
    select
      occurred_at,
      row_number() over (order by occurred_at) as rn
    from public.attendance_events
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
  insert into public.attendance_intervals (staff_id, event_date, kind, start_at, end_at)
  select
    p_staff_id,
    p_event_date,
    case when mod(rn, 2) = 1 then 'work' else 'break' end,
    start_at,
    end_at
  from closed_intervals
  order by rn;

  if mod(total_count, 2) = 1 then
    insert into public.attendance_intervals (staff_id, event_date, kind, start_at, end_at)
    values (p_staff_id, p_event_date, 'work', last_punch, null);
  end if;

  day_id := p_staff_id || '-' || to_char(p_event_date, 'YYYY-MM-DD');

  insert into public.attendance_records (id, staff_id, date, clock_in, clock_out, updated_at)
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

create or replace function public.shab_attendance_events_after_change()
returns trigger
language plpgsql
as $$
begin
  if tg_op = 'DELETE' then
    perform public.shab_rebuild_attendance_day(old.staff_id, old.event_date);
    return old;
  end if;

  if tg_op = 'UPDATE' then
    if old.staff_id is distinct from new.staff_id or old.event_date is distinct from new.event_date then
      perform public.shab_rebuild_attendance_day(old.staff_id, old.event_date);
    end if;
  end if;

  perform public.shab_rebuild_attendance_day(new.staff_id, new.event_date);
  return new;
end;
$$;

drop trigger if exists attendance_events_after_change on public.attendance_events;
create trigger attendance_events_after_change
after insert or update or delete on public.attendance_events
for each row
execute function public.shab_attendance_events_after_change();
