create or replace function public.shab_set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create table if not exists public.staff (
  id text primary key,
  full_name text not null default '',
  role text not null default '',
  department text not null default '',
  status text not null default 'Active',
  date_joined date,
  shift_pattern text not null default 'Normal',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists staff_department_idx on public.staff (department);
create index if not exists staff_status_idx on public.staff (status);

drop trigger if exists staff_set_updated_at on public.staff;
create trigger staff_set_updated_at
before update on public.staff
for each row
execute function public.shab_set_updated_at();

create table if not exists public.shift_patterns (
  pattern text primary key,
  working_days text not null default '',
  working_hours text not null default '',
  break_time text not null default '',
  notes text not null default '',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists shift_patterns_set_updated_at on public.shift_patterns;
create trigger shift_patterns_set_updated_at
before update on public.shift_patterns
for each row
execute function public.shab_set_updated_at();

insert into public.shift_patterns (pattern, working_days, working_hours, break_time, notes)
values
  ('Normal', 'Mon–Fri', '09:00–18:00', '13:00–14:00', 'Default'),
  ('Shift 1', 'Mon–Sat', '08:00–16:00', '12:00–13:00', 'Default'),
  ('Shift 2', 'Mon–Sat', '16:00–00:00', '20:00–20:30', 'Default'),
  ('Shift 3', 'Mon–Sat', '00:00–08:00', '04:00–04:30', 'Default')
on conflict (pattern) do nothing;
