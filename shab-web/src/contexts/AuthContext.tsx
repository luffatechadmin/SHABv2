import { ReactNode, useCallback, useEffect, useMemo, useState } from "react";

import { AuthContext } from "./auth-context";
import type { AuthUser } from "./auth-types";
import { useData } from "./useData";
import { isSupabaseConfigured, supabase } from "@/lib/supabase";
import type { AppModule, Staff, UserRole } from "./master-data-types";
import { defaultModulesForRoles } from "./master-data-types";

const STORAGE_KEY = "shab:authUser:v1";

const safeParse = (raw: string | null): AuthUser | null => {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as AuthUser;
    if (!parsed || typeof parsed !== "object") return null;
    if (typeof parsed.id !== "string") return null;
    return parsed;
  } catch {
    return null;
  }
};

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
    const rawRole = String(value ?? "").trim().toLowerCase();
    if (!rawRole) return null;
    const mapped =
      rawRole === "operations" || rawRole === "staff"
        ? "operator"
        : rawRole === "procurement"
          ? "procurement_manager"
          : rawRole === "sales"
            ? "sales_manager"
            : rawRole === "manager"
              ? "production_manager"
              : rawRole;
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
  const roles: UserRole[] = Array.isArray(rawRoles) ? (rawRoles.map(toRole).filter(Boolean) as UserRole[]) : [];
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

export function AuthProvider({ children }: { children: ReactNode }) {
  const { staff, isLoading } = useData();
  const [user, setUser] = useState<AuthUser | null>(null);

  useEffect(() => {
    if (isLoading) return;
    const stored = safeParse(window.localStorage.getItem(STORAGE_KEY));
    if (!stored) return;
    const current = staff.find((s) => s.id === stored.id);
    if (current) {
      if (current.status !== "active") return;
      setUser(current);
      return;
    }
    if (stored.status !== "active") return;
    setUser(stored);
  }, [isLoading, staff]);

  const login = useCallback(async (username: string, password: string): Promise<boolean> => {
    const identifier = username.trim().toLowerCase();
    const found = staff.find(
      (s) =>
        (s.username.trim().toLowerCase() === identifier || s.id.trim().toLowerCase() === identifier) &&
        s.passwordHash === password &&
        s.status === "active",
    );

    if (found) {
      setUser(found);
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(found));
      return true;
    }

    if (!isSupabaseConfigured) return false;

    try {
      const rpcAttempt = await supabase.rpc("staff_login", { identifier, password });
      if (!rpcAttempt.error && rpcAttempt.data) {
        const normalized = normalizeStaff(rpcAttempt.data);
        if (normalized && normalized.status === "active") {
          setUser(normalized);
          window.localStorage.setItem(STORAGE_KEY, JSON.stringify(normalized));
          return true;
        }
      }
    } catch {
      // ignore
    }

    const res = await supabase
      .from("staff")
      .select("*")
      .or(`username.ilike.${identifier},id.ilike.${identifier}`)
      .limit(1)
      .maybeSingle();

    if (res.error || !res.data) return false;

    const normalized = normalizeStaff(res.data);
    if (!normalized || normalized.status !== "active") return false;
    if (normalized.passwordHash !== password) return false;

    setUser(normalized);
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(normalized));
    return true;
  }, [staff]);

  const logout = useCallback(() => {
    setUser(null);
    window.localStorage.removeItem(STORAGE_KEY);
    window.location.assign("/login");
  }, []);

  const value = useMemo(
    () => ({
      user,
      login,
      logout,
      isAuthenticated: !!user,
    }),
    [user, login, logout],
  );

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
}
