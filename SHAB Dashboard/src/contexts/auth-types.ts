import type { Staff } from "./master-data-types";

export type AuthUser = Staff;

export interface AuthContextType {
  user: AuthUser | null;
  login: (username: string, password: string) => Promise<boolean>;
  logout: () => void;
  isAuthenticated: boolean;
}
