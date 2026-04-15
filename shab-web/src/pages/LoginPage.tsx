import { useEffect, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { BarChart3, Eye, EyeOff, Factory, Lock, Shield, ShoppingCart, User, Users, Wallet, Warehouse } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useAuth } from "@/contexts/useAuth";

export default function LoginPage() {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState("");
  const { login, user, isAuthenticated } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();

  useEffect(() => {
    if (isAuthenticated && user) {
      const fromPath = (location.state as { from?: Location } | undefined)?.from?.pathname;
      const targetPath = fromPath ?? (user.role === "superadmin" ? "/superadmin" : "/manager");
      if (location.pathname !== targetPath) {
        const timer = setTimeout(() => {
          navigate(targetPath, { replace: true });
        }, 0);
        return () => clearTimeout(timer);
      }
    }
  }, [isAuthenticated, user, navigate, location.pathname, location.state]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setError("");

    void (async () => {
      const ok = await login(username, password);
      if (!ok) setError("Invalid username or password");
    })();
  };

  return (
    <div className="min-h-screen bg-[hsl(220,20%,8%)] text-foreground font-sans selection:bg-[hsl(45,93%,47%)] selection:text-[hsl(220,26%,14%)]">
      <main className="min-h-screen bg-[hsl(220,20%,8%)] px-4 py-12">
        <div className="mx-auto grid w-full max-w-6xl items-center gap-10 lg:grid-cols-2">
          <div className="space-y-6">
            <div className="inline-flex items-center gap-3">
              <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-gradient-to-br from-[hsl(45,93%,47%)] to-[hsl(45,93%,58%)]">
                <Shield className="h-6 w-6 text-[hsl(220,26%,14%)]" />
              </div>
              <div>
                <div className="text-sm font-semibold tracking-wide text-[hsl(45,93%,47%)]">S.H.A.B</div>
                <div className="text-lg font-bold text-[hsl(0,0%,98%)]">ERP Management System</div>
              </div>
            </div>

            <h1 className="text-3xl font-bold leading-tight text-[hsl(0,0%,98%)] lg:text-4xl">
              Run operations with clarity, control, and accountability
            </h1>

            <p className="max-w-xl text-[hsl(220,10%,65%)]">
              SHAB ERP centralizes your core workflows into one secure platform. Track inventory, manage production, streamline sales,
              and support finance and Human Resource with a permission-driven interface configured by your Superadmin.
            </p>

            <div className="grid gap-3 sm:grid-cols-2">
              <div className="flex items-center gap-3 rounded-2xl border border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)] p-4">
                <Warehouse className="h-5 w-5 text-[hsl(45,93%,47%)]" />
                <div>
                  <div className="font-semibold text-[hsl(0,0%,98%)]">Inventory</div>
                  <div className="text-sm text-[hsl(220,10%,60%)]">Materials, lots, movements</div>
                </div>
              </div>
              <div className="flex items-center gap-3 rounded-2xl border border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)] p-4">
                <Factory className="h-5 w-5 text-[hsl(45,93%,47%)]" />
                <div>
                  <div className="font-semibold text-[hsl(0,0%,98%)]">Production</div>
                  <div className="text-sm text-[hsl(220,10%,60%)]">Work orders, outputs</div>
                </div>
              </div>
              <div className="flex items-center gap-3 rounded-2xl border border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)] p-4">
                <ShoppingCart className="h-5 w-5 text-[hsl(45,93%,47%)]" />
                <div>
                  <div className="font-semibold text-[hsl(0,0%,98%)]">Sales</div>
                  <div className="text-sm text-[hsl(220,10%,60%)]">Orders, delivery, invoices</div>
                </div>
              </div>
              <div className="flex items-center gap-3 rounded-2xl border border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)] p-4">
                <Wallet className="h-5 w-5 text-[hsl(45,93%,47%)]" />
                <div>
                  <div className="font-semibold text-[hsl(0,0%,98%)]">Finance</div>
                  <div className="text-sm text-[hsl(220,10%,60%)]">Billing and reporting</div>
                </div>
              </div>
              <div className="flex items-center gap-3 rounded-2xl border border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)] p-4">
                <Users className="h-5 w-5 text-[hsl(45,93%,47%)]" />
                <div>
                  <div className="font-semibold text-[hsl(0,0%,98%)]">Human Resource</div>
                  <div className="text-sm text-[hsl(220,10%,60%)]">Staff access and administration</div>
                </div>
              </div>
              <div className="flex items-center gap-3 rounded-2xl border border-[hsl(220,20%,18%)] bg-[hsl(220,20%,10%)] p-4">
                <BarChart3 className="h-5 w-5 text-[hsl(45,93%,47%)]" />
                <div>
                  <div className="font-semibold text-[hsl(0,0%,98%)]">Analytics</div>
                  <div className="text-sm text-[hsl(220,10%,60%)]">Dashboards and insights</div>
                </div>
              </div>
            </div>
          </div>

          <div className="w-full max-w-md justify-self-center">
            <div className="rounded-3xl border border-[hsl(220,20%,20%)] bg-[hsl(220,20%,12%)] p-8 shadow-2xl">
              <div className="mb-8">
                <h2 className="text-2xl font-bold text-[hsl(0,0%,98%)]">Sign In</h2>
                <p className="mt-1 text-sm text-[hsl(220,10%,60%)]">Use your Staff username (or Staff ID) and password.</p>
              </div>

              <form onSubmit={handleSubmit} className="space-y-6">
              <div className="space-y-2">
                <Label htmlFor="username" className="text-[hsl(220,10%,80%)]">
                  Username or Staff ID
                </Label>
                <div className="relative group">
                  <User className="absolute left-3 top-3 h-5 w-5 text-[hsl(220,10%,50%)] group-focus-within:text-[hsl(45,93%,47%)] transition-colors" />
                  <Input
                    id="username"
                    type="text"
                    placeholder="Enter your username or staff ID"
                    value={username}
                    onChange={(e) => setUsername(e.target.value)}
                    className="pl-10 bg-[hsl(220,20%,15%)] border-[hsl(220,20%,25%)] text-[hsl(0,0%,98%)] placeholder:text-[hsl(220,10%,40%)] focus:border-[hsl(45,93%,47%)] transition-all"
                    required
                  />
                </div>
              </div>

              <div className="space-y-2">
                <Label htmlFor="password" className="text-[hsl(220,10%,80%)]">
                  Password
                </Label>
                <div className="relative group">
                  <Lock className="absolute left-3 top-3 h-5 w-5 text-[hsl(220,10%,50%)] group-focus-within:text-[hsl(45,93%,47%)] transition-colors" />
                  <Input
                    id="password"
                    type={showPassword ? "text" : "password"}
                    placeholder="Enter your password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    className="pl-10 pr-10 bg-[hsl(220,20%,15%)] border-[hsl(220,20%,25%)] text-[hsl(0,0%,98%)] placeholder:text-[hsl(220,10%,40%)] focus:border-[hsl(45,93%,47%)] transition-all"
                    required
                  />
                  <button
                    type="button"
                    onClick={() => setShowPassword(!showPassword)}
                    className="absolute right-3 top-3 text-[hsl(220,10%,50%)] hover:text-[hsl(45,93%,47%)] transition-colors"
                  >
                    {showPassword ? (
                      <EyeOff className="h-5 w-5" />
                    ) : (
                      <Eye className="h-5 w-5" />
                    )}
                  </button>
                </div>
              </div>

              {error && (
                <div className="p-3 rounded-lg bg-red-500/10 border border-red-500/20 text-red-400 text-sm text-center">
                  {error}
                </div>
              )}

              <Button
                type="submit"
                className="w-full bg-gradient-to-r from-[hsl(45,93%,47%)] to-[hsl(45,93%,58%)] hover:from-[hsl(45,93%,50%)] hover:to-[hsl(45,93%,60%)] text-[hsl(220,26%,14%)] font-bold py-6 rounded-xl transition-all hover:scale-[1.02] shadow-lg shadow-[hsl(45,93%,47%)]/20"
              >
                Sign In
              </Button>
            </form>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
