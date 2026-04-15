import { useEffect } from "react";
import { Navigate, Route, Routes, useLocation } from "react-router-dom";

import { RequireAuth } from "@/auth/RequireAuth";
import { AuthProvider } from "@/contexts/AuthContext";
import { DataProvider } from "@/contexts/DataContext";
import { useAuth } from "@/contexts/useAuth";
import ManagerDashboard from "@/dashboards/ManagerDashboard";
import SuperadminDashboard from "@/dashboards/SuperadminDashboard";
import LoginPage from "@/pages/LoginPage";

// Restore scroll position on route change
function ScrollToTop() {
  const { pathname, hash } = useLocation();

  useEffect(() => {
    if (hash) {
      const element = document.getElementById(hash.substring(1));
      if (element) {
        element.scrollIntoView({ behavior: "smooth" });
      }
    } else {
      window.scrollTo(0, 0);
    }
  }, [pathname, hash]);

  return null;
}

function IndexRoute() {
  const { isAuthenticated, user } = useAuth();

  if (!isAuthenticated) return <LoginPage />;
  if (user?.role === "superadmin") return <Navigate to="/superadmin" replace />;
  return <Navigate to="/manager" replace />;
}

export default function App() {
  return (
    <DataProvider>
      <AuthProvider>
        <ScrollToTop />
        <Routes>
          <Route path="/" element={<IndexRoute />} />
          <Route path="/login" element={<LoginPage />} />
          <Route
            path="/manager"
            element={
              <RequireAuth>
                <ManagerDashboard />
              </RequireAuth>
            }
          />
          <Route
            path="/superadmin"
            element={
              <RequireAuth>
                <SuperadminDashboard />
              </RequireAuth>
            }
          />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </AuthProvider>
    </DataProvider>
  );
}
