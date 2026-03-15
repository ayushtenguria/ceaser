import { useEffect } from "react";
import { Outlet, useNavigate } from "react-router-dom";
import { useAuth } from "@clerk/clerk-react";
import { setAuthTokenGetter } from "@/lib/api";
import * as api from "@/lib/api";
import Sidebar from "@/components/layout/Sidebar";
import TopBar from "@/components/layout/TopBar";
import { useConnectionsStore } from "@/store/connections";

const clerkEnabled = !!import.meta.env.VITE_CLERK_PUBLISHABLE_KEY;

function AuthenticatedLayout() {
  const { setConnections } = useConnectionsStore();

  useEffect(() => {
    api.getConnections()
      .then(setConnections)
      .catch(() => {}); // Silent fail
  }, [setConnections]);

  return (
    <div className="flex h-screen overflow-hidden bg-background">
      <Sidebar />
      <div className="flex flex-1 flex-col overflow-hidden">
        <TopBar />
        <main className="flex-1 overflow-auto">
          <Outlet />
        </main>
      </div>
    </div>
  );
}

function ClerkAppLayout() {
  const { isSignedIn, isLoaded, getToken } = useAuth();
  const navigate = useNavigate();

  // Set the Clerk token getter for API calls
  useEffect(() => {
    if (isLoaded && isSignedIn) {
      setAuthTokenGetter(getToken);
    }
  }, [isLoaded, isSignedIn, getToken]);

  // Redirect unauthenticated users
  useEffect(() => {
    if (isLoaded && !isSignedIn) {
      navigate("/sign-in", { replace: true });
    }
  }, [isLoaded, isSignedIn, navigate]);

  if (!isLoaded) {
    return (
      <div className="flex h-screen items-center justify-center bg-background">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-primary border-t-transparent" />
      </div>
    );
  }

  if (!isSignedIn) {
    return null;
  }

  return <AuthenticatedLayout />;
}

export default function AppLayout() {
  if (!clerkEnabled) {
    // Dev mode: skip Clerk auth entirely
    return <AuthenticatedLayout />;
  }

  return <ClerkAppLayout />;
}
