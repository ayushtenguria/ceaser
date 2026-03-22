import { useEffect, useRef } from "react";
import { Outlet, useNavigate } from "react-router-dom";
import { useAuth, useUser } from "@clerk/clerk-react";
import { setAuthTokenGetter } from "@/lib/api";
import * as api from "@/lib/api";
import Sidebar from "@/components/layout/Sidebar";
import TopBar from "@/components/layout/TopBar";
import ToastContainer from "@/components/ui/toast-container";
import { useConnectionsStore } from "@/store/connections";

const clerkEnabled = !!import.meta.env.VITE_CLERK_PUBLISHABLE_KEY;

function AuthenticatedLayout() {
  const { setConnections, activeConnectionId, setActiveConnection, toggleConnectionId } = useConnectionsStore();

  useEffect(() => {
    api.getConnections()
      .then((conns) => {
        setConnections(conns);
        // Auto-select if user has exactly 1 connection and nothing selected
        if (conns.length === 1 && !activeConnectionId) {
          setActiveConnection(conns[0].id);
          toggleConnectionId(conns[0].id);
        }
      })
      .catch(() => {});
  }, [setConnections, activeConnectionId, setActiveConnection, toggleConnectionId]);

  return (
    <div className="flex h-screen overflow-hidden bg-background">
      <Sidebar />
      <div className="flex flex-1 flex-col overflow-hidden">
        <TopBar />
        <main className="flex-1 overflow-auto">
          <Outlet />
        </main>
      </div>
      <ToastContainer />
    </div>
  );
}

function ClerkAppLayout() {
  const { isSignedIn, isLoaded, getToken } = useAuth();
  const { user } = useUser();
  const navigate = useNavigate();
  const syncedRef = useRef(false);

  // Set the Clerk token getter for API calls
  useEffect(() => {
    if (isLoaded && isSignedIn) {
      setAuthTokenGetter(getToken);
    }
  }, [isLoaded, isSignedIn, getToken]);

  // Sync user to our DB after sign-in
  useEffect(() => {
    if (isLoaded && isSignedIn && user && !syncedRef.current) {
      syncedRef.current = true;
      const payload = {
        clerkId: user.id,
        email: user.primaryEmailAddress?.emailAddress || "",
        firstName: user.firstName || "",
        lastName: user.lastName || "",
        organizationId: null,
        imageUrl: user.imageUrl || null,
      };
      api.syncUser(payload).catch(() => {});
    }
  }, [isLoaded, isSignedIn, user]);

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
