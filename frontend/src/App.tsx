import { Routes, Route, Navigate } from "react-router-dom";
import { useAuth } from "@clerk/clerk-react";
import AppLayout from "@/components/layout/AppLayout";
import ErrorBoundary from "@/components/ui/error-boundary";
import SignInPage from "@/pages/SignInPage";
import SignUpPage from "@/pages/SignUpPage";
import ChatPage from "@/pages/ChatPage";
import ConnectionsPage from "@/pages/ConnectionsPage";
import FilesPage from "@/pages/FilesPage";
import ReportsPage from "@/pages/ReportsPage";
import MetricsPage from "@/pages/MetricsPage";
import OnboardingPage from "@/pages/OnboardingPage";
import AdminPage from "@/pages/AdminPage";
import OrgSettingsPage from "@/pages/OrgSettingsPage";
import AuditPage from "@/pages/AuditPage";
import NotebooksPage from "@/pages/NotebooksPage";
import NotebookEditorPage from "@/pages/NotebookEditorPage";

const clerkEnabled = !!import.meta.env.VITE_CLERK_PUBLISHABLE_KEY;

function ClerkRootRedirect() {
  const { isSignedIn, isLoaded } = useAuth();
  if (!isLoaded) return null;
  return isSignedIn ? <Navigate to="/chat" replace /> : <Navigate to="/sign-in" replace />;
}

function RootRedirect() {
  if (!clerkEnabled) return <Navigate to="/chat" replace />;
  return <ClerkRootRedirect />;
}

export default function App() {
  return (
    <ErrorBoundary>
      <Routes>
        <Route path="/" element={<RootRedirect />} />
        <Route path="/sign-in/*" element={<SignInPage />} />
        <Route path="/sign-up/*" element={<SignUpPage />} />
        <Route element={<AppLayout />}>
          <Route path="/chat" element={<ChatPage />} />
          <Route path="/chat/:conversationId" element={<ChatPage />} />
          <Route path="/connections" element={<ConnectionsPage />} />
          <Route path="/reports" element={<ReportsPage />} />
          <Route path="/metrics" element={<MetricsPage />} />
          <Route path="/files" element={<FilesPage />} />
          <Route path="/setup" element={<OnboardingPage />} />
          <Route path="/org-settings" element={<OrgSettingsPage />} />
          <Route path="/admin" element={<AdminPage />} />
          <Route path="/audit" element={<AuditPage />} />
          <Route path="/notebooks" element={<NotebooksPage />} />
          <Route path="/notebooks/:notebookId" element={<NotebookEditorPage />} />
        </Route>
      </Routes>
    </ErrorBoundary>
  );
}
