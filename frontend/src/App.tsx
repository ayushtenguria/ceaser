import { Routes, Route, Navigate } from "react-router-dom";
import { useAuth } from "@clerk/clerk-react";
import AppLayout from "@/components/layout/AppLayout";
import SignInPage from "@/pages/SignInPage";
import SignUpPage from "@/pages/SignUpPage";
import ChatPage from "@/pages/ChatPage";
import ConnectionsPage from "@/pages/ConnectionsPage";
import FilesPage from "@/pages/FilesPage";

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
    <Routes>
      <Route path="/" element={<RootRedirect />} />
      <Route path="/sign-in/*" element={<SignInPage />} />
      <Route path="/sign-up/*" element={<SignUpPage />} />
      <Route element={<AppLayout />}>
        <Route path="/chat" element={<ChatPage />} />
        <Route path="/chat/:conversationId" element={<ChatPage />} />
        <Route path="/connections" element={<ConnectionsPage />} />
        <Route path="/files" element={<FilesPage />} />
      </Route>
    </Routes>
  );
}
