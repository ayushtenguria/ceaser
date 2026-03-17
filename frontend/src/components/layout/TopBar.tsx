import { useLocation } from "react-router-dom";
import { useOrganization } from "@clerk/clerk-react";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";

const clerkEnabled = !!import.meta.env.VITE_CLERK_PUBLISHABLE_KEY;

const PAGE_TITLES: Record<string, string> = {
  "/chat": "Chat",
  "/connections": "Connections",
  "/files": "Files",
  "/reports": "Reports",
  "/metrics": "Business Metrics",
  "/setup": "Setup Guide",
  "/org-settings": "Organization Settings",
  "/audit": "Audit Log",
  "/admin": "Admin Dashboard",
};

function getPageTitle(pathname: string): string {
  for (const [prefix, title] of Object.entries(PAGE_TITLES)) {
    if (pathname.startsWith(prefix)) return title;
  }
  return "Ceaser";
}

function OrgBadge() {
  const { organization } = useOrganization();
  if (!organization) return null;
  return (
    <>
      <Separator orientation="vertical" className="h-5" />
      <Badge variant="secondary" className="text-xs">{organization.name}</Badge>
    </>
  );
}

export default function TopBar() {
  const location = useLocation();
  const pageTitle = getPageTitle(location.pathname);

  return (
    <header className="flex h-12 items-center justify-between border-b bg-card px-6">
      <div className="flex items-center gap-3">
        <h1 className="text-base font-semibold">{pageTitle}</h1>
        {clerkEnabled && <OrgBadge />}
      </div>
    </header>
  );
}
