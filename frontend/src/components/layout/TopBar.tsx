import { useLocation } from "react-router-dom";
import { useOrganization } from "@clerk/clerk-react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { useChatStore } from "@/store/chat";
import { useConnectionsStore } from "@/store/connections";

const clerkEnabled = !!import.meta.env.VITE_CLERK_PUBLISHABLE_KEY;

const PAGE_TITLES: Record<string, string> = {
  "/chat": "Chat",
  "/connections": "Connections",
  "/files": "Files",
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
  const { selectedModel, setSelectedModel } = useChatStore();
  const { connections, activeConnectionId, setActiveConnection } =
    useConnectionsStore();

  const pageTitle = getPageTitle(location.pathname);
  const isChat = location.pathname.startsWith("/chat");

  return (
    <header className="flex h-14 items-center justify-between border-b bg-card px-6">
      <div className="flex items-center gap-3">
        <h1 className="text-lg font-semibold">{pageTitle}</h1>
        {clerkEnabled && <OrgBadge />}
      </div>

      {isChat && (
        <div className="flex items-center gap-3">
          {/* Connection selector */}
          <Select
            value={activeConnectionId || "none"}
            onValueChange={(value) =>
              setActiveConnection(value === "none" ? null : value)
            }
          >
            <SelectTrigger className="w-[200px]">
              <SelectValue placeholder="No connection" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="none">No connection</SelectItem>
              {connections.map((conn) => (
                <SelectItem key={conn.id} value={conn.id}>
                  <span className="flex items-center gap-2">
                    <span
                      className={`inline-block h-2 w-2 rounded-full ${
                        conn.isConnected ? "bg-emerald-500" : "bg-red-500"
                      }`}
                    />
                    {conn.name}
                  </span>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          {/* Model selector */}
          <Select
            value={selectedModel}
            onValueChange={(value) =>
              setSelectedModel(value as "gemini" | "claude")
            }
          >
            <SelectTrigger className="w-[140px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="gemini">Gemini</SelectItem>
              <SelectItem value="claude">Claude</SelectItem>
            </SelectContent>
          </Select>
        </div>
      )}
    </header>
  );
}
