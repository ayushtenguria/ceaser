import { useCallback, useState, useEffect } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { UserButton } from "@clerk/clerk-react";
import {
  MessageSquare,
  Database,
  FileUp,
  FileText,
  Plus,
  BarChart3,
  BookOpen,
  Shield,
  Settings,
  Users,
  PanelLeftClose,
  PanelLeft,
  X,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useChatStore } from "@/store/chat";
import { cn } from "@/lib/utils";
import { formatRelativeTime } from "@/lib/utils";
import * as api from "@/lib/api";

const clerkEnabled = !!import.meta.env.VITE_CLERK_PUBLISHABLE_KEY;

const NAV_ITEMS = [
  { label: "Chat", icon: MessageSquare, path: "/chat", adminOnly: false },
  { label: "Reports", icon: BarChart3, path: "/reports", adminOnly: false },
  { label: "Connections", icon: Database, path: "/connections", adminOnly: false },
  { label: "Metrics", icon: BookOpen, path: "/metrics", adminOnly: false },
  { label: "Files", icon: FileUp, path: "/files", adminOnly: false },
  { label: "Setup", icon: Shield, path: "/setup", adminOnly: false },
  { label: "Org Settings", icon: Users, path: "/org-settings", adminOnly: false },
  { label: "Audit Log", icon: FileText, path: "/audit", adminOnly: true },
  { label: "Admin", icon: Settings, path: "/admin", adminOnly: true },
] as const;

export default function Sidebar() {
  const navigate = useNavigate();
  const location = useLocation();
  const [collapsed, setCollapsed] = useState(false);
  const [userRole, setUserRole] = useState<string>("member");

  const {
    conversations,
    activeConversationId,
    setActiveConversation,
  } = useChatStore();

  // Fetch user permissions to show/hide admin nav
  useEffect(() => {
    api.getPermissions()
      .then((data) => setUserRole(data.role || "member"))
      .catch(() => {});
  }, []);

  const visibleNavItems = NAV_ITEMS.filter(
    (item) => !item.adminOnly || userRole === "super_admin" || userRole === "admin"
  );

  const handleNewChat = useCallback(() => {
    setActiveConversation(null);
    navigate("/chat");
  }, [navigate, setActiveConversation]);

  const handleConversationClick = useCallback(
    (id: string) => {
      setActiveConversation(id);
      navigate(`/chat/${id}`);
    },
    [navigate, setActiveConversation]
  );

  const handleDeleteConversation = useCallback(
    async (id: string) => {
      try {
        await api.deleteConversation(id);
        useChatStore.getState().removeConversation(id);
        if (activeConversationId === id) {
          setActiveConversation(null);
          navigate("/chat");
        }
      } catch {
        // Silent fail
      }
    },
    [activeConversationId, setActiveConversation, navigate]
  );

  return (
    <TooltipProvider delayDuration={0}>
      <aside
        className={cn(
          "flex h-screen flex-col border-r bg-card transition-all duration-200",
          collapsed ? "w-16" : "w-64"
        )}
      >
        {/* Header */}
        <div className="flex h-14 items-center justify-between px-3">
          {!collapsed && (
            <button
              onClick={() => navigate("/chat")}
              className="flex items-center gap-2 font-semibold text-foreground"
            >
              <BarChart3 className="h-5 w-5 text-primary" />
              <span className="text-lg">Ceaser</span>
            </button>
          )}
          <Button
            variant="ghost"
            size="icon"
            className="h-8 w-8 shrink-0"
            onClick={() => setCollapsed((c) => !c)}
          >
            {collapsed ? (
              <PanelLeft className="h-4 w-4" />
            ) : (
              <PanelLeftClose className="h-4 w-4" />
            )}
          </Button>
        </div>

        <Separator />

        {/* New Chat */}
        <div className="p-3">
          {collapsed ? (
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="outline"
                  size="icon"
                  className="w-full"
                  onClick={handleNewChat}
                >
                  <Plus className="h-4 w-4" />
                </Button>
              </TooltipTrigger>
              <TooltipContent side="right">New Chat</TooltipContent>
            </Tooltip>
          ) : (
            <Button
              variant="outline"
              className="w-full justify-start gap-2"
              onClick={handleNewChat}
            >
              <Plus className="h-4 w-4" />
              New Chat
            </Button>
          )}
        </div>

        {/* Navigation */}
        <nav className="space-y-1 px-3">
          {visibleNavItems.map(({ label, icon: Icon, path }) => {
            const isActive = location.pathname.startsWith(path);
            return collapsed ? (
              <Tooltip key={path}>
                <TooltipTrigger asChild>
                  <Button
                    variant={isActive ? "secondary" : "ghost"}
                    size="icon"
                    className="w-full"
                    onClick={() => navigate(path)}
                  >
                    <Icon className="h-4 w-4" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent side="right">{label}</TooltipContent>
              </Tooltip>
            ) : (
              <Button
                key={path}
                variant={isActive ? "secondary" : "ghost"}
                className="w-full justify-start gap-2"
                onClick={() => navigate(path)}
              >
                <Icon className="h-4 w-4" />
                {label}
              </Button>
            );
          })}
        </nav>

        <Separator className="my-3" />

        {/* Recent Conversations */}
        {!collapsed && (
          <div className="flex-1 overflow-hidden px-3">
            <p className="mb-2 px-2 text-xs font-medium uppercase text-muted-foreground">
              Recent
            </p>
            <ScrollArea className="h-full">
              <div className="space-y-0.5 pb-4">
                {conversations.length === 0 && (
                  <p className="px-2 text-xs text-muted-foreground">
                    No conversations yet
                  </p>
                )}
                {conversations.map((convo) => (
                  <div key={convo.id} className="group relative">
                    <button
                      onClick={() => handleConversationClick(convo.id)}
                      className={cn(
                        "flex w-full flex-col items-start rounded-md px-2 py-1.5 text-left text-sm transition-colors hover:bg-accent",
                        activeConversationId === convo.id && "bg-accent"
                      )}
                    >
                      <span className="w-full truncate text-foreground pr-6">
                        {convo.title}
                      </span>
                      <span className="text-xs text-muted-foreground">
                        {formatRelativeTime(convo.updatedAt)}
                      </span>
                    </button>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        handleDeleteConversation(convo.id);
                      }}
                      className="absolute right-1 top-1.5 hidden rounded p-0.5 text-muted-foreground hover:text-destructive group-hover:block"
                    >
                      <X className="h-3.5 w-3.5" />
                    </button>
                  </div>
                ))}
              </div>
            </ScrollArea>
          </div>
        )}

        {/* User */}
        <div className="mt-auto border-t p-3">
          <div
            className={cn(
              "flex items-center",
              collapsed ? "justify-center" : "gap-3"
            )}
          >
            {clerkEnabled ? (
              <UserButton
                afterSignOutUrl="/sign-in"
                appearance={{
                  elements: {
                    avatarBox: "h-8 w-8",
                  },
                }}
              />
            ) : (
              <div className="flex h-8 w-8 items-center justify-center rounded-full bg-primary text-xs font-bold text-primary-foreground">
                D
              </div>
            )}
          </div>
        </div>
      </aside>
    </TooltipProvider>
  );
}
