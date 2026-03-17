import { useEffect, useState, useCallback } from "react";
import {
  Building2, Users, Send, Loader2, Shield, Trash2, UserPlus,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import * as api from "@/lib/api";

const ROLE_LABELS: Record<string, string> = {
  super_admin: "Super Admin",
  admin: "Admin",
  member: "Member",
  viewer: "Viewer",
};

const ROLE_COLORS: Record<string, string> = {
  super_admin: "text-amber-400",
  admin: "text-sky-400",
  member: "text-emerald-400",
  viewer: "text-muted-foreground",
};

export default function OrgSettingsPage() {
  const [users, setUsers] = useState<any[]>([]);
  const [permissions, setPermissions] = useState<any>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteRole, setInviteRole] = useState("member");
  const [inviting, setInviting] = useState(false);
  const [inviteSuccess, setInviteSuccess] = useState(false);

  useEffect(() => {
    Promise.all([api.getAdminUsers(), api.getPermissions()])
      .then(([u, p]) => {
        setUsers(Array.isArray(u) ? u : []);
        setPermissions(p);
      })
      .catch(() => {})
      .finally(() => setIsLoading(false));
  }, []);

  const canManage =
    permissions?.role === "super_admin" || permissions?.role === "admin";

  const handleInvite = useCallback(async () => {
    if (!inviteEmail.trim()) return;
    setInviting(true);
    try {
      // For now, create user directly in our DB
      // In production, this would go through Clerk invite
      await api.syncUser({
        clerkId: `invited-${Date.now()}`,
        email: inviteEmail,
        firstName: inviteEmail.split("@")[0],
        lastName: "",
        organizationId: permissions?.organizationId || "default",
        imageUrl: null,
      });
      setInviteSuccess(true);
      setInviteEmail("");
      // Refresh user list
      const u = await api.getAdminUsers();
      setUsers(Array.isArray(u) ? u : []);
      setTimeout(() => setInviteSuccess(false), 3000);
    } catch {
    } finally {
      setInviting(false);
    }
  }, [inviteEmail, inviteRole, permissions]);

  if (isLoading) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-4xl p-6">
      <div className="mb-6">
        <h2 className="text-2xl font-semibold">Organization Settings</h2>
        <p className="text-sm text-muted-foreground">
          Manage team members and their access levels
        </p>
      </div>

      {/* Role permissions overview */}
      <Card className="mb-6">
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <Shield className="h-4 w-4" />
            Role Permissions
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-muted-foreground">
                  <th className="pb-2 pr-4">Permission</th>
                  <th className="pb-2 px-3 text-center">Viewer</th>
                  <th className="pb-2 px-3 text-center">Member</th>
                  <th className="pb-2 px-3 text-center">Admin</th>
                </tr>
              </thead>
              <tbody className="text-xs">
                {[
                  ["Query data (chat)", false, true, true],
                  ["View reports & results", true, true, true],
                  ["Save reports", false, true, true],
                  ["Upload files", false, true, true],
                  ["Manage connections", false, false, true],
                  ["Define metrics", false, false, true],
                  ["Invite users", false, false, true],
                  ["View audit logs", false, false, true],
                ].map(([label, viewer, member, admin]) => (
                  <tr key={label as string} className="border-b border-border/50">
                    <td className="py-2 pr-4">{label as string}</td>
                    <td className="py-2 px-3 text-center">{viewer ? "✅" : "—"}</td>
                    <td className="py-2 px-3 text-center">{member ? "✅" : "—"}</td>
                    <td className="py-2 px-3 text-center">{admin ? "✅" : "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>

      {/* Invite new member */}
      {canManage && (
        <Card className="mb-6">
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-base">
              <UserPlus className="h-4 w-4" />
              Invite Team Member
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-end gap-3">
              <div className="flex-1 space-y-1">
                <label className="text-xs text-muted-foreground">Email</label>
                <Input
                  type="email"
                  placeholder="colleague@company.com"
                  value={inviteEmail}
                  onChange={(e) => setInviteEmail(e.target.value)}
                />
              </div>
              <div className="w-32 space-y-1">
                <label className="text-xs text-muted-foreground">Role</label>
                <Select value={inviteRole} onValueChange={setInviteRole}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="viewer">Viewer</SelectItem>
                    <SelectItem value="member">Member</SelectItem>
                    <SelectItem value="admin">Admin</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <Button onClick={handleInvite} disabled={inviting || !inviteEmail.trim()}>
                {inviting ? (
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                ) : (
                  <Send className="mr-2 h-4 w-4" />
                )}
                {inviteSuccess ? "Invited!" : "Invite"}
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Team members */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <Users className="h-4 w-4" />
            Team Members ({users.length})
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-2">
          {users.map((user) => (
            <div
              key={user.id}
              className="flex items-center justify-between rounded-lg border p-3"
            >
              <div className="flex items-center gap-3">
                <div className="flex h-9 w-9 items-center justify-center rounded-full bg-secondary text-sm font-medium">
                  {(user.firstName?.[0] || "?").toUpperCase()}
                </div>
                <div>
                  <p className="text-sm font-medium">
                    {user.firstName} {user.lastName}
                  </p>
                  <p className="text-xs text-muted-foreground">{user.email}</p>
                </div>
              </div>
              <Badge
                variant="outline"
                className={ROLE_COLORS[user.role || "member"]}
              >
                {ROLE_LABELS[user.role || "member"] || user.role}
              </Badge>
            </div>
          ))}
        </CardContent>
      </Card>
    </div>
  );
}
