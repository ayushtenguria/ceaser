import { useEffect, useState, useCallback } from "react";
import {
  Users, Building2, MessageSquare, Database, BarChart3,
  Plus, Send, Loader2, Shield,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger,
} from "@/components/ui/dialog";
import * as api from "@/lib/api";

export default function AdminPage() {
  const [stats, setStats] = useState<any>(null);
  const [orgs, setOrgs] = useState<any[]>([]);
  const [users, setUsers] = useState<any[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([
      api.getAdminStats(),
      api.getAdminOrganizations(),
      api.getAdminUsers(),
    ])
      .then(([s, o, u]) => {
        setStats(s);
        setOrgs(Array.isArray(o) ? o : []);
        setUsers(Array.isArray(u) ? u : []);
      })
      .catch((err) => setError(err?.response?.data?.detail || "Admin access required"))
      .finally(() => setIsLoading(false));
  }, []);

  if (isLoading) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-4">
        <Shield className="h-12 w-12 text-destructive" />
        <h2 className="text-xl font-semibold">Access Denied</h2>
        <p className="text-muted-foreground">{error}</p>
      </div>
    );
  }

  return (
    <div className="p-6">
      <div className="mb-6">
        <h2 className="text-2xl font-semibold">Admin Dashboard</h2>
        <p className="text-sm text-muted-foreground">Platform management and user administration</p>
      </div>

      {/* Stats cards */}
      {stats && (
        <div className="mb-8 grid gap-4 md:grid-cols-5">
          <StatCard icon={Users} label="Users" value={stats.totalUsers} />
          <StatCard icon={Building2} label="Organizations" value={stats.organizations?.length || 0} />
          <StatCard icon={MessageSquare} label="Conversations" value={stats.totalConversations} />
          <StatCard icon={Database} label="Connections" value={stats.totalConnections} />
          <StatCard icon={BarChart3} label="Reports" value={stats.totalReports} />
        </div>
      )}

      <div className="grid gap-6 lg:grid-cols-2">
        {/* Organizations */}
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-3">
            <CardTitle className="text-base">Organizations</CardTitle>
            <CreateOrgDialog onCreated={(org) => setOrgs((prev) => [org, ...prev])} />
          </CardHeader>
          <CardContent className="space-y-3">
            {orgs.length === 0 ? (
              <p className="text-sm text-muted-foreground">No organizations yet</p>
            ) : (
              orgs.map((org) => (
                <div key={org.id} className="flex items-center justify-between rounded-lg border p-3">
                  <div>
                    <p className="font-medium">{org.name}</p>
                    <p className="text-xs text-muted-foreground">{org.slug || org.id}</p>
                  </div>
                  <InviteDialog orgId={org.id} orgName={org.name} />
                </div>
              ))
            )}
          </CardContent>
        </Card>

        {/* Users */}
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-base">Users ({users.length})</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            {users.map((user) => (
              <div key={user.id} className="flex items-center justify-between rounded-lg border p-3">
                <div>
                  <p className="text-sm font-medium">
                    {user.firstName} {user.lastName}
                  </p>
                  <p className="text-xs text-muted-foreground">{user.email}</p>
                </div>
                <Badge variant="outline" className="text-xs">
                  {user.organizationId || "No org"}
                </Badge>
              </div>
            ))}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function StatCard({ icon: Icon, label, value }: { icon: any; label: string; value: number }) {
  return (
    <Card>
      <CardContent className="flex items-center gap-3 p-4">
        <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-primary/10">
          <Icon className="h-5 w-5 text-primary" />
        </div>
        <div>
          <p className="text-2xl font-bold">{value}</p>
          <p className="text-xs text-muted-foreground">{label}</p>
        </div>
      </CardContent>
    </Card>
  );
}

function CreateOrgDialog({ onCreated }: { onCreated: (org: any) => void }) {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [adminEmail, setAdminEmail] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  const handleCreate = async () => {
    if (!name.trim()) return;
    setSaving(true);
    setError(null);
    try {
      const org = await api.createAdminOrganization({ name });
      if (adminEmail.trim()) {
        try {
          await api.inviteUserToOrg(org.id, { email: adminEmail, role: "admin" });
        } catch {
        }
      }
      onCreated(org);
      setSuccess(true);
      setTimeout(() => {
        setOpen(false);
        setName("");
        setAdminEmail("");
        setSuccess(false);
      }, 1500);
    } catch (err: any) {
      const detail = err?.response?.data?.detail;
      if (typeof detail === "object") {
        setError(JSON.stringify(detail.errors?.[0]?.message || detail));
      } else {
        setError(detail || "Failed to create organization. Check Clerk configuration.");
      }
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={(v) => { setOpen(v); if (!v) setError(null); }}>
      <DialogTrigger asChild>
        <Button size="sm"><Plus className="mr-1 h-3.5 w-3.5" />New Org</Button>
      </DialogTrigger>
      <DialogContent className="max-w-sm">
        <DialogHeader><DialogTitle>Create Organization</DialogTitle></DialogHeader>
        <div className="space-y-4">
          <div className="space-y-1.5">
            <label className="text-sm font-medium">Organization Name</label>
            <Input placeholder="Acme Corp" value={name} onChange={(e) => setName(e.target.value)} />
          </div>
          <div className="space-y-1.5">
            <label className="text-sm font-medium">Admin Email <span className="text-muted-foreground">(optional)</span></label>
            <Input type="email" placeholder="admin@acme.com" value={adminEmail} onChange={(e) => setAdminEmail(e.target.value)} />
            <p className="text-xs text-muted-foreground">This person will receive an invite and get admin access to the org.</p>
          </div>
          {error && (
            <div className="rounded-md border border-destructive/50 bg-destructive/10 px-3 py-2 text-xs text-destructive">
              {error}
            </div>
          )}
          {success && (
            <div className="rounded-md border border-emerald-500/50 bg-emerald-500/10 px-3 py-2 text-xs text-emerald-400">
              Organization created successfully!
            </div>
          )}
          <Button className="w-full" onClick={handleCreate} disabled={saving || !name.trim() || success}>
            {saving && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            {success ? "Created!" : "Create Organization"}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}

function InviteDialog({ orgId, orgName }: { orgId: string; orgName: string }) {
  const [open, setOpen] = useState(false);
  const [email, setEmail] = useState("");
  const [sending, setSending] = useState(false);
  const [sent, setSent] = useState(false);

  const handleInvite = async () => {
    if (!email.trim()) return;
    setSending(true);
    try {
      await api.inviteUserToOrg(orgId, { email });
      setSent(true);
      setTimeout(() => { setSent(false); setEmail(""); setOpen(false); }, 2000);
    } catch {} finally {
      setSending(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm"><Send className="mr-1 h-3 w-3" />Invite</Button>
      </DialogTrigger>
      <DialogContent className="max-w-sm">
        <DialogHeader><DialogTitle>Invite to {orgName}</DialogTitle></DialogHeader>
        <div className="space-y-4">
          <Input type="email" placeholder="user@company.com" value={email} onChange={(e) => setEmail(e.target.value)} />
          <Button className="w-full" onClick={handleInvite} disabled={sending || !email.trim()}>
            {sent ? "Invited!" : sending ? <><Loader2 className="mr-2 h-4 w-4 animate-spin" />Sending...</> : "Send Invite"}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
