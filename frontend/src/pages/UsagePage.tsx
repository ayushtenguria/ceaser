import { useEffect, useState } from "react";
import { BarChart3, MessageSquare, Database, FileUp, FileText, Loader2 } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import * as api from "@/lib/api";

export default function UsagePage() {
  const [stats, setStats] = useState<any>(null);
  const [permissions, setPermissions] = useState<any>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      api.getAuditStats().catch(() => null),
      api.getPermissions().catch(() => null),
    ]).then(([s, p]) => {
      setStats(s);
      setPermissions(p);
    }).finally(() => setIsLoading(false));
  }, []);

  if (isLoading) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  const todayQueries = stats?.today?.byAction?.chat_query || 0;

  return (
    <div className="p-6">
      <div className="mb-6">
        <h2 className="text-2xl font-semibold">Usage</h2>
        <p className="text-sm text-muted-foreground">Your plan usage and limits</p>
      </div>

      {/* Plan badge */}
      {permissions && (
        <div className="mb-6 flex items-center gap-3">
          <Badge variant="secondary" className="text-sm px-3 py-1">
            {permissions.role === "super_admin" ? "Super Admin" : permissions.role}
          </Badge>
          <span className="text-sm text-muted-foreground">
            {permissions.permissions?.length || 0} permissions
          </span>
        </div>
      )}

      {/* Usage cards */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <UsageCard
          icon={MessageSquare}
          label="Queries Today"
          value={todayQueries}
          limit={null}
        />
        <UsageCard
          icon={BarChart3}
          label="Total Actions Today"
          value={stats?.today?.totalActions || 0}
          limit={null}
        />
        <UsageCard
          icon={Database}
          label="Connections"
          value={null}
          limit={null}
        />
        <UsageCard
          icon={FileText}
          label="Reports This Month"
          value={null}
          limit={null}
        />
      </div>

      {/* Activity breakdown */}
      {stats?.today?.byAction && Object.keys(stats.today.byAction).length > 0 && (
        <Card className="mt-6">
          <CardContent className="p-6">
            <h3 className="mb-4 text-sm font-medium">Today's Activity</h3>
            <div className="space-y-3">
              {Object.entries(stats.today.byAction).map(([action, count]) => (
                <div key={action} className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">{action.replace(/_/g, " ")}</span>
                  <span className="text-sm font-medium">{count as number}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

function UsageCard({ icon: Icon, label, value, limit }: {
  icon: any; label: string; value: number | null; limit: number | null;
}) {
  return (
    <Card>
      <CardContent className="flex items-center gap-3 p-4">
        <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-primary/10">
          <Icon className="h-5 w-5 text-primary" />
        </div>
        <div>
          <p className="text-2xl font-bold">{value ?? "—"}</p>
          <p className="text-xs text-muted-foreground">
            {label}
            {limit && <span className="text-muted-foreground"> / {limit}</span>}
          </p>
        </div>
      </CardContent>
    </Card>
  );
}
