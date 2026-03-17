import { useEffect, useState } from "react";
import { FileText, Loader2 } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import * as api from "@/lib/api";
import { formatRelativeTime } from "@/lib/utils";

export default function AuditPage() {
  const [logs, setLogs] = useState<any[]>([]);
  const [stats, setStats] = useState<any>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    Promise.all([api.getAuditLogs({ limit: 50 }), api.getAuditStats()])
      .then(([l, s]) => { setLogs(l); setStats(s); })
      .catch(() => {})
      .finally(() => setIsLoading(false));
  }, []);

  if (isLoading) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  return (
    <div className="p-6">
      <div className="mb-6">
        <h2 className="text-2xl font-semibold">Audit Log</h2>
        <p className="text-sm text-muted-foreground">Track all queries and actions on the platform</p>
      </div>

      {stats?.today && (
        <Card className="mb-6">
          <CardContent className="flex items-center gap-6 p-4">
            <div>
              <p className="text-2xl font-bold">{stats.today.totalActions}</p>
              <p className="text-xs text-muted-foreground">Actions today</p>
            </div>
            {Object.entries(stats.today.byAction || {}).map(([action, count]) => (
              <div key={action}>
                <p className="text-lg font-semibold">{count as number}</p>
                <p className="text-xs text-muted-foreground">{action.replace("_", " ")}</p>
              </div>
            ))}
          </CardContent>
        </Card>
      )}

      <div className="space-y-2">
        {logs.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-16">
            <FileText className="mb-4 h-8 w-8 text-muted-foreground" />
            <p className="text-sm text-muted-foreground">No audit logs yet</p>
          </div>
        ) : (
          logs.map((log) => (
            <Card key={log.id}>
              <CardContent className="flex items-center justify-between p-3">
                <div className="flex items-center gap-3">
                  <Badge variant="outline" className="text-xs">{log.action}</Badge>
                  <span className="text-sm">{log.resourceType}</span>
                  {log.details?.question && (
                    <span className="max-w-md truncate text-xs text-muted-foreground">
                      "{log.details.question}"
                    </span>
                  )}
                </div>
                <span className="text-xs text-muted-foreground">
                  {formatRelativeTime(log.createdAt)}
                </span>
              </CardContent>
            </Card>
          ))
        )}
      </div>
    </div>
  );
}
