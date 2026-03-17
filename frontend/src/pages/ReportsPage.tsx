import { useEffect, useState, useCallback } from "react";
import { RefreshCw, Pin, PinOff, Trash2, Clock, BarChart3, Table2, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardFooter } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import PlotlyChart from "@/components/visualizations/PlotlyChart";
import DataTable from "@/components/visualizations/DataTable";
import * as api from "@/lib/api";
import { formatRelativeTime } from "@/lib/utils";

export default function ReportsPage() {
  const [reports, setReports] = useState<any[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [refreshingIds, setRefreshingIds] = useState<Set<string>>(new Set());

  useEffect(() => {
    api.getReports().then(setReports).catch(() => {}).finally(() => setIsLoading(false));
  }, []);

  const handleRefresh = useCallback(async (id: string) => {
    setRefreshingIds((prev) => new Set(prev).add(id));
    try {
      const updated = await api.refreshReport(id);
      setReports((prev) => prev.map((r) => (r.id === id ? updated : r)));
    } catch {} finally {
      setRefreshingIds((prev) => { const next = new Set(prev); next.delete(id); return next; });
    }
  }, []);

  const handlePin = useCallback(async (id: string, pinned: boolean) => {
    try {
      const updated = await api.updateReport(id, { isPinned: !pinned });
      setReports((prev) => prev.map((r) => (r.id === id ? updated : r)));
    } catch {}
  }, []);

  const handleSchedule = useCallback(async (id: string, schedule: string) => {
    try {
      const updated = await api.updateReport(id, { schedule: schedule === "none" ? "" : schedule });
      setReports((prev) => prev.map((r) => (r.id === id ? updated : r)));
    } catch {}
  }, []);

  const handleDelete = useCallback(async (id: string) => {
    try {
      await api.deleteReport(id);
      setReports((prev) => prev.filter((r) => r.id !== id));
    } catch {}
  }, []);

  if (isLoading) {
    return (
      <div className="p-6">
        <h2 className="mb-6 text-2xl font-semibold">Reports</h2>
        <div className="grid gap-4 md:grid-cols-2">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="h-64 animate-pulse rounded-lg border bg-card" />
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="p-6">
      <div className="mb-6">
        <h2 className="text-2xl font-semibold">Reports</h2>
        <p className="text-sm text-muted-foreground">
          Saved analyses that auto-refresh on schedule
        </p>
      </div>

      {reports.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-24">
          <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-2xl bg-secondary">
            <BarChart3 className="h-8 w-8 text-muted-foreground" />
          </div>
          <h3 className="mb-1 text-lg font-medium">No saved reports</h3>
          <p className="text-sm text-muted-foreground">
            Chat with your data and click "Save as Report" to create one
          </p>
        </div>
      ) : (
        <div className="grid gap-4 md:grid-cols-2">
          {reports.map((report) => (
            <Card key={report.id} className="flex flex-col">
              <CardHeader className="pb-3">
                <div className="flex items-start justify-between">
                  <div>
                    <CardTitle className="text-base">{report.name}</CardTitle>
                    {report.description && (
                      <p className="mt-1 text-xs text-muted-foreground">{report.description}</p>
                    )}
                  </div>
                  <div className="flex items-center gap-1">
                    {report.isPinned && <Pin className="h-3.5 w-3.5 text-primary" />}
                    {report.schedule && (
                      <Badge variant="secondary" className="text-xs">
                        <Clock className="mr-1 h-3 w-3" />
                        {report.schedule}
                      </Badge>
                    )}
                  </div>
                </div>
                {report.originalQuestion && (
                  <p className="mt-2 text-sm italic text-muted-foreground">
                    "{report.originalQuestion}"
                  </p>
                )}
              </CardHeader>

              <CardContent className="flex-1 pb-3">
                {report.plotlyFigure && (
                  <div className="mb-3">
                    <PlotlyChart figure={report.plotlyFigure} />
                  </div>
                )}
                {report.tableData && !report.plotlyFigure && (
                  <div className="mb-3">
                    <DataTable data={report.tableData} />
                  </div>
                )}
                {report.summaryText && (
                  <p className="text-sm text-muted-foreground">{report.summaryText.slice(0, 200)}</p>
                )}
                {report.lastRunAt && (
                  <p className="mt-2 text-xs text-muted-foreground">
                    Last refreshed {formatRelativeTime(report.lastRunAt)}
                  </p>
                )}
              </CardContent>

              <CardFooter className="gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => handleRefresh(report.id)}
                  disabled={refreshingIds.has(report.id)}
                >
                  {refreshingIds.has(report.id) ? (
                    <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
                  ) : (
                    <RefreshCw className="mr-1 h-3.5 w-3.5" />
                  )}
                  Refresh
                </Button>

                <Select
                  value={report.schedule || "none"}
                  onValueChange={(v) => handleSchedule(report.id, v)}
                >
                  <SelectTrigger className="h-8 w-[110px] text-xs">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="none">No schedule</SelectItem>
                    <SelectItem value="hourly">Hourly</SelectItem>
                    <SelectItem value="daily">Daily</SelectItem>
                    <SelectItem value="weekly">Weekly</SelectItem>
                  </SelectContent>
                </Select>

                <Button
                  variant="ghost"
                  size="icon"
                  className="h-8 w-8"
                  onClick={() => handlePin(report.id, report.isPinned)}
                >
                  {report.isPinned ? <PinOff className="h-3.5 w-3.5" /> : <Pin className="h-3.5 w-3.5" />}
                </Button>

                <Button
                  variant="ghost"
                  size="icon"
                  className="ml-auto h-8 w-8 text-muted-foreground hover:text-destructive"
                  onClick={() => handleDelete(report.id)}
                >
                  <Trash2 className="h-3.5 w-3.5" />
                </Button>
              </CardFooter>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
