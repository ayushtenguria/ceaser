import { useState, useCallback, useRef, useEffect } from "react";
import {
  FileDown, Loader2, X, BarChart3, TrendingUp, AlertCircle,
  CheckCircle2, FileText,
} from "lucide-react";
import ReactMarkdown from "react-markdown";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import PlotlyChart from "@/components/visualizations/PlotlyChart";
import DataTable from "@/components/visualizations/DataTable";
import * as api from "@/lib/api";
import { cn } from "@/lib/utils";

interface ReportSheetProps {
  conversationId: string | null;
  open: boolean;
  onClose: () => void;
}

interface ReportData {
  title: string;
  subtitle: string;
  executiveSummary: string;
  keyMetrics: { label: string; value: string }[];
  sections: {
    order: number;
    title: string;
    narrative: string;
    tableData: any;
    chartData: any;
  }[];
  recommendations: string[];
  totalMessagesAnalyzed: number;
}

export default function ReportSheet({ conversationId, open, onClose }: ReportSheetProps) {
  const [isGenerating, setIsGenerating] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [stage, setStage] = useState("");
  const [report, setReport] = useState<ReportData | null>(null);
  const [hasNewMessages, setHasNewMessages] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const reportRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open || !conversationId) return;
    setIsLoading(true);
    setError(null);

    api.getSavedReport(conversationId).then((saved) => {
      if (saved && saved.report) {
        setReport(saved.report);
        setHasNewMessages(saved.hasNewMessages || false);
      } else {
        setReport(null);
        setHasNewMessages(false);
      }
    }).catch(() => {
      setReport(null);
    }).finally(() => {
      setIsLoading(false);
    });
  }, [open, conversationId]);

  const handleGenerate = useCallback(async () => {
    if (!conversationId) return;
    setIsGenerating(true);
    setProgress(0);
    setStage("Starting...");
    setError(null);
    setReport(null);

    try {
      for await (const event of api.generateReport(conversationId)) {
        if (event.type === "report_status") {
          setProgress(event.progress || 0);
          setStage(event.stage || "");
        } else if (event.type === "report_complete") {
          setReport(event.report);
          setProgress(100);
          setStage("Complete");
        } else if (event.type === "report_error") {
          setError(event.error || "Failed to generate report");
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Report generation failed");
    } finally {
      setIsGenerating(false);
    }
  }, [conversationId]);

  const [isPdfMode, setIsPdfMode] = useState(false);

  const handleDownloadPDF = useCallback(async () => {
    if (!reportRef.current) return;
    setIsPdfMode(true);
    await new Promise((r) => setTimeout(r, 100));

    const html2pdf = (await import("html2pdf.js")).default;
    await html2pdf()
      .set({
        margin: [12, 10, 12, 10],
        filename: `${report?.title || "report"}.pdf`,
        image: { type: "jpeg", quality: 0.95 },
        html2canvas: { scale: 2, useCORS: true, scrollY: 0 },
        jsPDF: { unit: "mm", format: "a4", orientation: "portrait" },
        pagebreak: { mode: ["avoid-all", "css", "legacy"] },
      })
      .from(reportRef.current)
      .save();

    setIsPdfMode(false);
  }, [report]);

  if (!open) return null;

  return (
    <>
      {/* Backdrop */}
      <div className="fixed inset-0 z-40 bg-black/50" onClick={onClose} />

      {/* Sheet */}
      <div className="fixed inset-y-0 right-0 z-50 flex w-full max-w-2xl flex-col bg-background shadow-2xl animate-in slide-in-from-right">
        {/* Header */}
        <div className="flex h-14 items-center justify-between border-b px-6">
          <div className="flex items-center gap-3">
            <FileText className="h-5 w-5 text-primary" />
            <h2 className="text-lg font-semibold">
              {report ? report.title : "Generate Report"}
            </h2>
          </div>
          <div className="flex items-center gap-2">
            {report && !isGenerating && (
              <>
                <Button variant="ghost" size="sm" onClick={handleGenerate} className="text-xs">
                  Regenerate
                </Button>
                <Button variant="outline" size="sm" onClick={handleDownloadPDF}>
                  <FileDown className="mr-2 h-4 w-4" />
                  Download PDF
                </Button>
              </>
            )}
            <Button variant="ghost" size="icon" className="h-8 w-8" onClick={onClose}>
              <X className="h-4 w-4" />
            </Button>
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto">
          {/* Loading existing report */}
          {isLoading && (
            <div className="flex flex-col items-center justify-center py-24 px-6">
              <Loader2 className="mb-4 h-8 w-8 animate-spin text-primary" />
              <p className="text-sm text-muted-foreground">Loading report...</p>
            </div>
          )}

          {/* Initial state — generate button */}
          {!report && !isGenerating && !isLoading && !error && (
            <div className="flex flex-col items-center justify-center py-24 px-6">
              <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-2xl bg-primary/10">
                <BarChart3 className="h-8 w-8 text-primary" />
              </div>
              <h3 className="mb-2 text-lg font-medium">Generate Report</h3>
              <p className="mb-6 max-w-md text-center text-sm text-muted-foreground">
                Create a professional analysis report from this conversation.
                The AI will analyze all messages, charts, and insights to produce
                a structured report with executive summary and recommendations.
              </p>
              <Button size="lg" onClick={handleGenerate}>
                <BarChart3 className="mr-2 h-4 w-4" />
                Generate Report
              </Button>
            </div>
          )}

          {/* Generating — progress */}
          {isGenerating && (
            <div className="flex flex-col items-center justify-center py-24 px-6">
              <Loader2 className="mb-4 h-10 w-10 animate-spin text-primary" />
              <h3 className="mb-2 text-lg font-medium">{stage}</h3>
              <div className="mb-2 w-64">
                <div className="h-2 w-full rounded-full bg-muted">
                  <div
                    className="h-full rounded-full bg-primary transition-all duration-500"
                    style={{ width: `${progress}%` }}
                  />
                </div>
              </div>
              <p className="text-sm text-muted-foreground">{progress}%</p>
            </div>
          )}

          {/* Error */}
          {error && (
            <div className="flex flex-col items-center justify-center py-24 px-6">
              <AlertCircle className="mb-4 h-10 w-10 text-destructive" />
              <h3 className="mb-2 text-lg font-medium">Generation Failed</h3>
              <p className="mb-4 text-sm text-muted-foreground">{error}</p>
              <Button onClick={handleGenerate}>Try Again</Button>
            </div>
          )}

          {/* New messages banner */}
          {report && hasNewMessages && !isGenerating && (
            <div className="flex items-center justify-between bg-amber-500/10 border-b border-amber-500/30 px-6 py-2">
              <p className="text-xs text-amber-400">
                This conversation has new messages since the last report.
              </p>
              <Button variant="outline" size="sm" className="text-xs h-7" onClick={handleGenerate}>
                Regenerate
              </Button>
            </div>
          )}

          {/* Report content — dark on screen, light for PDF */}
          {report && !isGenerating && (
            <div
              ref={reportRef}
              className="p-6 space-y-5"
              style={isPdfMode
                ? { backgroundColor: "#ffffff", color: "#1a1a1a" }
                : {}
              }
            >
              {/* Title block */}
              <div className="text-center pb-3">
                <h1 className={isPdfMode ? "" : "text-2xl font-bold"} style={isPdfMode ? { fontSize: "22px", fontWeight: 700, color: "#111" } : {}}>{report.title}</h1>
                {report.subtitle && (
                  <p className={isPdfMode ? "" : "mt-1 text-muted-foreground text-sm"} style={isPdfMode ? { fontSize: "13px", color: "#666", marginTop: "4px" } : {}}>{report.subtitle}</p>
                )}
                <p className={isPdfMode ? "" : "mt-1 text-xs text-muted-foreground"} style={isPdfMode ? { fontSize: "11px", color: "#999", marginTop: "4px" } : {}}>
                  Based on {report.totalMessagesAnalyzed} messages analyzed · {new Date().toLocaleDateString()}
                </p>
              </div>

              {isPdfMode ? <hr style={{ border: "none", borderTop: "1px solid #e5e5e5" }} /> : <Separator />}

              {/* Key Metrics — compact inline badges */}
              {report.keyMetrics && report.keyMetrics.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {report.keyMetrics.map((m, i) => (
                    isPdfMode ? (
                      <span key={i} style={{ display: "inline-flex", alignItems: "center", gap: "4px", padding: "3px 10px", borderRadius: "12px", fontSize: "12px", backgroundColor: "#f0f4f8", color: "#333", border: "1px solid #e0e0e0" }}>
                        <strong>{m.label}:</strong> {m.value}
                      </span>
                    ) : (
                      <Badge key={i} variant="secondary" className="text-xs gap-1">
                        <span className="text-muted-foreground">{m.label}:</span> {m.value}
                      </Badge>
                    )
                  ))}
                </div>
              )}

              {/* Executive Summary */}
              {report.executiveSummary && (
                <div>
                  <h2 className={isPdfMode ? "" : "mb-2 text-lg font-semibold"} style={isPdfMode ? { fontSize: "16px", fontWeight: 600, color: "#111", marginBottom: "8px" } : {}}>Executive Summary</h2>
                  <div className={isPdfMode ? "" : "prose prose-sm prose-invert max-w-none rounded-lg border bg-card p-4"} style={isPdfMode ? { fontSize: "13px", lineHeight: "1.6", color: "#333", padding: "12px 16px", backgroundColor: "#f8fafc", borderRadius: "8px", border: "1px solid #e5e5e5" } : {}}>
                    <ReactMarkdown>{report.executiveSummary}</ReactMarkdown>
                  </div>
                </div>
              )}

              {/* Sections */}
              {report.sections.map((section, i) => (
                <div key={i} style={{ marginTop: "16px", pageBreakInside: "avoid", breakInside: "avoid" }}>
                  <h2
                    className={isPdfMode ? "" : "mb-2 flex items-center gap-2 text-lg font-semibold"}
                    style={isPdfMode ? { fontSize: "15px", fontWeight: 600, color: "#111", display: "flex", alignItems: "center", gap: "8px", marginBottom: "8px" } : {}}
                  >
                    <span
                      className={isPdfMode ? "" : "flex h-6 w-6 items-center justify-center rounded-full bg-primary text-xs font-bold text-primary-foreground"}
                      style={isPdfMode ? { display: "inline-flex", alignItems: "center", justifyContent: "center", width: "22px", height: "22px", borderRadius: "50%", backgroundColor: "#3b82f6", color: "#fff", fontSize: "11px", fontWeight: 700 } : {}}
                    >
                      {i + 1}
                    </span>
                    {section.title}
                  </h2>

                  <div
                    className={isPdfMode ? "" : "prose prose-sm prose-invert max-w-none mb-3"}
                    style={isPdfMode ? { fontSize: "13px", lineHeight: "1.6", color: "#333", marginBottom: "10px" } : {}}
                  >
                    <ReactMarkdown>{section.narrative}</ReactMarkdown>
                  </div>

                  {section.chartData && (
                    <div style={{ marginBottom: "10px", pageBreakInside: "avoid", breakInside: "avoid" }}>
                      {isPdfMode ? (
                        <div style={{ maxHeight: "280px", overflow: "hidden" }}>
                          <PlotlyChart figure={{
                            data: section.chartData.data,
                            layout: {
                              ...section.chartData.layout,
                              template: undefined,
                              height: 260,
                              paper_bgcolor: "#ffffff",
                              plot_bgcolor: "#f8f8f8",
                              font: { color: "#333", size: 11 },
                              xaxis: {
                                ...section.chartData.layout?.xaxis,
                                gridcolor: "#e5e5e5",
                                linecolor: "#ccc",
                                tickfont: { color: "#333" },
                              },
                              yaxis: {
                                ...section.chartData.layout?.yaxis,
                                gridcolor: "#e5e5e5",
                                linecolor: "#ccc",
                                tickfont: { color: "#333" },
                              },
                              colorway: ["#3b82f6", "#ef4444", "#10b981", "#f59e0b", "#8b5cf6", "#06b6d4"],
                            },
                          }} />
                        </div>
                      ) : (
                        <PlotlyChart figure={section.chartData} />
                      )}
                    </div>
                  )}

                  {section.tableData && (
                    <div style={{ marginBottom: "10px", pageBreakInside: "avoid", breakInside: "avoid" }}>
                      {isPdfMode ? (
                        <ReportTable data={section.tableData} />
                      ) : (
                        <DataTable data={section.tableData} />
                      )}
                    </div>
                  )}
                </div>
              ))}

              {/* Recommendations */}
              {report.recommendations && report.recommendations.length > 0 && (
                <div style={{ marginTop: "16px" }}>
                  <h2
                    className={isPdfMode ? "" : "mb-3 text-lg font-semibold"}
                    style={isPdfMode ? { fontSize: "15px", fontWeight: 600, color: "#111", marginBottom: "10px" } : {}}
                  >Recommendations</h2>
                  <div className={isPdfMode ? "" : "space-y-2"} style={isPdfMode ? { display: "flex", flexDirection: "column", gap: "6px" } : {}}>
                    {report.recommendations.map((rec, i) => (
                      isPdfMode ? (
                        <div key={i} style={{ display: "flex", alignItems: "flex-start", gap: "8px", padding: "8px 12px", borderRadius: "6px", backgroundColor: "#f0fdf4", border: "1px solid #bbf7d0" }}>
                          <span style={{ color: "#16a34a", fontSize: "14px", marginTop: "1px" }}>✓</span>
                          <p style={{ fontSize: "13px", color: "#333", margin: 0 }}>{rec}</p>
                        </div>
                      ) : (
                        <div key={i} className="flex items-start gap-3 rounded-lg border bg-card p-3">
                          <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-emerald-400" />
                          <p className="text-sm">{rec}</p>
                        </div>
                      )
                    ))}
                  </div>
                </div>
              )}

              {/* Footer */}
              {isPdfMode ? (
                <>
                  <hr style={{ border: "none", borderTop: "1px solid #e5e5e5", marginTop: "20px" }} />
                  <p style={{ textAlign: "center", fontSize: "11px", color: "#999" }}>
                    Report generated by Ceaser AI — {new Date().toLocaleDateString()}
                  </p>
                </>
              ) : (
                <>
                  <Separator />
                  <p className="text-center text-xs text-muted-foreground">
                    Report generated by Ceaser AI — {new Date().toLocaleDateString()}
                  </p>
                </>
              )}
            </div>
          )}
        </div>
      </div>
    </>
  );
}

/** Light-themed table for PDF export — uses inline styles, not Tailwind dark classes */
function ReportTable({ data }: { data: any }) {
  const columns: string[] = data?.columns || [];
  const rows: Record<string, any>[] = data?.rows || [];
  const totalRows = data?.totalRows ?? data?.total_rows ?? rows.length;

  return (
    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "12px" }}>
      <thead>
        <tr>
          {columns.map((col) => (
            <th key={col} style={{ textAlign: "left", padding: "6px 10px", borderBottom: "2px solid #ddd", backgroundColor: "#f5f5f5", color: "#333", fontWeight: 600 }}>
              {col}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, idx) => (
          <tr key={idx}>
            {columns.map((col) => (
              <td key={col} style={{ padding: "5px 10px", borderBottom: "1px solid #eee", color: "#333", backgroundColor: idx % 2 === 1 ? "#fafafa" : "#fff" }}>
                {row[col] === null || row[col] === undefined ? "—" : String(row[col])}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
      <tfoot>
        <tr>
          <td colSpan={columns.length} style={{ padding: "4px 10px", fontSize: "11px", color: "#999" }}>
            Showing {rows.length} of {totalRows} rows
          </td>
        </tr>
      </tfoot>
    </table>
  );
}
