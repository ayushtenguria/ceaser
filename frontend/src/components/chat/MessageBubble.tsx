import { AlertCircle, Bot, Bookmark, ChevronRight, Copy, Check, Download, Info, Maximize2, Minus, Shield, ShieldCheck, ShieldAlert, Table2, ThumbsUp, ThumbsDown, TrendingDown, TrendingUp, User } from "lucide-react";
import { useState, useCallback } from "react";
import ReactMarkdown from "react-markdown";
import { Button } from "@/components/ui/button";
import PlotlyChart from "@/components/visualizations/PlotlyChart";
import DataExplorer from "@/components/data/DataExplorer";
import DataExplorerSheet from "@/components/data/DataExplorerSheet";
import type { Message } from "@/types";
import { cn, formatRelativeTime } from "@/lib/utils";
import * as api from "@/lib/api";

interface MessageBubbleProps {
  message: Message;
}

export default function MessageBubble({ message }: MessageBubbleProps) {
  const isUser = message.role === "user";

  return (
    <div
      className={cn("flex gap-3", isUser ? "flex-row-reverse" : "flex-row")}
    >
      {/* Avatar */}
      <div
        className={cn(
          "flex h-8 w-8 shrink-0 items-center justify-center rounded-full",
          isUser ? "bg-primary" : "bg-secondary"
        )}
      >
        {isUser ? (
          <User className="h-4 w-4 text-primary-foreground" />
        ) : (
          <Bot className="h-4 w-4 text-secondary-foreground" />
        )}
      </div>

      {/* Content */}
      <div
        className={cn(
          "flex max-w-[85%] flex-col gap-2",
          isUser ? "items-end" : "items-start"
        )}
      >
        {/* Text bubble */}
        {message.content && (
          <div
            className={cn(
              "rounded-2xl px-4 py-2.5 text-sm leading-relaxed",
              isUser
                ? "bg-primary text-primary-foreground"
                : "bg-secondary text-secondary-foreground"
            )}
          >
            {isUser ? (
              <p className="whitespace-pre-wrap">{message.content}</p>
            ) : (
              <div className="prose prose-sm prose-invert max-w-none [&>p]:my-1 [&>ul]:my-2 [&>ol]:my-2 [&>li]:my-0.5 [&>h1]:text-base [&>h2]:text-sm [&>h3]:text-sm [&>h4]:text-sm [&>hr]:my-3 [&>hr]:border-white/10">
                <ReactMarkdown>{message.content}</ReactMarkdown>
              </div>
            )}
          </div>
        )}

        {/* SQL query block */}
        {message.sqlQuery && <SqlBlock sql={message.sqlQuery} />}

        {/* Query reasoning trail */}
        {message.queryReasoning && (
          <div className="flex items-start gap-2 rounded-lg border border-blue-500/20 bg-blue-500/5 px-3 py-2 text-xs text-blue-300/80">
            <Info className="mt-0.5 h-3.5 w-3.5 shrink-0 text-blue-400/60" />
            <p>{message.queryReasoning}</p>
          </div>
        )}

        {/* Confidence indicator */}
        {message.confidence && (
          <ConfidenceBadge level={message.confidence} />
        )}

        {/* Code block */}
        {message.codeBlock && <CodeBlock code={message.codeBlock} />}

        {/* KPI Metric Card — big number display for single-value results */}
        {message.metricCard && <MetricCardDisplay metric={message.metricCard} />}

        {/* Table data — collapsible (multiple supported) */}
        {message.tableDatas && message.tableDatas.length > 1
          ? message.tableDatas.map((td, i) => <CollapsibleTable key={i} data={td} />)
          : message.tableData && !message.metricCard && <CollapsibleTable data={message.tableData} />
        }

        {/* Charts (multiple supported) */}
        {message.plotlyFigures && message.plotlyFigures.length > 1
          ? message.plotlyFigures.map((fig, i) => <PlotlyChart key={i} figure={fig} />)
          : message.plotlyFigure && <PlotlyChart figure={message.plotlyFigure} />
        }

        {/* Error — only show if there's no text content (avoid duplication) */}
        {message.error && message.messageType === "error" && !message.content && (
          <div className="flex items-start gap-2 rounded-lg border border-destructive/50 bg-destructive/10 px-4 py-3 text-sm text-destructive">
            <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
            <p>{message.error}</p>
          </div>
        )}

        {/* Timestamp, Feedback & Save */}
        <div className="flex items-center gap-2 px-1">
          <span className="text-xs text-muted-foreground">
            {formatRelativeTime(message.createdAt)}
          </span>
          {!isUser && !message.id.startsWith("temp-") && (
            <FeedbackButtons messageId={message.id} existing={message.feedback} />
          )}
          {!isUser && (message.tableData || message.plotlyFigure) && (
            <SaveReportButton message={message} />
          )}
        </div>
      </div>
    </div>
  );
}

function SqlBlock({ sql }: { sql: string }) {
  const [copied, setCopied] = useState(false);
  const [expanded, setExpanded] = useState(false);

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(sql);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [sql]);

  return (
    <div className="w-full overflow-hidden rounded-lg border bg-[hsl(222,47%,5%)]">
      <button
        onClick={() => setExpanded((e) => !e)}
        className="flex w-full items-center justify-between px-4 py-2 text-left hover:bg-white/5 transition-colors"
      >
        <span className="flex items-center gap-2 text-xs font-medium text-muted-foreground">
          <ChevronRight className={cn("h-3 w-3 transition-transform", expanded && "rotate-90")} />
          SQL Query
        </span>
        {expanded && (
          <Button variant="ghost" size="sm" className="h-6 text-xs" onClick={(e) => { e.stopPropagation(); handleCopy(); }}>
            {copied ? <Check className="mr-1 h-3 w-3" /> : <Copy className="mr-1 h-3 w-3" />}
            {copied ? "Copied" : "Copy"}
          </Button>
        )}
      </button>
      {expanded && (
        <pre className="overflow-x-auto border-t px-4 py-3 text-sm text-emerald-400">
          <code>{sql}</code>
        </pre>
      )}
    </div>
  );
}

function CodeBlock({ code }: { code: string }) {
  const [copied, setCopied] = useState(false);
  const [expanded, setExpanded] = useState(false);

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [code]);

  return (
    <div className="w-full overflow-hidden rounded-lg border bg-[hsl(222,47%,5%)]">
      <button
        onClick={() => setExpanded((e) => !e)}
        className="flex w-full items-center justify-between px-4 py-2 text-left hover:bg-white/5 transition-colors"
      >
        <span className="flex items-center gap-2 text-xs font-medium text-muted-foreground">
          <ChevronRight className={cn("h-3 w-3 transition-transform", expanded && "rotate-90")} />
          Python Code
        </span>
        {expanded && (
          <Button variant="ghost" size="sm" className="h-6 text-xs" onClick={(e) => { e.stopPropagation(); handleCopy(); }}>
            {copied ? <Check className="mr-1 h-3 w-3" /> : <Copy className="mr-1 h-3 w-3" />}
            {copied ? "Copied" : "Copy"}
          </Button>
        )}
      </button>
      {expanded && (
        <pre className="overflow-x-auto border-t px-4 py-3 text-sm text-sky-400">
          <code>{code}</code>
        </pre>
      )}
    </div>
  );
}

function CollapsibleTable({ data }: { data: import("@/types").TableData }) {
  const [expanded, setExpanded] = useState(false);
  const [sheetOpen, setSheetOpen] = useState(false);
  const rowCount = data.totalRows ?? (data as any).total_rows ?? data.rows?.length ?? 0;

  return (
    <>
      <div className="w-full overflow-hidden rounded-lg border">
        <button
          onClick={() => setExpanded((e) => !e)}
          className="flex w-full items-center justify-between px-4 py-2 text-left hover:bg-muted/30 transition-colors"
        >
          <span className="flex items-center gap-2 text-xs font-medium text-muted-foreground">
            <ChevronRight className={cn("h-3 w-3 transition-transform", expanded && "rotate-90")} />
            <Table2 className="h-3.5 w-3.5" />
            Data Table ({rowCount} {rowCount === 1 ? "row" : "rows"}, {(data.columns || []).length} columns)
          </span>
          {expanded && (
            <button
              onClick={(e) => { e.stopPropagation(); setSheetOpen(true); }}
              className="text-xs text-muted-foreground hover:text-foreground flex items-center gap-1"
            >
              <Maximize2 className="h-3 w-3" />
              Explore
            </button>
          )}
        </button>
        {expanded && (
          <DataExplorer data={data} compact />
        )}
      </div>

      <DataExplorerSheet
        data={data}
        open={sheetOpen}
        onClose={() => setSheetOpen(false)}
      />
    </>
  );
}

function SaveReportButton({ message }: { message: Message }) {
  const [saved, setSaved] = useState(false);
  const [saving, setSaving] = useState(false);

  const handleSave = useCallback(async () => {
    setSaving(true);
    try {
      await api.createReport({
        name: message.content?.slice(0, 80) || "Untitled Report",
        sqlQuery: message.sqlQuery,
        pythonCode: message.codeBlock,
        originalQuestion: "",
        tableData: message.tableData,
        plotlyFigure: message.plotlyFigure,
        summaryText: message.content || "",
      });
      setSaved(true);
    } catch {} finally {
      setSaving(false);
    }
  }, [message]);

  if (saved) {
    return (
      <span className="flex items-center gap-1 text-xs text-emerald-400">
        <Bookmark className="h-3 w-3" /> Saved to Reports
      </span>
    );
  }

  return (
    <Button variant="ghost" size="sm" className="h-6 gap-1 text-xs text-muted-foreground" onClick={handleSave} disabled={saving}>
      <Bookmark className="h-3 w-3" />
      {saving ? "Saving..." : "Save as Report"}
    </Button>
  );
}


function FeedbackButtons({ messageId, existing }: {
  messageId: string;
  existing?: { rating: "up" | "down"; correctionNote?: string; category?: string };
}) {
  const [rating, setRating] = useState<"up" | "down" | null>(existing?.rating || null);
  const [showForm, setShowForm] = useState(false);
  const [note, setNote] = useState("");
  const [category, setCategory] = useState("");
  const [saving, setSaving] = useState(false);

  const handleRate = useCallback(async (value: "up" | "down") => {
    if (saving) return;

    if (value === "down") {
      setRating(value);
      setShowForm(true);
      return;
    }

    // Thumbs up — submit immediately
    setSaving(true);
    try {
      await api.submitFeedback(messageId, { rating: value });
      setRating(value);
    } catch { /* ignore */ }
    setSaving(false);
  }, [messageId, saving]);

  const handleSubmitDown = useCallback(async () => {
    setSaving(true);
    try {
      await api.submitFeedback(messageId, {
        rating: "down",
        correctionNote: note || undefined,
        category: category || undefined,
      });
      setShowForm(false);
    } catch { /* ignore */ }
    setSaving(false);
  }, [messageId, note, category]);

  if (rating === "up") {
    return (
      <span className="flex items-center gap-1 text-xs text-emerald-400">
        <ThumbsUp className="h-3 w-3" /> Helpful
      </span>
    );
  }

  if (rating === "down" && !showForm) {
    return (
      <span className="flex items-center gap-1 text-xs text-red-400">
        <ThumbsDown className="h-3 w-3" /> Reported
      </span>
    );
  }

  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-center gap-1">
        <button
          onClick={() => handleRate("up")}
          className="rounded p-1 text-muted-foreground/50 hover:text-emerald-400 hover:bg-emerald-400/10 transition-colors"
          title="Helpful"
        >
          <ThumbsUp className="h-3.5 w-3.5" />
        </button>
        <button
          onClick={() => handleRate("down")}
          className="rounded p-1 text-muted-foreground/50 hover:text-red-400 hover:bg-red-400/10 transition-colors"
          title="Wrong or unhelpful"
        >
          <ThumbsDown className="h-3.5 w-3.5" />
        </button>
      </div>

      {showForm && (
        <div className="flex flex-col gap-2 rounded-lg border bg-card px-3 py-2 text-xs">
          <p className="font-medium text-muted-foreground">What went wrong?</p>
          <select
            value={category}
            onChange={(e) => setCategory(e.target.value)}
            className="rounded border bg-background px-2 py-1 text-xs"
          >
            <option value="">Select category...</option>
            <option value="wrong_data">Wrong data / numbers</option>
            <option value="wrong_join">Wrong table join</option>
            <option value="wrong_metric">Wrong metric / calculation</option>
            <option value="wrong_filter">Wrong filter / condition</option>
            <option value="other">Other</option>
          </select>
          <textarea
            value={note}
            onChange={(e) => setNote(e.target.value)}
            placeholder="What was the correct answer? (optional)"
            rows={2}
            className="rounded border bg-background px-2 py-1 text-xs resize-none"
          />
          <div className="flex gap-2">
            <button
              onClick={handleSubmitDown}
              disabled={saving}
              className="rounded bg-red-500/20 px-2 py-1 text-red-400 hover:bg-red-500/30 transition-colors"
            >
              {saving ? "Sending..." : "Submit"}
            </button>
            <button
              onClick={() => { setShowForm(false); setRating(null); }}
              className="rounded px-2 py-1 text-muted-foreground hover:bg-muted transition-colors"
            >
              Cancel
            </button>
          </div>
        </div>
      )}
    </div>
  );
}


function MetricCardDisplay({ metric }: { metric: NonNullable<Message["metricCard"]> }) {
  const TrendIcon = metric.changeDirection === "up" ? TrendingUp
    : metric.changeDirection === "down" ? TrendingDown : Minus;

  const trendColor = metric.changeDirection === "up" ? "text-emerald-400"
    : metric.changeDirection === "down" ? "text-red-400" : "text-muted-foreground";

  const trendBg = metric.changeDirection === "up" ? "bg-emerald-500/10"
    : metric.changeDirection === "down" ? "bg-red-500/10" : "bg-muted/50";

  return (
    <div className="rounded-xl border bg-card p-5">
      <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
        {metric.label}
      </p>
      <div className="mt-2 flex items-baseline gap-3">
        <span className="text-3xl font-bold tracking-tight">{metric.formatted}</span>
        {metric.changePct !== undefined && (
          <span className={cn("flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium", trendBg, trendColor)}>
            <TrendIcon className="h-3 w-3" />
            {metric.changePct > 0 ? "+" : ""}{metric.changePct}%
          </span>
        )}
      </div>
      {metric.previousFormatted && (
        <p className="mt-1 text-xs text-muted-foreground">
          Previous: {metric.previousFormatted}
        </p>
      )}
    </div>
  );
}


function ConfidenceBadge({ level }: { level: string }) {
  const config: Record<string, { icon: typeof ShieldCheck; color: string; label: string; hint: string }> = {
    high: {
      icon: ShieldCheck,
      color: "text-emerald-400 border-emerald-500/20 bg-emerald-500/5",
      label: "High confidence",
      hint: "Exact column matches, clear join path",
    },
    medium: {
      icon: Shield,
      color: "text-amber-400 border-amber-500/20 bg-amber-500/5",
      label: "Medium confidence",
      hint: "Used inferred joins or column aliases — verify the results",
    },
    low: {
      icon: ShieldAlert,
      color: "text-red-400 border-red-500/20 bg-red-500/5",
      label: "Low confidence",
      hint: "Ambiguous terms or unclear join path — audit recommended",
    },
  };

  const c = config[level] || config.medium;
  const Icon = c.icon;

  return (
    <div className={cn("flex items-center gap-1.5 rounded-md border px-2 py-1 text-xs", c.color)}>
      <Icon className="h-3.5 w-3.5" />
      <span className="font-medium">{c.label}</span>
      <span className="text-muted-foreground/70">— {c.hint}</span>
    </div>
  );
}
