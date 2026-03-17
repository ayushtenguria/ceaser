import { AlertCircle, Bot, Bookmark, ChevronRight, Copy, Check, Table2, User } from "lucide-react";
import { useState, useCallback } from "react";
import ReactMarkdown from "react-markdown";
import { Button } from "@/components/ui/button";
import PlotlyChart from "@/components/visualizations/PlotlyChart";
import DataTable from "@/components/visualizations/DataTable";
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

        {/* Code block */}
        {message.codeBlock && <CodeBlock code={message.codeBlock} />}

        {/* Table data — collapsible (multiple supported) */}
        {message.tableDatas && message.tableDatas.length > 1
          ? message.tableDatas.map((td, i) => <CollapsibleTable key={i} data={td} />)
          : message.tableData && <CollapsibleTable data={message.tableData} />
        }

        {/* Charts (multiple supported) */}
        {message.plotlyFigures && message.plotlyFigures.length > 1
          ? message.plotlyFigures.map((fig, i) => <PlotlyChart key={i} figure={fig} />)
          : message.plotlyFigure && <PlotlyChart figure={message.plotlyFigure} />
        }

        {/* Error */}
        {message.error && message.messageType === "error" && (
          <div className="flex items-start gap-2 rounded-lg border border-destructive/50 bg-destructive/10 px-4 py-3 text-sm text-destructive">
            <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
            <p>{message.error}</p>
          </div>
        )}

        {/* Timestamp & Save */}
        <div className="flex items-center gap-2 px-1">
          <span className="text-xs text-muted-foreground">
            {formatRelativeTime(message.createdAt)}
          </span>
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
  const rowCount = data.totalRows ?? (data as any).total_rows ?? data.rows?.length ?? 0;

  return (
    <div className="w-full overflow-hidden rounded-lg border">
      <button
        onClick={() => setExpanded((e) => !e)}
        className="flex w-full items-center justify-between px-4 py-2 text-left hover:bg-muted/30 transition-colors"
      >
        <span className="flex items-center gap-2 text-xs font-medium text-muted-foreground">
          <ChevronRight className={cn("h-3 w-3 transition-transform", expanded && "rotate-90")} />
          <Table2 className="h-3.5 w-3.5" />
          Data Table ({rowCount} {rowCount === 1 ? "row" : "rows"})
        </span>
      </button>
      {expanded && <DataTable data={data} />}
    </div>
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
