import { AlertCircle, Bot, Copy, Check, User } from "lucide-react";
import { useState, useCallback } from "react";
import { Button } from "@/components/ui/button";
import PlotlyChart from "@/components/visualizations/PlotlyChart";
import DataTable from "@/components/visualizations/DataTable";
import type { Message } from "@/types";
import { cn, formatRelativeTime } from "@/lib/utils";

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
            <p className="whitespace-pre-wrap">{message.content}</p>
          </div>
        )}

        {/* SQL query block */}
        {message.sqlQuery && <SqlBlock sql={message.sqlQuery} />}

        {/* Code block */}
        {message.codeBlock && <CodeBlock code={message.codeBlock} />}

        {/* Table data */}
        {message.tableData && <DataTable data={message.tableData} />}

        {/* Chart */}
        {message.plotlyFigure && <PlotlyChart figure={message.plotlyFigure} />}

        {/* Error */}
        {message.error && message.messageType === "error" && (
          <div className="flex items-start gap-2 rounded-lg border border-destructive/50 bg-destructive/10 px-4 py-3 text-sm text-destructive">
            <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
            <p>{message.error}</p>
          </div>
        )}

        {/* Timestamp */}
        <span className="px-1 text-xs text-muted-foreground">
          {formatRelativeTime(message.createdAt)}
        </span>
      </div>
    </div>
  );
}

function SqlBlock({ sql }: { sql: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(sql);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [sql]);

  return (
    <div className="w-full overflow-hidden rounded-lg border bg-[hsl(222,47%,5%)]">
      <div className="flex items-center justify-between border-b px-4 py-2">
        <span className="text-xs font-medium text-muted-foreground">SQL</span>
        <Button variant="ghost" size="sm" className="h-7" onClick={handleCopy}>
          {copied ? (
            <Check className="mr-1 h-3 w-3" />
          ) : (
            <Copy className="mr-1 h-3 w-3" />
          )}
          {copied ? "Copied" : "Copy"}
        </Button>
      </div>
      <pre className="overflow-x-auto p-4 text-sm text-emerald-400">
        <code>{sql}</code>
      </pre>
    </div>
  );
}

function CodeBlock({ code }: { code: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [code]);

  return (
    <div className="w-full overflow-hidden rounded-lg border bg-[hsl(222,47%,5%)]">
      <div className="flex items-center justify-between border-b px-4 py-2">
        <span className="text-xs font-medium text-muted-foreground">
          Python
        </span>
        <Button variant="ghost" size="sm" className="h-7" onClick={handleCopy}>
          {copied ? (
            <Check className="mr-1 h-3 w-3" />
          ) : (
            <Copy className="mr-1 h-3 w-3" />
          )}
          {copied ? "Copied" : "Copy"}
        </Button>
      </div>
      <pre className="overflow-x-auto p-4 text-sm text-sky-400">
        <code>{code}</code>
      </pre>
    </div>
  );
}
