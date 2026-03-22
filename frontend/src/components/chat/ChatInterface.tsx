import { useEffect, useRef, useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { BarChart3, BookMarked, Database, FileSpreadsheet, FileText, Loader2, MessageSquare } from "lucide-react";
import { Button } from "@/components/ui/button";
import ChatInput from "@/components/chat/ChatInput";
import MessageBubble from "@/components/chat/MessageBubble";
import ReportSheet from "@/components/chat/ReportSheet";
import NotebookDraftSheet from "@/components/chat/NotebookDraftSheet";
import { useChat } from "@/hooks/useChat";
import { useConnectionsStore } from "@/store/connections";
import * as api from "@/lib/api";

export default function ChatInterface() {
  const navigate = useNavigate();
  const { messages, isStreaming, streamStatus, sendMessage, suggestions, activeConversationId } = useChat();
  const bottomRef = useRef<HTMLDivElement>(null);
  const [reportOpen, setReportOpen] = useState(false);
  const [notebookDraftOpen, setNotebookDraftOpen] = useState(false);

  // Auto-scroll to bottom on new messages or streaming updates
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, isStreaming]);

  function _formatStatus(status: string): string {
    if (!status) return "Analyzing your question...";
    const s = status.toLowerCase();
    if (s.includes("analysing") || s.includes("analyzing")) return "Understanding your question...";
    if (s.includes("decided to use: sql")) return "Preparing database query...";
    if (s.includes("decided to use: python")) return "Preparing analysis code...";
    if (s.includes("decided to use: analyze")) return "Running deep analysis...";
    if (s.includes("decided to use: respond")) return "Composing response...";
    if (s.includes("generating sql") || s.includes("sql agent")) return "Writing SQL query...";
    if (s.includes("executing") || s.includes("sql execute")) return "Running query on database...";
    if (s.includes("fixing query")) return "Fixing query...";
    if (s.includes("verifying")) return "Checking results...";
    if (s.includes("breaking into")) return "Breaking into sub-queries...";
    if (s.includes("part ")) return status; // "Part 1/2: ..." — show as-is
    if (s.includes("multi-database")) return "Querying multiple databases...";
    if (s.includes("loading schemas")) return "Loading database schemas...";
    if (s.includes("planning")) return "Planning analysis strategy...";
    if (s.includes("deep analysis")) return "Running comprehensive analysis...";
    if (s.includes("joining")) return "Merging results from multiple sources...";
    // Strip technical prefixes
    return status.replace(/^(decided to use: |sql |code )/, "").trim() || "Processing...";
  }

  return (
    <div className="flex h-full flex-col">
      {/* Report button — shown when there are messages */}
      {messages.length > 0 && !isStreaming && (
        <div className="flex items-center justify-end gap-2 border-b px-4 py-1.5">
          <Button
            variant="outline"
            size="sm"
            className="gap-1.5 text-xs"
            onClick={() => setNotebookDraftOpen(true)}
          >
            <BookMarked className="h-3.5 w-3.5" />
            Save as Notebook
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="gap-1.5 text-xs"
            onClick={() => setReportOpen(true)}
          >
            <FileText className="h-3.5 w-3.5" />
            Create Report
          </Button>
        </div>
      )}

      {/* Messages */}
      <div className="flex-1 overflow-y-auto">
        {messages.length === 0 ? (
          <EmptyState onSuggestionClick={sendMessage} />
        ) : (
          <div className="mx-auto w-full max-w-4xl space-y-6 px-4 py-6">
            {messages.map((message) => (
              <MessageBubble key={message.id} message={message} />
            ))}
            {isStreaming && (
                <div className="rounded-lg border bg-card/50 px-4 py-3 mx-4">
                  <div className="flex items-center gap-3">
                    <div className="relative h-5 w-5 shrink-0">
                      <div className="absolute inset-0 animate-ping rounded-full bg-primary/20" />
                      <div className="relative h-5 w-5 animate-spin rounded-full border-2 border-primary border-t-transparent" />
                    </div>
                    <div className="min-w-0">
                      <p className="text-sm font-medium truncate">
                        {_formatStatus(streamStatus)}
                      </p>
                    </div>
                  </div>
                </div>
            )}
            <div ref={bottomRef} />
          </div>
        )}
      </div>

      {/* Follow-up suggestions */}
      {suggestions.length > 0 && !isStreaming && messages.length > 0 && (
        <div className="border-t bg-card/50 px-4 py-3">
          <div className="mx-auto max-w-4xl">
            <p className="mb-2 text-xs font-medium text-muted-foreground">Suggestions</p>
            <div className="flex flex-wrap gap-2">
              {suggestions.map((s, i) => (
                <button
                  key={i}
                  onClick={() => sendMessage(s)}
                  className="rounded-lg border bg-background px-3 py-1.5 text-xs text-foreground transition-colors hover:bg-accent"
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Input */}
      <div className="border-t bg-card p-4">
        <div className="mx-auto max-w-4xl">
          <ChatInput onSend={sendMessage} isStreaming={isStreaming} />
        </div>
      </div>

      {/* Report side sheet */}
      <ReportSheet
        conversationId={activeConversationId}
        open={reportOpen}
        onClose={() => setReportOpen(false)}
      />

      {/* Notebook draft sheet */}
      <NotebookDraftSheet
        conversationId={activeConversationId}
        open={notebookDraftOpen}
        onClose={() => setNotebookDraftOpen(false)}
        onSaved={(id) => {
          setNotebookDraftOpen(false);
          navigate(`/notebooks/${id}`);
        }}
      />
    </div>
  );
}

function EmptyState({ onSuggestionClick }: { onSuggestionClick: (message: string) => void }) {
  const navigate = useNavigate();
  const { connections, activeConnectionId } = useConnectionsStore();
  const hasConnection = connections.length > 0 || activeConnectionId;
  const [dynamicSuggestions, setDynamicSuggestions] = useState<string[]>([]);
  const [loadingSuggestions, setLoadingSuggestions] = useState(false);

  // Fetch schema-aware suggestions when connection is active
  useEffect(() => {
    if (activeConnectionId) {
      setLoadingSuggestions(true);
      api.getSuggestions(activeConnectionId)
        .then((s) => setDynamicSuggestions(s))
        .catch(() => {})
        .finally(() => setLoadingSuggestions(false));
    }
  }, [activeConnectionId]);

  const displaySuggestions = dynamicSuggestions.length > 0 ? dynamicSuggestions : SUGGESTIONS;

  return (
    <div className="flex h-full flex-1 flex-col items-center justify-center py-24">
      <div className="mb-6 flex h-16 w-16 items-center justify-center rounded-2xl bg-primary/10">
        <BarChart3 className="h-8 w-8 text-primary" />
      </div>
      <h2 className="mb-2 text-xl font-semibold">Welcome to Ceaser</h2>
      {hasConnection ? (
        <>
          <p className="mb-8 max-w-md text-center text-muted-foreground">
            Start by asking a question about your data. Your connected sources
            are ready for AI-powered insights.
          </p>
          <div className="grid max-w-lg gap-3">
            {loadingSuggestions ? (
              <div className="flex items-center justify-center py-4">
                <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
              </div>
            ) : (
              displaySuggestions.map((suggestion) => (
                <button
                  key={suggestion}
                  onClick={() => onSuggestionClick(suggestion)}
                  className="flex items-start gap-3 rounded-lg border bg-card p-3 text-left text-sm transition-colors hover:bg-accent"
                >
                  <MessageSquare className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground" />
                  <span>{suggestion}</span>
                </button>
              ))
            )}
          </div>
        </>
      ) : (
        <>
          <p className="mb-6 max-w-md text-center text-muted-foreground">
            Get started in 2 minutes — connect your database or upload an Excel file.
          </p>

          {/* Quick start cards */}
          <div className="grid max-w-lg gap-4 md:grid-cols-2">
            <button
              onClick={() => navigate("/connections")}
              className="flex flex-col items-center gap-3 rounded-xl border bg-card p-6 text-center transition-all hover:border-primary/50 hover:shadow-lg"
            >
              <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-primary/10">
                <Database className="h-6 w-6 text-primary" />
              </div>
              <div>
                <p className="font-medium">Connect Database</p>
                <p className="mt-1 text-xs text-muted-foreground">PostgreSQL, MySQL, SQLite</p>
              </div>
            </button>
            <button
              onClick={() => navigate("/files")}
              className="flex flex-col items-center gap-3 rounded-xl border bg-card p-6 text-center transition-all hover:border-primary/50 hover:shadow-lg"
            >
              <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-emerald-500/10">
                <FileSpreadsheet className="h-6 w-6 text-emerald-400" />
              </div>
              <div>
                <p className="font-medium">Upload Excel / CSV</p>
                <p className="mt-1 text-xs text-muted-foreground">Drag & drop or click to upload</p>
              </div>
            </button>
          </div>

          <p className="mt-6 text-xs text-muted-foreground">
            Need help? Check the <a href="/setup" className="text-primary underline">Setup Guide</a>
          </p>
        </>
      )}
    </div>
  );
}

const SUGGESTIONS = [
  "Show me the top 10 customers by revenue this quarter",
  "What are the trends in monthly active users over the past year?",
  "Create a breakdown of sales by product category",
  "Find any anomalies in the transaction data from last month",
];
