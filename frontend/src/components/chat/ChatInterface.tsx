import { useEffect, useRef, useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { BarChart3, BookMarked, Database, FileText, Loader2, MessageSquare } from "lucide-react";
import { Button } from "@/components/ui/button";
import ChatInput from "@/components/chat/ChatInput";
import MessageBubble from "@/components/chat/MessageBubble";
import ReportSheet from "@/components/chat/ReportSheet";
import { useChat } from "@/hooks/useChat";
import { useConnectionsStore } from "@/store/connections";
import * as api from "@/lib/api";

export default function ChatInterface() {
  const navigate = useNavigate();
  const { messages, isStreaming, sendMessage, suggestions, activeConversationId } = useChat();
  const bottomRef = useRef<HTMLDivElement>(null);
  const [reportOpen, setReportOpen] = useState(false);
  const [savingNotebook, setSavingNotebook] = useState(false);

  const handleSaveAsNotebook = useCallback(async () => {
    if (!activeConversationId) return;
    setSavingNotebook(true);
    try {
      const result = await api.saveConversationAsNotebook(activeConversationId);
      navigate(`/notebooks/${result.notebookId}`);
    } catch {
    } finally {
      setSavingNotebook(false);
    }
  }, [activeConversationId, navigate]);

  // Auto-scroll to bottom on new messages or streaming updates
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, isStreaming]);

  return (
    <div className="flex h-full flex-col">
      {/* Report button — shown when there are messages */}
      {messages.length > 0 && !isStreaming && (
        <div className="flex items-center justify-end gap-2 border-b px-4 py-1.5">
          <Button
            variant="outline"
            size="sm"
            className="gap-1.5 text-xs"
            onClick={handleSaveAsNotebook}
            disabled={savingNotebook}
          >
            {savingNotebook ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <BookMarked className="h-3.5 w-3.5" />}
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
              <div className="flex items-center gap-2 px-4 text-sm text-muted-foreground">
                <div className="h-2 w-2 animate-pulse rounded-full bg-primary" />
                Thinking...
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
    </div>
  );
}

function EmptyState({ onSuggestionClick }: { onSuggestionClick: (message: string) => void }) {
  const navigate = useNavigate();
  const { connections, activeConnectionId } = useConnectionsStore();
  const hasConnection = connections.length > 0 || activeConnectionId;

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
            {SUGGESTIONS.map((suggestion) => (
              <button
                key={suggestion}
                onClick={() => onSuggestionClick(suggestion)}
                className="flex items-start gap-3 rounded-lg border bg-card p-3 text-left text-sm transition-colors hover:bg-accent"
              >
                <MessageSquare className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground" />
                <span>{suggestion}</span>
              </button>
            ))}
          </div>
        </>
      ) : (
        <>
          <p className="mb-6 max-w-md text-center text-muted-foreground">
            Connect a database or upload a file to get started with AI-powered
            data insights.
          </p>
          <div className="flex gap-3">
            <Button variant="outline" onClick={() => navigate("/connections")}>
              <Database className="mr-2 h-4 w-4" />
              Connect Database
            </Button>
            <Button variant="outline" onClick={() => navigate("/files")}>
              <MessageSquare className="mr-2 h-4 w-4" />
              Upload File
            </Button>
          </div>
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
