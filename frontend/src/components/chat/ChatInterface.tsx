import { useEffect, useRef } from "react";
import { BarChart3, MessageSquare } from "lucide-react";
import { ScrollArea } from "@/components/ui/scroll-area";
import ChatInput from "@/components/chat/ChatInput";
import MessageBubble from "@/components/chat/MessageBubble";
import { useChat } from "@/hooks/useChat";

export default function ChatInterface() {
  const { messages, isStreaming, sendMessage } = useChat();
  const scrollRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  return (
    <div className="flex h-full flex-col">
      {/* Messages */}
      <ScrollArea className="flex-1">
        <div ref={scrollRef} className="flex flex-col">
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
            </div>
          )}
        </div>
      </ScrollArea>

      {/* Input */}
      <div className="border-t bg-card p-4">
        <div className="mx-auto max-w-4xl">
          <ChatInput onSend={sendMessage} isStreaming={isStreaming} />
        </div>
      </div>
    </div>
  );
}

function EmptyState({ onSuggestionClick }: { onSuggestionClick: (message: string) => void }) {
  return (
    <div className="flex h-full flex-1 flex-col items-center justify-center py-24">
      <div className="mb-6 flex h-16 w-16 items-center justify-center rounded-2xl bg-primary/10">
        <BarChart3 className="h-8 w-8 text-primary" />
      </div>
      <h2 className="mb-2 text-xl font-semibold">Welcome to Ceaser</h2>
      <p className="mb-8 max-w-md text-center text-muted-foreground">
        Start by asking a question about your data. Connect a database or upload
        a file to get insights powered by AI.
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
    </div>
  );
}

const SUGGESTIONS = [
  "Show me the top 10 customers by revenue this quarter",
  "What are the trends in monthly active users over the past year?",
  "Create a breakdown of sales by product category",
  "Find any anomalies in the transaction data from last month",
];
