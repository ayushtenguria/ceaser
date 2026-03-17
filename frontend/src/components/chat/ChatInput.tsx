import { useState, useRef, useCallback, type KeyboardEvent } from "react";
import {
  ArrowUp, Paperclip, Link2, SlidersHorizontal, Bot, Atom,
  Loader2, X, ChevronDown,
} from "lucide-react";
import * as api from "@/lib/api";
import { Button } from "@/components/ui/button";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import { useConnectionsStore } from "@/store/connections";
import { cn } from "@/lib/utils";

interface ChatInputProps {
  onSend: (message: string, fileId?: string) => void;
  isStreaming: boolean;
}

export default function ChatInput({ onSend, isStreaming }: ChatInputProps) {
  const [value, setValue] = useState("");
  const [attachedFile, setAttachedFile] = useState<{ id: string; name: string } | null>(null);
  const [isUploadingFile, setIsUploadingFile] = useState(false);
  const [advancedReasoning, setAdvancedReasoning] = useState(false);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const { connections, activeConnectionId, setActiveConnection } = useConnectionsStore();

  const activeConnection = connections.find((c) => c.id === activeConnectionId);

  const handleSubmit = useCallback(() => {
    const trimmed = value.trim();
    if (!trimmed || isStreaming) return;
    onSend(trimmed, attachedFile?.id);
    setValue("");
    setAttachedFile(null);
    if (textareaRef.current) textareaRef.current.style.height = "auto";
  }, [value, isStreaming, onSend, attachedFile]);

  const handleKeyDown = useCallback(
    (e: KeyboardEvent<HTMLTextAreaElement>) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        handleSubmit();
      }
    },
    [handleSubmit]
  );

  const handleInput = useCallback(() => {
    const textarea = textareaRef.current;
    if (!textarea) return;
    textarea.style.height = "auto";
    textarea.style.height = `${Math.min(textarea.scrollHeight, 200)}px`;
  }, []);

  const handleFileChange = useCallback(async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    e.target.value = "";
    setIsUploadingFile(true);
    try {
      const uploaded = await api.uploadFile(file);
      setAttachedFile({ id: uploaded.id, name: uploaded.filename });
    } catch {} finally {
      setIsUploadingFile(false);
    }
  }, []);

  return (
    <div className="relative flex flex-col gap-1.5">
      {/* Attached file chip */}
      {attachedFile && (
        <div className="flex items-center gap-2 rounded-md border bg-muted/50 px-2.5 py-1 text-xs">
          <Paperclip className="h-3 w-3 text-muted-foreground" />
          <span className="truncate max-w-[200px]">{attachedFile.name}</span>
          <button onClick={() => setAttachedFile(null)} className="text-muted-foreground hover:text-foreground">
            <X className="h-3 w-3" />
          </button>
        </div>
      )}
      {isUploadingFile && (
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <Loader2 className="h-3 w-3 animate-spin" />
          Uploading file...
        </div>
      )}

      {/* Main input area */}
      <div className="rounded-xl border bg-background focus-within:ring-2 focus-within:ring-ring">
        {/* Textarea */}
        <div className="flex items-end px-3 py-2">
          <textarea
            ref={textareaRef}
            value={value}
            onChange={(e) => setValue(e.target.value)}
            onKeyDown={handleKeyDown}
            onInput={handleInput}
            placeholder="Send a message..."
            disabled={isStreaming}
            rows={1}
            className="max-h-[200px] flex-1 resize-none bg-transparent py-1 text-sm outline-none placeholder:text-muted-foreground disabled:opacity-50"
          />
        </div>

        {/* Toolbar */}
        <div className="flex items-center justify-between border-t px-2 py-1.5">
          <div className="flex items-center gap-0.5">
            {/* Attach file */}
            <Button
              variant="ghost"
              size="sm"
              className="h-7 w-7 px-0 text-muted-foreground hover:text-foreground"
              onClick={() => fileInputRef.current?.click()}
              disabled={isStreaming}
            >
              <Paperclip className="h-4 w-4" />
            </Button>

            {/* Connectors */}
            <Select
              value={activeConnectionId || "none"}
              onValueChange={(v) => setActiveConnection(v === "none" ? null : v)}
            >
              <SelectTrigger className="h-7 w-auto gap-1 border-0 bg-transparent px-2 text-xs text-muted-foreground hover:text-foreground [&>svg:last-child]:h-3 [&>svg:last-child]:w-3">
                <Link2 className="h-3.5 w-3.5 shrink-0" />
                <span>{activeConnection?.name || "Connectors"}</span>
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="none">No connection</SelectItem>
                {connections.map((conn) => (
                  <SelectItem key={conn.id} value={conn.id}>
                    <span className="flex items-center gap-2">
                      <span className={cn(
                        "h-1.5 w-1.5 rounded-full",
                        conn.isConnected ? "bg-emerald-500" : "bg-red-500"
                      )} />
                      {conn.name}
                    </span>
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>

            {/* Tools */}
            <Select value="auto" onValueChange={() => {}}>
              <SelectTrigger className="h-7 w-auto gap-1 border-0 bg-transparent px-2 text-xs text-muted-foreground hover:text-foreground [&>svg:last-child]:h-3 [&>svg:last-child]:w-3">
                <SlidersHorizontal className="h-3.5 w-3.5 shrink-0" />
                <span>Tools</span>
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="auto">Auto (SQL + Python + Charts)</SelectItem>
                <SelectItem value="sql">SQL Only</SelectItem>
                <SelectItem value="python">Python Only</SelectItem>
              </SelectContent>
            </Select>

            {/* Agent */}
            <Select value="auto" onValueChange={() => {}}>
              <SelectTrigger className="h-7 w-auto gap-1 border-0 bg-transparent px-2 text-xs text-muted-foreground hover:text-foreground [&>svg:last-child]:h-3 [&>svg:last-child]:w-3">
                <Bot className="h-3.5 w-3.5 shrink-0" />
                <span>Agent</span>
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="auto">Auto Router</SelectItem>
                <SelectItem value="analyst">Data Analyst</SelectItem>
                <SelectItem value="sql">SQL Agent</SelectItem>
              </SelectContent>
            </Select>

            {/* Advanced Reasoning — toggle */}
            <button
              onClick={() => setAdvancedReasoning((v) => !v)}
              className={cn(
                "flex h-7 items-center gap-1.5 rounded-md px-2 text-xs transition-colors",
                advancedReasoning
                  ? "bg-primary/15 text-primary"
                  : "text-muted-foreground hover:text-foreground"
              )}
            >
              <Atom className="h-3.5 w-3.5" />
              <span>Advanced Reasoning</span>
            </button>
          </div>

          {/* Send button */}
          <Button
            size="icon"
            className="h-7 w-7 shrink-0 rounded-lg"
            onClick={handleSubmit}
            disabled={!value.trim() || isStreaming}
          >
            <ArrowUp className="h-3.5 w-3.5" />
          </Button>
        </div>
      </div>

      {/* Hidden file input */}
      <input
        ref={fileInputRef}
        type="file"
        accept=".csv,.xlsx,.xls,.json,.parquet"
        className="hidden"
        onChange={handleFileChange}
      />

      <p className="px-1 text-center text-xs text-muted-foreground">
        Ceaser can make mistakes. Verify important results.
      </p>
    </div>
  );
}
