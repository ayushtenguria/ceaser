import { useState, useRef, useCallback, type KeyboardEvent } from "react";
import { ArrowUp, Paperclip, Database, Loader2 } from "lucide-react";
import * as api from "@/lib/api";
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
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
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const { connections, activeConnectionId } = useConnectionsStore();

  const activeConnection = connections.find(
    (c) => c.id === activeConnectionId
  );

  const handleSubmit = useCallback(() => {
    const trimmed = value.trim();
    if (!trimmed || isStreaming) return;

    onSend(trimmed, attachedFile?.id);
    setValue("");
    setAttachedFile(null);

    // Reset textarea height
    if (textareaRef.current) {
      textareaRef.current.style.height = "auto";
    }
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

  const handleFileClick = useCallback(() => {
    fileInputRef.current?.click();
  }, []);

  const handleFileChange = useCallback(async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    e.target.value = "";

    setIsUploadingFile(true);
    try {
      const uploaded = await api.uploadFile(file);
      setAttachedFile({ id: uploaded.id, name: uploaded.filename });
    } catch {
      // Could show error toast
    } finally {
      setIsUploadingFile(false);
    }
  }, []);

  return (
    <TooltipProvider delayDuration={0}>
      <div className="relative flex flex-col gap-2">
        {/* Active connection indicator */}
        {activeConnection && (
          <div className="flex items-center gap-1.5 px-1 text-xs text-muted-foreground">
            <Database className="h-3 w-3" />
            <span>
              Connected to{" "}
              <span className="font-medium text-foreground">
                {activeConnection.name}
              </span>
            </span>
            <span
              className={cn(
                "h-1.5 w-1.5 rounded-full",
                activeConnection.isConnected ? "bg-emerald-500" : "bg-red-500"
              )}
            />
          </div>
        )}

        {/* Input area */}
        <div className="flex items-end gap-2 rounded-xl border bg-background p-2 focus-within:ring-2 focus-within:ring-ring">
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                size="icon"
                className="h-8 w-8 shrink-0"
                onClick={handleFileClick}
                disabled={isStreaming}
              >
                <Paperclip className="h-4 w-4" />
              </Button>
            </TooltipTrigger>
            <TooltipContent>Attach file</TooltipContent>
          </Tooltip>

          <textarea
            ref={textareaRef}
            value={value}
            onChange={(e) => setValue(e.target.value)}
            onKeyDown={handleKeyDown}
            onInput={handleInput}
            placeholder="Ask a question about your data..."
            disabled={isStreaming}
            rows={1}
            className="max-h-[200px] flex-1 resize-none bg-transparent py-1.5 text-sm outline-none placeholder:text-muted-foreground disabled:opacity-50"
          />

          <Button
            size="icon"
            className="h-8 w-8 shrink-0 rounded-lg"
            onClick={handleSubmit}
            disabled={!value.trim() || isStreaming}
          >
            <ArrowUp className="h-4 w-4" />
          </Button>
        </div>

        {/* Attached file chip */}
        {attachedFile && (
          <div className="flex items-center gap-2 rounded-md border bg-muted/50 px-2 py-1 text-xs">
            <Paperclip className="h-3 w-3" />
            <span className="truncate max-w-[200px]">{attachedFile.name}</span>
            <button
              onClick={() => setAttachedFile(null)}
              className="text-muted-foreground hover:text-foreground"
            >
              ×
            </button>
          </div>
        )}
        {isUploadingFile && (
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <Loader2 className="h-3 w-3 animate-spin" />
            Uploading file...
          </div>
        )}

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
    </TooltipProvider>
  );
}
