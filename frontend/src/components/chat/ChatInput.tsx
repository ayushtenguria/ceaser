import { useState, useRef, useCallback, type KeyboardEvent } from "react";
import {
  ArrowUp, Paperclip, Link2, SlidersHorizontal, Bot, Atom,
  Loader2, X, ChevronDown, FileSpreadsheet, FileText, File,
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
  const [attachedFile, setAttachedFile] = useState<{ id: string; name: string; size: number; type: string } | null>(null);
  const [isUploadingFile, setIsUploadingFile] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploadStage, setUploadStage] = useState("");
  const [advancedReasoning, setAdvancedReasoning] = useState(false);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const { connections, activeConnectionId, activeConnectionIds, setActiveConnection, setActiveConnectionIds, toggleConnectionId } = useConnectionsStore();
  const [connDropdownOpen, setConnDropdownOpen] = useState(false);

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

  // File name + extension available immediately on select (before upload completes)
  const [pendingFileName, setPendingFileName] = useState("");
  const [pendingFileSize, setPendingFileSize] = useState(0);
  const [pendingFileType, setPendingFileType] = useState("");

  const handleFileChange = useCallback(async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    e.target.value = "";

    const ext = file.name.split(".").pop()?.toLowerCase() || "file";
    setPendingFileName(file.name);
    setPendingFileSize(file.size);
    setPendingFileType(ext);
    setIsUploadingFile(true);
    setUploadProgress(10);
    setUploadStage("Uploading");

    try {
      // Stage 1: Upload (10→40%)
      setUploadProgress(20);
      setUploadStage("Uploading");
      const uploaded = await api.uploadFile(file);

      // Stage 2: Parsing (40→60%)
      setUploadProgress(50);
      setUploadStage("Parsing sheets");

      // Stage 3: Analyzing (60→80%)
      setUploadProgress(70);
      setUploadStage("Analyzing relationships");

      // Stage 4: Ready (80→100%)
      setUploadProgress(90);
      setUploadStage("Preparing for chat");

      // Small delay to show final stage
      await new Promise((r) => setTimeout(r, 300));
      setUploadProgress(100);
      setUploadStage("Ready");

      setAttachedFile({
        id: uploaded.id,
        name: uploaded.filename || file.name,
        size: file.size,
        type: ext,
      });
    } catch {
      setUploadProgress(0);
      setUploadStage("");
      setPendingFileName("");
    } finally {
      setIsUploadingFile(false);
    }
  }, []);

  return (
    <div className="relative flex flex-col gap-1.5">
      {/* Attached file card — Julius style */}
      {(attachedFile || isUploadingFile) && (
        <FileCard
          file={attachedFile}
          isUploading={isUploadingFile}
          progress={uploadProgress}
          stage={uploadStage}
          pendingName={pendingFileName}
          pendingSize={pendingFileSize}
          pendingType={pendingFileType}
          onRemove={() => { setAttachedFile(null); setPendingFileName(""); }}
        />
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

            {/* Connector selector — multi-select */}
            <div className="relative">
              <button
                onClick={() => setConnDropdownOpen((v) => !v)}
                className="flex h-7 items-center gap-1 rounded-md px-2 text-xs text-muted-foreground hover:text-foreground"
              >
                <Link2 className="h-3.5 w-3.5 shrink-0" />
                <span>
                  {activeConnectionIds.length > 1
                    ? `${activeConnectionIds.length} databases`
                    : activeConnection?.name || "Connectors"}
                </span>
                <ChevronDown className="h-3 w-3" />
              </button>
              {connDropdownOpen && (
                <div className="absolute bottom-full left-0 mb-1 w-56 rounded-lg border bg-popover p-1 shadow-lg z-50">
                  {connections.length === 0 ? (
                    <p className="px-2 py-1.5 text-xs text-muted-foreground">No connections</p>
                  ) : (
                    <>
                      {connections.length > 1 && (
                        <button
                          onClick={() => {
                            const allIds = connections.map((c) => c.id);
                            setActiveConnectionIds(
                              activeConnectionIds.length === connections.length ? [] : allIds
                            );
                          }}
                          className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-xs hover:bg-accent"
                        >
                          <div className={cn(
                            "h-3.5 w-3.5 rounded border flex items-center justify-center",
                            activeConnectionIds.length === connections.length ? "bg-primary border-primary" : "border-muted-foreground"
                          )}>
                            {activeConnectionIds.length === connections.length && <span className="text-[8px] text-primary-foreground">&#10003;</span>}
                          </div>
                          All Databases ({connections.length})
                        </button>
                      )}
                      {connections.map((conn) => (
                        <button
                          key={conn.id}
                          onClick={() => toggleConnectionId(conn.id)}
                          className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-xs hover:bg-accent"
                        >
                          <div className={cn(
                            "h-3.5 w-3.5 rounded border flex items-center justify-center",
                            activeConnectionIds.includes(conn.id) ? "bg-primary border-primary" : "border-muted-foreground"
                          )}>
                            {activeConnectionIds.includes(conn.id) && <span className="text-[8px] text-primary-foreground">&#10003;</span>}
                          </div>
                          <span className={cn("h-1.5 w-1.5 rounded-full", conn.isConnected ? "bg-emerald-500" : "bg-red-500")} />
                          {conn.name}
                        </button>
                      ))}
                    </>
                  )}
                </div>
              )}
            </div>

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


// ---------------------------------------------------------------------------
// File Card Component — Julius-style file attachment display
// ---------------------------------------------------------------------------

const FILE_ICONS: Record<string, { icon: typeof FileSpreadsheet; color: string; label: string }> = {
  xlsx: { icon: FileSpreadsheet, color: "text-emerald-400", label: "spreadsheet" },
  xls:  { icon: FileSpreadsheet, color: "text-emerald-400", label: "spreadsheet" },
  csv:  { icon: FileText, color: "text-sky-400", label: "CSV" },
  json: { icon: FileText, color: "text-amber-400", label: "JSON" },
  parquet: { icon: File, color: "text-purple-400", label: "parquet" },
};

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

function FileCard({
  file,
  isUploading,
  progress,
  stage,
  pendingName,
  pendingSize,
  pendingType,
  onRemove,
}: {
  file: { id: string; name: string; size: number; type: string } | null;
  isUploading: boolean;
  progress: number;
  stage: string;
  pendingName: string;
  pendingSize: number;
  pendingType: string;
  onRemove: () => void;
}) {
  const ext = file?.type || pendingType || "file";
  const name = file?.name || pendingName || "File";
  const size = file?.size || pendingSize || 0;
  const meta = FILE_ICONS[ext] || { icon: File, color: "text-muted-foreground", label: ext };
  const Icon = meta.icon;
  const done = !isUploading && file;

  return (
    <div className="relative overflow-hidden rounded-lg border bg-card">
      <div className="flex items-center gap-3 px-3 py-2.5">
        {/* File type icon — always shows the real icon, never a spinner */}
        <div className={cn("flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-muted/50", meta.color)}>
          <Icon className="h-5 w-5" />
        </div>

        {/* File info */}
        <div className="min-w-0 flex-1">
          <p className="truncate text-sm font-medium">{name}</p>
          <p className="text-xs text-muted-foreground">
            {isUploading
              ? `${stage}... ${progress}%`
              : `${meta.label} — ${formatFileSize(size)}`
            }
          </p>
        </div>

        {/* Remove button — always visible */}
        <button
          onClick={onRemove}
          className="shrink-0 rounded-md p-1 text-muted-foreground hover:bg-muted hover:text-foreground"
        >
          <X className="h-4 w-4" />
        </button>
      </div>

      {/* Progress bar — only during processing */}
      {isUploading && (
        <div className="h-1 w-full bg-muted">
          <div
            className={cn(
              "h-full transition-all duration-500 ease-out",
              progress >= 100 ? "bg-emerald-500" : "bg-primary"
            )}
            style={{ width: `${progress}%` }}
          />
        </div>
      )}
    </div>
  );
}
