import { useState, useRef, useCallback, useEffect, type KeyboardEvent } from "react";
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
  onSend: (message: string, fileId?: string, fileIds?: string[]) => void;
  isStreaming: boolean;
  preselectedFileIds?: string[];
}

export default function ChatInput({ onSend, isStreaming, preselectedFileIds }: ChatInputProps) {
  const [value, setValue] = useState("");
  const [attachedFile, setAttachedFile] = useState<{
    id: string; name: string; size: number; type: string;
    allFileIds?: string[];
    qualityIssues?: string[]; qualitySeverity?: string;
  } | null>(null);

  // Pre-attach files selected from the Files page
  useEffect(() => {
    if (!preselectedFileIds || preselectedFileIds.length === 0 || attachedFile) return;
    const ids = preselectedFileIds;
    // Set immediately with placeholder, then load names
    setAttachedFile({
      id: ids[ids.length - 1],
      allFileIds: ids.length > 1 ? ids : undefined,
      name: ids.length > 1 ? `${ids.length} files selected` : "Selected file",
      size: 0,
      type: "file",
    });
    // Load file details to get real names
    api.getFiles().then((files) => {
      const selected = files.filter((f) => ids.includes(f.id));
      if (selected.length > 0) {
        const totalSize = selected.reduce((s, f) => s + f.sizeBytes, 0);
        const ext = selected[0].filename.split(".").pop()?.toLowerCase() || "file";
        setAttachedFile({
          id: ids[ids.length - 1],
          allFileIds: ids.length > 1 ? ids : undefined,
          name: selected.length > 1
            ? `${selected.length} files (${selected.map((f) => f.filename).join(", ")})`
            : selected[0].filename,
          size: totalSize,
          type: ext,
        });
      }
    }).catch(() => {});
  }, [preselectedFileIds]);
  const [isUploadingFile, setIsUploadingFile] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploadStage, setUploadStage] = useState("");
  const [advancedReasoning, setAdvancedReasoning] = useState(false);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const connDropdownRef = useRef<HTMLDivElement>(null);
  const { connections, activeConnectionId, activeConnectionIds, setActiveConnection, setActiveConnectionIds, toggleConnectionId } = useConnectionsStore();
  const [connDropdownOpen, setConnDropdownOpen] = useState(false);

  useEffect(() => {
    if (!connDropdownOpen) return;
    const handler = (e: MouseEvent) => {
      if (connDropdownRef.current && !connDropdownRef.current.contains(e.target as Node)) {
        setConnDropdownOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [connDropdownOpen]);

  const activeConnection = connections.find((c) => c.id === activeConnectionId);

  const handleSubmit = useCallback(() => {
    const trimmed = value.trim();
    if (!trimmed || isStreaming) return;
    onSend(trimmed, attachedFile?.id, attachedFile?.allFileIds);
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

  const [pendingFileName, setPendingFileName] = useState("");
  const [pendingFileSize, setPendingFileSize] = useState(0);
  const [pendingFileType, setPendingFileType] = useState("");

  const handleFileChange = useCallback(async (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (!files || files.length === 0) return;

    const fileList = Array.from(files);
    e.target.value = "";

    if (fileList.length === 0 || !fileList[0]) return;
    const firstFile = fileList[0];
    const ext = firstFile.name.split(".").pop()?.toLowerCase() || "file";
    setPendingFileName(fileList.length > 1 ? `${fileList.length} files` : firstFile.name);
    setPendingFileSize(fileList.reduce((s, f) => s + f.size, 0));
    setPendingFileType(ext);
    setIsUploadingFile(true);
    setUploadProgress(10);
    setUploadStage("Uploading");

    try {
      let lastUploaded: any = null;
      const allUploadedIds: string[] = [];
      const totalFiles = fileList.length;

      for (let i = 0; i < totalFiles; i++) {
        const file = fileList[i];
        const pctBase = Math.round((i / totalFiles) * 80) + 10;

        setUploadProgress(pctBase);
        setUploadStage(totalFiles > 1 ? `Uploading ${file.name} (${i + 1}/${totalFiles})` : "Uploading");

        lastUploaded = await api.uploadFile(file);
        allUploadedIds.push(lastUploaded.id);

        setUploadProgress(pctBase + Math.round(40 / totalFiles));
        setUploadStage(totalFiles > 1 ? `Processing ${file.name}` : "Analyzing");
      }

      setUploadProgress(95);
      setUploadStage("Ready");
      await new Promise((r) => setTimeout(r, 300));
      setUploadProgress(100);

      const qr = lastUploaded?.excelMetadata?.quality_report;
      setAttachedFile({
        id: lastUploaded.id,
        allFileIds: allUploadedIds.length > 1 ? allUploadedIds : undefined,
        name: fileList.length > 1 ? `${fileList.length} files (${fileList.map(f => f.name).join(", ")})` : lastUploaded.filename || firstFile.name,
        size: fileList.reduce((s, f) => s + f.size, 0),
        type: ext,
        qualityIssues: qr?.items || [],
        qualitySeverity: qr?.severity || "clean",
      });
    } catch (err) {
      console.error("[ChatInput] File upload failed:", err);
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
            <div className="relative" ref={connDropdownRef}>
              <button
                onClick={() => setConnDropdownOpen((v) => !v)}
                className="flex h-7 items-center gap-1 rounded-md px-2 text-xs text-muted-foreground hover:text-foreground"
              >
                <Link2 className="h-3.5 w-3.5 shrink-0" />
                <span className="max-w-[200px] truncate">
                  {activeConnectionIds.length > 1
                    ? `${activeConnectionIds.length} databases`
                    : activeConnection?.name || "Connectors"}
                </span>
                <ChevronDown className="h-3 w-3" />
              </button>
              {connDropdownOpen && (
                <div className="absolute bottom-full left-0 mb-1 min-w-[14rem] max-w-[22rem] w-max rounded-lg border bg-popover p-1 shadow-lg z-50">
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
                            "h-3.5 w-3.5 rounded border flex items-center justify-center shrink-0",
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
                            "h-3.5 w-3.5 rounded border flex items-center justify-center shrink-0",
                            activeConnectionIds.includes(conn.id) ? "bg-primary border-primary" : "border-muted-foreground"
                          )}>
                            {activeConnectionIds.includes(conn.id) && <span className="text-[8px] text-primary-foreground">&#10003;</span>}
                          </div>
                          <span className={cn("h-1.5 w-1.5 rounded-full shrink-0", conn.isConnected ? "bg-emerald-500" : "bg-red-500")} />
                          <span className="truncate">{conn.name}</span>
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
        multiple
        className="hidden"
        onChange={handleFileChange}
      />

      <p className="px-1 text-center text-xs text-muted-foreground">
        Ceaser can make mistakes. Verify important results.
      </p>
    </div>
  );
}



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
  file: { id: string; name: string; size: number; type: string; qualityIssues?: string[]; qualitySeverity?: string } | null;
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
  const issues = file?.qualityIssues || [];
  const severity = file?.qualitySeverity || "clean";
  const hasIssues = done && issues.length > 0 && severity !== "clean";

  return (
    <div className="relative overflow-hidden rounded-lg border bg-card">
      <div className="flex items-center gap-3 px-3 py-2.5">
        {/* File type icon */}
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

        {/* Remove button */}
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

      {/* Data quality warnings */}
      {hasIssues && (
        <div className="border-t border-amber-500/20 bg-amber-500/5 px-3 py-2">
          <p className="mb-1 flex items-center gap-1.5 text-xs font-medium text-amber-400">
            <svg className="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.999L13.732 4.001c-.77-1.333-2.694-1.333-3.464 0L3.34 16.001C2.57 17.334 3.532 19.001 5.072 19.001z" />
            </svg>
            {issues.length} data quality {issues.length === 1 ? "warning" : "warnings"}
          </p>
          <ul className="space-y-0.5">
            {issues.slice(0, 3).map((item, i) => (
              <li key={i} className="text-xs text-muted-foreground truncate">{item}</li>
            ))}
            {issues.length > 3 && (
              <li className="text-xs text-muted-foreground/60">+{issues.length - 3} more</li>
            )}
          </ul>
        </div>
      )}
    </div>
  );
}
