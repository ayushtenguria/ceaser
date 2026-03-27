import { useEffect, useState, useCallback, useRef } from "react";
import {
  FileUp,
  File,
  Trash2,
  Loader2,
  Upload,
  AlertTriangle,
  CheckCircle2,
  XCircle,
  ChevronRight,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import * as api from "@/lib/api";
import type { FileUpload } from "@/types";
import { formatBytes, formatRelativeTime } from "@/lib/utils";
import { cn } from "@/lib/utils";

export default function FilesPage() {
  const [files, setFiles] = useState<FileUpload[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isUploading, setIsUploading] = useState(false);
  const [isDragging, setIsDragging] = useState(false);
  const [deletingIds, setDeletingIds] = useState<Set<string>>(new Set());
  const fileInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const data = await api.getFiles();
        if (!cancelled) setFiles(data);
      } catch {
        // Silent fail
      } finally {
        if (!cancelled) setIsLoading(false);
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, []);

  const handleUpload = useCallback(async (file: File) => {
    setIsUploading(true);
    try {
      const uploaded = await api.uploadFile(file);
      setFiles((prev) => [uploaded, ...prev]);
    } catch {
      // Could show toast error here
    } finally {
      setIsUploading(false);
    }
  }, []);

  const handleFileChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      if (file) handleUpload(file);
      // Reset input so the same file can be selected again
      e.target.value = "";
    },
    [handleUpload]
  );

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setIsDragging(false);
      const file = e.dataTransfer.files?.[0];
      if (file) handleUpload(file);
    },
    [handleUpload]
  );

  const handleDelete = useCallback(async (id: string) => {
    setDeletingIds((prev) => new Set(prev).add(id));
    try {
      await api.deleteFile(id);
      setFiles((prev) => prev.filter((f) => f.id !== id));
    } catch {
      // Silent fail
    } finally {
      setDeletingIds((prev) => {
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
    }
  }, []);

  const getFileTypeColor = (fileType: string): string => {
    if (fileType.includes("csv")) return "text-emerald-400";
    if (fileType.includes("json")) return "text-yellow-400";
    if (fileType.includes("excel") || fileType.includes("spreadsheet"))
      return "text-green-400";
    if (fileType.includes("parquet")) return "text-purple-400";
    return "text-muted-foreground";
  };


  return (
    <div className="p-6">
      <div className="mb-6">
        <h2 className="text-2xl font-semibold">Files</h2>
        <p className="text-sm text-muted-foreground">
          Upload CSV, Excel, JSON, or Parquet files for AI analysis
        </p>
      </div>

      {/* Upload area */}
      <div
        onDragOver={(e) => {
          e.preventDefault();
          setIsDragging(true);
        }}
        onDragLeave={() => setIsDragging(false)}
        onDrop={handleDrop}
        className={cn(
          "mb-6 flex cursor-pointer flex-col items-center justify-center rounded-lg border-2 border-dashed p-12 transition-colors",
          isDragging
            ? "border-primary bg-primary/5"
            : "border-border hover:border-muted-foreground/50"
        )}
        onClick={() => fileInputRef.current?.click()}
      >
        {isUploading ? (
          <>
            <Loader2 className="mb-3 h-10 w-10 animate-spin text-primary" />
            <p className="text-sm font-medium">Uploading...</p>
          </>
        ) : (
          <>
            <Upload className="mb-3 h-10 w-10 text-muted-foreground" />
            <p className="text-sm font-medium">
              Drop files here or click to upload
            </p>
            <p className="mt-1 text-xs text-muted-foreground">
              Supports CSV, Excel, JSON, and Parquet files
            </p>
          </>
        )}
        <input
          ref={fileInputRef}
          type="file"
          accept=".csv,.xlsx,.xls,.json,.parquet"
          className="hidden"
          onChange={handleFileChange}
        />
      </div>

      {/* Files list */}
      {isLoading ? (
        <div className="space-y-3">
          {[1, 2, 3].map((i) => (
            <div
              key={i}
              className="h-16 animate-pulse rounded-lg border bg-card"
            />
          ))}
        </div>
      ) : files.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-16">
          <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-2xl bg-secondary">
            <FileUp className="h-8 w-8 text-muted-foreground" />
          </div>
          <h3 className="mb-1 text-lg font-medium">No files uploaded</h3>
          <p className="text-sm text-muted-foreground">
            Upload a data file to start analyzing it with AI
          </p>
        </div>
      ) : (
        <div className="space-y-2">
          {files.map((file) => (
            <FileCard
              key={file.id}
              file={file}
              getFileTypeColor={getFileTypeColor}
              onDelete={handleDelete}
              isDeleting={deletingIds.has(file.id)}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function FileCard({ file, getFileTypeColor, onDelete, isDeleting }: {
  file: FileUpload;
  getFileTypeColor: (t: string) => string;
  onDelete: (id: string) => void;
  isDeleting: boolean;
}) {
  const [showQuality, setShowQuality] = useState(false);
  const qr = file.excelMetadata?.quality_report;
  const info = file.columnInfo;
  const sheets = file.excelMetadata?.insight?.sheets;

  const qualityColor = !qr ? "text-muted-foreground/40" :
    qr.severity === "clean" ? "text-emerald-400" :
    qr.severity === "minor" ? "text-amber-400" : "text-red-400";

  const QualityIcon = !qr ? CheckCircle2 :
    qr.severity === "clean" ? CheckCircle2 :
    qr.severity === "minor" ? AlertTriangle : XCircle;

  return (
    <Card>
      <CardContent className="p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <File className={cn("h-5 w-5", getFileTypeColor(file.fileType))} />
            <div>
              <p className="text-sm font-medium">{file.filename}</p>
              <div className="flex items-center gap-2 text-xs text-muted-foreground">
                <span>{formatBytes(file.sizeBytes)}</span>
                {info && <span>{info.row_count.toLocaleString()} rows, {info.column_count} cols</span>}
                {sheets && sheets.length > 1 && <span>{sheets.length} sheets</span>}
                <span>{formatRelativeTime(file.uploadedAt)}</span>
              </div>
            </div>
          </div>
          <div className="flex items-center gap-2">
            {/* Quality badge */}
            {qr && qr.total_issues > 0 ? (
              <button
                onClick={() => setShowQuality(!showQuality)}
                className={cn("flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs transition-colors hover:bg-muted/50", qualityColor)}
              >
                <QualityIcon className="h-3 w-3" />
                {qr.total_issues} {qr.total_issues === 1 ? "issue" : "issues"}
                <ChevronRight className={cn("h-3 w-3 transition-transform", showQuality && "rotate-90")} />
              </button>
            ) : qr ? (
              <span className="flex items-center gap-1 text-xs text-emerald-400/70">
                <CheckCircle2 className="h-3 w-3" /> Clean
              </span>
            ) : null}

            <Badge variant="outline" className="text-xs">
              {file.fileType}
            </Badge>
            <Button
              variant="ghost"
              size="icon"
              className="h-8 w-8 text-muted-foreground hover:text-destructive"
              onClick={() => onDelete(file.id)}
              disabled={isDeleting}
            >
              {isDeleting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Trash2 className="h-4 w-4" />}
            </Button>
          </div>
        </div>

        {/* Expandable quality details */}
        {showQuality && qr && qr.items.length > 0 && (
          <div className="mt-3 rounded-lg border border-amber-500/20 bg-amber-500/5 px-3 py-2">
            <p className="mb-1.5 text-xs font-medium text-amber-400">Data Quality Warnings</p>
            <ul className="space-y-1">
              {qr.items.map((item, i) => (
                <li key={i} className="flex items-start gap-2 text-xs text-muted-foreground">
                  <AlertTriangle className="mt-0.5 h-3 w-3 shrink-0 text-amber-400/60" />
                  {item}
                </li>
              ))}
            </ul>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
