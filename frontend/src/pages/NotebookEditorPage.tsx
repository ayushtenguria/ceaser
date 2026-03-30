import { useEffect, useState, useCallback, useRef } from "react";
import { useParams } from "react-router-dom";
import {
  Play, Loader2, Trash2, GripVertical, FileText, FileUp,
  TextCursorInput, Bot, Code2, Clock, ChevronRight, Plus, Upload,
} from "lucide-react";
import ReactMarkdown from "react-markdown";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import PlotlyChart from "@/components/visualizations/PlotlyChart";
import DataExplorer from "@/components/data/DataExplorer";
import * as api from "@/lib/api";
import { cn } from "@/lib/utils";

const CELL_TYPES = [
  { value: "text", label: "Text", icon: FileText, color: "text-muted-foreground" },
  { value: "file", label: "File Upload", icon: FileUp, color: "text-emerald-400" },
  { value: "input", label: "User Input", icon: TextCursorInput, color: "text-sky-400" },
  { value: "prompt", label: "AI Prompt", icon: Bot, color: "text-purple-400" },
  { value: "code", label: "Python Code", icon: Code2, color: "text-amber-400" },
];

export default function NotebookEditorPage() {
  const { notebookId } = useParams<{ notebookId: string }>();
  const [notebook, setNotebook] = useState<any>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isRunning, setIsRunning] = useState(false);
  const [cellResults, setCellResults] = useState<Record<string, any>>({});
  const [runningCellId, setRunningCellId] = useState<string | null>(null);
  const [userInputs, setUserInputs] = useState<Record<string, any>>({});
  const [fileUploads, setFileUploads] = useState<Record<string, { id: string; name: string }>>({});
  const [activeTab, setActiveTab] = useState<"editor" | "history">("editor");
  const [runHistory, setRunHistory] = useState<any[]>([]);
  const [expandedRunId, setExpandedRunId] = useState<string | null>(null);
  const [addingCell, setAddingCell] = useState(false);

  const reload = useCallback(async () => {
    if (!notebookId) return;
    const nb = await api.getNotebook(notebookId);
    setNotebook(nb);
    return nb;
  }, [notebookId]);

  useEffect(() => {
    if (!notebookId) return;
    reload().finally(() => setIsLoading(false));
    api.getNotebookRuns(notebookId).then(setRunHistory).catch(() => {});
  }, [notebookId, reload]);

  const handleAddCell = useCallback(async (cellType: string) => {
    if (!notebookId || addingCell) return;
    setAddingCell(true);
    try {
      await api.addNotebookCell(notebookId, {
        cellType,
        content: cellType === "text" ? "## New Section" : cellType === "prompt" ? "Analyze the data" : "",
        config: cellType === "input" ? { inputType: "text", label: "Parameter", default: "" } : cellType === "file" ? { acceptedTypes: [".xlsx", ".csv"], description: "Upload data file" } : null,
      });
      await reload();
    } finally {
      setAddingCell(false);
    }
  }, [notebookId, reload, addingCell]);

  const handleDeleteCell = useCallback(async (cellId: string) => {
    if (!notebookId) return;
    await api.deleteNotebookCell(notebookId, cellId);
    await reload();
  }, [notebookId, reload]);

  const handleFileUpload = useCallback(async (cellId: string, file: File) => {
    try {
      const uploaded = await api.uploadFile(file);
      setFileUploads((prev) => ({ ...prev, [cellId]: { id: uploaded.id, name: uploaded.filename } }));
    } catch {
      // Silent
    }
  }, []);

  const handleRun = useCallback(async () => {
    if (!notebookId) return;
    setIsRunning(true);
    setCellResults({});

    // Build file mapping for run
    const files: Record<string, string> = {};
    for (const [cellId, upload] of Object.entries(fileUploads)) {
      files[cellId] = upload.id;
    }

    try {
      for await (const event of api.runNotebook(notebookId, userInputs, files)) {
        if (event.type === "cell_start") {
          setRunningCellId(event.cellId);
        } else if (event.type === "cell_complete") {
          setCellResults((prev) => ({ ...prev, [event.cellId]: event }));
          setRunningCellId(null);
        }
      }
      // Refresh run history
      api.getNotebookRuns(notebookId).then(setRunHistory).catch(() => {});
    } catch {} finally {
      setIsRunning(false);
      setRunningCellId(null);
    }
  }, [notebookId, userInputs, fileUploads]);

  if (isLoading || !notebook) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  const cells = notebook.cells || [];

  return (
    <div className="mx-auto max-w-4xl p-6">
      {/* Header */}
      <div className="mb-4 flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-semibold">{notebook.name}</h2>
          {notebook.description && <p className="text-sm text-muted-foreground">{notebook.description}</p>}
        </div>
        <Button onClick={handleRun} disabled={isRunning}>
          {isRunning ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Play className="mr-2 h-4 w-4" />}
          {isRunning ? "Running..." : "Run All"}
        </Button>
      </div>

      {/* Tabs */}
      <div className="mb-4 flex gap-1 border-b">
        <button
          onClick={() => setActiveTab("editor")}
          className={cn("px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors",
            activeTab === "editor" ? "border-primary text-primary" : "border-transparent text-muted-foreground hover:text-foreground")}
        >
          Editor ({cells.length} cells)
        </button>
        <button
          onClick={() => setActiveTab("history")}
          className={cn("flex items-center gap-1.5 px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors",
            activeTab === "history" ? "border-primary text-primary" : "border-transparent text-muted-foreground hover:text-foreground")}
        >
          <Clock className="h-3.5 w-3.5" />
          Run History ({runHistory.length})
        </button>
      </div>

      {/* Run History Tab */}
      {activeTab === "history" && (
        <div className="space-y-3">
          {runHistory.length === 0 ? (
            <div className="py-16 text-center">
              <Clock className="mx-auto mb-3 h-8 w-8 text-muted-foreground" />
              <p className="text-sm text-muted-foreground">No runs yet. Click "Run All" to execute.</p>
            </div>
          ) : (
            runHistory.map((run: any) => {
              const isExpanded = expandedRunId === run.id;
              return (
                <Card key={run.id} className="transition-colors">
                  <CardContent
                    className="flex items-center justify-between p-4 cursor-pointer hover:bg-muted/30"
                    onClick={() => setExpandedRunId(isExpanded ? null : run.id)}
                  >
                    <div className="flex items-center gap-3">
                      <div className={cn(
                        "h-2.5 w-2.5 rounded-full",
                        run.status === "completed" ? "bg-emerald-500" : run.status === "failed" ? "bg-destructive" : "bg-amber-500"
                      )} />
                      <div>
                        <p className="text-sm font-medium">
                          {new Date(run.startedAt || run.createdAt).toLocaleDateString()} at {new Date(run.startedAt || run.createdAt).toLocaleTimeString()}
                        </p>
                        <p className="text-xs text-muted-foreground">
                          {run.status} · {run.totalExecutionMs ? `${(run.totalExecutionMs / 1000).toFixed(1)}s` : "—"} · {run.cellResults?.length || 0} cells
                        </p>
                      </div>
                    </div>
                    <ChevronRight className={cn("h-4 w-4 text-muted-foreground transition-transform", isExpanded && "rotate-90")} />
                  </CardContent>

                  {/* Expanded cell results */}
                  {isExpanded && run.cellResults && run.cellResults.length > 0 && (
                    <div className="border-t px-4 pb-4 pt-2 space-y-3">
                      {run.cellResults.map((cr: any, idx: number) => (
                        <div key={cr.id || idx} className="rounded-lg border bg-muted/20 p-3">
                          <div className="flex items-center gap-2 mb-2">
                            <div className={cn(
                              "h-2 w-2 rounded-full",
                              cr.status === "success" ? "bg-emerald-500" : cr.status === "error" ? "bg-destructive" : "bg-muted-foreground"
                            )} />
                            <span className="text-xs font-medium">Cell {cr.cellOrder + 1}</span>
                            {cr.executionTimeMs > 0 && (
                              <span className="text-xs text-muted-foreground">{(cr.executionTimeMs / 1000).toFixed(1)}s</span>
                            )}
                          </div>

                          {/* Output text */}
                          {cr.outputText && (
                            <div className="prose prose-sm prose-invert max-w-none text-xs mb-2">
                              <ReactMarkdown>{cr.outputText}</ReactMarkdown>
                            </div>
                          )}

                          {/* Output table */}
                          {cr.outputTable && (
                            <div className="mb-2">
                              <DataExplorer data={cr.outputTable} compact />
                            </div>
                          )}

                          {/* Output chart */}
                          {cr.outputChart && (
                            <PlotlyChart figure={cr.outputChart} />
                          )}

                          {/* Output code */}
                          {cr.outputCode && (
                            <pre className="mt-2 rounded bg-muted/50 px-3 py-2 text-xs text-emerald-400 overflow-x-auto">
                              <code>{cr.outputCode}</code>
                            </pre>
                          )}

                          {/* Error */}
                          {cr.error && (
                            <div className="mt-2 rounded border border-destructive/50 bg-destructive/10 px-3 py-2 text-xs text-destructive">
                              {cr.error}
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </Card>
              );
            })
          )}
        </div>
      )}

      {/* Cells (Editor Tab) */}
      {activeTab === "editor" && (
      <>
      <div className="space-y-3">
        {cells.map((cell: any) => (
          <CellEditor
            key={cell.id}
            cell={cell}
            notebookId={notebookId!}
            result={cellResults[cell.id]}
            isRunning={runningCellId === cell.id}
            userInput={userInputs[cell.id] || ""}
            fileUpload={fileUploads[cell.id]}
            onDelete={() => handleDeleteCell(cell.id)}
            onInputChange={(val) => setUserInputs((prev) => ({ ...prev, [cell.id]: val }))}
            onFileUpload={(file) => handleFileUpload(cell.id, file)}
            onReload={reload}
          />
        ))}
      </div>

      {/* Add cell menu */}
      <div className="mt-4 flex items-center justify-center gap-2">
        {CELL_TYPES.map((ct) => (
          <Button key={ct.value} variant="outline" size="sm"
            onClick={() => handleAddCell(ct.value)}
            disabled={addingCell}
            className="gap-1.5 text-xs">
            {addingCell ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <ct.icon className={cn("h-3.5 w-3.5", ct.color)} />}
            {ct.label}
          </Button>
        ))}
      </div>
      </>
      )}
    </div>
  );
}


// ─── Cell Editor Component ──────────────────────────────────────────

function CellEditor({ cell, notebookId, result, isRunning, userInput, fileUpload, onDelete, onInputChange, onFileUpload, onReload }: {
  cell: any;
  notebookId: string;
  result: any;
  isRunning: boolean;
  userInput: string;
  fileUpload?: { id: string; name: string };
  onDelete: () => void;
  onInputChange: (val: string) => void;
  onFileUpload: (file: File) => void;
  onReload: () => Promise<any>;
}) {
  const meta = CELL_TYPES.find((t) => t.value === cell.cellType) || CELL_TYPES[0];
  const Icon = meta.icon;
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Local content state for debounced saving (prevents API call on every keystroke)
  const [localContent, setLocalContent] = useState(cell.content || "");
  const saveTimer = useRef<ReturnType<typeof setTimeout>>();

  // Sync local content when cell changes from server
  useEffect(() => {
    setLocalContent(cell.content || "");
  }, [cell.id, cell.content]);

  const handleContentChange = useCallback((value: string) => {
    setLocalContent(value);
    // Debounce: save after 500ms of no typing
    if (saveTimer.current) clearTimeout(saveTimer.current);
    saveTimer.current = setTimeout(async () => {
      try {
        await api.updateNotebookCell(notebookId, cell.id, {
          cellType: cell.cellType,
          content: value,
          config: cell.config,
          outputVariable: cell.outputVariable,
        });
      } catch {
        // Silent
      }
    }, 500);
  }, [notebookId, cell.id, cell.cellType, cell.config, cell.outputVariable]);

  return (
    <Card className={cn(
      "transition-all",
      isRunning && "ring-2 ring-primary",
      result?.status === "error" && "border-destructive/50",
      result?.status === "success" && "border-emerald-500/30",
    )}>
      <CardHeader className="flex flex-row items-center gap-2 py-2 px-4">
        <GripVertical className="h-4 w-4 text-muted-foreground cursor-grab" />
        <Icon className={cn("h-4 w-4", meta.color)} />
        <Badge variant="outline" className="text-xs">{meta.label}</Badge>
        {isRunning && <Loader2 className="h-3.5 w-3.5 animate-spin text-primary" />}
        <div className="flex-1" />
        <Button variant="ghost" size="icon" className="h-7 w-7 text-muted-foreground hover:text-destructive"
          onClick={onDelete}>
          <Trash2 className="h-3.5 w-3.5" />
        </Button>
      </CardHeader>

      <CardContent className="px-4 pb-4">
        {/* Cell editor based on type */}
        {cell.cellType === "text" && (
          <Textarea
            value={localContent}
            onChange={(e) => handleContentChange(e.target.value)}
            placeholder="Markdown text..."
            rows={3}
            className="text-sm"
          />
        )}
        {cell.cellType === "prompt" && (
          <Textarea
            value={localContent}
            onChange={(e) => handleContentChange(e.target.value)}
            placeholder="Ask the AI to analyze data..."
            rows={2}
            className="text-sm"
          />
        )}
        {cell.cellType === "code" && (
          <Textarea
            value={localContent}
            onChange={(e) => handleContentChange(e.target.value)}
            placeholder="Python code..."
            rows={4}
            className="font-mono text-sm"
          />
        )}
        {cell.cellType === "input" && (
          <div className="space-y-2">
            <p className="text-sm font-medium">{cell.config?.label || "Input"}</p>
            <Input
              placeholder={cell.config?.default || "Enter value..."}
              value={userInput}
              onChange={(e) => onInputChange(e.target.value)}
            />
          </div>
        )}
        {cell.cellType === "file" && (
          <div
            className={cn(
              "rounded-lg border-2 border-dashed p-6 text-center text-sm cursor-pointer transition-colors hover:border-primary/50",
              fileUpload ? "border-emerald-500/30 bg-emerald-500/5" : "text-muted-foreground"
            )}
            onClick={() => fileInputRef.current?.click()}
          >
            {fileUpload ? (
              <div className="flex items-center justify-center gap-2">
                <FileUp className="h-5 w-5 text-emerald-400" />
                <span className="font-medium text-emerald-400">{fileUpload.name}</span>
                <Badge variant="outline" className="text-xs">Uploaded</Badge>
              </div>
            ) : (
              <div className="flex flex-col items-center gap-2">
                <Upload className="h-8 w-8 text-muted-foreground" />
                <span>{cell.config?.description || "Click to upload a file"}</span>
                <span className="text-xs text-muted-foreground/70">
                  Accepts: {(cell.config?.acceptedTypes || [".xlsx", ".csv"]).join(", ")}
                </span>
              </div>
            )}
            <input
              ref={fileInputRef}
              type="file"
              accept={(cell.config?.acceptedTypes || [".xlsx", ".csv"]).join(",")}
              className="hidden"
              onChange={(e) => {
                const file = e.target.files?.[0];
                if (file) onFileUpload(file);
                e.target.value = "";
              }}
            />
          </div>
        )}

        {/* Cell output */}
        {result && result.status === "success" && (
          <div className="mt-3 space-y-2 border-t pt-3">
            {result.text && (
              <div className="prose prose-sm prose-invert max-w-none text-sm">
                <ReactMarkdown>{result.text}</ReactMarkdown>
              </div>
            )}
            {result.table && <DataExplorer data={result.table} compact />}
            {result.chart && <PlotlyChart figure={result.chart} />}
          </div>
        )}
        {result && result.status === "error" && (
          <div className="mt-3 rounded-md border border-destructive/50 bg-destructive/10 px-3 py-2 text-xs text-destructive">
            {result.error}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
