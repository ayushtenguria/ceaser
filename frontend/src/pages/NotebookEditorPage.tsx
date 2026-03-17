import { useEffect, useState, useCallback } from "react";
import { useParams } from "react-router-dom";
import {
  Play, Loader2, Trash2, GripVertical, FileText, FileUp,
  TextCursorInput, Bot, Code2,
} from "lucide-react";
import ReactMarkdown from "react-markdown";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import PlotlyChart from "@/components/visualizations/PlotlyChart";
import DataTable from "@/components/visualizations/DataTable";
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

  useEffect(() => {
    if (!notebookId) return;
    api.getNotebook(notebookId).then(setNotebook).catch(() => {}).finally(() => setIsLoading(false));
  }, [notebookId]);

  const handleAddCell = useCallback(async (cellType: string) => {
    if (!notebookId) return;
    const nb = await api.addNotebookCell(notebookId, {
      cellType,
      content: cellType === "text" ? "## New Section" : cellType === "prompt" ? "Analyze the data" : "",
      config: cellType === "input" ? { inputType: "text", label: "Parameter", default: "" } : cellType === "file" ? { acceptedTypes: [".xlsx", ".csv"], description: "Upload data" } : null,
    });
    setNotebook(nb);
  }, [notebookId]);

  const handleUpdateCell = useCallback(async (cellId: string, content: string) => {
    if (!notebookId) return;
    const cell = notebook?.cells?.find((c: any) => c.id === cellId);
    if (!cell) return;
    const nb = await api.updateNotebookCell(notebookId, cellId, {
      cellType: cell.cellType,
      content,
      config: cell.config,
      outputVariable: cell.outputVariable,
    });
    setNotebook(nb);
  }, [notebookId, notebook]);

  const handleDeleteCell = useCallback(async (cellId: string) => {
    if (!notebookId) return;
    const nb = await api.deleteNotebookCell(notebookId, cellId);
    setNotebook(nb);
  }, [notebookId]);

  const handleRun = useCallback(async () => {
    if (!notebookId) return;
    setIsRunning(true);
    setCellResults({});

    try {
      for await (const event of api.runNotebook(notebookId, userInputs, {})) {
        if (event.type === "cell_start") {
          setRunningCellId(event.cellId);
        } else if (event.type === "cell_complete") {
          setCellResults((prev) => ({ ...prev, [event.cellId]: event }));
          setRunningCellId(null);
        }
      }
    } catch {} finally {
      setIsRunning(false);
      setRunningCellId(null);
    }
  }, [notebookId, userInputs]);

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
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-semibold">{notebook.name}</h2>
          {notebook.description && <p className="text-sm text-muted-foreground">{notebook.description}</p>}
        </div>
        <Button onClick={handleRun} disabled={isRunning}>
          {isRunning ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Play className="mr-2 h-4 w-4" />}
          {isRunning ? "Running..." : "Run All"}
        </Button>
      </div>

      {/* Cells */}
      <div className="space-y-3">
        {cells.map((cell: any) => {
          const meta = CELL_TYPES.find((t) => t.value === cell.cellType) || CELL_TYPES[0];
          const Icon = meta.icon;
          const result = cellResults[cell.id];
          const isCellRunning = runningCellId === cell.id;

          return (
            <Card key={cell.id} className={cn(
              "transition-all",
              isCellRunning && "ring-2 ring-primary",
              result?.status === "error" && "border-destructive/50",
              result?.status === "success" && "border-emerald-500/30",
            )}>
              <CardHeader className="flex flex-row items-center gap-2 py-2 px-4">
                <GripVertical className="h-4 w-4 text-muted-foreground cursor-grab" />
                <Icon className={cn("h-4 w-4", meta.color)} />
                <Badge variant="outline" className="text-xs">{meta.label}</Badge>
                {isCellRunning && <Loader2 className="h-3.5 w-3.5 animate-spin text-primary" />}
                <div className="flex-1" />
                <Button variant="ghost" size="icon" className="h-7 w-7 text-muted-foreground hover:text-destructive"
                  onClick={() => handleDeleteCell(cell.id)}>
                  <Trash2 className="h-3.5 w-3.5" />
                </Button>
              </CardHeader>

              <CardContent className="px-4 pb-4">
                {/* Cell editor based on type */}
                {cell.cellType === "text" && (
                  <Textarea
                    value={cell.content}
                    onChange={(e) => handleUpdateCell(cell.id, e.target.value)}
                    placeholder="Markdown text..."
                    rows={3}
                    className="text-sm"
                  />
                )}
                {cell.cellType === "prompt" && (
                  <Textarea
                    value={cell.content}
                    onChange={(e) => handleUpdateCell(cell.id, e.target.value)}
                    placeholder="Ask the AI to analyze data..."
                    rows={2}
                    className="text-sm"
                  />
                )}
                {cell.cellType === "code" && (
                  <Textarea
                    value={cell.content}
                    onChange={(e) => handleUpdateCell(cell.id, e.target.value)}
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
                      value={userInputs[cell.id] || ""}
                      onChange={(e) => setUserInputs((prev) => ({ ...prev, [cell.id]: e.target.value }))}
                    />
                  </div>
                )}
                {cell.cellType === "file" && (
                  <div className="rounded-lg border-2 border-dashed p-6 text-center text-sm text-muted-foreground">
                    {cell.config?.description || "Upload a file to analyze"}
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
                    {result.table && <DataTable data={result.table} />}
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
        })}
      </div>

      {/* Add cell menu */}
      <div className="mt-4 flex items-center justify-center gap-2">
        {CELL_TYPES.map((ct) => (
          <Button key={ct.value} variant="outline" size="sm" onClick={() => handleAddCell(ct.value)}
            className="gap-1.5 text-xs">
            <ct.icon className={cn("h-3.5 w-3.5", ct.color)} />
            {ct.label}
          </Button>
        ))}
      </div>
    </div>
  );
}
