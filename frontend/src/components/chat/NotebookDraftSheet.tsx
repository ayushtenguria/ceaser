import { useState, useCallback, useEffect } from "react";
import {
  X, Loader2, BookMarked, Check, Trash2, GripVertical, Bot, AlertCircle, Save,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import * as api from "@/lib/api";
import { cn } from "@/lib/utils";

interface NotebookDraftSheetProps {
  conversationId: string | null;
  open: boolean;
  onClose: () => void;
  onSaved: (notebookId: string) => void;
}

interface DraftStep {
  label: string;
  prompt: string;
  produces_chart: boolean;
  original_question: string;
  cell_type: string;
  included: boolean;
}

export default function NotebookDraftSheet({
  conversationId, open, onClose, onSaved,
}: NotebookDraftSheetProps) {
  const [isLoading, setIsLoading] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [title, setTitle] = useState("");
  const [description, setDescription] = useState("");
  const [steps, setSteps] = useState<DraftStep[]>([]);
  const [skipped, setSkipped] = useState<any[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open || !conversationId) return;
    setIsLoading(true);
    setError(null);

    api.getNotebookDraft(conversationId)
      .then((draft) => {
        setTitle(draft.title || "Analysis Notebook");
        setDescription(draft.description || "");
        setSteps(draft.steps || []);
        setSkipped(draft.skipped || []);
      })
      .catch((err) => {
        setError(err instanceof Error ? err.message : "Failed to generate draft");
      })
      .finally(() => setIsLoading(false));
  }, [open, conversationId]);

  const toggleStep = useCallback((index: number) => {
    setSteps((prev) => prev.map((s, i) =>
      i === index ? { ...s, included: !s.included } : s
    ));
  }, []);

  const removeStep = useCallback((index: number) => {
    setSteps((prev) => prev.filter((_, i) => i !== index));
  }, []);

  const updatePrompt = useCallback((index: number, prompt: string) => {
    setSteps((prev) => prev.map((s, i) =>
      i === index ? { ...s, prompt } : s
    ));
  }, []);

  const handleSave = useCallback(async () => {
    if (!conversationId) return;
    const includedSteps = steps.filter((s) => s.included);
    if (includedSteps.length === 0) return;

    setIsSaving(true);
    try {
      const result = await api.saveConversationAsNotebook(conversationId, {
        title,
        description,
        steps: includedSteps,
      });
      onSaved(result.notebookId);
    } catch {
      setError("Failed to save notebook");
    } finally {
      setIsSaving(false);
    }
  }, [conversationId, title, description, steps, onSaved]);

  if (!open) return null;

  const includedCount = steps.filter((s) => s.included).length;

  return (
    <>
      <div className="fixed inset-0 z-40 bg-black/50" onClick={onClose} />

      <div className="fixed inset-y-0 right-0 z-50 flex w-full max-w-xl flex-col bg-background shadow-2xl animate-in slide-in-from-right">
        {/* Header */}
        <div className="flex h-14 items-center justify-between border-b px-6">
          <div className="flex items-center gap-3">
            <BookMarked className="h-5 w-5 text-primary" />
            <h2 className="text-lg font-semibold">Save as Notebook</h2>
          </div>
          <div className="flex items-center gap-2">
            {!isLoading && steps.length > 0 && (
              <Button size="sm" onClick={handleSave} disabled={isSaving || includedCount === 0}>
                {isSaving ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Save className="mr-2 h-4 w-4" />}
                Save ({includedCount} steps)
              </Button>
            )}
            <Button variant="ghost" size="icon" className="h-8 w-8" onClick={onClose}>
              <X className="h-4 w-4" />
            </Button>
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {/* Loading */}
          {isLoading && (
            <div className="flex flex-col items-center justify-center py-24">
              <Loader2 className="mb-4 h-8 w-8 animate-spin text-primary" />
              <p className="text-sm text-muted-foreground">Analyzing conversation...</p>
              <p className="mt-1 text-xs text-muted-foreground">Filtering out corrections and failed queries</p>
            </div>
          )}

          {/* Error */}
          {error && !isLoading && (
            <div className="flex flex-col items-center justify-center py-24">
              <AlertCircle className="mb-4 h-8 w-8 text-destructive" />
              <p className="text-sm">{error}</p>
            </div>
          )}

          {/* Draft preview */}
          {!isLoading && !error && steps.length > 0 && (
            <div className="space-y-6">
              {/* Notebook name/description */}
              <div className="space-y-3">
                <div>
                  <label className="text-xs font-medium text-muted-foreground">Notebook Name</label>
                  <Input value={title} onChange={(e) => setTitle(e.target.value)} className="mt-1" />
                </div>
                <div>
                  <label className="text-xs font-medium text-muted-foreground">Description</label>
                  <Input value={description} onChange={(e) => setDescription(e.target.value)} className="mt-1" />
                </div>
              </div>

              {/* Steps */}
              <div>
                <div className="mb-3 flex items-center justify-between">
                  <h3 className="text-sm font-medium">Analysis Steps</h3>
                  <span className="text-xs text-muted-foreground">
                    {includedCount}/{steps.length} included
                  </span>
                </div>

                <div className="space-y-2">
                  {steps.map((step, i) => (
                    <div
                      key={i}
                      className={cn(
                        "rounded-lg border p-3 transition-opacity",
                        step.included ? "opacity-100" : "opacity-40",
                      )}
                    >
                      <div className="flex items-start gap-3">
                        {/* Toggle */}
                        <button
                          onClick={() => toggleStep(i)}
                          className={cn(
                            "mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded border",
                            step.included ? "bg-primary border-primary" : "border-muted-foreground",
                          )}
                        >
                          {step.included && <Check className="h-3 w-3 text-primary-foreground" />}
                        </button>

                        <div className="flex-1 min-w-0">
                          {/* Label */}
                          <div className="flex items-center gap-2 mb-1">
                            <Bot className="h-3.5 w-3.5 text-purple-400" />
                            <span className="text-xs font-medium">{step.label}</span>
                            {step.produces_chart && (
                              <Badge variant="secondary" className="text-[10px] h-4">chart</Badge>
                            )}
                          </div>

                          {/* Editable prompt */}
                          <textarea
                            value={step.prompt}
                            onChange={(e) => updatePrompt(i, e.target.value)}
                            rows={2}
                            className="w-full rounded-md border bg-muted/30 px-2 py-1 text-xs resize-none focus:outline-none focus:ring-1 focus:ring-ring"
                          />

                          {/* Original question (if different) */}
                          {step.original_question && step.original_question !== step.prompt && (
                            <p className="mt-1 text-[10px] text-muted-foreground">
                              Original: "{step.original_question.slice(0, 80)}"
                            </p>
                          )}
                        </div>

                        {/* Remove */}
                        <button
                          onClick={() => removeStep(i)}
                          className="shrink-0 text-muted-foreground hover:text-destructive"
                        >
                          <Trash2 className="h-3.5 w-3.5" />
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Skipped messages */}
              {skipped.length > 0 && (
                <div>
                  <h3 className="mb-2 text-sm font-medium text-muted-foreground">
                    Excluded ({skipped.length} messages)
                  </h3>
                  <div className="space-y-1">
                    {skipped.slice(0, 10).map((s, i) => (
                      <div key={i} className="flex items-center gap-2 text-xs text-muted-foreground">
                        <X className="h-3 w-3 shrink-0 text-destructive/50" />
                        <span className="truncate">{s.content}</span>
                        <Badge variant="outline" className="shrink-0 text-[9px] h-4">{s.reason}</Badge>
                      </div>
                    ))}
                    {skipped.length > 10 && (
                      <p className="text-xs text-muted-foreground">
                        ... and {skipped.length - 10} more
                      </p>
                    )}
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Empty state */}
          {!isLoading && !error && steps.length === 0 && (
            <div className="flex flex-col items-center justify-center py-24">
              <BookMarked className="mb-4 h-8 w-8 text-muted-foreground" />
              <p className="text-sm">No meaningful analysis steps found</p>
              <p className="mt-1 text-xs text-muted-foreground">
                Try having a longer conversation with data queries first
              </p>
            </div>
          )}
        </div>
      </div>
    </>
  );
}
