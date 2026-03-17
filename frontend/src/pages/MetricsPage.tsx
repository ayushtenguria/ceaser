import { useEffect, useState, useCallback } from "react";
import { Plus, Trash2, Pencil, BookOpen, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import * as api from "@/lib/api";

interface Metric {
  id: string;
  name: string;
  description: string;
  sqlExpression: string;
  category: string;
  connectionId: string | null;
  createdAt: string;
}

export default function MetricsPage() {
  const [metrics, setMetrics] = useState<Metric[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingMetric, setEditingMetric] = useState<Metric | null>(null);

  useEffect(() => {
    api.getMetrics().then(setMetrics).catch(() => {}).finally(() => setIsLoading(false));
  }, []);

  const handleDelete = useCallback(async (id: string) => {
    try {
      await api.deleteMetric(id);
      setMetrics((prev) => prev.filter((m) => m.id !== id));
    } catch {}
  }, []);

  const handleSave = useCallback(async (data: { name: string; description: string; sqlExpression: string; category: string }) => {
    try {
      if (editingMetric) {
        const updated = await api.updateMetric(editingMetric.id, data);
        setMetrics((prev) => prev.map((m) => (m.id === editingMetric.id ? updated : m)));
      } else {
        const created = await api.createMetric(data);
        setMetrics((prev) => [...prev, created]);
      }
      setDialogOpen(false);
      setEditingMetric(null);
    } catch {}
  }, [editingMetric]);

  const grouped = metrics.reduce<Record<string, Metric[]>>((acc, m) => {
    const cat = m.category || "general";
    (acc[cat] = acc[cat] || []).push(m);
    return acc;
  }, {});

  return (
    <div className="p-6">
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-semibold">Business Metrics</h2>
          <p className="text-sm text-muted-foreground">
            Define business metrics so the AI uses consistent definitions
          </p>
        </div>
        <Dialog open={dialogOpen} onOpenChange={(open) => { setDialogOpen(open); if (!open) setEditingMetric(null); }}>
          <DialogTrigger asChild>
            <Button><Plus className="mr-2 h-4 w-4" />Add Metric</Button>
          </DialogTrigger>
          <DialogContent className="max-w-lg">
            <DialogHeader>
              <DialogTitle>{editingMetric ? "Edit Metric" : "Define Business Metric"}</DialogTitle>
            </DialogHeader>
            <MetricForm
              initial={editingMetric}
              onSave={handleSave}
              onCancel={() => { setDialogOpen(false); setEditingMetric(null); }}
            />
          </DialogContent>
        </Dialog>
      </div>

      {isLoading ? (
        <div className="space-y-3">{[1, 2, 3].map((i) => <div key={i} className="h-20 animate-pulse rounded-lg border bg-card" />)}</div>
      ) : metrics.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-24">
          <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-2xl bg-secondary">
            <BookOpen className="h-8 w-8 text-muted-foreground" />
          </div>
          <h3 className="mb-1 text-lg font-medium">No metrics defined</h3>
          <p className="mb-4 max-w-md text-center text-sm text-muted-foreground">
            Define business metrics like "Revenue", "Churn Rate", "Active Customers" so the AI
            always uses the same SQL definition — no more inconsistent results.
          </p>
          <Button onClick={() => setDialogOpen(true)}><Plus className="mr-2 h-4 w-4" />Define Your First Metric</Button>
        </div>
      ) : (
        <div className="space-y-6">
          {Object.entries(grouped).map(([category, items]) => (
            <div key={category}>
              <h3 className="mb-3 text-sm font-medium uppercase text-muted-foreground">{category}</h3>
              <div className="space-y-2">
                {items.map((metric) => (
                  <Card key={metric.id}>
                    <CardContent className="flex items-center justify-between p-4">
                      <div className="flex-1">
                        <div className="flex items-center gap-2">
                          <p className="font-medium">{metric.name}</p>
                          <Badge variant="outline" className="text-xs">{metric.category}</Badge>
                        </div>
                        {metric.description && <p className="mt-0.5 text-xs text-muted-foreground">{metric.description}</p>}
                        <pre className="mt-2 rounded bg-muted/50 px-3 py-1.5 text-xs text-emerald-400">{metric.sqlExpression}</pre>
                      </div>
                      <div className="ml-4 flex gap-1">
                        <Button variant="ghost" size="icon" className="h-8 w-8" onClick={() => { setEditingMetric(metric); setDialogOpen(true); }}>
                          <Pencil className="h-3.5 w-3.5" />
                        </Button>
                        <Button variant="ghost" size="icon" className="h-8 w-8 text-muted-foreground hover:text-destructive" onClick={() => handleDelete(metric.id)}>
                          <Trash2 className="h-3.5 w-3.5" />
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function MetricForm({ initial, onSave, onCancel }: { initial: Metric | null; onSave: (data: any) => void; onCancel: () => void }) {
  const [name, setName] = useState(initial?.name || "");
  const [description, setDescription] = useState(initial?.description || "");
  const [sqlExpression, setSqlExpression] = useState(initial?.sqlExpression || "");
  const [category, setCategory] = useState(initial?.category || "general");
  const [isSaving, setIsSaving] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim() || !sqlExpression.trim()) return;
    setIsSaving(true);
    await onSave({ name, description, sqlExpression, category });
    setIsSaving(false);
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <div className="space-y-1.5">
        <label className="text-sm font-medium">Metric Name</label>
        <Input placeholder="e.g., Monthly Recurring Revenue" value={name} onChange={(e) => setName(e.target.value)} />
      </div>
      <div className="space-y-1.5">
        <label className="text-sm font-medium">Description</label>
        <Input placeholder="What this metric means in business terms" value={description} onChange={(e) => setDescription(e.target.value)} />
      </div>
      <div className="space-y-1.5">
        <label className="text-sm font-medium">SQL Expression</label>
        <Textarea
          placeholder="SUM(revenue.amount) WHERE revenue.type = 'subscription'"
          value={sqlExpression}
          onChange={(e) => setSqlExpression(e.target.value)}
          rows={3}
          className="font-mono text-sm"
        />
        <p className="text-xs text-muted-foreground">The AI will use this exact expression when the user references this metric.</p>
      </div>
      <div className="space-y-1.5">
        <label className="text-sm font-medium">Category</label>
        <Input placeholder="e.g., revenue, engagement, support" value={category} onChange={(e) => setCategory(e.target.value)} />
      </div>
      <div className="flex justify-end gap-2 pt-2">
        <Button type="button" variant="outline" onClick={onCancel}>Cancel</Button>
        <Button type="submit" disabled={isSaving || !name.trim() || !sqlExpression.trim()}>
          {isSaving && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
          {initial ? "Update" : "Create"}
        </Button>
      </div>
    </form>
  );
}
