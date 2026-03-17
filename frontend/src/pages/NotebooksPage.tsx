import { useEffect, useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import {
  Plus, BookOpen, Trash2, Play, Loader2, Clock,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle, CardFooter } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger,
} from "@/components/ui/dialog";
import * as api from "@/lib/api";
import { formatRelativeTime } from "@/lib/utils";

export default function NotebooksPage() {
  const navigate = useNavigate();
  const [notebooks, setNotebooks] = useState<any[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [createOpen, setCreateOpen] = useState(false);
  const [newName, setNewName] = useState("");
  const [newDesc, setNewDesc] = useState("");
  const [creating, setCreating] = useState(false);

  useEffect(() => {
    api.getNotebooks().then(setNotebooks).catch(() => {}).finally(() => setIsLoading(false));
  }, []);

  const handleCreate = useCallback(async () => {
    if (!newName.trim()) return;
    setCreating(true);
    try {
      const nb = await api.createNotebook({
        name: newName,
        description: newDesc,
        cells: [
          { cellType: "text", content: `# ${newName}\n${newDesc}`, order: 0 },
          { cellType: "prompt", content: "Analyze the data", order: 1 },
        ],
      });
      navigate(`/notebooks/${nb.id}`);
    } catch {} finally {
      setCreating(false);
    }
  }, [newName, newDesc, navigate]);

  const handleDelete = useCallback(async (id: string) => {
    try {
      await api.deleteNotebook(id);
      setNotebooks((prev) => prev.filter((n) => n.id !== id));
    } catch {}
  }, []);

  if (isLoading) {
    return (
      <div className="p-6">
        <h2 className="mb-6 text-2xl font-semibold">Notebooks</h2>
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {[1, 2, 3].map((i) => <div key={i} className="h-40 animate-pulse rounded-lg border bg-card" />)}
        </div>
      </div>
    );
  }

  return (
    <div className="p-6">
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-semibold">Notebooks</h2>
          <p className="text-sm text-muted-foreground">Reusable analysis pipelines -- build once, run anytime</p>
        </div>
        <Dialog open={createOpen} onOpenChange={setCreateOpen}>
          <DialogTrigger asChild>
            <Button><Plus className="mr-2 h-4 w-4" />New Notebook</Button>
          </DialogTrigger>
          <DialogContent className="max-w-md">
            <DialogHeader><DialogTitle>Create Notebook</DialogTitle></DialogHeader>
            <div className="space-y-3">
              <Input placeholder="Notebook name" value={newName} onChange={(e) => setNewName(e.target.value)} />
              <Input placeholder="Description (optional)" value={newDesc} onChange={(e) => setNewDesc(e.target.value)} />
              <Button className="w-full" onClick={handleCreate} disabled={creating || !newName.trim()}>
                {creating && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                Create
              </Button>
            </div>
          </DialogContent>
        </Dialog>
      </div>

      {notebooks.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-24">
          <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-2xl bg-secondary">
            <BookOpen className="h-8 w-8 text-muted-foreground" />
          </div>
          <h3 className="mb-1 text-lg font-medium">No notebooks yet</h3>
          <p className="mb-4 text-sm text-muted-foreground">
            Create a notebook to build reusable analysis pipelines
          </p>
          <Button onClick={() => setCreateOpen(true)}>
            <Plus className="mr-2 h-4 w-4" />Create Your First Notebook
          </Button>
        </div>
      ) : (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {notebooks.map((nb) => (
            <Card key={nb.id} className="flex flex-col cursor-pointer hover:border-primary/50 transition-colors"
              onClick={() => navigate(`/notebooks/${nb.id}`)}
            >
              <CardHeader className="pb-2">
                <div className="flex items-start justify-between">
                  <CardTitle className="text-base">{nb.name}</CardTitle>
                  {nb.isTemplate && <Badge variant="secondary" className="text-xs">Template</Badge>}
                </div>
                {nb.description && (
                  <p className="text-xs text-muted-foreground line-clamp-2">{nb.description}</p>
                )}
              </CardHeader>
              <CardContent className="flex-1 pb-2">
                <div className="flex items-center gap-3 text-xs text-muted-foreground">
                  <span>{nb.cells?.length || 0} cells</span>
                  <span>{nb.runCount || 0} runs</span>
                  {nb.lastRunAt && (
                    <span className="flex items-center gap-1">
                      <Clock className="h-3 w-3" />
                      {formatRelativeTime(nb.lastRunAt)}
                    </span>
                  )}
                </div>
              </CardContent>
              <CardFooter className="gap-2">
                <Button variant="outline" size="sm" className="flex-1" onClick={(e) => { e.stopPropagation(); navigate(`/notebooks/${nb.id}`); }}>
                  <Play className="mr-1.5 h-3.5 w-3.5" />Open
                </Button>
                <Button variant="ghost" size="icon" className="h-8 w-8 text-muted-foreground hover:text-destructive"
                  onClick={(e) => { e.stopPropagation(); handleDelete(nb.id); }}>
                  <Trash2 className="h-3.5 w-3.5" />
                </Button>
              </CardFooter>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
