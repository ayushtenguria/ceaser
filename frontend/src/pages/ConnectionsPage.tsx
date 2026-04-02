import { useEffect, useState } from "react";
import { Plus, Database } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import ConnectionCard from "@/components/connections/ConnectionCard";
import ConnectionForm from "@/components/connections/ConnectionForm";
import { useConnectionsStore } from "@/store/connections";
import * as api from "@/lib/api";

export default function ConnectionsPage() {
  const { connections, setConnections } = useConnectionsStore();
  const [dialogOpen, setDialogOpen] = useState(false);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const data = await api.getConnections();
        if (!cancelled) setConnections(data);
      } catch {
      } finally {
        if (!cancelled) setIsLoading(false);
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, [setConnections]);

  return (
    <div className="p-6">
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-semibold">Database Connections</h2>
          <p className="text-sm text-muted-foreground">
            Connect your databases to start querying with AI
          </p>
        </div>

        <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
          <DialogTrigger asChild>
            <Button>
              <Plus className="mr-2 h-4 w-4" />
              Add Connection
            </Button>
          </DialogTrigger>
          <DialogContent className="max-w-lg">
            <DialogHeader>
              <DialogTitle>Add Database Connection</DialogTitle>
              <DialogDescription>
                Configure a new database connection for AI-powered analysis.
              </DialogDescription>
            </DialogHeader>
            <ConnectionForm onSuccess={() => setDialogOpen(false)} />
          </DialogContent>
        </Dialog>
      </div>

      {isLoading ? (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {[1, 2, 3].map((i) => (
            <div
              key={i}
              className="h-48 animate-pulse rounded-lg border bg-card"
            />
          ))}
        </div>
      ) : connections.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-24">
          <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-2xl bg-secondary">
            <Database className="h-8 w-8 text-muted-foreground" />
          </div>
          <h3 className="mb-1 text-lg font-medium">No connections yet</h3>
          <p className="mb-6 text-sm text-muted-foreground">
            Add a database connection to start analyzing your data
          </p>
          <Button onClick={() => setDialogOpen(true)}>
            <Plus className="mr-2 h-4 w-4" />
            Add Your First Connection
          </Button>
        </div>
      ) : (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {connections.map((conn) => (
            <ConnectionCard key={conn.id} connection={conn} />
          ))}
        </div>
      )}
    </div>
  );
}
