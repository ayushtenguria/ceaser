import { useCallback, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Database, Trash2, MessageSquare, Loader2 } from "lucide-react";
import {
  Card,
  CardContent,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useConnectionsStore } from "@/store/connections";
import * as api from "@/lib/api";
import type { DatabaseConnection, DatabaseType } from "@/types";
import { cn } from "@/lib/utils";

interface ConnectionCardProps {
  connection: DatabaseConnection;
}

const DB_TYPE_COLORS: Record<DatabaseType, string> = {
  postgresql: "text-sky-400",
  mysql: "text-orange-400",
  sqlite: "text-emerald-400",
  bigquery: "text-blue-400",
  snowflake: "text-cyan-400",
};

const DB_TYPE_LABELS: Record<DatabaseType, string> = {
  postgresql: "PostgreSQL",
  mysql: "MySQL",
  sqlite: "SQLite",
  bigquery: "BigQuery",
  snowflake: "Snowflake",
};

export default function ConnectionCard({ connection }: ConnectionCardProps) {
  const navigate = useNavigate();
  const { removeConnection, setActiveConnection } = useConnectionsStore();
  const [isDeleting, setIsDeleting] = useState(false);

  const handleDelete = useCallback(async () => {
    setIsDeleting(true);
    try {
      await api.deleteConnection(connection.id);
      removeConnection(connection.id);
    } catch {
    } finally {
      setIsDeleting(false);
    }
  }, [connection.id, removeConnection]);

  const handleUseInChat = useCallback(() => {
    setActiveConnection(connection.id);
    navigate("/chat");
  }, [connection.id, setActiveConnection, navigate]);

  return (
    <Card className="flex flex-col">
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-secondary">
              <Database
                className={cn("h-5 w-5", DB_TYPE_COLORS[connection.dbType])}
              />
            </div>
            <div>
              <CardTitle className="text-base">{connection.name}</CardTitle>
              <Badge variant="secondary" className="mt-1 text-xs">
                {DB_TYPE_LABELS[connection.dbType]}
              </Badge>
            </div>
          </div>
          <div className="flex items-center gap-1.5">
            <span
              className={cn(
                "h-2.5 w-2.5 rounded-full",
                connection.isConnected ? "bg-emerald-500" : "bg-red-500"
              )}
            />
            <span className="text-xs text-muted-foreground">
              {connection.isConnected ? "Connected" : "Disconnected"}
            </span>
          </div>
        </div>
      </CardHeader>

      <CardContent className="flex-1 pb-3">
        <dl className="space-y-1 text-sm">
          <div className="flex justify-between">
            <dt className="text-muted-foreground">Host</dt>
            <dd className="font-mono text-xs">{connection.host}</dd>
          </div>
          <div className="flex justify-between">
            <dt className="text-muted-foreground">Database</dt>
            <dd className="font-mono text-xs">{connection.database}</dd>
          </div>
          {connection.port > 0 && (
            <div className="flex justify-between">
              <dt className="text-muted-foreground">Port</dt>
              <dd className="font-mono text-xs">{connection.port}</dd>
            </div>
          )}
          {connection.schema && (
            <div className="flex justify-between">
              <dt className="text-muted-foreground">Tables</dt>
              <dd className="font-mono text-xs">
                {connection.schema.tables.length}
              </dd>
            </div>
          )}
        </dl>
      </CardContent>

      <CardFooter className="gap-2">
        <Button
          variant="outline"
          size="sm"
          className="flex-1"
          onClick={handleUseInChat}
        >
          <MessageSquare className="mr-1.5 h-3.5 w-3.5" />
          Use in Chat
        </Button>
        <Button
          variant="ghost"
          size="icon"
          className="h-8 w-8 text-muted-foreground hover:text-destructive"
          onClick={handleDelete}
          disabled={isDeleting}
        >
          {isDeleting ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <Trash2 className="h-4 w-4" />
          )}
        </Button>
      </CardFooter>
    </Card>
  );
}
