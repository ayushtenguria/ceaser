import { useState, useCallback, type FormEvent } from "react";
import { Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import * as api from "@/lib/api";
import { useConnectionsStore } from "@/store/connections";
import type { DatabaseType } from "@/types";

interface ConnectionFormProps {
  onSuccess: () => void;
}

interface FormState {
  name: string;
  dbType: DatabaseType;
  host: string;
  port: string;
  database: string;
  username: string;
  password: string;
  projectId: string;
  credentialsJson: string;
  filePath: string;
}

const INITIAL_FORM: FormState = {
  name: "",
  dbType: "postgresql",
  host: "localhost",
  port: "5432",
  database: "",
  username: "",
  password: "",
  projectId: "",
  credentialsJson: "",
  filePath: "",
};

const DEFAULT_PORTS: Record<DatabaseType, string> = {
  postgresql: "5432",
  mysql: "3306",
  sqlite: "",
  bigquery: "",
  snowflake: "443",
};

const DB_TYPE_LABELS: Record<DatabaseType, string> = {
  postgresql: "PostgreSQL",
  mysql: "MySQL",
  sqlite: "SQLite",
  bigquery: "BigQuery",
  snowflake: "Snowflake",
};

export default function ConnectionForm({ onSuccess }: ConnectionFormProps) {
  const [form, setForm] = useState<FormState>(INITIAL_FORM);
  const [testResult, setTestResult] = useState<{
    success: boolean;
    error?: string;
  } | null>(null);
  const [isTesting, setIsTesting] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [errors, setErrors] = useState<Partial<Record<keyof FormState, string>>>({});

  const { addConnection } = useConnectionsStore();

  const updateField = useCallback(
    <K extends keyof FormState>(field: K, value: FormState[K]) => {
      setForm((prev) => ({ ...prev, [field]: value }));
      setErrors((prev) => ({ ...prev, [field]: undefined }));
      setTestResult(null);
    },
    []
  );

  const handleTypeChange = useCallback(
    (type: DatabaseType) => {
      updateField("dbType", type);
      setForm((prev) => ({ ...prev, dbType: type, port: DEFAULT_PORTS[type] }));
    },
    [updateField]
  );

  const validate = useCallback((): boolean => {
    const newErrors: Partial<Record<keyof FormState, string>> = {};

    if (!form.name.trim()) newErrors.name = "Name is required";

    if (form.dbType === "sqlite") {
      if (!form.filePath.trim()) newErrors.filePath = "File path is required";
    } else if (form.dbType === "bigquery") {
      if (!form.projectId.trim()) newErrors.projectId = "Project ID is required";
      if (!form.credentialsJson.trim())
        newErrors.credentialsJson = "Credentials JSON is required";
    } else {
      if (!form.host.trim()) newErrors.host = "Host is required";
      if (!form.database.trim()) newErrors.database = "Database name is required";
      if (!form.username.trim()) newErrors.username = "Username is required";
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  }, [form]);

  const buildPayload = useCallback(() => {
    return {
      name: form.name,
      dbType: form.dbType,
      host: form.dbType === "sqlite" ? form.filePath : form.host,
      port: form.port ? parseInt(form.port, 10) : 0,
      database: form.dbType === "bigquery" ? form.projectId : form.database,
      username: form.username,
      password: form.password,
    };
  }, [form]);

  const handleTest = useCallback(async () => {
    if (!validate()) return;

    setIsTesting(true);
    setTestResult(null);

    try {
      // Create the connection first, then test it
      const connection = await api.createConnection(buildPayload());
      const result = await api.testConnection(connection.id);
      setTestResult(result);

      if (result.success) {
        // Connection works — add it to the store
        addConnection(connection);
        onSuccess();
      } else {
        // Test failed — clean up the created connection
        await api.deleteConnection(connection.id).catch(() => {});
      }
    } catch {
      setTestResult({ success: false, error: "Failed to test connection" });
    } finally {
      setIsTesting(false);
    }
  }, [validate, buildPayload, addConnection, onSuccess]);

  const handleSubmit = useCallback(
    async (e: FormEvent) => {
      e.preventDefault();
      if (!validate()) return;

      setIsSaving(true);

      try {
        const connection = await api.createConnection(buildPayload());
        addConnection(connection);
        onSuccess();
      } catch {
        setErrors({ name: "Failed to save connection" });
      } finally {
        setIsSaving(false);
      }
    },
    [validate, buildPayload, addConnection, onSuccess]
  );

  const showStandardFields =
    form.dbType !== "sqlite" && form.dbType !== "bigquery";

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      {/* Name */}
      <div className="space-y-1.5">
        <label className="text-sm font-medium">Connection Name</label>
        <Input
          placeholder="My Production DB"
          value={form.name}
          onChange={(e) => updateField("name", e.target.value)}
        />
        {errors.name && (
          <p className="text-xs text-destructive">{errors.name}</p>
        )}
      </div>

      {/* Type */}
      <div className="space-y-1.5">
        <label className="text-sm font-medium">Database Type</label>
        <Select value={form.dbType} onValueChange={handleTypeChange}>
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {(Object.keys(DB_TYPE_LABELS) as DatabaseType[]).map((type) => (
              <SelectItem key={type} value={type}>
                {DB_TYPE_LABELS[type]}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {/* SQLite fields */}
      {form.dbType === "sqlite" && (
        <div className="space-y-1.5">
          <label className="text-sm font-medium">File Path</label>
          <Input
            placeholder="/path/to/database.db"
            value={form.filePath}
            onChange={(e) => updateField("filePath", e.target.value)}
          />
          {errors.filePath && (
            <p className="text-xs text-destructive">{errors.filePath}</p>
          )}
        </div>
      )}

      {/* BigQuery fields */}
      {form.dbType === "bigquery" && (
        <>
          <div className="space-y-1.5">
            <label className="text-sm font-medium">Project ID</label>
            <Input
              placeholder="my-gcp-project"
              value={form.projectId}
              onChange={(e) => updateField("projectId", e.target.value)}
            />
            {errors.projectId && (
              <p className="text-xs text-destructive">{errors.projectId}</p>
            )}
          </div>
          <div className="space-y-1.5">
            <label className="text-sm font-medium">
              Service Account Credentials (JSON)
            </label>
            <Textarea
              placeholder='{"type": "service_account", ...}'
              value={form.credentialsJson}
              onChange={(e) => updateField("credentialsJson", e.target.value)}
              rows={4}
              className="font-mono text-xs"
            />
            {errors.credentialsJson && (
              <p className="text-xs text-destructive">
                {errors.credentialsJson}
              </p>
            )}
          </div>
        </>
      )}

      {/* Standard fields: host, port, database, username, password */}
      {showStandardFields && (
        <>
          <div className="grid grid-cols-3 gap-3">
            <div className="col-span-2 space-y-1.5">
              <label className="text-sm font-medium">Host</label>
              <Input
                placeholder="localhost"
                value={form.host}
                onChange={(e) => updateField("host", e.target.value)}
              />
              {errors.host && (
                <p className="text-xs text-destructive">{errors.host}</p>
              )}
            </div>
            <div className="space-y-1.5">
              <label className="text-sm font-medium">Port</label>
              <Input
                placeholder={DEFAULT_PORTS[form.dbType]}
                value={form.port}
                onChange={(e) => updateField("port", e.target.value)}
                type="number"
              />
            </div>
          </div>

          <div className="space-y-1.5">
            <label className="text-sm font-medium">Database</label>
            <Input
              placeholder="my_database"
              value={form.database}
              onChange={(e) => updateField("database", e.target.value)}
            />
            {errors.database && (
              <p className="text-xs text-destructive">{errors.database}</p>
            )}
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1.5">
              <label className="text-sm font-medium">Username</label>
              <Input
                placeholder="postgres"
                value={form.username}
                onChange={(e) => updateField("username", e.target.value)}
              />
              {errors.username && (
                <p className="text-xs text-destructive">{errors.username}</p>
              )}
            </div>
            <div className="space-y-1.5">
              <label className="text-sm font-medium">Password</label>
              <Input
                type="password"
                placeholder="********"
                value={form.password}
                onChange={(e) => updateField("password", e.target.value)}
              />
            </div>
          </div>
        </>
      )}

      {/* Test result */}
      {testResult && (
        <div className="flex items-center gap-2">
          <Badge variant={testResult.success ? "default" : "destructive"}>
            {testResult.success ? "Connected" : "Failed"}
          </Badge>
          {testResult.error && (
            <span className="text-xs text-destructive">{testResult.error}</span>
          )}
        </div>
      )}

      {/* Actions */}
      <div className="flex justify-end gap-2 pt-2">
        <Button
          type="button"
          variant="outline"
          onClick={handleTest}
          disabled={isTesting || isSaving}
        >
          {isTesting && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
          Test Connection
        </Button>
        <Button type="submit" disabled={isSaving || isTesting}>
          {isSaving && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
          Save Connection
        </Button>
      </div>
    </form>
  );
}
