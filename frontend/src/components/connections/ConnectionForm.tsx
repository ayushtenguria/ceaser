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
  // Ad platforms
  accountId: string;
  accessToken: string;
  refreshToken: string;
  developerToken: string;
  // Snowflake extras
  warehouse: string;
  sfSchema: string;
  role: string;
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
  accountId: "",
  accessToken: "",
  refreshToken: "",
  developerToken: "",
  warehouse: "",
  sfSchema: "PUBLIC",
  role: "",
};

const DEFAULT_PORTS: Record<DatabaseType, string> = {
  postgresql: "5432",
  mysql: "3306",
  sqlite: "",
  bigquery: "",
  snowflake: "443",
  meta_ads: "",
  google_ads: "",
};

const DB_TYPE_LABELS: Record<DatabaseType, string> = {
  postgresql: "PostgreSQL",
  mysql: "MySQL",
  sqlite: "SQLite",
  bigquery: "BigQuery",
  snowflake: "Snowflake",
  meta_ads: "Meta Ads",
  google_ads: "Google Ads",
};

const ADS_TYPES = new Set<DatabaseType>(["meta_ads", "google_ads"]);

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

    if (form.dbType === "meta_ads") {
      if (!form.accountId.trim()) newErrors.accountId = "Ad Account ID is required";
      if (!form.accessToken.trim()) newErrors.accessToken = "Access Token is required";
    } else if (form.dbType === "google_ads") {
      if (!form.accountId.trim()) newErrors.accountId = "Customer ID is required";
      if (!form.accessToken.trim()) newErrors.accessToken = "Access Token is required";
      if (!form.developerToken.trim()) newErrors.developerToken = "Developer Token is required";
    } else if (form.dbType === "snowflake") {
      if (!form.host.trim()) newErrors.host = "Account URL is required";
      if (!form.database.trim()) newErrors.database = "Database is required";
      if (!form.username.trim()) newErrors.username = "Username is required";
    } else if (form.dbType === "sqlite") {
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
    if (form.dbType === "meta_ads") {
      return {
        name: form.name, dbType: form.dbType,
        host: "", port: 0,
        database: form.accountId,
        username: "",
        password: form.accessToken,
      };
    }
    if (form.dbType === "google_ads") {
      return {
        name: form.name, dbType: form.dbType,
        host: "", port: 0,
        database: form.accountId,
        username: form.developerToken,
        password: JSON.stringify({
          access_token: form.accessToken,
          refresh_token: form.refreshToken || undefined,
        }),
      };
    }
    if (form.dbType === "snowflake") {
      return {
        name: form.name, dbType: form.dbType,
        host: form.host, port: 443,
        database: form.database,
        username: form.username,
        password: form.password,
      };
    }
    return {
      name: form.name,
      dbType: form.dbType,
      host: form.dbType === "sqlite" ? form.filePath : form.host,
      port: form.port ? parseInt(form.port, 10) : 0,
      database: form.dbType === "bigquery" ? form.projectId : form.database,
      username: form.username,
      password: form.dbType === "bigquery" ? form.credentialsJson : form.password,
    };
  }, [form]);

  const handleTest = useCallback(async () => {
    if (!validate()) return;

    setIsTesting(true);
    setTestResult(null);

    try {
      const connection = await api.createConnection(buildPayload());
      const result = await api.testConnection(connection.id);
      setTestResult(result);

      if (result.success) {
        addConnection(connection);
        onSuccess();
      } else {
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
    !ADS_TYPES.has(form.dbType) && form.dbType !== "sqlite" && form.dbType !== "bigquery" && form.dbType !== "snowflake";

  function _humanizeConnectionError(error: string): string {
    const e = error.toLowerCase();
    if (e.includes("connection refused") || e.includes("could not connect"))
      return "Connection refused — check that your database is running and the host/port are correct. If you have a firewall, whitelist Ceaser's IP above.";
    if (e.includes("timeout") || e.includes("timed out"))
      return "Connection timed out — your database may be behind a firewall. Whitelist Ceaser's IP above, or check if the host is reachable.";
    if (e.includes("password") || e.includes("authentication"))
      return "Authentication failed — double-check your username and password. Make sure the user has access to the specified database.";
    if (e.includes("does not exist") || e.includes("unknown database"))
      return "Database not found — verify the database name exists on the server.";
    if (e.includes("ssl") || e.includes("certificate"))
      return "SSL error — your database may require SSL. Contact support for help.";
    if (e.includes("too many connections"))
      return "Too many connections — your database has reached its connection limit. Try again in a few minutes.";
    return error;
  }

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

      {/* Meta Ads fields */}
      {form.dbType === "meta_ads" && (
        <>
          <div className="space-y-1.5">
            <label className="text-sm font-medium">Ad Account ID</label>
            <Input placeholder="act_123456789" value={form.accountId} onChange={(e) => updateField("accountId", e.target.value)} />
            {errors.accountId && <p className="text-xs text-destructive">{errors.accountId}</p>}
          </div>
          <div className="space-y-1.5">
            <label className="text-sm font-medium">Access Token</label>
            <Input type="password" placeholder="Paste your Meta access token" value={form.accessToken} onChange={(e) => updateField("accessToken", e.target.value)} />
            {errors.accessToken && <p className="text-xs text-destructive">{errors.accessToken}</p>}
          </div>
          <div className="rounded-lg border border-blue-500/20 bg-blue-500/5 px-3 py-2">
            <p className="text-xs text-muted-foreground">
              Get your access token from{" "}
              <a href="https://developers.facebook.com/tools/explorer/" target="_blank" rel="noopener noreferrer" className="text-primary underline">Graph API Explorer</a>
              {" "}with <strong>ads_read</strong> permission. Account ID is in your Ad Account settings.
            </p>
          </div>
        </>
      )}

      {/* Google Ads fields */}
      {form.dbType === "google_ads" && (
        <>
          <div className="space-y-1.5">
            <label className="text-sm font-medium">Customer ID</label>
            <Input placeholder="123-456-7890" value={form.accountId} onChange={(e) => updateField("accountId", e.target.value)} />
            {errors.accountId && <p className="text-xs text-destructive">{errors.accountId}</p>}
          </div>
          <div className="space-y-1.5">
            <label className="text-sm font-medium">Developer Token</label>
            <Input type="password" placeholder="Your Google Ads developer token" value={form.developerToken} onChange={(e) => updateField("developerToken", e.target.value)} />
            {errors.developerToken && <p className="text-xs text-destructive">{errors.developerToken}</p>}
          </div>
          <div className="space-y-1.5">
            <label className="text-sm font-medium">Access Token</label>
            <Input type="password" placeholder="OAuth2 access token" value={form.accessToken} onChange={(e) => updateField("accessToken", e.target.value)} />
            {errors.accessToken && <p className="text-xs text-destructive">{errors.accessToken}</p>}
          </div>
          <div className="space-y-1.5">
            <label className="text-sm font-medium">Refresh Token (optional)</label>
            <Input type="password" placeholder="OAuth2 refresh token for auto-renewal" value={form.refreshToken} onChange={(e) => updateField("refreshToken", e.target.value)} />
          </div>
          <div className="rounded-lg border border-blue-500/20 bg-blue-500/5 px-3 py-2">
            <p className="text-xs text-muted-foreground">
              Get credentials from{" "}
              <a href="https://console.cloud.google.com/apis/credentials" target="_blank" rel="noopener noreferrer" className="text-primary underline">Google API Console</a>.
              Developer token from Google Ads API Center.
            </p>
          </div>
        </>
      )}

      {/* Snowflake fields */}
      {form.dbType === "snowflake" && (
        <>
          <div className="space-y-1.5">
            <label className="text-sm font-medium">Account URL</label>
            <Input placeholder="abc123.us-east-1.snowflakecomputing.com" value={form.host} onChange={(e) => updateField("host", e.target.value)} />
            {errors.host && <p className="text-xs text-destructive">{errors.host}</p>}
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1.5">
              <label className="text-sm font-medium">Warehouse</label>
              <Input placeholder="COMPUTE_WH" value={form.warehouse} onChange={(e) => updateField("warehouse", e.target.value)} />
            </div>
            <div className="space-y-1.5">
              <label className="text-sm font-medium">Schema</label>
              <Input placeholder="PUBLIC" value={form.sfSchema} onChange={(e) => updateField("sfSchema", e.target.value)} />
            </div>
          </div>
          <div className="space-y-1.5">
            <label className="text-sm font-medium">Database</label>
            <Input placeholder="MY_DATABASE" value={form.database} onChange={(e) => updateField("database", e.target.value)} />
            {errors.database && <p className="text-xs text-destructive">{errors.database}</p>}
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1.5">
              <label className="text-sm font-medium">Username</label>
              <Input placeholder="my_user" value={form.username} onChange={(e) => updateField("username", e.target.value)} />
              {errors.username && <p className="text-xs text-destructive">{errors.username}</p>}
            </div>
            <div className="space-y-1.5">
              <label className="text-sm font-medium">Password</label>
              <Input type="password" placeholder="********" value={form.password} onChange={(e) => updateField("password", e.target.value)} />
            </div>
          </div>
          <div className="space-y-1.5">
            <label className="text-sm font-medium">Role (optional)</label>
            <Input placeholder="ANALYST" value={form.role} onChange={(e) => updateField("role", e.target.value)} />
          </div>
        </>
      )}

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

      {/* Firewall notice */}
      {showStandardFields && (
        <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 px-3 py-2">
          <p className="text-xs text-amber-400 font-medium mb-1">Firewall Whitelisting</p>
          <p className="text-xs text-muted-foreground">
            If your database has a firewall, allow inbound connections from Ceaser's IP:
          </p>
          <code className="mt-1 block rounded bg-muted px-2 py-1 text-xs font-mono select-all">
            {window.location.hostname === "localhost" ? "127.0.0.1 (local dev)" : window.location.hostname}
          </code>
          <p className="text-[10px] text-muted-foreground mt-1">
            Also ensure you've created a <strong>read-only database user</strong>. See the <a href="/setup" className="text-primary underline">Setup Guide</a>.
          </p>
        </div>
      )}

      {/* Test result */}
      {testResult && (
        <div className="rounded-lg border p-3">
          <div className="flex items-center gap-2 mb-1">
            <Badge variant={testResult.success ? "default" : "destructive"}>
              {testResult.success ? "Connected" : "Failed"}
            </Badge>
          </div>
          {testResult.error && (
            <p className="text-xs text-destructive mt-1">
              {_humanizeConnectionError(testResult.error)}
            </p>
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
