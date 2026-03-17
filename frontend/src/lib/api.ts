import axios from "axios";
import type {
  ChatRequest,
  Conversation,
  DatabaseConnection,
  FileUpload,
  Message,
  StreamChunk,
} from "@/types";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

const api = axios.create({
  baseURL: `${API_URL}/api/v1`,
  headers: {
    "Content-Type": "application/json",
  },
});

/**
 * Attach Clerk auth token to every request.
 * Call this once after Clerk loads with a getToken function.
 */
let _getToken: (() => Promise<string | null>) | null = null;

export function setAuthTokenGetter(getter: () => Promise<string | null>) {
  _getToken = getter;
}

api.interceptors.request.use(async (config) => {
  if (_getToken) {
    const token = await _getToken();
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
  }
  return config;
});

// Retry on network errors (max 2 retries)
api.interceptors.response.use(
  (response) => response,
  async (error) => {
    const config = error.config;
    if (!config || config._retryCount >= 2) return Promise.reject(error);

    // Only retry on network errors or 5xx
    if (!error.response || error.response.status >= 500) {
      config._retryCount = (config._retryCount || 0) + 1;
      await new Promise((r) => setTimeout(r, 1000 * config._retryCount));
      return api(config);
    }

    return Promise.reject(error);
  }
);

// --- Auth ---

export async function syncUser(payload: {
  clerkId: string;
  email: string;
  firstName: string;
  lastName: string;
  organizationId: string | null;
  imageUrl: string | null;
}): Promise<any> {
  const { data } = await api.post("/auth/sync", payload);
  return data;
}

export async function getPermissions(): Promise<{ role: string; isSuperAdmin: boolean; permissions: string[] }> {
  const { data } = await api.get("/auth/me/permissions");
  return data;
}

export async function getSuggestions(connectionId?: string): Promise<string[]> {
  const params = connectionId ? { connection_id: connectionId } : {};
  const { data } = await api.get<{ suggestions: string[] }>("/suggestions", { params });
  return data.suggestions;
}

// --- Conversations ---

export async function getConversations(): Promise<Conversation[]> {
  const { data } = await api.get<Conversation[]>("/conversations");
  return data;
}

export async function getConversation(id: string): Promise<Conversation> {
  const { data } = await api.get<Conversation>(`/conversations/${id}`);
  return data;
}

export async function deleteConversation(id: string): Promise<void> {
  await api.delete(`/conversations/${id}`);
}

// --- Messages ---

export async function getMessages(conversationId: string): Promise<Message[]> {
  const { data } = await api.get<Message[]>(
    `/conversations/${conversationId}/messages`
  );
  return data;
}

/**
 * Send a chat message and stream the response via SSE.
 * Returns an async generator yielding StreamChunk objects.
 */
export async function* sendMessage(
  request: ChatRequest
): AsyncGenerator<StreamChunk> {
  const token = _getToken ? await _getToken() : null;

  // Use AbortController with 3-minute timeout for long-running analyses
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 180_000);

  const response = await fetch(`${API_URL}/api/v1/chat`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(request),
    signal: controller.signal,
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Chat request failed: ${response.status} ${errorText}`);
  }

  const reader = response.body?.getReader();
  if (!reader) {
    throw new Error("No response body");
  }

  const decoder = new TextDecoder();
  let buffer = "";

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";

      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed || !trimmed.startsWith("data: ")) continue;

        const jsonStr = trimmed.slice(6);
        if (jsonStr === "[DONE]") return;

        try {
          const chunk = JSON.parse(jsonStr) as StreamChunk;
          yield chunk;
        } catch {
          // Skip malformed chunks
        }
      }
    }
  } finally {
    clearTimeout(timeoutId);
    reader.releaseLock();
  }
}

// --- Connections ---

export async function getConnections(): Promise<DatabaseConnection[]> {
  const { data } = await api.get<DatabaseConnection[]>("/connections");
  return data;
}

export async function createConnection(
  connection: Omit<DatabaseConnection, "id" | "isConnected" | "organizationId" | "createdAt" | "schema">
): Promise<DatabaseConnection> {
  const { data } = await api.post<DatabaseConnection>("/connections", connection);
  return data;
}

export async function testConnection(
  id: string
): Promise<{ success: boolean; error?: string }> {
  const { data } = await api.post<{ success: boolean; error?: string }>(
    `/connections/${id}/test`
  );
  return data;
}

export async function deleteConnection(id: string): Promise<void> {
  await api.delete(`/connections/${id}`);
}

// --- Files ---

export async function getFiles(): Promise<FileUpload[]> {
  const { data } = await api.get<FileUpload[]>("/files");
  return data;
}

export async function uploadFile(file: File): Promise<FileUpload> {
  const formData = new FormData();
  formData.append("file", file);

  const { data } = await api.post<FileUpload>("/files/upload", formData, {
    headers: { "Content-Type": "multipart/form-data" },
  });
  return data;
}

export async function deleteFile(id: string): Promise<void> {
  await api.delete(`/files/${id}`);
}

// --- Reports ---

export async function getReports(): Promise<any[]> {
  const { data } = await api.get("/reports");
  return data;
}

export async function createReport(report: {
  name: string;
  description?: string;
  connectionId?: string;
  fileId?: string;
  sqlQuery?: string;
  pythonCode?: string;
  originalQuestion?: string;
  tableData?: any;
  plotlyFigure?: any;
  summaryText?: string;
  schedule?: string;
}): Promise<any> {
  const { data } = await api.post("/reports", report);
  return data;
}

export async function updateReport(id: string, updates: {
  name?: string;
  description?: string;
  schedule?: string;
  isPinned?: boolean;
  isActive?: boolean;
}): Promise<any> {
  const { data } = await api.patch(`/reports/${id}`, updates);
  return data;
}

export async function refreshReport(id: string): Promise<any> {
  const { data } = await api.post(`/reports/${id}/refresh`);
  return data;
}

export async function deleteReport(id: string): Promise<void> {
  await api.delete(`/reports/${id}`);
}

// --- Metrics (Semantic Layer) ---

export async function getMetrics(): Promise<any[]> {
  const { data } = await api.get("/metrics");
  return data;
}

export async function createMetric(metric: {
  name: string;
  description?: string;
  sqlExpression: string;
  category?: string;
  connectionId?: string;
}): Promise<any> {
  const { data } = await api.post("/metrics", metric);
  return data;
}

export async function updateMetric(id: string, updates: {
  name?: string;
  description?: string;
  sqlExpression?: string;
  category?: string;
}): Promise<any> {
  const { data } = await api.patch(`/metrics/${id}`, updates);
  return data;
}

export async function deleteMetric(id: string): Promise<void> {
  await api.delete(`/metrics/${id}`);
}

// --- Audit ---

export async function getAuditLogs(params?: {
  action?: string;
  resourceType?: string;
  limit?: number;
  offset?: number;
}): Promise<any[]> {
  const { data } = await api.get("/audit", { params });
  return data;
}

export async function getAuditStats(): Promise<any> {
  const { data } = await api.get("/audit/stats");
  return data;
}

// --- Admin ---

export async function getAdminStats(): Promise<any> {
  const { data } = await api.get("/admin/stats");
  return data;
}

export async function getAdminOrganizations(): Promise<any[]> {
  const { data } = await api.get("/admin/organizations");
  return data;
}

export async function createAdminOrganization(org: { name: string; slug?: string }): Promise<any> {
  const { data } = await api.post("/admin/organizations", org);
  return data;
}

export async function inviteUserToOrg(orgId: string, invite: { email: string; role?: string }): Promise<any> {
  const { data } = await api.post(`/admin/organizations/${orgId}/invite`, invite);
  return data;
}

export async function getAdminUsers(): Promise<any[]> {
  const { data } = await api.get("/admin/users");
  return data;
}
