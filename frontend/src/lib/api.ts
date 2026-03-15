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

  const response = await fetch(`${API_URL}/api/v1/chat`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(request),
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
