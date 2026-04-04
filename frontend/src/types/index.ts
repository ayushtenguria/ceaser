import type Plotly from "plotly.js";

export interface User {
  id: string;
  email: string;
  firstName: string;
  lastName: string;
  organizationId: string | null;
  imageUrl: string | null;
}

export interface Organization {
  id: string;
  name: string;
  slug: string;
}

export interface DatabaseConnection {
  id: string;
  name: string;
  dbType: DatabaseType;
  host: string;
  port: number;
  database: string;
  username: string;
  isConnected: boolean;
  organizationId: string;
  createdAt: string;
  schema?: SchemaInfo;
}

export type DatabaseType =
  | "postgresql"
  | "mysql"
  | "sqlite"
  | "bigquery"
  | "snowflake";

export interface SchemaInfo {
  tables: TableInfo[];
}

export interface TableInfo {
  name: string;
  columns: ColumnInfo[];
  rowCount?: number;
}

export interface ColumnInfo {
  name: string;
  type: string;
  nullable: boolean;
}

export interface Conversation {
  id: string;
  title: string;
  connectionId: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface Message {
  id: string;
  conversationId: string;
  role: "user" | "assistant";
  content: string;
  messageType: MessageType;
  sqlQuery?: string;
  codeBlock?: string;
  plotlyFigure?: PlotlyFigure;
  tableData?: TableData;
  plotlyFigures?: PlotlyFigure[];
  tableDatas?: TableData[];
  queryReasoning?: string;
  confidence?: string;
  feedback?: { rating: "up" | "down"; correctionNote?: string; category?: string };
  disambiguationData?: any;
  error?: string;
  createdAt: string;
}

export type MessageType =
  | "text"
  | "sql_result"
  | "visualization"
  | "code_execution"
  | "error";

export interface PlotlyFigure {
  data: Plotly.Data[];
  layout: Partial<Plotly.Layout>;
}

export interface TableData {
  columns: string[];
  rows: Record<string, unknown>[];
  totalRows?: number;
  total_rows?: number;
}

export interface FileUpload {
  id: string;
  filename: string;
  fileType: string;
  sizeBytes: number;
  uploadedAt: string;
  organizationId: string;
  columnInfo?: {
    row_count: number;
    column_count: number;
    columns: Array<{ name: string; dtype: string; null_count: number; unique_count: number }>;
  };
  excelMetadata?: {
    quality_report?: {
      severity: "clean" | "minor" | "major";
      total_issues: number;
      items: string[];
    };
    insight?: {
      summary: string;
      sheets: Array<{ name: string; rows: number; columns: number }>;
    };
  };
}

export interface ChatRequest {
  message: string;
  conversationId?: string;
  connectionId?: string;
  connectionIds?: string[];
  fileId?: string;
  fileIds?: string[];
  model?: "gemini" | "claude";
  disambiguationChoice?: Record<string, string>;
}

export interface StreamChunk {
  type: "text" | "sql" | "code" | "table" | "chart" | "error" | "done" | "status" | "conversation_id" | "suggestions";
  content: string;
  data?: unknown;
}
