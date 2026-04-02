import { ScrollArea, ScrollBar } from "@/components/ui/scroll-area";
import type { TableData } from "@/types";

interface DataTableProps {
  data: TableData;
}

export default function DataTable({ data }: DataTableProps) {
  const columns = data.columns || [];
  const rows = data.rows || [];
  const totalRows = data.totalRows ?? (data as any).total_rows ?? rows.length;

  return (
    <div className="w-full overflow-hidden rounded-lg border">
      <ScrollArea className="w-full">
        <div className="min-w-full">
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr className="border-b bg-muted/50">
                {columns.map((col) => (
                  <th
                    key={col}
                    className="sticky top-0 whitespace-nowrap bg-muted/50 px-4 py-2.5 text-left font-medium text-muted-foreground"
                  >
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, idx) => (
                <tr
                  key={idx}
                  className="border-b transition-colors hover:bg-muted/30 even:bg-muted/10"
                >
                  {columns.map((col) => (
                    <td
                      key={col}
                      className="whitespace-nowrap px-4 py-2 text-foreground"
                    >
                      {formatCellValue(row[col])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <ScrollBar orientation="horizontal" />
      </ScrollArea>

      {/* Row count footer */}
      <div className="border-t bg-muted/30 px-4 py-2 text-xs text-muted-foreground">
        Showing {rows.length} of {totalRows.toLocaleString()} rows
      </div>
    </div>
  );
}

function formatCellValue(value: unknown): string {
  if (value === null || value === undefined) return "--";
  if (typeof value === "number") return value.toLocaleString();
  if (typeof value === "boolean") return value ? "true" : "false";
  return String(value);
}
