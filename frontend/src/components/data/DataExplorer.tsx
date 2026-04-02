import { useState, useMemo, useCallback } from "react";
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  flexRender,
  type ColumnDef,
  type SortingState,
  type ColumnFiltersState,
} from "@tanstack/react-table";
import {
  ArrowUpDown, ArrowUp, ArrowDown, Search, Download, ChevronLeft, ChevronRight,
  ChevronsLeft, ChevronsRight, Hash, Type, Calendar, ToggleLeft, X, Maximize2,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import type { TableData } from "@/types";

interface DataExplorerProps {
  data: TableData;
  title?: string;
  onExpand?: () => void;
  compact?: boolean;
}

type CellValue = string | number | boolean | null | undefined;

function detectColumnType(rows: Record<string, unknown>[], col: string): "number" | "date" | "boolean" | "string" {
  let numCount = 0;
  let dateCount = 0;
  let boolCount = 0;
  let checked = 0;
  for (const row of rows.slice(0, 50)) {
    const v = row[col];
    if (v === null || v === undefined || v === "") continue;
    checked++;
    if (typeof v === "boolean") { boolCount++; continue; }
    if (typeof v === "number") { numCount++; continue; }
    const s = String(v);
    if (!isNaN(Number(s)) && s.trim() !== "") { numCount++; continue; }
    if (/^\d{4}-\d{2}-\d{2}/.test(s)) { dateCount++; continue; }
  }
  if (!checked) return "string";
  if (boolCount / checked > 0.8) return "boolean";
  if (numCount / checked > 0.8) return "number";
  if (dateCount / checked > 0.8) return "date";
  return "string";
}

function formatCell(value: unknown, colType: string): string {
  if (value === null || value === undefined) return "";
  if (colType === "number" && typeof value === "number") return value.toLocaleString();
  if (colType === "number" && !isNaN(Number(value))) return Number(value).toLocaleString();
  return String(value);
}

const TypeIcon = ({ type }: { type: string }) => {
  const cls = "h-3 w-3 text-muted-foreground/60";
  switch (type) {
    case "number": return <Hash className={cls} />;
    case "date": return <Calendar className={cls} />;
    case "boolean": return <ToggleLeft className={cls} />;
    default: return <Type className={cls} />;
  }
};

export default function DataExplorer({ data, title, onExpand, compact = false }: DataExplorerProps) {
  const columns = data.columns || [];
  const rows = data.rows || [];
  const totalRows = data.totalRows ?? (data as any).total_rows ?? rows.length;

  const [sorting, setSorting] = useState<SortingState>([]);
  const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([]);
  const [globalFilter, setGlobalFilter] = useState("");
  const [activeFilter, setActiveFilter] = useState<string | null>(null);

  const colTypes = useMemo(() => {
    const types: Record<string, string> = {};
    for (const col of columns) {
      types[col] = detectColumnType(rows, col);
    }
    return types;
  }, [columns, rows]);

  const tableColumns = useMemo<ColumnDef<Record<string, unknown>>[]>(
    () =>
      columns.map((col) => ({
        accessorKey: col,
        header: ({ column }) => (
          <button
            className="flex items-center gap-1.5 text-left font-medium hover:text-foreground transition-colors"
            onClick={() => column.toggleSorting()}
          >
            <TypeIcon type={colTypes[col]} />
            <span className="truncate max-w-[150px]">{col}</span>
            {column.getIsSorted() === "asc" ? (
              <ArrowUp className="h-3 w-3 text-primary" />
            ) : column.getIsSorted() === "desc" ? (
              <ArrowDown className="h-3 w-3 text-primary" />
            ) : (
              <ArrowUpDown className="h-3 w-3 opacity-30" />
            )}
          </button>
        ),
        cell: ({ getValue }) => {
          const v = getValue() as CellValue;
          const isEmpty = v === null || v === undefined || v === "";
          return (
            <span className={isEmpty ? "text-muted-foreground/40 italic" : colTypes[col] === "number" ? "tabular-nums" : ""}>
              {isEmpty ? "null" : formatCell(v, colTypes[col])}
            </span>
          );
        },
        filterFn: "includesString",
      })),
    [columns, colTypes]
  );

  const table = useReactTable({
    data: rows,
    columns: tableColumns,
    state: { sorting, columnFilters, globalFilter },
    onSortingChange: setSorting,
    onColumnFiltersChange: setColumnFilters,
    onGlobalFilterChange: setGlobalFilter,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: { pagination: { pageSize: compact ? 10 : 50 } },
  });

  const handleExportCsv = useCallback(() => {
    const csvRows = [
      columns.join(","),
      ...rows.map((row) =>
        columns
          .map((col) => {
            const val = row[col];
            const str = val === null || val === undefined ? "" : String(val);
            return str.includes(",") || str.includes('"') || str.includes("\n")
              ? `"${str.replace(/"/g, '""')}"` : str;
          })
          .join(",")
      ),
    ];
    const blob = new Blob([csvRows.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${title || "data"}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }, [columns, rows, title]);

  const handleExportJson = useCallback(() => {
    const blob = new Blob([JSON.stringify(rows, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${title || "data"}.json`;
    a.click();
    URL.revokeObjectURL(url);
  }, [rows, title]);

  const pageCount = table.getPageCount();
  const pageIndex = table.getState().pagination.pageIndex;
  const filteredCount = table.getFilteredRowModel().rows.length;

  return (
    <div className="flex flex-col w-full rounded-lg border bg-card overflow-hidden">
      {/* Toolbar */}
      <div className="flex items-center gap-2 border-b px-3 py-2 bg-muted/30">
        {/* Global search */}
        <div className="relative flex-1 max-w-xs">
          <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="Search all columns..."
            value={globalFilter}
            onChange={(e) => setGlobalFilter(e.target.value)}
            className="h-7 pl-8 text-xs bg-background"
          />
          {globalFilter && (
            <button
              onClick={() => setGlobalFilter("")}
              className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
            >
              <X className="h-3 w-3" />
            </button>
          )}
        </div>

        {/* Stats */}
        <span className="text-xs text-muted-foreground whitespace-nowrap">
          {filteredCount !== rows.length
            ? `${filteredCount} of ${totalRows.toLocaleString()} rows`
            : `${totalRows.toLocaleString()} rows`}
          {" · "}{columns.length} cols
        </span>

        {/* Export */}
        <div className="flex items-center gap-1 ml-auto">
          <Button variant="ghost" size="sm" className="h-7 text-xs gap-1" onClick={handleExportCsv}>
            <Download className="h-3 w-3" /> CSV
          </Button>
          <Button variant="ghost" size="sm" className="h-7 text-xs gap-1" onClick={handleExportJson}>
            <Download className="h-3 w-3" /> JSON
          </Button>
          {onExpand && (
            <Button variant="ghost" size="sm" className="h-7 text-xs gap-1" onClick={onExpand}>
              <Maximize2 className="h-3 w-3" /> Expand
            </Button>
          )}
        </div>
      </div>

      {/* Column filter row */}
      {activeFilter && (
        <div className="flex items-center gap-2 border-b px-3 py-1.5 bg-muted/20">
          <span className="text-xs text-muted-foreground">Filter: {activeFilter}</span>
          <Input
            autoFocus
            placeholder={`Filter ${activeFilter}...`}
            value={(table.getColumn(activeFilter)?.getFilterValue() as string) ?? ""}
            onChange={(e) => table.getColumn(activeFilter)?.setFilterValue(e.target.value)}
            className="h-6 w-48 text-xs bg-background"
          />
          <button
            onClick={() => {
              table.getColumn(activeFilter)?.setFilterValue("");
              setActiveFilter(null);
            }}
            className="text-xs text-muted-foreground hover:text-foreground"
          >
            <X className="h-3 w-3" />
          </button>
        </div>
      )}

      {/* Table */}
      <div className="overflow-auto" style={{ maxHeight: compact ? "400px" : "600px" }}>
        <table className="w-full border-collapse text-sm">
          <thead className="sticky top-0 z-10">
            {table.getHeaderGroups().map((hg) => (
              <tr key={hg.id} className="border-b bg-muted/50">
                <th className="w-10 px-2 py-2 text-center text-xs font-normal text-muted-foreground/50">#</th>
                {hg.headers.map((header) => (
                  <th
                    key={header.id}
                    className="whitespace-nowrap px-3 py-2 text-left text-xs text-muted-foreground bg-muted/50 cursor-pointer select-none"
                    onDoubleClick={() => setActiveFilter(header.column.id)}
                    title="Double-click to filter"
                  >
                    {flexRender(header.column.columnDef.header, header.getContext())}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map((row, idx) => (
              <tr
                key={row.id}
                className="border-b transition-colors hover:bg-muted/30 even:bg-muted/5"
              >
                <td className="w-10 px-2 py-1.5 text-center text-xs text-muted-foreground/40 tabular-nums">
                  {pageIndex * table.getState().pagination.pageSize + idx + 1}
                </td>
                {row.getVisibleCells().map((cell) => (
                  <td key={cell.id} className="px-3 py-1.5 text-foreground max-w-[300px] truncate">
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))}
            {table.getRowModel().rows.length === 0 && (
              <tr>
                <td colSpan={columns.length + 1} className="px-4 py-8 text-center text-sm text-muted-foreground">
                  No matching rows
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {pageCount > 1 && (
        <div className="flex items-center justify-between border-t px-3 py-2 bg-muted/20">
          <span className="text-xs text-muted-foreground">
            Page {pageIndex + 1} of {pageCount}
          </span>
          <div className="flex items-center gap-1">
            <Button variant="ghost" size="sm" className="h-7 w-7 p-0" onClick={() => table.setPageIndex(0)} disabled={!table.getCanPreviousPage()}>
              <ChevronsLeft className="h-3.5 w-3.5" />
            </Button>
            <Button variant="ghost" size="sm" className="h-7 w-7 p-0" onClick={() => table.previousPage()} disabled={!table.getCanPreviousPage()}>
              <ChevronLeft className="h-3.5 w-3.5" />
            </Button>
            <Button variant="ghost" size="sm" className="h-7 w-7 p-0" onClick={() => table.nextPage()} disabled={!table.getCanNextPage()}>
              <ChevronRight className="h-3.5 w-3.5" />
            </Button>
            <Button variant="ghost" size="sm" className="h-7 w-7 p-0" onClick={() => table.setPageIndex(pageCount - 1)} disabled={!table.getCanNextPage()}>
              <ChevronsRight className="h-3.5 w-3.5" />
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}
