import { X } from "lucide-react";
import { Button } from "@/components/ui/button";
import DataExplorer from "./DataExplorer";
import type { TableData } from "@/types";

interface DataExplorerSheetProps {
  data: TableData;
  title?: string;
  open: boolean;
  onClose: () => void;
}

export default function DataExplorerSheet({ data, title, open, onClose }: DataExplorerSheetProps) {
  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />

      {/* Sheet */}
      <div className="relative ml-auto flex h-full w-full max-w-[85vw] flex-col bg-background shadow-2xl animate-in slide-in-from-right duration-200">
        {/* Header */}
        <div className="flex items-center justify-between border-b px-4 py-3">
          <div>
            <h3 className="text-sm font-semibold">Data Explorer</h3>
            {title && <p className="text-xs text-muted-foreground">{title}</p>}
          </div>
          <Button variant="ghost" size="sm" className="h-8 w-8 p-0" onClick={onClose}>
            <X className="h-4 w-4" />
          </Button>
        </div>

        {/* Explorer */}
        <div className="flex-1 overflow-hidden p-4">
          <DataExplorer data={data} title={title} />
        </div>
      </div>
    </div>
  );
}
