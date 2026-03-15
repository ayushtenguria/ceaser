import { useState, lazy, Suspense } from "react";
import type { PlotlyFigure } from "@/types";

const Plot = lazy(() => import("react-plotly.js"));

interface PlotlyChartProps {
  figure: PlotlyFigure;
}

const DARK_LAYOUT: Partial<Plotly.Layout> = {
  paper_bgcolor: "transparent",
  plot_bgcolor: "transparent",
  font: { color: "#94a3b8", size: 12 },
  xaxis: {
    gridcolor: "rgba(148, 163, 184, 0.1)",
    zerolinecolor: "rgba(148, 163, 184, 0.2)",
  },
  yaxis: {
    gridcolor: "rgba(148, 163, 184, 0.1)",
    zerolinecolor: "rgba(148, 163, 184, 0.2)",
  },
  margin: { t: 40, r: 20, b: 40, l: 60 },
  autosize: true,
  legend: {
    bgcolor: "transparent",
    font: { color: "#94a3b8" },
  },
};

export default function PlotlyChart({ figure }: PlotlyChartProps) {
  const [isLoading, setIsLoading] = useState(true);

  const mergedLayout: Partial<Plotly.Layout> = {
    ...DARK_LAYOUT,
    ...figure.layout,
    font: { ...DARK_LAYOUT.font, ...figure.layout?.font },
    xaxis: { ...DARK_LAYOUT.xaxis, ...figure.layout?.xaxis },
    yaxis: { ...DARK_LAYOUT.yaxis, ...figure.layout?.yaxis },
  };

  return (
    <div className="w-full overflow-hidden rounded-lg border bg-card">
      {isLoading && (
        <div className="flex h-64 items-center justify-center">
          <div className="h-6 w-6 animate-spin rounded-full border-2 border-primary border-t-transparent" />
        </div>
      )}
      <Suspense
        fallback={
          <div className="flex h-64 items-center justify-center">
            <div className="h-6 w-6 animate-spin rounded-full border-2 border-primary border-t-transparent" />
          </div>
        }
      >
        <Plot
          data={figure.data}
          layout={mergedLayout}
          config={{
            responsive: true,
            displayModeBar: true,
            displaylogo: false,
            modeBarButtonsToRemove: ["lasso2d", "select2d"],
          }}
          useResizeHandler
          className="h-auto w-full"
          style={{ width: "100%", minHeight: 300 }}
          onInitialized={() => setIsLoading(false)}
          onUpdate={() => setIsLoading(false)}
        />
      </Suspense>
    </div>
  );
}
