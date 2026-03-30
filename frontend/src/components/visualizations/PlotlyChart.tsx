import { useEffect, useRef, useCallback } from "react";
import type { PlotlyFigure } from "@/types";

let _Plotly: any = null;

function getPlotly(): Promise<any> {
  if (_Plotly) return Promise.resolve(_Plotly);
  return import("plotly.js-dist-min").then((mod) => {
    _Plotly = mod.default || mod;
    return _Plotly;
  });
}

const DARK_LAYOUT = {
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
  margin: { t: 40, r: 20, b: 60, l: 60 },
  autosize: true,
  legend: {
    bgcolor: "transparent",
    font: { color: "#94a3b8" },
  },
};

export default function PlotlyChart({ figure }: { figure: PlotlyFigure }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const plotRef = useRef<boolean>(false);

  const renderChart = useCallback(async () => {
    const el = containerRef.current;
    if (!el || !figure?.data?.length) return;

    const Plotly = await getPlotly();

    // Merge layout — remove any fixed width/height from the backend so autosize works
    const backendLayout = { ...(figure.layout || {}) };
    delete backendLayout.width;
    delete backendLayout.height;

    const mergedLayout = {
      ...DARK_LAYOUT,
      ...backendLayout,
      autosize: true,
      font: { ...DARK_LAYOUT.font, ...(backendLayout.font || {}) },
      xaxis: { ...DARK_LAYOUT.xaxis, ...(backendLayout.xaxis || {}) },
      yaxis: { ...DARK_LAYOUT.yaxis, ...(backendLayout.yaxis || {}) },
    };

    // If already plotted, relayout instead of full re-render
    if (plotRef.current) {
      try {
        Plotly.relayout(el, { autosize: true });
      } catch {
        // ignore
      }
      return;
    }

    plotRef.current = true;
    Plotly.newPlot(el, figure.data, mergedLayout, {
      responsive: true,
      displayModeBar: true,
      displaylogo: false,
      modeBarButtonsToRemove: ["lasso2d", "select2d"],
    });
  }, [figure]);

  // Initial render
  useEffect(() => {
    renderChart();

    return () => {
      if (containerRef.current && _Plotly) {
        try { _Plotly.purge(containerRef.current); } catch {}
      }
      plotRef.current = false;
    };
  }, [renderChart]);

  // ResizeObserver — re-layout chart when container width changes
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const observer = new ResizeObserver(() => {
      if (plotRef.current && _Plotly && el) {
        try { _Plotly.Plots.resize(el); } catch {}
      }
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  if (!figure || !figure.data || !Array.isArray(figure.data) || figure.data.length === 0) {
    return (
      <div className="flex h-32 items-center justify-center rounded-lg border bg-card text-sm text-muted-foreground">
        No chart data available
      </div>
    );
  }

  return (
    <div className="w-full overflow-hidden rounded-lg border bg-card">
      <div ref={containerRef} style={{ width: "100%", minHeight: 400 }} />
    </div>
  );
}
