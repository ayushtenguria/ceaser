"""Sheet Extractor Agent — extracts one sheet into a clean DataFrame.

Single job: given a file + sheet name, return a clean DataFrame.
DEFENSIVE: every operation wrapped in try/except. If one sheet fails,
others continue. If one column fails, others continue.

Handles: offset headers, merged cells, duplicate columns, mixed types,
formula errors (#REF!, #N/A), special chars, encoding issues, large files.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from app.agents.excel.edge_case_logger import log_edge_case
from app.agents.excel.inspector import WorkbookInspection

logger = logging.getLogger(__name__)

_HEADER_SCAN_ROWS = 20
_SAMPLE_SIZE = 3_000
_MAX_COL_NAME_LEN = 60


@dataclass
class ExtractedSheet:
    """A single extracted and cleaned sheet."""

    name: str
    df: pd.DataFrame
    row_count: int = 0
    column_count: int = 0
    column_types: dict[str, str] = field(default_factory=dict)
    sample_values: dict[str, list] = field(default_factory=list)
    original_header_row: int = 0
    warnings: list[str] = field(default_factory=list)

    def __post_init__(self):
        self.row_count = len(self.df)
        self.column_count = len(self.df.columns)


def extract_sheet(
    file_path: str,
    sheet_name: str | None = None,
    inspection: WorkbookInspection | None = None,
    max_rows: int = 2_000_000,
) -> ExtractedSheet:
    """Extract one sheet from a file into a clean DataFrame."""
    path = Path(file_path)
    suffix = path.suffix.lower()

    try:
        if suffix == ".csv":
            return _extract_csv(path, inspection, max_rows)
        elif suffix in (".xlsx", ".xls", ".xlsm"):
            return _extract_excel_sheet(path, sheet_name or "", max_rows)
        else:
            log_edge_case(
                file_name=path.name,
                category="parse",
                description=f"Unknown extension '{suffix}', trying CSV fallback",
            )
            return _extract_csv(path, inspection, max_rows)
    except Exception as exc:
        log_edge_case(
            file_name=path.name,
            sheet_name=sheet_name or "",
            category="parse",
            description=f"Complete extraction failure: {exc}",
            raw_error=str(exc),
        )
        return ExtractedSheet(
            name=sheet_name or path.stem, df=pd.DataFrame(), warnings=[f"Failed to extract: {exc}"]
        )


def extract_all_sheets(
    file_path: str,
    inspection: WorkbookInspection | None = None,
    max_rows: int = 2_000_000,
) -> list[ExtractedSheet]:
    """Extract ALL sheets. Never fails completely — returns whatever it can."""
    path = Path(file_path)
    suffix = path.suffix.lower()

    if suffix == ".csv":
        result = _safe_extract(path.name, "", lambda: _extract_csv(path, inspection, max_rows))
        return [result] if result.row_count > 0 else []

    elif suffix in (".xlsx", ".xls", ".xlsm"):
        try:
            xl = pd.ExcelFile(path)
            sheet_names = xl.sheet_names
            xl.close()
        except Exception as exc:
            log_edge_case(
                file_name=path.name,
                category="parse",
                description=f"Cannot read sheet names: {exc}",
                raw_error=str(exc),
            )
            try:
                import openpyxl

                wb = openpyxl.load_workbook(path, read_only=True)
                sheet_names = wb.sheetnames
                wb.close()
            except Exception:
                return []

        sheets = []
        for i, name in enumerate(sheet_names):
            logger.info("Extracting sheet '%s' (%d/%d)...", name, i + 1, len(sheet_names))
            result = _safe_extract(
                path.name, name, lambda n=name: _extract_excel_sheet(path, n, max_rows)
            )
            if result.row_count > 0:
                sheets.append(result)
                logger.info("  → %d rows, %d cols", result.row_count, result.column_count)
            else:
                logger.info("  → empty or failed, skipped")

        return sheets

    else:
        result = _safe_extract(path.name, "", lambda: _extract_csv(path, inspection, max_rows))
        return [result] if result.row_count > 0 else []


def _safe_extract(file_name: str, sheet_name: str, fn) -> ExtractedSheet:
    """Safely call an extraction function. Never raises."""
    try:
        return fn()
    except MemoryError:
        log_edge_case(
            file_name=file_name,
            sheet_name=sheet_name,
            category="memory",
            description="Out of memory during extraction",
        )
        return ExtractedSheet(
            name=sheet_name or file_name,
            df=pd.DataFrame(),
            warnings=["Sheet too large — out of memory"],
        )
    except Exception as exc:
        log_edge_case(
            file_name=file_name,
            sheet_name=sheet_name,
            category="parse",
            description=f"Extraction failed: {type(exc).__name__}: {exc}",
            raw_error=str(exc),
        )
        return ExtractedSheet(
            name=sheet_name or file_name, df=pd.DataFrame(), warnings=[f"Failed: {exc}"]
        )


def _extract_csv(
    path: Path, inspection: WorkbookInspection | None, max_rows: int
) -> ExtractedSheet:
    """Extract a CSV file with encoding/delimiter fallback chain."""
    encoding = inspection.encoding if inspection else "utf-8"
    delimiter = inspection.delimiter if inspection else ","
    warnings: list[str] = []

    header_row = 0
    for enc in [encoding, "utf-8", "latin-1", "cp1252", "iso-8859-1"]:
        try:
            raw_df = pd.read_csv(
                path,
                encoding=enc,
                sep=delimiter,
                header=None,
                nrows=_HEADER_SCAN_ROWS,
                on_bad_lines="skip",
            )
            header_row = _detect_header_row(raw_df)
            encoding = enc
            break
        except Exception:
            continue

    df = None
    for enc in [encoding, "utf-8", "latin-1", "cp1252"]:
        try:
            df = pd.read_csv(
                path,
                encoding=enc,
                sep=delimiter,
                header=header_row,
                on_bad_lines="skip",
                low_memory=False,
                nrows=max_rows,
            )
            break
        except Exception:
            continue

    if df is None:
        try:
            df = pd.read_csv(
                path, encoding="utf-8", errors="replace", on_bad_lines="skip", nrows=max_rows
            )
            warnings.append("Encoding issues — some characters may be corrupted")
        except Exception:
            df = pd.DataFrame()

    if len(df) >= max_rows:
        warnings.append(f"Truncated to {max_rows:,} rows")

    df = _clean_dataframe(df, path.name)
    col_types = _detect_column_types(df, path.name)
    samples = _extract_sample_values(df)

    return ExtractedSheet(
        name=path.stem,
        df=df,
        column_types=col_types,
        sample_values=samples,
        original_header_row=header_row,
        warnings=warnings,
    )


def _extract_excel_sheet(path: Path, sheet_name: str, max_rows: int) -> ExtractedSheet:
    """Extract a single Excel sheet with full edge case handling."""
    warnings: list[str] = []

    try:
        raw_df = pd.read_excel(path, sheet_name=sheet_name, header=None, nrows=_HEADER_SCAN_ROWS)
    except Exception as exc:
        log_edge_case(
            file_name=path.name,
            sheet_name=sheet_name,
            category="parse",
            description=f"Cannot read sheet for header detection: {exc}",
        )
        return ExtractedSheet(name=sheet_name, df=pd.DataFrame(), warnings=[str(exc)])

    if raw_df.empty or raw_df.shape[1] < 1:
        return ExtractedSheet(name=sheet_name, df=pd.DataFrame(), warnings=["Empty sheet"])

    header_row = _detect_header_row(raw_df)

    try:
        df = pd.read_excel(path, sheet_name=sheet_name, header=header_row, nrows=max_rows)
    except Exception as exc:
        log_edge_case(
            file_name=path.name,
            sheet_name=sheet_name,
            category="header",
            description=f"Read with header={header_row} failed, trying header=0",
            raw_error=str(exc),
        )
        try:
            df = pd.read_excel(path, sheet_name=sheet_name, header=0, nrows=max_rows)
            header_row = 0
        except Exception:
            return ExtractedSheet(name=sheet_name, df=pd.DataFrame(), warnings=[str(exc)])

    if df.empty:
        return ExtractedSheet(name=sheet_name, df=pd.DataFrame(), warnings=["No data rows"])

    if len(df) >= max_rows:
        warnings.append(f"Truncated to {max_rows:,} rows")

    df = _clean_dataframe(df, path.name, sheet_name)
    col_types = _detect_column_types(df, path.name, sheet_name)
    samples = _extract_sample_values(df)

    return ExtractedSheet(
        name=sheet_name,
        df=df,
        column_types=col_types,
        sample_values=samples,
        original_header_row=header_row,
        warnings=warnings,
    )


def _detect_header_row(raw_df: pd.DataFrame) -> int:
    """Detect which row is the header. Returns 0 on any failure."""
    try:
        best_row = 0
        best_score = -1

        for i in range(min(len(raw_df), _HEADER_SCAN_ROWS)):
            row = raw_df.iloc[i]
            non_null = int(row.notna().sum())
            if non_null == 0:
                continue

            total = len(row)

            str_vals = []
            for v in row:
                try:
                    if pd.notna(v):
                        s = str(v).strip()
                        if s and s.lower() != "nan":
                            str_vals.append(s)
                except Exception:
                    pass

            str_count = len(str_vals)
            if str_count == 0:
                continue

            str_ratio = str_count / total
            unique_ratio = len(set(str_vals)) / max(str_count, 1)
            non_null_ratio = non_null / total

            numeric_count = 0
            for v in row:
                try:
                    if isinstance(v, (int, float)) and not isinstance(v, bool):
                        numeric_count += 1
                except Exception:
                    pass
            numeric_penalty = numeric_count / total

            header_like = sum(
                1
                for s in str_vals
                if 2 <= len(s) <= 50
                and not s.replace(".", "")
                .replace("-", "")
                .replace("/", "")
                .replace(",", "")
                .isdigit()
            )
            header_bonus = header_like / max(str_count, 1)

            sparse_penalty = max(0, 0.5 - non_null_ratio) * 2

            score = (
                (str_ratio * 3)
                + (unique_ratio * 2)
                + (non_null_ratio * 2)
                + (header_bonus * 3)
                - (numeric_penalty * 2)
                - (sparse_penalty * 2)
            )

            if score > best_score:
                best_score = score
                best_row = i

        return best_row

    except Exception as exc:
        logger.debug("Header detection failed, defaulting to 0: %s", exc)
        return 0


def _clean_dataframe(df: pd.DataFrame, file_name: str = "", sheet_name: str = "") -> pd.DataFrame:
    """Clean DataFrame. Never raises — always returns something usable."""
    if df.empty:
        return df

    try:
        df.columns = _clean_column_names(df.columns)
    except Exception as exc:
        log_edge_case(
            file_name=file_name,
            sheet_name=sheet_name,
            category="header",
            description=f"Column name cleaning failed: {exc}",
        )
        df.columns = [f"col_{i}" for i in range(len(df.columns))]

    try:
        df = df.dropna(how="all")
        df = df.dropna(axis=1, how="all")
    except Exception:
        pass

    try:
        seen: dict[str, int] = {}
        new_cols = []
        for col in df.columns:
            if col in seen:
                seen[col] += 1
                new_cols.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                new_cols.append(col)
        df.columns = new_cols
    except Exception:
        pass

    try:
        cols_to_drop = []
        for col in df.columns:
            if col.startswith("unnamed"):
                try:
                    null_ratio = df[col].isna().sum() / max(len(df), 1)
                    if null_ratio > 0.9:
                        cols_to_drop.append(col)
                except Exception:
                    pass
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
    except Exception:
        pass

    try:
        for col in df.select_dtypes(include=["object"]).columns:
            try:
                series = df[col]
                if isinstance(series, pd.DataFrame):
                    continue
                df[col] = series.astype(str).str.strip()
                df[col] = df[col].replace({"nan": None, "None": None, "": None, "NaT": None})
            except Exception:
                pass
    except Exception:
        pass

    try:
        formula_errors = {"#REF!", "#N/A", "#DIV/0!", "#VALUE!", "#NAME?", "#NULL!", "#NUM!"}
        for col in df.columns:
            try:
                if df[col].dtype == object:
                    mask = df[col].isin(formula_errors)
                    if mask.any():
                        df.loc[mask, col] = None
                        log_edge_case(
                            file_name=file_name,
                            sheet_name=sheet_name,
                            category="data",
                            description=f"Column '{col}' had {mask.sum()} formula error values, replaced with null",
                        )
            except Exception:
                pass
    except Exception:
        pass

    try:
        df = df.reset_index(drop=True)
    except Exception:
        pass

    return df


def _clean_column_names(columns: pd.Index) -> list[str]:
    """Clean column names: lowercase, strip, replace special chars. Never raises."""
    clean = []
    seen: dict[str, int] = {}

    for col in columns:
        try:
            name = str(col).strip().lower()
            name = name.replace("\n", " ").replace("\r", " ")
            name = re.sub(r"[^\w\s]", "_", name)
            name = re.sub(r"\s+", "_", name)
            name = re.sub(r"_+", "_", name).strip("_")
            name = name[:_MAX_COL_NAME_LEN]
            if not name or name == "nan":
                name = "unnamed"
        except Exception:
            name = f"col_{len(clean)}"

        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        clean.append(name)

    return clean


def _detect_column_types(
    df: pd.DataFrame, file_name: str = "", sheet_name: str = ""
) -> dict[str, str]:
    """Detect column types. Never raises — defaults to 'string' on any failure."""
    types: dict[str, str] = {}
    sample = df.head(_SAMPLE_SIZE) if len(df) > _SAMPLE_SIZE else df

    for col in sample.columns:
        try:
            series = sample[col].dropna()
            if isinstance(series, pd.DataFrame):
                types[col] = "string"
                continue
            if series.empty:
                types[col] = "empty"
                continue
            if pd.api.types.is_numeric_dtype(series):
                types[col] = "numeric"
                continue
            if pd.api.types.is_datetime64_any_dtype(series):
                types[col] = "date"
                continue

            str_vals = series.astype(str)

            try:
                numeric = pd.to_numeric(str_vals.str.replace(",", "", regex=False), errors="coerce")
                if numeric.notna().sum() / max(len(str_vals), 1) > 0.8:
                    types[col] = "numeric"
                    continue
            except Exception:
                pass

            try:
                dates = pd.to_datetime(str_vals, errors="coerce", infer_datetime_format=True)
                if dates.notna().sum() / max(len(str_vals), 1) > 0.6:
                    types[col] = "date"
                    continue
            except Exception:
                pass

            types[col] = "string"

        except Exception as exc:
            types[col] = "string"
            log_edge_case(
                file_name=file_name,
                sheet_name=sheet_name,
                category="type",
                description=f"Type detection failed for '{col}': {exc}",
            )

    return types


def _extract_sample_values(df: pd.DataFrame, n: int = 5) -> dict[str, list]:
    """Extract sample values. Never raises."""
    samples: dict[str, list] = {}
    for col in df.columns:
        try:
            series = df[col]
            if isinstance(series, pd.DataFrame):
                samples[col] = []
                continue
            unique = series.dropna().unique()[:n]
            samples[col] = [v.item() if hasattr(v, "item") else str(v) for v in unique]
        except Exception:
            samples[col] = []
    return samples
