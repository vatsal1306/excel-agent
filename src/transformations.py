import datetime
import datetime as dt
import math
import os.path
import re
from copy import copy
from io import BytesIO
from typing import Optional, Tuple

import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.utils.datetime import from_excel
from openpyxl.workbook.workbook import Workbook
from pandas.api.types import is_datetime64_any_dtype

from src import OUTPUT_ROOT
from src.Logging import logger


def step_01(df: pd.DataFrame, save: bool = False) -> pd.DataFrame:
    """ STEP 1 """

    cols_to_delete = [
        'Sales Office', 'Sales Group', 'Document Date', 'Payer', 'Name of additional partner',
        'Incoterms', 'Plant Name', 'Open Indicator', 'MSP Category', 'Material', 'Usage Description',
        'Gross weight', 'Weight unit'
    ]

    for col in cols_to_delete:
        try:
            df.drop(columns=[col], inplace=True)
        except KeyError:
            logger.error(f"Column not found, skipping: {col}")

    # --- Force "Last G/I Date" to TEXT DD/MM/YYYY (do NOT keep datetime dtype) ---
    date_col = "Last G/I Date"
    if date_col in df.columns:
        s = df[date_col]

        # Case 1: already a pandas datetime dtype
        if is_datetime64_any_dtype(s):
            df[date_col] = s.dt.strftime("%d/%m/%Y")  # :contentReference[oaicite:3]{index=3}
        else:
            # Case 2: mixed/object column – normalize any datetime-like objects
            def _fmt_one(x):
                if x is None or (isinstance(x, float) and pd.isna(x)):
                    return ""
                if isinstance(x, pd.Timestamp):
                    x = x.to_pydatetime()
                if isinstance(x, datetime.datetime):
                    d = x.date()
                    return f"{d.day:02d}/{d.month:02d}/{d.year}"
                if isinstance(x, datetime.date):
                    return f"{x.day:02d}/{x.month:02d}/{x.year}"

                # Case 3: strings like "2025-03-26 00:00:00" or other parseable variants
                if isinstance(x, str):
                    t = x.strip()
                    if t == "":
                        return ""
                    parsed = pd.to_datetime(t, dayfirst=True, errors="coerce")  # :contentReference[oaicite:4]{index=4}
                    if pd.notna(parsed):
                        d = parsed.date()
                        return f"{d.day:02d}/{d.month:02d}/{d.year}"
                    return t  # leave non-date strings as-is

                # fallback: stringify
                return str(x)

            df[date_col] = s.apply(_fmt_one).astype(str)

        # Ensure blanks don't show as "nan"/"NaT"
        df[date_col] = df[date_col].replace({"nan": "", "NaT": ""})

    else:
        logger.warning("Column 'Last G/I Date' not found; no date coercion applied.")

    logger.info(f"Remaining columns after deletion: {list(df.columns)}")

    if save:
        df.to_excel(os.path.join(OUTPUT_ROOT, 'output_step1.xlsx'), index=False)

    return df


def _norm(s) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip().casefold()


def _to_float(v):
    """Convert Excel cell values to float when possible; return None for blanks/non-numeric."""
    if v is None:
        return None
    if isinstance(v, bool):
        return float(int(v))
    if isinstance(v, (int, float)):
        try:
            if v != v:  # NaN
                return None
        except Exception:
            pass
        return float(v)
    if isinstance(v, str):
        t = v.strip()
        if t == "":
            return None
        t = re.sub(r"[,\u20B9$]", "", t)  # remove commas/currency symbols
        try:
            return float(t)
        except Exception:
            return None
    return None


def _excel_date_to_ddmmyyyy_string(v) -> str:
    """
    Convert various Excel/openpyxl date representations to 'DD/MM/YYYY' string.
    Keeps non-date strings as-is (stringified).
    openpyxl can return Python datetime/date when a cell is a date. :contentReference[oaicite:0]{index=0}
    Excel serial numbers can be converted with from_excel(). :contentReference[oaicite:1]{index=1}
    """
    if v is None:
        return ""
    if isinstance(v, datetime.datetime):
        d = v.date()
        return f"{d.day:02d}/{d.month:02d}/{d.year}"
    if isinstance(v, datetime.date):
        return f"{v.day:02d}/{v.month:02d}/{v.year}"
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        try:
            if isinstance(v, float) and math.isnan(v):
                return ""
        except Exception:
            pass
        try:
            d = from_excel(v)  # excel serial -> python datetime/date :contentReference[oaicite:2]{index=2}
            if isinstance(d, datetime.datetime):
                d = d.date()
            if isinstance(d, datetime.date):
                return f"{d.day:02d}/{d.month:02d}/{d.year}"
        except Exception:
            # not a date-serial; fall through to stringification
            pass
    # already text (or something else) -> keep as string
    return str(v)


def step_02(
        file_in: str | os.PathLike | pd.DataFrame,
        sheet_name: str | None = None,
        header_scan_rows: int = 20,
        keep_net_value_blanks: bool = True,
        save: bool = False,
):
    """ Step 2 """

    # ---- Load workbook ----
    if isinstance(file_in, pd.DataFrame):
        df = file_in.copy()

        # If DF already has 'Last G/I Date', force it to a DD/MM/YYYY text representation
        # before writing to Excel (prevents Excel/openpyxl date coercion downstream).
        if "Last G/I Date" in df.columns:
            s = df["Last G/I Date"]
            if pd.api.types.is_datetime64_any_dtype(s):
                df["Last G/I Date"] = s.dt.strftime("%d/%m/%Y")
            else:
                df["Last G/I Date"] = s.astype(str).replace("NaT", "").replace("nan", "")

        excel_stream = BytesIO()
        df.to_excel(
            excel_stream,
            index=False,
            sheet_name=sheet_name or "Sheet1",
            engine="openpyxl",
        )
        excel_stream.seek(0)
        wb = load_workbook(excel_stream)
    elif isinstance(file_in, (str, os.PathLike)):
        wb = load_workbook(file_in)
    else:
        raise TypeError("file_in must be a path to .xlsx (str/PathLike) or a pandas DataFrame")

    ws = wb[sheet_name] if sheet_name else wb.active
    targets = ["Net Value of Confirmed Quantity", "Invoice Qty", "MSP Invoice Qty"]

    # ---- Find header row (best match among first N rows) ----
    best_row, best_hits, best_map = None, -1, {}
    for r in range(1, min(header_scan_rows, ws.max_row) + 1):
        row_vals = [ws.cell(r, c).value for c in range(1, ws.max_column + 1)]
        col_map = {}
        for c, v in enumerate(row_vals, start=1):
            nv = _norm(v)
            for t in targets:
                if nv == _norm(t):
                    col_map[t] = c
        if len(col_map) > best_hits:
            best_hits, best_row, best_map = len(col_map), r, col_map

    if not best_row or best_hits == 0:
        raise ValueError("Could not locate header row / required columns in the first scanned rows.")

    header_row = best_row

    # ---- Force "Last G/I Date" to remain TEXT in DD/MM/YYYY ----
    # (This is the key change you asked for.)
    date_col = None
    for c in range(1, ws.max_column + 1):
        if _norm(ws.cell(header_row, c).value) == _norm("Last G/I Date"):
            date_col = c
            break

    if date_col is None:
        logger.warning("Missing column: 'Last G/I Date' (no date coercion applied).")
    else:
        for r in range(header_row + 1, ws.max_row + 1):
            cell = ws.cell(r, date_col)
            cell.value = _excel_date_to_ddmmyyyy_string(cell.value)
            # Force Excel to treat it as text (number formats are controlled via .number_format). :contentReference[oaicite:3]{index=3}
            cell.number_format = "@"

    # ---- Reset hidden rows so script is idempotent ----
    for r in range(1, ws.max_row + 1):
        if ws.row_dimensions[r].hidden:
            ws.row_dimensions[r].hidden = False

    net_col = best_map.get("Net Value of Confirmed Quantity")
    inv_col = best_map.get("Invoice Qty")

    # ---- Apply "filters" by hiding rows (deterministic) ----
    hidden_count = 0
    for r in range(header_row + 1, ws.max_row + 1):
        hide = False

        # 1) Net Value of Confirmed Quantity -> uncheck all zeros (exclude zeros)
        if net_col is not None:
            v = _to_float(ws.cell(r, net_col).value)
            if v == 0.0:
                hide = True
            elif v is None and not keep_net_value_blanks:
                hide = True
        else:
            logger.error("Missing column: 'Net Value of Confirmed Quantity' (skipping filter 1).")

        # 2) Invoice Qty -> only keep zeros checked (include only zeros)
        if inv_col is not None:
            v = _to_float(ws.cell(r, inv_col).value)
            if v != 0.0:  # includes None and non-zero
                hide = True
        else:
            logger.error("Missing column: 'Invoice Qty' (skipping filter 2).")

        if hide:
            ws.row_dimensions[r].hidden = True
            hidden_count += 1

    logger.info(f"Rows hidden by criteria: {hidden_count}")

    # ---- Ensure filter dropdowns exist in Excel UI ----
    ws.auto_filter.ref = f"A{header_row}:{get_column_letter(ws.max_column)}{ws.max_row}"

    # ---- Hide specified columns ----
    for col_name in targets:
        c = best_map.get(col_name)
        if c is None:
            logger.warning(f"Missing column: '{col_name}' (cannot hide).")
            continue
        ws.column_dimensions[get_column_letter(c)].hidden = True

    if save:
        wb.save(os.path.join(OUTPUT_ROOT, "output_step2.xlsx"))

    return wb


def _parse_date(v) -> Optional[dt.date]:
    """Parse many Excel-ish date representations into dt.date (for sorting only)."""
    if v is None or v == "":
        return None

    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v

    # Excel serial numbers sometimes come in as int/float
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        try:
            if isinstance(v, float) and math.isnan(v):
                return None
        except Exception:
            pass
        try:
            d = from_excel(v)
            return d.date() if isinstance(d, dt.datetime) else d
        except Exception:
            return None

    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None

        # Prefer DD/MM/YYYY and common variants
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y", "%Y-%m-%d", "%Y/%m/%d"):
            try:
                return dt.datetime.strptime(s, fmt).date()
            except Exception:
                pass

        # Optional: if python-dateutil is installed, this catches odd strings
        try:
            from dateutil import parser
            return parser.parse(s, dayfirst=True, fuzzy=True).date()
        except Exception:
            return None

    return None


def _date_to_d_mmyyyy(d: dt.date) -> str:
    # D/MM/YYYY (day no leading zero; month 2 digits)
    return f"{d.day}/{d.month:02d}/{d.year}"


def _find_header_and_col(ws, header_text: str, header_scan_rows: int = 20) -> Tuple[int, int]:
    target = _norm(header_text)
    for r in range(1, min(header_scan_rows, ws.max_row) + 1):
        for c in range(1, ws.max_column + 1):
            if _norm(ws.cell(r, c).value) == target:
                return r, c
    raise ValueError(f"Could not find column header '{header_text}' in first {header_scan_rows} rows.")


def _snapshot_rows(ws, start_row: int, end_row: int):
    """Snapshot rows including values + style so formatting survives reordering."""
    max_col = ws.max_column
    snap = []
    for r in range(start_row, end_row + 1):
        row_cells = []
        for c in range(1, max_col + 1):
            cell = ws.cell(r, c)
            row_cells.append({
                "value": cell.value,
                "style": copy(cell._style),
                "number_format": cell.number_format,
                "hyperlink": copy(cell.hyperlink) if cell.hyperlink else None,
                "comment": cell.comment,
            })
        rd = ws.row_dimensions[r]
        snap.append({
            "cells": row_cells,
            "row_hidden": bool(rd.hidden),
            "row_height": rd.height,
            "outline": rd.outlineLevel,
        })
    return snap


def _write_snapshot(ws, start_row: int, snap, text_col: Optional[int] = None, text_col_is_date=False):
    """Write rows back in new order; optionally force one column to TEXT strings."""
    max_col = ws.max_column
    for i, row in enumerate(snap):
        tr = start_row + i

        rd = ws.row_dimensions[tr]
        rd.hidden = row["row_hidden"]
        rd.height = row["row_height"]
        rd.outlineLevel = row["outline"]

        for c in range(1, max_col + 1):
            cell = ws.cell(tr, c)
            src = row["cells"][c - 1]
            cell._style = copy(src["style"])
            cell.number_format = src["number_format"]
            cell.value = src["value"]
            cell._hyperlink = copy(src["hyperlink"]) if src["hyperlink"] else None
            cell.comment = src["comment"]

        # Force a specific column to remain TEXT in the saved workbook
        if text_col is not None:
            dc = ws.cell(tr, text_col)
            if text_col_is_date:
                d = _parse_date(dc.value)
                if d is None:
                    dc.value = "" if dc.value is None else str(dc.value)
                else:
                    dc.value = _date_to_d_mmyyyy(d)  # store as string
            else:
                dc.value = "" if dc.value is None else str(dc.value)
            dc.number_format = "@"  # Excel TEXT format :contentReference[oaicite:2]{index=2}


def step_03(input_wb: Workbook, header: str, treat_as_date: bool = False, header_scan_rows: int = 20,
            blanks_last: bool = True, save_name: str = None):
    """
    Independent sort: loads input fresh each time, sorts by ONE column only, saves output.
    """
    bio = BytesIO()
    input_wb.save(bio)  # save workbook to memory
    bio.seek(0)
    wb = load_workbook(bio)
    ws = wb.active

    header_row, col = _find_header_and_col(ws, header, header_scan_rows=header_scan_rows)
    start = header_row + 1
    end = ws.max_row

    rows = _snapshot_rows(ws, start, end)

    def key_func(row):
        v = row["cells"][col - 1]["value"]

        if treat_as_date:
            d = _parse_date(v)
            if d is None:
                return dt.date.max if blanks_last else dt.date.min
            return d

        # A→Z (case-insensitive), stable with original as tie-breaker
        s = "" if v is None else str(v).strip()
        if blanks_last and s == "":
            return ("\uffff",)  # pushes blanks to end
        return (s.casefold(), s)

    # One-key sort only (exactly what you asked)
    rows_sorted = sorted(rows,
                         key=key_func)  # Python sorting tools described here

    _write_snapshot(ws, start, rows_sorted, text_col=col, text_col_is_date=treat_as_date)

    if save_name:
        wb.save(os.path.join(OUTPUT_ROOT, save_name))
