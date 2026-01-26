import os
import re
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd


@dataclass
class StepResult:
    """
    Represents the result of a single transformation step.

    Attributes:
        step_no: Sequential step number (1-based).
        description: Human-readable description of what the step did.
        output_path: Path to the saved workbook produced by this step.
    """
    step_no: int
    description: str
    output_path: str


def _normalize_header(value: str) -> str:
    """
    Normalize a column header for robust comparisons.

    Args:
        value: Header value.

    Returns:
        Normalized header: lowercased with collapsed whitespace and punctuation simplified.
    """
    s = str(value).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def sanitize_sheet_name(name: str) -> str:
    """
    Sanitize a string into a valid Excel sheet name.

    Rules:
    - Max length 31
    - Cannot contain: : \\ / ? * [ ]
    - Cannot be empty

    Args:
        name: Desired sheet name.

    Returns:
        A safe sheet name.
    """
    bad = r"[:\\/?*\[\]]"
    cleaned = re.sub(bad, " ", str(name)).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = cleaned[:31].strip()
    return cleaned if cleaned else "Sheet"


def make_unique_sheet_name(existing: Sequence[str], desired: str) -> str:
    """
    Make a unique Excel sheet name given existing names.

    Args:
        existing: Existing sheet names.
        desired: Desired name.

    Returns:
        A unique sheet name not in `existing`.
    """
    base = sanitize_sheet_name(desired)
    if base not in existing:
        return base

    for i in range(2, 1000):
        candidate = sanitize_sheet_name(f"{base[:28]} {i}")
        if candidate not in existing:
            return candidate

    raise RuntimeError("Unable to create a unique sheet name (too many collisions).")


@dataclass(frozen=True)
class FilterRule:
    """
    Stores an Excel autofilter rule to be applied at save-time.

    Attributes:
        column_name: Column header to apply the filter on.
        criteria: XlsxWriter criteria string (e.g., 'x != 0', 'x == 0').
    """
    column_name: str
    criteria: str


@dataclass(frozen=True)
class MergeInstruction:
    """
    Represents a single merge instruction to apply in an output worksheet.

    Attributes:
        sheet_name: Sheet to apply the merge on.
        row_df_index: Row index in DataFrame space (0-based for data rows; header is separate).
        first_col: First column index (0-based).
        last_col: Last column index (0-based).
        text: Text to put in the merged cell.
        fmt_key: A logical key to choose a format (e.g., 'yellow_header').
    """
    sheet_name: str
    row_df_index: int
    first_col: int
    last_col: int
    text: str
    fmt_key: str


WorksheetPostProcessor = Callable[[object, object, pd.DataFrame, str, Dict[str, object]], None]
"""
Signature:
    postprocess(workbook, worksheet, df, sheet_name, format_cache) -> None
"""


class OpenOrderReportBuilder:
    """
    Read/transform/write helper for the Open Order report.

    Key design:
    - Steps live outside the class (in transformations.py).
    - This class stores workbook state (DataFrames) + Excel-view instructions
      (filters, hidden columns, merge instructions, per-sheet postprocessors).
    - Outputs are written using XlsxWriter (write-only), so we save snapshots after steps.
    """

    def __init__(self, input_path: str, output_dir: str = "outputs") -> None:
        """
        Initialize and load all sheets from the input workbook.

        Args:
            input_path: Path to the source .xlsx workbook.
            output_dir: Directory where step outputs will be written.
        """
        self.input_path = input_path
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        self._sheets: Dict[str, pd.DataFrame] = pd.read_excel(self.input_path, sheet_name=None)

        # Excel-view settings that must persist across saves:
        self._hidden_columns_by_name: List[str] = []
        self._filter_rules: List[FilterRule] = []
        self._merge_instructions: List[MergeInstruction] = []
        self._postprocessors: List[WorksheetPostProcessor] = []

    # ---------- state access ----------

    def get_sheets(self) -> Dict[str, pd.DataFrame]:
        """
        Get the current in-memory workbook state.

        Returns:
            Dict mapping sheet_name -> DataFrame.
        """
        return self._sheets

    def set_sheets(self, sheets: Dict[str, pd.DataFrame]) -> None:
        """
        Replace the current in-memory workbook state.

        Args:
            sheets: Dict mapping sheet_name -> DataFrame.
        """
        self._sheets = sheets

    def get_primary_sheet_name(self) -> str:
        """
        Get the "primary" sheet name (first loaded sheet).

        Returns:
            The first sheet name in the workbook.
        """
        return next(iter(self._sheets.keys()))

    def get_primary_df(self) -> pd.DataFrame:
        """
        Get the primary sheet DataFrame.

        Returns:
            The primary sheet DataFrame.
        """
        return self._sheets[self.get_primary_sheet_name()]

    def set_primary_df(self, df: pd.DataFrame) -> None:
        """
        Replace the primary sheet DataFrame.

        Args:
            df: New DataFrame for the primary sheet.
        """
        name = self.get_primary_sheet_name()
        self._sheets[name] = df

    # ---------- transformations helpers ----------

    def drop_first_n_columns(self, n: int, df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Drop the first N columns (Excel-style: A.. etc.) from a DataFrame.

        Args:
            n: Number of leading columns to remove.
            df: Optional DataFrame. If None, uses the primary DataFrame.

        Returns:
            A new DataFrame with the first N columns removed.

        Raises:
            ValueError: If the DataFrame has fewer than N columns.
        """
        base = df if df is not None else self.get_primary_df()
        if base.shape[1] < n:
            raise ValueError(f"Cannot drop first {n} columns; DataFrame only has {base.shape[1]} columns.")
        return base.iloc[:, n:].copy()

    def drop_columns_by_names(
            self,
            columns: Iterable[str],
            df: Optional[pd.DataFrame] = None,
            ignore_missing: bool = True,
            synonyms: Optional[Dict[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """
        Drop columns by header name, with optional synonyms mapping.

        Args:
            columns: Target column names to drop.
            df: Optional DataFrame. If None, uses the primary DataFrame.
            ignore_missing: If True, missing columns are ignored.
            synonyms: Optional mapping {canonical_name: [alternate header spellings]}.

        Returns:
            A new DataFrame with specified columns removed (when present).
        """
        base = df if df is not None else self.get_primary_df()

        # Build a normalized header -> original header map for matching.
        norm_to_original: Dict[str, str] = {_normalize_header(c): c for c in base.columns}

        to_drop: List[str] = []
        for col in columns:
            candidates = [col]
            if synonyms and col in synonyms:
                candidates += synonyms[col]

            found = None
            for cand in candidates:
                key = _normalize_header(cand)
                if key in norm_to_original:
                    found = norm_to_original[key]
                    break

            if found is not None:
                to_drop.append(found)
            elif not ignore_missing:
                raise KeyError(f"Column '{col}' (or its synonyms) not found.")

        if not to_drop:
            return base.copy()

        return base.drop(columns=to_drop, errors="ignore").copy()

    def sort_by_columns(
            self,
            sort_cols: List[Tuple[str, bool]],
            df: Optional[pd.DataFrame] = None,
            date_cols: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Sort DataFrame by multiple columns.

        Args:
            sort_cols: List of tuples (column_name, ascending).
            df: Optional DataFrame. If None, uses the primary DataFrame.
            date_cols: Optional list of columns to parse as datetime before sorting.

        Returns:
            Sorted DataFrame.
        """
        base = df if df is not None else self.get_primary_df()
        out = base.copy()

        # Normalize mapping for robust column matching.
        norm_to_original: Dict[str, str] = {_normalize_header(c): c for c in out.columns}

        def resolve(name: str) -> str:
            key = _normalize_header(name)
            if key not in norm_to_original:
                raise KeyError(f"Sort column '{name}' not found in DataFrame.")
            return norm_to_original[key]

        # Parse date columns if requested.
        if date_cols:
            for c in date_cols:
                rc = resolve(c)
                out[rc] = pd.to_datetime(out[rc], errors="coerce")

        resolved_cols = [resolve(c) for c, _ in sort_cols]
        ascending = [asc for _, asc in sort_cols]

        return out.sort_values(by=resolved_cols, ascending=ascending, kind="mergesort").reset_index(drop=True)

    # ---------- Excel-view instructions (persist across saves) ----------

    def register_hidden_column(self, column_name: str) -> None:
        """
        Mark a column (by name) to be hidden at save-time (if present).

        Args:
            column_name: Column header to hide.
        """
        self._hidden_columns_by_name.append(column_name)

    def register_filter_rule(self, column_name: str, criteria: str) -> None:
        """
        Register an Excel autofilter rule to apply at save-time.

        Args:
            column_name: Column header to filter.
            criteria: XlsxWriter filter criteria string, e.g. 'x != 0', 'x == 0'.
        """
        self._filter_rules.append(FilterRule(column_name=column_name, criteria=criteria))

    def add_merge_instruction(self, instr: MergeInstruction) -> None:
        """
        Add a merge instruction to apply at save-time.

        Args:
            instr: MergeInstruction describing the merge operation.
        """
        self._merge_instructions.append(instr)

    def add_postprocessor(self, fn: WorksheetPostProcessor) -> None:
        """
        Add a worksheet postprocessor. All postprocessors run on every save.

        Args:
            fn: Callback invoked after a sheet is written.
        """
        self._postprocessors.append(fn)

    def find_column_index(self, df: pd.DataFrame, column_name: str) -> Optional[int]:
        """
        Find a column index by name using normalized matching.

        Args:
            df: DataFrame to search.
            column_name: Header name to locate.

        Returns:
            Zero-based column index, or None if not found.
        """
        target = _normalize_header(column_name)
        for i, c in enumerate(df.columns):
            if _normalize_header(c) == target:
                return i
        return None

    # ---------- output ----------

    def save_snapshot(self, step_no: int, description: str, filename_slug: str) -> StepResult:
        """
        Save a snapshot workbook reflecting the current in-memory sheets and
        all registered Excel-view instructions.

        Includes:
        - header bold
        - freeze panes (row 1)
        - width auto-sizing (sampled)
        - autofilter range + registered filter rules
        - hidden columns
        - registered merges
        - registered postprocessors

        Args:
            step_no: Sequential step number (1-based).
            description: Human-readable description.
            filename_slug: Slug for the output filename.

        Returns:
            StepResult with output path.
        """
        safe_slug = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in filename_slug).strip("_")
        out_path = os.path.join(self.output_dir, f"output_step_{step_no:02d}_{safe_slug}.xlsx")

        with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
            workbook = writer.book
            format_cache: Dict[str, object] = {}

            # Common formats
            format_cache["header_bold"] = workbook.add_format({"bold": True})
            format_cache["yellow_header"] = workbook.add_format(
                {"bold": True, "align": "center", "valign": "vcenter", "bg_color": "#FFFF00", "border": 1}
            )
            format_cache["red_line"] = workbook.add_format({"top": 2, "top_color": "red"})
            format_cache["yellow_fill"] = workbook.add_format({"bg_color": "#FFFF00"})

            for sheet_name, df in self._sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                worksheet = writer.sheets[sheet_name]

                # Header bold + widths
                for col_idx, col_name in enumerate(df.columns):
                    worksheet.write(0, col_idx, col_name, format_cache["header_bold"])

                    sample = df[col_name].astype(str).head(2000)
                    max_len = max([len(str(col_name))] + sample.map(len).tolist()) if len(sample) else len(
                        str(col_name))
                    width = min(max(10, max_len + 2), 60)
                    worksheet.set_column(col_idx, col_idx, width)

                worksheet.freeze_panes(1, 0)

                # Apply autofilter across full used range if we have any filter rules.
                if self._filter_rules:
                    last_row = len(df)  # header row is 0; data ends at len(df)
                    last_col = max(len(df.columns) - 1, 0)
                    worksheet.autofilter(0, 0, last_row, last_col)

                    # Apply all filter rules that match columns in this sheet.
                    for rule in self._filter_rules:
                        ci = self.find_column_index(df, rule.column_name)
                        if ci is not None:
                            worksheet.filter_column(ci, rule.criteria)

                # Hide any registered columns present in this sheet.
                for col_name in self._hidden_columns_by_name:
                    ci = self.find_column_index(df, col_name)
                    if ci is not None:
                        worksheet.set_column(ci, ci, None, None, {"hidden": 1})

                # Apply merges registered for this sheet.
                for instr in self._merge_instructions:
                    if instr.sheet_name != sheet_name:
                        continue
                    excel_row = instr.row_df_index + 1  # DataFrame row 0 -> Excel row 2 (index 1)
                    fmt = format_cache.get(instr.fmt_key, None)
                    worksheet.merge_range(excel_row, instr.first_col, excel_row, instr.last_col, instr.text, fmt)

                # Run any extra postprocessors (e.g., highlight a column).
                for fn in self._postprocessors:
                    fn(workbook, worksheet, df, sheet_name, format_cache)

        return StepResult(step_no=step_no, description=description, output_path=out_path)
