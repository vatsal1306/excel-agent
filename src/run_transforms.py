import datetime as dt
import os
import time
from io import BytesIO
from typing import Optional, Union

import pandas as pd

import src.transformations as T
from src import OUTPUT_ROOT
from src.Logging import logger

PathLikeOrBytes = Union[str, os.PathLike, bytes]


def _load_input_dataframe(input_file: PathLikeOrBytes) -> pd.DataFrame:
    if isinstance(input_file, bytes):
        return pd.read_excel(BytesIO(input_file))
    return pd.read_excel(input_file)


def run_pipeline(
        input_file: PathLikeOrBytes,
        *,
        output_root: Optional[Union[str, os.PathLike]] = None,
        report_date: Optional[dt.date] = None,
) -> str:
    resolved_output_root = os.fspath(output_root) if output_root is not None else OUTPUT_ROOT
    os.makedirs(resolved_output_root, exist_ok=True)

    df = _load_input_dataframe(input_file)

    logger.info("Excel file loaded successfully.")
    logger.info(f"Initial columns: {list(df.columns)}")
    logger.info(f"Output directory: {resolved_output_root}")

    break_template = f"{'-' * 30} X {'-' * 30}"

    # Apply transformation steps
    # ==================================== STEP 1 ====================================
    try:
        logger.info(break_template.replace('X', 'STEP 1'))
        ts = time.perf_counter()
        df = T.step_01(df, save=True, output_root=resolved_output_root)
        logger.info(f"Step 1 done in {time.perf_counter() - ts} seconds.")
    except Exception as e:
        logger.exception(f"Error applying transformation step 'step_01': {e}")
        raise

    # ==================================== STEP 2 ====================================
    try:
        logger.info(break_template.replace('X', 'STEP 2'))
        ts = time.perf_counter()
        wb = T.step_02(
            file_in=df,
            sheet_name=None,
            header_scan_rows=20,
            keep_net_value_blanks=True,
            save=True,
            output_root=resolved_output_root,
        )
        logger.info(f"Step 2 done in {time.perf_counter() - ts} seconds.")
    except Exception as e:
        logger.exception(f"Error applying transformation step 'step_02': {e}")
        raise

    # ==================================== STEP 3 ====================================
    try:
        logger.info(break_template.replace('X', 'STEP 3'))
        ts = time.perf_counter()
        wb = T.step_03(
            wb,
            "Last G/I Date",
            treat_as_date=True,
            save_name='step3_sorted_by_date.xlsx',
            output_root=resolved_output_root,
        )
        wb = T.step_03(
            wb,
            "Name 2",
            save_name='step3_sorted_by_name2.xlsx',
            output_root=resolved_output_root,
        )
        wb = T.step_03(
            wb,
            "Name of ship-to party",
            save_name='step3_sorted_by_shipto.xlsx',
            output_root=resolved_output_root,
        )
        logger.info(f"Step 3 done in {time.perf_counter() - ts} seconds.")
    except Exception as e:
        logger.exception(f"Error applying transformation step 'step_03': {e}")
        raise

    # ==================================== STEP 4 ====================================
    try:
        logger.info(break_template.replace("X", "STEP 4"))
        ts = time.perf_counter()
        wb = T.step_04_create_distribution_tabs(
            wb,
            source_sheet_name=None,
            header_scan_rows=20,
            save=True,
            save_name="step4_distribution_tabs.xlsx",
            output_root=resolved_output_root,
        )
        logger.info(f"Step 4 done in {time.perf_counter() - ts} seconds.")
    except Exception as e:
        logger.exception(f"Error applying transformation step 'step_04': {e}")
        raise

    # ==================================== STEP 5 ====================================
    try:
        logger.info(break_template.replace("X", "STEP 5"))
        ts = time.perf_counter()
        wb = T.step_05_create_orders_on_hold_tabs(
            wb,
            source_sheet_name="Sheet1",
            header_scan_rows=20,
            save=True,
            save_name="step5_orders_on_hold.xlsx",
            output_root=resolved_output_root,
        )
        logger.info(f"Step 5 done in {time.perf_counter() - ts} seconds.")
    except Exception as e:
        logger.exception(f"Error applying transformation step 'step_05': {e}")
        raise

    # ==================================== STEP 6 ====================================
    try:
        logger.info(break_template.replace("X", "STEP 6"))
        ts = time.perf_counter()
        wb = T.step_06_create_contractor_tabs(
            wb,
            min_lines=4,
            header_scan_rows=20,
            save=True,
            save_name="step6_contractor_tabs.xlsx",
            output_root=resolved_output_root,
        )
        logger.info(f"Step 6 done in {time.perf_counter() - ts} seconds.")
    except Exception as e:
        logger.exception(f"Error applying transformation step 'step_06': {e}")
        raise

    # ==================================== STEP 7 ====================================
    try:
        logger.info(break_template.replace("X", "STEP 7"))
        ts = time.perf_counter()
        T.step_07_export_tabs_to_pdfs(
            workbook_path=os.path.join(resolved_output_root, "step6_contractor_tabs.xlsx"),
            output_dir=os.path.join(resolved_output_root, "pdf_exports"),
            report_date=report_date or dt.date.today(),
            exclude_sheets=["Sheet1"],
        )
        logger.info(f"Step 7 done in {time.perf_counter() - ts} seconds.")
    except Exception as e:
        logger.exception(f"Error applying transformation step 'step_07': {e}")
        raise

    return resolved_output_root


def main():
    run_pipeline('data/input/ZOTCM_0010_0002_11M_11N (3).xlsx')


if __name__ == "__main__":
    main()
