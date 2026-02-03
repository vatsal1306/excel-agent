import os
import time

import pandas as pd

import src.transformations as T
from src import OUTPUT_ROOT
from src.Logging import logger


def main():
    input_xlsx = 'data/input/ZOTCM_0010_0002_11M_11N (2).xlsx'
    df = pd.read_excel(input_xlsx)

    os.makedirs(OUTPUT_ROOT, exist_ok=True)

    logger.info("Excel file loaded successfully.")
    logger.info(f"Initial columns: {list(df.columns)}")

    break_template = f"{'-' * 20} X {'-' * 20}"

    # Apply transformation steps
    # ==================================== STEP 1 ====================================
    try:
        logger.info(break_template.replace('X', 'STEP 1'))
        ts = time.perf_counter()

        df = T.step_01(df, save=True)

        logger.info(f"✅ Step 1 done in {time.perf_counter() - ts} seconds.")
    except Exception as e:
        logger.error(f"Error applying transformation step 'step_01_del_cols': {e}")

    # ==================================== STEP 2 ====================================
    try:
        logger.info(break_template.replace('X', 'STEP 2'))
        ts = time.perf_counter()

        wb = T.step_02(
            file_in=df,
            sheet_name=None,
            header_scan_rows=20,
            keep_net_value_blanks=True,
            save=True
        )

        logger.info(f"✅ Step 2 done in {time.perf_counter() - ts} seconds.")
    except Exception as e:
        logger.error(f"Error applying transformation step: {e}")

    # ==================================== STEP 3 ====================================
    try:
        logger.info(break_template.replace('X', 'STEP 3'))
        ts = time.perf_counter()

        wb = T.step_03(wb, "Last G/I Date", treat_as_date=True, save_name='step3_sorted_by_date.xlsx')
        wb = T.step_03(wb, "Name 2", save_name='step3_sorted_by_name2.xlsx')
        wb = T.step_03(wb, "Name of ship-to party", save_name='step3_sorted_by_shipto.xlsx')

        logger.info(f"✅ Step 3 done in {time.perf_counter() - ts} seconds.")
    except Exception as e:
        logger.error(f"Error applying transformation step: {e}")

    # ==================================== STEP 4 ====================================
    try:
        logger.info(break_template.replace("X", "STEP 4"))
        ts = time.perf_counter()

        T.step_04_create_distribution_tabs(
            wb,
            source_sheet_name=None,
            header_scan_rows=20,
            save=True,
            save_name="step4_distribution_tabs.xlsx",
        )

        logger.info(f"✅ Step 4 done in {time.perf_counter() - ts} seconds.")
    except Exception as e:
        logger.error(f"Error applying transformation step 'step_04': {e}")


if __name__ == "__main__":
    main()
