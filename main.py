import os

import pandas as pd

import src.transformations as T
from src import OUTPUT_ROOT
from src.Logging import logger


def main():
    input_xlsx = 'data/input/ZOTCM_0010_0002_11M_11N.xlsx'
    df = pd.read_excel(input_xlsx)

    os.makedirs(OUTPUT_ROOT, exist_ok=True)

    logger.info("Excel file loaded successfully.")
    logger.info(f"Initial columns: {list(df.columns)}")

    break_template = f"{'-' * 20} X {'-' * 20}"

    # Apply transformation steps
    # ==================================== STEP 1 ====================================
    try:
        logger.info(break_template.replace('X', 'STEP 1'))
        df = T.step_01(df, save=True)
        logger.info("✅ Transformation step applied successfully.")
    except Exception as e:
        logger.error(f"Error applying transformation step 'step_01_del_cols': {e}")

    # ==================================== STEP 2 ====================================
    try:
        logger.info(break_template.replace('X', 'STEP 2'))
        wb = T.step_02(
            file_in=df,
            sheet_name=None,
            header_scan_rows=20,
            keep_net_value_blanks=True,
            save=True
        )
        logger.info("✅ Transformation step applied successfully.")
    except Exception as e:
        logger.error(f"Error applying transformation step: {e}")

    # ==================================== STEP 3 ====================================
    try:
        logger.info(break_template.replace('X', 'STEP 3'))
        T.step_03(wb, "Last G/I Date", treat_as_date=True, save_name='step3_sorted_by_date.xlsx')
        T.step_03(wb, "Name 2", save_name='step3_sorted_by_name2.xlsx')
        T.step_03(wb, "Name of ship-to party", save_name='step3_sorted_by_shipto.xlsx')
        logger.info("✅ Transformation step applied successfully.")
    except Exception as e:
        logger.error(f"Error applying transformation step: {e}")


if __name__ == "__main__":
    main()
