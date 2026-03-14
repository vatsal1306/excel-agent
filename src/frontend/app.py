import os
import sys

import streamlit as st

APP_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR_PATH = os.path.dirname(os.path.dirname(APP_DIR))
if ROOT_DIR_PATH not in sys.path:
    sys.path.insert(0, ROOT_DIR_PATH)

from src import ROOT_DIR
from src.run_transforms import run_pipeline

DATA_DIR = os.path.join(ROOT_DIR, "data", "runs", "ui")


def _next_run_dir() -> str:
    os.makedirs(DATA_DIR, exist_ok=True)
    numeric_dirs = []

    for name in os.listdir(DATA_DIR):
        path = os.path.join(DATA_DIR, name)
        if os.path.isdir(path) and name.isdigit():
            numeric_dirs.append(int(name))

    next_run = max(numeric_dirs, default=0) + 1
    run_dir = os.path.join(DATA_DIR, str(next_run))
    os.makedirs(run_dir, exist_ok=False)
    return run_dir


def _list_output_files(run_dir: str) -> list[str]:
    files = []
    for root, _, filenames in os.walk(run_dir):
        for name in filenames:
            files.append(os.path.join(root, name))
    return sorted(files)


def _render_downloads(run_dir: str) -> None:
    files = _list_output_files(run_dir)
    if not files:
        st.info("No output files were generated.")
        return

    st.subheader("Output files")
    for file_path in files:
        relative_name = os.path.relpath(file_path, run_dir).replace(os.sep, "/")
        with open(file_path, "rb") as fh:
            st.download_button(
                label=f"Download {relative_name}",
                data=fh.read(),
                file_name=os.path.basename(file_path),
                mime="application/octet-stream",
                key=f"download-{relative_name}",
            )


def _render_job_runner() -> None:
    st.subheader("Run Transformations")
    uploaded_file = st.file_uploader("Upload an Excel file", type=["xlsx", "xls"])

    if uploaded_file is None:
        return

    st.caption(f"Selected file: {uploaded_file.name}")

    if st.button("Run job", type="primary"):
        try:
            uploaded_bytes = uploaded_file.getvalue()
            run_dir = _next_run_dir()

            with st.spinner("Running transformations..."):
                run_pipeline(uploaded_bytes, output_root=run_dir)

            st.session_state["last_run_dir"] = run_dir
            st.success(f"Job completed. Run folder: {os.path.basename(run_dir)}")
        except Exception as exc:
            st.error(f"Job failed: {exc}")

    last_run_dir = st.session_state.get("last_run_dir")
    if last_run_dir:
        _render_downloads(last_run_dir)


def main() -> None:
    st.set_page_config(page_title="Excel Transformations", layout="centered")
    st.title("Excel Transformations")

    if "show_runner" not in st.session_state:
        st.session_state["show_runner"] = False

    if not st.session_state["show_runner"]:
        st.write("Run the workbook transformation pipeline from the browser.")
        if st.button("Run a transformations job", type="primary"):
            st.session_state["show_runner"] = True
            st.rerun()
        return

    _render_job_runner()


if __name__ == "__main__":
    main()
