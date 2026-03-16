import io
import os
import sys
import threading
import zipfile

import streamlit as st

APP_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR_PATH = os.path.dirname(os.path.dirname(APP_DIR))
if ROOT_DIR_PATH not in sys.path:
    sys.path.insert(0, ROOT_DIR_PATH)

from src import ROOT_DIR
from src.run_transforms import run_pipeline

DATA_DIR = os.path.join(ROOT_DIR, "data", "runs", "ui")
EXPECTED_RUNTIME_MINUTES = 3

_dir_lock = threading.Lock()
_pipeline_lock = threading.Lock()


def _next_run_dir() -> str:
    with _dir_lock:
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


def _list_download_files(run_dir: str) -> list[tuple[str, str]]:
    files = []

    final_workbook = os.path.join(run_dir, "step6_contractor_tabs.xlsx")
    if os.path.isfile(final_workbook):
        files.append(("Final workbook", final_workbook))

    pdf_dir = os.path.join(run_dir, "pdf_exports")
    if os.path.isdir(pdf_dir):
        for name in sorted(os.listdir(pdf_dir)):
            path = os.path.join(pdf_dir, name)
            if os.path.isfile(path):
                files.append(("PDF export", path))

    return files


def _build_download_bundle(run_dir: str, files: list[tuple[str, str]]) -> bytes:
    buffer = io.BytesIO()

    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for category, file_path in files:
            if not os.path.isfile(file_path):
                continue
            if category == "PDF export":
                arcname = os.path.join("pdf_exports", os.path.basename(file_path))
            else:
                arcname = os.path.basename(file_path)
            archive.write(file_path, arcname=arcname)

    buffer.seek(0)
    return buffer.getvalue()


def _render_downloads(run_dir: str) -> None:
    files = _list_download_files(run_dir)
    if not files:
        st.info("No final workbook or PDF exports were found for this run.")
        return

    st.subheader("Downloads")
    st.write(
        "Only the final workbook (including base sheet, distributors, orders on hold, and contractors) and the PDF exports are shown here.")

    bundle_bytes = _build_download_bundle(run_dir, files)
    st.download_button(
        "Download all files",
        data=bundle_bytes,
        file_name=f"CRS_Agent_{os.path.basename(run_dir)}.zip",
        mime="application/zip",
        use_container_width=True,
        type="primary",
    )

    for category, file_path in files:
        if not os.path.isfile(file_path):
            continue
        st.write(f"{category}: `{os.path.basename(file_path)}`")
        with open(file_path, "rb") as fh:
            st.download_button(
                f"Download {os.path.basename(file_path)}",
                data=fh.read(),
                file_name=os.path.basename(file_path),
                mime="application/octet-stream",
                key=f"download-{file_path}",
                use_container_width=True,
            )


def _render_landing() -> None:
    st.title("CRS Excel Agent")
    st.write("Run the existing Excel transformation pipeline from the browser.")
    st.write(
        f"The job usually takes about {EXPECTED_RUNTIME_MINUTES} minutes and returns the final workbook plus PDF exports."
    )

    if st.button("Manual Trigger Agent", type="primary"):
        st.session_state["ui_step"] = "upload"
        st.rerun()


def _render_job_runner() -> None:
    st.title("Upload Workbook")
    st.write("Upload a single `.xlsx` file to start the pipeline.")
    st.caption(f"Expected runtime: about {EXPECTED_RUNTIME_MINUTES} minutes.")

    top_left, top_right = st.columns([5, 1])
    with top_right:
        if st.button("Back", use_container_width=True):
            st.session_state["ui_step"] = "landing"
            st.rerun()

    uploaded_file = st.file_uploader("Excel file", type=["xlsx"])

    if uploaded_file is None:
        last_run_dir = st.session_state.get("last_run_dir")
        if last_run_dir:
            _render_downloads(last_run_dir)
        return

    if not uploaded_file.name.lower().endswith(".xlsx"):
        st.error("Only .xlsx files are allowed.")
        return

    st.write(f"Selected file: `{uploaded_file.name}`")

    if st.button("Run job", type="primary", use_container_width=True):
        if not _pipeline_lock.acquire(blocking=False):
            st.warning("Another job is already running. Please wait for it to finish.")
            return
        try:
            uploaded_bytes = uploaded_file.getvalue()
            run_dir = _next_run_dir()

            with st.status("Running workbook transformations...", expanded=True) as status:
                status.write(f"Expected runtime: about {EXPECTED_RUNTIME_MINUTES} minutes.")
                status.write("Executing the existing transformation pipeline.")
                run_pipeline(uploaded_bytes, output_root=run_dir)
                status.update(label="Run complete", state="complete", expanded=False)

            st.session_state["last_run_dir"] = run_dir
            st.success("Job completed.")
        except Exception as exc:
            st.error(f"Job failed: {exc}")
        finally:
            _pipeline_lock.release()

    last_run_dir = st.session_state.get("last_run_dir")
    if last_run_dir:
        _render_downloads(last_run_dir)


def main() -> None:
    st.set_page_config(page_title="CRS Excel Agent", layout="centered")

    if "ui_step" not in st.session_state:
        st.session_state["ui_step"] = "landing"

    if st.session_state["ui_step"] == "landing":
        _render_landing()
    else:
        _render_job_runner()


if __name__ == "__main__":
    main()
