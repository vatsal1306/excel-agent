# Excel Agent

## Requirements

- **Python 3.10**
- **LibreOffice** (for PDF export)
- **uv** (for dependency management)

## Setup

### 1. Python

Install Python 3.10 and double check version:

```bash
python3 --version
```

### 2. Dependency Installation

This project uses `uv` for fast dependency management.

Install `uv`:

```bash
pip install uv
```

Install all dependencies:

```bash
uv sync
```

### 3. OpenAI Configuration

The Streamlit AI chat feature requires an OpenAI API key. Add this to your shell environment or to a `.env` file at the project root:

```bash
OPENAI_API_KEY="your_api_key_here"
```

Optionally set `OPENAI_MODEL` to override the default chat model:

```bash
OPENAI_MODEL="gpt-5-mini"
```

### 4. LibreOffice Setup

PDF export (step 7) requires LibreOffice's `soffice` executable.

#### macOS

- Download LibreOffice from [libreoffice.org](https://www.libreoffice.org/download/download/).
- Install and ensure `/Applications/LibreOffice.app/Contents/MacOS/soffice` exists.
- Optionally, add to PATH:
  ```bash
  export PATH="/Applications/LibreOffice.app/Contents/MacOS:$PATH"
  ```

#### Ubuntu/Linux

- Install via package manager:
  ```bash
  sudo apt update
  sudo apt install libreoffice
  ```
- Ensure `soffice` is available in your PATH:
  ```bash
  which soffice
  ```

#### Windows

- Not supported for PDF export in this pipeline.

### 5. Running the Pipeline

Run the main script:

```bash
python src/run_transforms.py
```

Run the Streamlit app:

```bash
streamlit run src/frontend/app.py
```

## Notes

- LibreOffice is used for headless PDF export in step 7 (`step_07_export_tabs_to_pdfs`). The script auto-detects `soffice` or `libreoffice` in PATH.
- If you encounter errors about LibreOffice not found, ensure it is installed and available in your PATH.
