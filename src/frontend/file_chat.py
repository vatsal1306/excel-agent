import os
import re
from dataclasses import dataclass
from io import BytesIO
from typing import Iterable

import pandas as pd
from pypdf import PdfReader


DEFAULT_OPENAI_MODEL = "gpt-5-mini"
MAX_EXCERPT_CHARS = 1_500
MAX_CONTEXT_CHUNKS = 8
EXCEL_ROWS_PER_CHUNK = 40
ANSWER_INSTRUCTIONS = """
You are a file analysis assistant for end users who uploaded Excel and PDF files.

Goal:
- Answer the user's question directly using the uploaded file excerpts whenever the answer, a close match, or a reasonable interpretation can be found there.
- Be helpful and decisive. Do not tell the user to inspect the file, open a sheet, search manually, or verify something themselves when the provided excerpts contain enough information for you to answer.

Evidence rules:
- Use only the uploaded file inventory and source excerpts provided in the request.
- Cite source labels in square brackets for factual claims, using the labels exactly as provided.
- If the excerpts contain related but not exact information, answer from the closest relevant evidence and briefly state the assumption.
- If the excerpts do not contain enough evidence, say that the uploaded files do not provide enough information to answer that specific question. Do not invent missing facts.

Answer style:
- Start with the answer, not with process notes.
- Keep the response concise, but include enough detail to be useful.
- If calculations or comparisons are needed, perform them from the visible excerpts and mention any short assumption used.
- Keep assumptions very short, for example: "Assumption: blank quantity means zero."
""".strip()

_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{1,}")
_STOP_WORDS = {
    "about",
    "after",
    "again",
    "also",
    "and",
    "any",
    "are",
    "because",
    "been",
    "but",
    "can",
    "could",
    "did",
    "does",
    "for",
    "from",
    "had",
    "has",
    "have",
    "how",
    "into",
    "its",
    "more",
    "not",
    "of",
    "on",
    "only",
    "or",
    "our",
    "please",
    "show",
    "than",
    "that",
    "the",
    "their",
    "then",
    "there",
    "these",
    "this",
    "to",
    "was",
    "were",
    "what",
    "when",
    "where",
    "which",
    "who",
    "why",
    "with",
    "would",
    "you",
    "your",
}


@dataclass(frozen=True)
class SourceChunk:
    file_name: str
    label: str
    text: str

    @property
    def excerpt(self) -> str:
        normalized = " ".join(self.text.split())
        if len(normalized) <= MAX_EXCERPT_CHARS:
            return normalized
        return f"{normalized[:MAX_EXCERPT_CHARS].rstrip()}..."


def parse_uploaded_file(file_name: str, data: bytes) -> list[SourceChunk]:
    extension = os.path.splitext(file_name)[1].lower()
    if extension == ".pdf":
        return _parse_pdf(file_name, data)
    if extension in {".xlsx", ".xls"}:
        return _parse_excel(file_name, data)
    raise ValueError("Only PDF and Excel files are supported.")


def retrieve_chunks(
    question: str,
    chunks: Iterable[SourceChunk],
    *,
    limit: int = MAX_CONTEXT_CHUNKS,
) -> tuple[list[SourceChunk], bool]:
    query_tokens = _tokens(question)
    if not query_tokens:
        return [], False

    scored: list[tuple[int, int, SourceChunk]] = []
    for index, chunk in enumerate(chunks):
        chunk_tokens = _tokens(chunk.text)
        overlap = query_tokens.intersection(chunk_tokens)
        if not overlap:
            continue
        score = sum(chunk.text.lower().count(token) for token in overlap)
        scored.append((score, -index, chunk))

    if not scored:
        return [], False

    scored.sort(reverse=True)
    return [chunk for _, _, chunk in scored[:limit]], True


def build_file_inventory(chunks: Iterable[SourceChunk]) -> str:
    files: dict[str, int] = {}
    for chunk in chunks:
        files[chunk.file_name] = files.get(chunk.file_name, 0) + 1
    if not files:
        return "No readable file content was extracted."
    return "\n".join(
        f"- {file_name}: {chunk_count} extracted source chunk(s)"
        for file_name, chunk_count in sorted(files.items())
    )


def answer_question(
    question: str,
    source_chunks: list[SourceChunk],
    *,
    file_inventory: str,
    model: str | None = None,
    api_key: str | None = None,
) -> str:
    from openai import OpenAI

    selected_model = model or os.environ.get("OPENAI_MODEL") or DEFAULT_OPENAI_MODEL
    context = _format_context(source_chunks)
    client = OpenAI(api_key=api_key) if api_key else OpenAI()

    response = client.responses.create(
        model=selected_model,
        instructions=ANSWER_INSTRUCTIONS,
        input=[
            {
                "role": "user",
                "content": (
                    f"Uploaded files:\n{file_inventory}\n\n"
                    f"Source excerpts:\n{context}\n\n"
                    f"Question: {question}"
                ),
            },
        ],
    )

    return response.output_text.strip()


def _parse_pdf(file_name: str, data: bytes) -> list[SourceChunk]:
    reader = PdfReader(BytesIO(data))
    chunks: list[SourceChunk] = []

    for page_index, page in enumerate(reader.pages, start=1):
        text = page.extract_text() or ""
        text = text.strip()
        if not text:
            continue
        for part_index, part in enumerate(_split_text(text), start=1):
            label = f"[{file_name} p. {page_index}]"
            if part_index > 1:
                label = f"[{file_name} p. {page_index} part {part_index}]"
            chunks.append(SourceChunk(file_name=file_name, label=label, text=part))

    return chunks


def _parse_excel(file_name: str, data: bytes) -> list[SourceChunk]:
    workbook = pd.ExcelFile(BytesIO(data))
    chunks: list[SourceChunk] = []

    for sheet_name in workbook.sheet_names:
        frame = workbook.parse(sheet_name=sheet_name, dtype=str)
        frame = frame.fillna("")
        if frame.empty:
            continue

        for start in range(0, len(frame), EXCEL_ROWS_PER_CHUNK):
            end = min(start + EXCEL_ROWS_PER_CHUNK, len(frame))
            row_start = start + 2
            row_end = end + 1
            row_block = frame.iloc[start:end]
            text = _dataframe_to_text(sheet_name, row_start, row_block)
            if not text.strip():
                continue
            label = f"[{file_name} / {sheet_name} rows {row_start}-{row_end}]"
            chunks.append(SourceChunk(file_name=file_name, label=label, text=text))

    return chunks


def _dataframe_to_text(sheet_name: str, row_start: int, frame: pd.DataFrame) -> str:
    lines = [f"Sheet: {sheet_name}", f"Columns: {', '.join(map(str, frame.columns))}"]
    for offset, (_, row) in enumerate(frame.iterrows()):
        excel_row = row_start + offset
        values = []
        for column, value in row.items():
            value_text = str(value).strip()
            if value_text:
                values.append(f"{column}: {value_text}")
        if values:
            lines.append(f"Row {excel_row}: " + "; ".join(values))
    return "\n".join(lines)


def _split_text(text: str, *, max_chars: int = 4_000) -> list[str]:
    if len(text) <= max_chars:
        return [text]

    parts: list[str] = []
    current: list[str] = []
    current_size = 0

    for paragraph in re.split(r"\n\s*\n", text):
        paragraph = paragraph.strip()
        if not paragraph:
            continue
        if current and current_size + len(paragraph) > max_chars:
            parts.append("\n\n".join(current))
            current = []
            current_size = 0
        current.append(paragraph)
        current_size += len(paragraph)

    if current:
        parts.append("\n\n".join(current))
    return parts or [text[:max_chars]]


def _format_context(chunks: list[SourceChunk]) -> str:
    if not chunks:
        return (
            "No directly matching excerpts were retrieved. Use the file inventory "
            "only to explain that the uploaded files do not provide enough evidence."
        )

    return "\n\n".join(
        f"{chunk.label}\n{chunk.excerpt}"
        for chunk in chunks
    )


def _tokens(text: str) -> set[str]:
    return {
        token.lower()
        for token in _TOKEN_RE.findall(text)
        if token.lower() not in _STOP_WORDS
    }
