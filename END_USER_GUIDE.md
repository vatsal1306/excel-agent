# CRS Excel Agent: End User Guide

## What This Does

The CRS Excel Agent lets you upload one Excel workbook, run the processing flow, and download the final workbook and PDF files from your browser. It also includes an AI chat section where you can upload Excel or PDF files and ask questions about their contents.

## Before You Start

- Make sure you have the input file in `.xlsx` format.
- Open the app in your browser at [http://5.161.45.186](http://5.161.45.186/).
- If your browser asks whether to continue to the site, choose to continue.
- Keep the browser tab open while the job is running.
- For AI chat, the server must have `OPENAI_API_KEY` configured in the environment or `.env` file.
- Optional: set `OPENAI_MODEL` to choose a different OpenAI model. If it is not set, the app uses `gpt-5-mini`.

## How To Run The Excel Agent

1. Open the app homepage.
2. Click `Manual Trigger Agent`.
3. Upload your Excel workbook.
4. Click `Run job`.
5. Wait for the run to finish. The job usually takes about 3 minutes.

## How To Use AI Chat

1. Open the app homepage.
2. Click `Ask AI / Chat with AI`.
3. Upload one or more Excel or PDF files.
4. Ask questions in the chat box.
5. Review the answer and the `Sources used` section when sources are available.

## What You Will Receive

When the job is complete, you can:

1. Download all files as a single `.zip` file containing everything
2. Download the individual workbook and PDF files shown on the page

## Important Notes

- Only one job can run at a time. If another job is already running, wait and try again.
- The manual Excel agent supports `.xlsx` files.
- AI chat supports `.xlsx`, `.xls`, and `.pdf` files.
- AI chat files and messages are kept only in your current browser session.
- If the page shows `Job failed`, retry once with the same file. If it fails again, please contact us at [vatsal1399@gmail.com](mailto:vatsal1399@gmail.com) or WhatsApp at +91-9727942236.
