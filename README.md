# Document Ingestion Check

This is a clean, separate app for the first slice of the new system: document ingestion and text extraction.

It supports:

- `PDF` text extraction
- scanned PDF detection and rejection
- `DOCX` extraction
- `TXT`, `CSV`, `MD`, `LOG` extraction
- image OCR for `PNG`, `JPG`, `TIFF`, `BMP` via Tesseract
- background extraction jobs so UI does not block
- file-hash cache for fast repeated runs
- two-stage search over extracted text (shortlist + rerank)

It does not do O*NET mapping yet.

## Folder

Project path:

`C:\Users\aditi\job-duty-ingestion-ui`

## Setup

Use Python `3.11` or `3.12`.

`python-docx` is intentionally not used. DOCX files are read directly from Office XML to avoid `lxml` issues on Windows environments.

Create a virtual environment and install dependencies:

```powershell
cd C:\Users\aditi\job-duty-ingestion-ui
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt
```

Install Tesseract OCR:

```powershell
winget install --id UB-Mannheim.TesseractOCR -e
```

## Run

```powershell
cd C:\Users\aditi\job-duty-ingestion-ui
.\.venv\Scripts\Activate.ps1
streamlit run app.py
```

## What to test

1. Upload a few sample files or point the app at a folder.
2. Confirm the extracted text is actually readable.
3. Note which files return warnings or empty output.
4. Scanned PDFs are explicitly marked as non-processable.
5. Run the same folder twice and confirm the second run is faster from cache.

That validation is the gate before building parsing, chunking, and semantic retrieval.
