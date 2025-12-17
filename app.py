from __future__ import annotations

import re
import uuid
import csv
import json
from datetime import date
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from converter import (
    CsvParseOptions,
    SortKey,
    csv_text_to_markdown_notebooklm,
    csv_text_to_markdown_table,
    decode_csv_bytes_with_encoding,
)

APP_TITLE = "CSV → Markdown"
PREVIEW_MAX_LINES = 40

BASE_CSS = """
:root {
  --bg: #F6F5EE;
  --surface: #FFFFFF;
  --surface-2: #E4DCCF;
  --border: #E4DCCF;
  --text: #111111;
  --muted: #444444;
  --shadow: 0 10px 30px rgba(0,0,0,0.08);
  --accent: #FCB02F;
  --accent-hover: #F4A81F;
}

* { box-sizing: border-box; }
html, body { height: 100%; }
body {
  margin: 0;
  font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, "Apple Color Emoji", "Segoe UI Emoji";
  color: var(--text);
  background: var(--bg);
}

.container { max-width: 980px; margin: 0 auto; padding: 48px 18px; }
.topbar { display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-bottom: 22px; }
.brand { display: flex; align-items: center; gap: 12px; }
.logo-box {
  width: 84px;
  height: 84px;
  border-radius: 21px;
  border: 1px solid var(--border);
  background: #fff;
  box-shadow: 0 6px 16px rgba(0,0,0,0.08);
  display: flex;
  align-items: center;
  justify-content: center;
}
.logo-img {
  width: 66px;
  height: 66px;
  object-fit: contain;
  display: block;
}
.title { margin: 0; font-size: 28px; letter-spacing: -0.02em; }
.subtitle { margin: 6px 0 0; color: var(--muted); font-size: 14px; }

.card {
  border: 1px solid var(--border);
  background: var(--surface);
  border-radius: 18px;
  padding: 20px;
  box-shadow: var(--shadow);
}

.grid { display: grid; grid-template-columns: 1.1fr 0.9fr; gap: 16px; }
@media (max-width: 820px) { .grid { grid-template-columns: 1fr; } }

.drop {
  border: 1px dashed var(--border);
  border-radius: 16px;
  padding: 18px;
  background: rgba(0,0,0,0.02);
}
.drop strong { display: block; font-size: 15px; margin-bottom: 6px; }
.drop span { color: var(--muted); font-size: 13px; }
.drop .subline { display: block; margin-bottom: 14px; }
.row { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-top: 14px; flex-wrap: wrap; }

.pill {
  display: inline-flex; align-items: center;
  padding: 8px 10px;
  border-radius: 999px;
  border: 1px solid var(--border);
  background: var(--surface-2);
  color: var(--muted);
  font-size: 12px;
  text-decoration: none;
}
.pill:hover { filter: brightness(0.98); }

input[type="file"] { display: none; }
.btn {
  display: inline-flex; align-items: center; justify-content: center; gap: 8px;
  padding: 10px 14px;
  border-radius: 12px;
  border: 1px solid var(--border);
  background: var(--surface);
  color: var(--text);
  text-decoration: none;
  cursor: pointer;
  transition: transform .08s ease, background .15s ease, border-color .15s ease;
  user-select: none;
}
.btn-lg {
  height: 48px;
  padding: 0 16px;
  font-size: 15px;
  font-weight: 600;
}
.btn:hover { background: rgba(0,0,0,0.03); border-color: rgba(0,0,0,0.18); }
.btn:active { transform: translateY(1px); }
.btn-primary {
  background: var(--accent);
  border-color: rgba(0,0,0,0.18);
  color: #000;
}
.btn-primary:hover { background: var(--accent-hover); }

.btn-black {
  background: #111;
  border-color: #111;
  color: #fff;
}
.btn-black:hover { background: #000; border-color: #000; }

/* File button label truncation (ellipsis) */
.btn-file {
  justify-content: flex-start;
  min-width: 0;
}
.btn-file .btn-text {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  min-width: 0;
  width: 100%;
}

.btn-pill {
  background: var(--surface-2);
  border-color: var(--border);
  color: var(--muted);
  font-weight: 600;
}
.btn-pill:hover { background: var(--surface-2); border-color: var(--border); }
.btn-pill:disabled { opacity: 1; cursor: default; }

.w-full { width: 100%; }
.form-grid { display: grid; gap: 10px; }
.row-split { display: grid; grid-template-columns: 2fr 1fr; gap: 12px; width: 100%; }
.row-half { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; width: 100%; }
@media (max-width: 640px) {
  .row-split, .row-half { grid-template-columns: 1fr; }
}

.ml-auto { margin-left: auto; }

.ad-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; }
.ad-row p { margin: 0; color: var(--muted); }
.ad-row b { color: var(--text); }

.kvs { display: grid; gap: 10px; }
.kv { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 12px; border-radius: 14px; border: 1px solid var(--border); background: rgba(0,0,0,0.02); }
.kv b { font-size: 13px; }
.kv code { color: rgba(0,0,0,0.82); }
.hint { color: var(--muted); font-size: 12px; margin: 10px 0 0; }

select {
  padding: 10px 12px;
  height: 44px;
  border-radius: 12px;
  border: 1px solid var(--border);
  background: var(--surface);
  color: #111;
}
select option { color: #111; }

input[type="text"], input[type="number"] {
  padding: 10px 12px;
  height: 44px;
  border-radius: 12px;
  border: 1px solid var(--border);
  background: var(--surface);
  color: #111;
}
input[type="number"] { width: 140px; }
.check { display: inline-flex; align-items: center; gap: 8px; }
.check input { width: 16px; height: 16px; }
details { margin-top: 12px; }
summary { cursor: pointer; color: rgba(0,0,0,0.86); }

pre {
  margin: 0;
  white-space: pre;
  overflow: auto;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 16px;
  padding: 14px;
  color: rgba(0,0,0,0.88);
}
.section-title { margin: 0 0 10px; font-size: 14px; color: rgba(0,0,0,0.88); }
"""


def _render_page(title: str, content_html: str) -> str:
    return f"""<!doctype html>
<html lang="cs">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{title}</title>
    <style>{BASE_CSS}</style>
  </head>
  <body>
    <div class="container">
      <div class="topbar">
        <div class="brand">
          <div class="logo-box">
            <img class="logo-img" src="/static/logo.png" alt="LK logo" />
          </div>
          <div>
            <h1 class="title">{APP_TITLE}</h1>
            <p class="subtitle">Nahraj CSV a stáhni Markdown tabulku (včetně SEO hlavičky).</p>
          </div>
        </div>
        <a class="pill" href="https://lukaskoula.com/" target="_blank" rel="noopener noreferrer">lukaskoula.com</a>
      </div>
      {content_html}
    </div>
  </body>
</html>
"""


def _cz_date(d: date) -> str:
    return f"{d.day}. {d.month}. {d.year}"


def default_context(d: date | None = None) -> str:
    if d is None:
        d = date.today()
    return f"""# SEO data – organická klíčová slova
Zdroj: CSV export
Datum: {_cz_date(d)}

"""

BASE_DIR = Path(__file__).resolve().parent
STORAGE_DIR = BASE_DIR / "storage"
UPLOADS_DIR = STORAGE_DIR / "uploads"
OUTPUTS_DIR = STORAGE_DIR / "outputs"
META_DIR = STORAGE_DIR / "meta"
STATIC_DIR = BASE_DIR / "static"

JOB_ID_RE = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)

app = FastAPI(title=APP_TITLE)
STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

@app.exception_handler(Exception)
async def _unhandled_exception_handler(_: Request, exc: Exception) -> HTMLResponse:
    # Last-resort safety net so the user doesn't get a blank "Internal Server Error" page.
    msg = (f"{type(exc).__name__}: {exc}").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    content = f"""
      <div class="card">
        <p class="section-title">Nastala neočekávaná chyba</p>
        <p class="hint">Pošli mi prosím text chyby níže a opravím to.</p>
        <pre>{msg}</pre>
        <div class="row" style="margin-top:14px;">
          <a class="btn btn-primary" href="/">Zpět na nahrání</a>
        </div>
      </div>
    """
    return HTMLResponse(_render_page(f"Chyba – {APP_TITLE}", content), status_code=500)


def _ensure_storage_dirs() -> None:
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    META_DIR.mkdir(parents=True, exist_ok=True)


def _safe_basename(filename: str) -> str:
    # Strip any path parts (some browsers may send full paths).
    name = (filename or "").replace("\\", "/").split("/")[-1].strip()
    if not name:
        return "data"
    # Replace problematic characters; keep unicode word chars, spaces, dots, dashes, underscores.
    name = re.sub(r"[^\w.\- ]+", "_", name, flags=re.UNICODE)
    name = re.sub(r"\s+", " ", name).strip().rstrip(". ")
    if not name:
        return "data"
    # Avoid overly long filenames.
    return name[:120]


def _download_md_filename(original_filename: str) -> str:
    base = _safe_basename(original_filename)
    stem = Path(base).stem or "data"
    return f"{stem}.md"


@app.on_event("startup")
def _startup() -> None:
    _ensure_storage_dirs()


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    content = """
      <div class="card">
        <div class="grid">
          <div class="drop">
            <strong>Nahrát soubor</strong>
            <span class="subline">Podporované: <code>.csv</code> (UTF‑8 / CP1250 / UTF‑16 z Excelu).</span>
            <form action="/convert" method="post" enctype="multipart/form-data">
              <div class="form-grid">
                <div class="row-split">
                  <label class="btn btn-black btn-lg w-full btn-file" for="file"><span class="btn-text">Vybrat CSV</span></label>
                  <button class="btn btn-primary btn-lg w-full" type="submit">Převést na .md</button>
                </div>
                <input id="file" type="file" name="file" accept=".csv,text/csv" required />

                <div class="row-half">
                  <select class="w-full" name="encoding" aria-label="Kódování">
                  <option value="auto" selected>Encoding: Auto</option>
                  <option value="utf-8">UTF‑8</option>
                  <option value="utf-16">UTF‑16 (Excel)</option>
                  <option value="cp1250">Windows‑1250</option>
                  </select>
                  <select class="w-full" name="format" aria-label="Formát">
                  <option value="notebooklm" selected>Notebook LM</option>
                  <option value="table">Markdown (.md) tabulka</option>
                  <option value="jira">Jira tabulka</option>
                  </select>
                </div>

                <div class="row" style="justify-content:flex-start;">
                  <button class="btn btn-pill" type="button" disabled>Vygeneruje se odkaz ke stažení</button>
                </div>

                <details style="width:100%;">
                  <summary>Pokročilé</summary>
                  <div class="grid" style="margin-top:12px;">
                    <div class="kvs">
                      <label class="check">
                        <input type="checkbox" name="has_header" value="1" checked />
                        <span>První řádek je hlavička</span>
                      </label>
                      <div class="row" style="justify-content:flex-start;">
                        <label class="pill">Skip</label>
                        <input type="number" name="skip_lines" min="0" value="0" />
                        <label class="pill">Limit</label>
                        <input type="number" name="limit_lines" min="0" placeholder="(prázdné)" />
                      </div>
                      <div class="row" style="justify-content:flex-start;">
                        <label class="pill">Sloupce</label>
                        <input type="text" name="fields" placeholder="např. 2,1,3" />
                      </div>
                    </div>
                    <div class="kvs">
                      <div class="row" style="justify-content:flex-start;">
                        <label class="pill">Delimiter</label>
                        <select name="delimiter" aria-label="Oddělovač">
                          <option value="auto" selected>Auto</option>
                          <option value=",">, (comma)</option>
                          <option value=";">; (semicolon)</option>
                          <option value="tab">Tab</option>
                          <option value="pipe">| (pipe)</option>
                        </select>
                      </div>
                      <div class="row" style="justify-content:flex-start;">
                        <label class="pill">Sort field #</label>
                        <input type="number" name="sort_field" min="1" placeholder="(prázdné)" />
                        <select name="sort_type" aria-label="Sort type">
                          <option value="string" selected>String</option>
                          <option value="numeric">Numeric</option>
                        </select>
                        <select name="sort_dir" aria-label="Sort direction">
                          <option value="asc" selected>Asc</option>
                          <option value="desc">Desc</option>
                        </select>
                      </div>
                      <label class="check">
                        <input type="checkbox" name="ignore_case" value="1" checked />
                        <span>Ignore case (pro string sort)</span>
                      </label>
                      <label class="check">
                        <input type="checkbox" name="line_numbers" value="1" />
                        <span>Add line numbers (jen tabulka)</span>
                      </label>
                      <div class="row" style="justify-content:flex-start;">
                        <label class="pill">EOL</label>
                        <select name="eol" aria-label="EOL">
                          <option value="lf" selected>LF</option>
                          <option value="crlf">CRLF</option>
                        </select>
                        <label class="pill">Quotes</label>
                        <select name="quote_mode" aria-label="Quotes">
                          <option value="standard" selected>Standard</option>
                          <option value="no_quotes">Treat quotes as data</option>
                        </select>
                        <label class="pill">Quote char</label>
                        <input type="text" name="quotechar" value="&quot;" style="width:80px;" />
                      </div>
                    </div>
                  </div>
                </details>
              </div>
            </form>
            <p class="hint">
              Tip: pro tabulku můžeš nastavit <b>Sloupce</b> (např. <code>2,1,3</code>), <b>Řazení</b> a <b>Číslování řádků</b>.
            </p>
          </div>
          <div class="kvs">
            <div class="kv"><b>Upload</b><code>storage/uploads/&lt;id&gt;.csv</code></div>
            <div class="kv"><b>Výstup</b><code>storage/outputs/&lt;id&gt;.md</code></div>
            <div class="kv"><b>Hlavička</b><code>Datum: dnes (automaticky)</code></div>
          </div>
        </div>
      </div>

      <!-- Contact block -->
      <div style="height: 14px;"></div>
      <div class="card">
        <div class="ad-row">
          <p><b>Chceš také svou appku?</b> Kontaktuj mě.</p>
          <a class="btn btn-primary" href="https://lukaskoula.com/kontakt/" target="_blank" rel="noopener noreferrer"><b>Kontakt</b></a>
        </div>
      </div>
      <script>
        const input = document.getElementById('file');
        const label = document.querySelector('label[for="file"]');
        const labelText = label?.querySelector?.('.btn-text');
        const original = labelText?.textContent || label?.textContent || '';
        input?.addEventListener('change', () => {
          const name = input.files?.[0]?.name;
          if (labelText) labelText.textContent = name ? `Vybráno: ${name}` : original;
          if (label) label.title = name || '';
        });
      </script>
    """
    return _render_page(APP_TITLE, content)


@app.post("/convert", response_class=HTMLResponse)
async def convert(
    file: UploadFile = File(...),
    format: str = Form("notebooklm"),
    encoding: str = Form("auto"),
    delimiter: str = Form("auto"),
    has_header: bool = Form(True),
    skip_lines: str = Form("0"),
    limit_lines: str = Form(""),
    fields: str = Form(""),
    sort_field: str = Form(""),
    sort_type: str = Form("string"),
    sort_dir: str = Form("asc"),
    ignore_case: bool = Form(True),
    line_numbers: bool = Form(False),
    eol: str = Form("lf"),
    quote_mode: str = Form("standard"),
    quotechar: str = Form("\""),
) -> str:
    _ensure_storage_dirs()

    job_id = uuid.uuid4().hex  # 32 hex chars
    upload_path = UPLOADS_DIR / f"{job_id}.csv"
    output_path = OUTPUTS_DIR / f"{job_id}.md"
    meta_path = META_DIR / f"{job_id}.json"

    def _int_or_none(s: str) -> int | None:
        s = (s or "").strip()
        if s == "":
            return None
        try:
            return int(s)
        except Exception:
            return None

    raw = await file.read()
    csv_text = decode_csv_bytes_with_encoding(raw, encoding=encoding)

    upload_path.write_bytes(raw)

    try:
        parse = CsvParseOptions(
            delimiter=delimiter,
            has_header=bool(has_header),
            skip_lines=int(_int_or_none(skip_lines) or 0),
            limit_lines=_int_or_none(limit_lines),
            quote_mode=(quote_mode or "standard"),
            quotechar=(quotechar or "\""),
        )

        sort: SortKey | None = None
        sf = _int_or_none(sort_field)
        if sf is not None and sf > 0:
            sort = SortKey(
                field_1based=sf,
                type=(sort_type or "string"),
                direction=(sort_dir or "asc"),
                ignore_case=bool(ignore_case),
            )

        if format == "table":
            body_md = csv_text_to_markdown_table(
                csv_text,
                parse=parse,
                positions=fields or None,
                add_line_numbers=bool(line_numbers),
                sort=sort,
                eol=eol,
                table_style="markdown",
            )
        elif format == "jira":
            body_md = csv_text_to_markdown_table(
                csv_text,
                parse=parse,
                positions=fields or None,
                add_line_numbers=bool(line_numbers),
                sort=sort,
                eol=eol,
                table_style="jira",
            )
        else:
            body_md = csv_text_to_markdown_notebooklm(
                csv_text,
                detail="full",
                parse=parse,
                positions=fields or None,
                sort=sort,
            )
    except csv.Error as e:
        content = f"""
          <div class="card">
            <p class="section-title">Nepovedlo se převést CSV</p>
            <p class="hint">Parser narazil na problém ve formátu CSV (typicky neuzavřené uvozovky nebo „divné“ konce řádků).</p>
            <div class="kvs">
              <div class="kv"><b>Chyba</b><code>{str(e).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")}</code></div>
              <div class="kv"><b>Tip</b><code>Zkus CSV znovu vyexportovat (UTF‑8) nebo otevřít a uložit v Excelu</code></div>
            </div>
            <div class="row" style="margin-top:14px;">
              <a class="btn btn-primary" href="/">Zkusit znovu</a>
            </div>
          </div>
        """
        return HTMLResponse(_render_page(f"Chyba – {APP_TITLE}", content), status_code=400)

    md_full = default_context() + body_md
    output_path.write_text(md_full, encoding="utf-8")

    # Persist original name for download (NotebookLM-friendly).
    download_name = _download_md_filename(getattr(file, "filename", "") or "")
    meta_path.write_text(
        json.dumps(
            {
                "input_filename": (getattr(file, "filename", "") or ""),
                "download_filename": download_name,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    # Post/Redirect/Get: avoid resubmitting the form on refresh.
    return RedirectResponse(url=f"/result/{job_id}", status_code=303)


@app.get("/result/{job_id}", response_class=HTMLResponse)
def result(job_id: str) -> str:
    if not JOB_ID_RE.match(job_id):
        raise HTTPException(status_code=404, detail="Not found")

    path = OUTPUTS_DIR / f"{job_id}.md"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Not found")

    meta_path = META_DIR / f"{job_id}.json"
    download_name = f"{job_id}.md"
    if meta_path.exists():
        try:
            download_name = json.loads(meta_path.read_text(encoding="utf-8")).get("download_filename") or download_name
        except Exception:
            pass

    md_full = path.read_text(encoding="utf-8")
    lines = md_full.splitlines()
    truncated = len(lines) > PREVIEW_MAX_LINES
    preview_text = "\n".join(lines[:PREVIEW_MAX_LINES])
    if truncated:
        preview_text += "\n...\n"
    preview = preview_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    content = f"""
      <div class="card">
        <div class="grid">
          <div>
            <p class="section-title">Výsledek</p>
            <div class="row">
              <a class="btn btn-primary" href="/download/{job_id}">Stáhnout .md</a>
              <a class="btn" href="/">Nahrát další</a>
              <span class="pill">Soubor: <code>{download_name}</code></span>
            </div>
            <p class="hint">Soubor je uložený do <code>storage/outputs/{job_id}.md</code> (výstup je vždy UTF‑8).</p>
          </div>
          <div class="kvs">
            <div class="kv"><b>Hlavička</b><code>Datum: automaticky</code></div>
            <div class="kv"><b>Stažení</b><code>/download/{job_id}</code></div>
            <div class="kv"><b>Uložení</b><code>storage/outputs/</code></div>
          </div>
        </div>
        <div style="margin-top:16px;">
          <p class="section-title">Náhled (.md) – prvních {PREVIEW_MAX_LINES} řádků{" (zkráceno)" if truncated else ""}</p>
          <pre>{preview}</pre>
        </div>
      </div>
    """
    return _render_page(f"Hotovo – {APP_TITLE}", content)


@app.get("/download/{job_id}")
def download(job_id: str) -> FileResponse:
    if not JOB_ID_RE.match(job_id):
        # Avoid path traversal; only accept UUID hex.
        raise HTTPException(status_code=404, detail="Not found")

    path = OUTPUTS_DIR / f"{job_id}.md"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Not found")

    meta_path = META_DIR / f"{job_id}.json"
    download_name = f"{job_id}.md"
    if meta_path.exists():
        try:
            download_name = json.loads(meta_path.read_text(encoding="utf-8")).get("download_filename") or download_name
        except Exception:
            pass

    return FileResponse(
        path=path,
        media_type="text/markdown; charset=utf-8",
        filename=download_name,
    )


