import csv
import io
import itertools
from dataclasses import dataclass
from typing import List


def decode_csv_bytes(raw: bytes) -> str:
    """
    Decode CSV bytes into text.
    Handles common Windows/Excel exports (UTF-16 with BOM) and UTF-8 with BOM.
    """
    # UTF-16 BOMs (Excel on Windows often exports TSV/CSV like this)
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16")

    # UTF-8 (with optional BOM)
    try:
        text = raw.decode("utf-8-sig")
        # If the file was actually UTF-16 but decoded as UTF-8, it will contain lots of NULs.
        if "\x00" in text:
            return raw.decode("utf-16")
        return text
    except UnicodeDecodeError:
        pass

    # Common Central European fallback
    try:
        return raw.decode("cp1250")
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace")


def decode_csv_bytes_with_encoding(raw: bytes, encoding: str = "auto") -> str:
    enc = (encoding or "auto").strip().lower()
    if enc == "auto":
        return decode_csv_bytes(raw)
    if enc in {"utf-16", "utf16"}:
        return raw.decode("utf-16")
    if enc in {"utf-8", "utf8"}:
        return raw.decode("utf-8-sig")
    if enc in {"cp1250", "windows-1250", "win1250"}:
        return raw.decode("cp1250")
    # Last resort
    return raw.decode(enc, errors="replace")


def _escape_md_cell(value: str) -> str:
    # Minimal escaping for Markdown tables.
    # - pipes break table columns
    # - newlines break rows
    return (
        value.replace("\x00", "")
        .replace("|", "\\|")
        .replace("\r\n", " ")
        .replace("\n", " ")
        .replace("\r", " ")
    )


def _normalize_preamble(preamble: str) -> str:
    if not preamble:
        return ""
    if not preamble.endswith("\n"):
        preamble += "\n"
    if not preamble.endswith("\n\n"):
        preamble += "\n"
    return preamble


def _clean_header(h: str) -> str:
    return h.replace("\x00", "").lstrip("\ufeff").strip()


@dataclass(frozen=True)
class CsvParseOptions:
    delimiter: str = "auto"  # auto | "," | ";" | "\t" | "|" (pipe)
    has_header: bool = True
    skip_lines: int = 0
    limit_lines: int | None = None
    quote_mode: str = "standard"  # standard | no_quotes
    quotechar: str = '"'


@dataclass(frozen=True)
class SortKey:
    field_1based: int
    type: str = "string"  # string | numeric
    direction: str = "asc"  # asc | desc
    ignore_case: bool = True


def _parse_positions(positions: str | None) -> list[int] | None:
    if not positions:
        return None
    parts = [p.strip() for p in positions.split(",") if p.strip()]
    out: list[int] = []
    for p in parts:
        if not p.isdigit():
            continue
        out.append(int(p))
    return out or None


def _parse_csv(csv_text: str, opts: CsvParseOptions) -> tuple[list[str], list[list[str]]]:
    # Try to sniff delimiter (often "," or ";" in CZ exports). If sniffing fails, fall back to excel dialect.
    dialect: csv.Dialect = csv.excel
    delim = (opts.delimiter or "auto")
    if delim == "tab":
        delim = "\t"
    if delim == "pipe":
        delim = "|"
    if delim != "auto":
        # create a dialect-ish object
        dialect = csv.excel
        dialect.delimiter = delim  # type: ignore[attr-defined]
    else:
        try:
            dialect = csv.Sniffer().sniff(csv_text[:4096], delimiters=",;\t|")
        except csv.Error:
            pass

    quotechar = (opts.quotechar or '"')[:1]
    quoting = csv.QUOTE_MINIMAL
    if opts.quote_mode == "no_quotes":
        quoting = csv.QUOTE_NONE

    # Apply quoting options to dialect
    try:
        dialect.quotechar = quotechar  # type: ignore[attr-defined]
        dialect.quoting = quoting  # type: ignore[attr-defined]
        dialect.doublequote = True  # type: ignore[attr-defined]
    except Exception:
        pass

    normalized = csv_text.replace("\r\n", "\n").replace("\r", "\n")

    # Preferred parsing path: mimic `open(..., newline="")` behavior even though we read from text.
    try:
        reader = list(csv.reader(io.StringIO(normalized, newline=""), dialect, quotechar=quotechar, quoting=quoting))
    except csv.Error:
        # Best-effort fallback for some broken exports that trigger:
        # `_csv.Error: new-line character seen in unquoted field`
        # We split into physical lines (no newline chars inside each provided "line"),
        # which avoids the csv module interpreting embedded newlines inside a field.
        try:
            reader = list(csv.reader(normalized.splitlines(), dialect, quotechar=quotechar, quoting=quoting))
        except csv.Error:
            # Last-resort fallback: naive split (ignores quoting). This prioritizes "generate something"
            # over strict CSV correctness and avoids hard failures for messy exports.
            lines = [ln for ln in normalized.split("\n") if ln != ""]
            first = lines[0] if lines else ""
            candidates = [getattr(dialect, "delimiter", ","), ";", ",", "\t", "|"]
            delim = max(candidates, key=lambda d: first.count(d) if d else -1) or ","
            reader = [ln.split(delim) for ln in lines]

    # skip / limit
    skip = max(0, int(opts.skip_lines or 0))
    if skip:
        reader = reader[skip:]

    if opts.limit_lines is not None:
        lim = max(0, int(opts.limit_lines))
        reader = reader[:lim]

    if not reader:
        return [], []

    if opts.has_header:
        headers = [_clean_header(h) for h in reader[0]]
        rows = [list(r) for r in reader[1:]]
    else:
        width = max((len(r) for r in reader), default=0)
        headers = [f"Field {i+1}" for i in range(width)]
        rows = [list(r) for r in reader]
    return headers, rows


def _apply_positions(headers: list[str], rows: list[list[str]], positions_1based: list[int] | None) -> tuple[list[str], list[list[str]]]:
    if not positions_1based:
        return headers, rows
    idxs = [p - 1 for p in positions_1based if p > 0]
    idxs = [i for i in idxs if i < len(headers)]
    if not idxs:
        return headers, rows
    new_headers = [headers[i] for i in idxs]
    new_rows: list[list[str]] = []
    for r in rows:
        new_rows.append([r[i] if i < len(r) else "" for i in idxs])
    return new_headers, new_rows


def _apply_sort(rows: list[list[str]], key: SortKey, headers_len: int) -> list[list[str]]:
    idx = max(0, min(headers_len - 1, key.field_1based - 1))

    def get_val(r: list[str]) -> str:
        return r[idx] if idx < len(r) else ""

    def to_num(v: str) -> float:
        v = (v or "").strip().replace(",", ".")
        try:
            return float(v)
        except Exception:
            return float("-inf")

    reverse = (key.direction or "asc").lower() == "desc"
    typ = (key.type or "string").lower()
    ignore_case = bool(key.ignore_case)

    if typ == "numeric":
        return sorted(rows, key=lambda r: to_num(get_val(r)), reverse=reverse)

    if ignore_case:
        return sorted(rows, key=lambda r: get_val(r).casefold(), reverse=reverse)
    return sorted(rows, key=lambda r: get_val(r), reverse=reverse)


def csv_text_to_markdown_table(
    csv_text: str,
    preamble: str = "",
    *,
    parse: CsvParseOptions | None = None,
    positions: str | None = None,
    add_line_numbers: bool = False,
    sort: SortKey | None = None,
    eol: str = "lf",  # lf | crlf
    table_style: str = "markdown",  # markdown | jira
) -> str:
    """
    Convert CSV text to a Markdown table (or Jira table).
    """
    if parse is None:
        parse = CsvParseOptions()
    headers, rows = _parse_csv(csv_text, parse)

    headers, rows = _apply_positions(headers, rows, _parse_positions(positions))
    if sort and headers:
        rows = _apply_sort(rows, sort, len(headers))

    if add_line_numbers:
        headers = ["#"] + headers
        rows = [[str(i + 1)] + r for i, r in enumerate(rows)]

    headers_escaped = [_escape_md_cell(h) for h in headers]

    out: List[str] = []
    if table_style == "jira":
        out.append("|| " + " || ".join(headers_escaped) + " ||")
    else:
        out.append("| " + " | ".join(headers_escaped) + " |")
        out.append("|" + "|".join(["---"] * len(headers_escaped)) + "|")

    for row in rows:
        row_escaped = [_escape_md_cell(c) for c in row]
        if table_style == "jira":
            out.append("| " + " | ".join(row_escaped) + " |")
        else:
            out.append("| " + " | ".join(row_escaped) + " |")

    line_ending = "\r\n" if (eol or "lf").lower() == "crlf" else "\n"
    table = line_ending.join(out) + line_ending

    return _normalize_preamble(preamble) + table


NOTEBOOKLM_MIN_FIELDS = [
    "Keyword",
    "Country",
    "Languages",
    "Volume",
    "Global volume",
    "KD",
    "Difficulty",
    "Keyword difficulty",
    "CPC",
    "CPS",
    "Traffic potential",
    "Global traffic potential",
    "SERP features",
    "SERP Features",
    "Current organic traffic",
    "Previous organic traffic",
    "Organic traffic change",
    "Current position",
    "Previous position",
    "Current URL",
    "Previous URL",
    "Parent Keyword",
    "Parent keyword",
    "Last Update",
    "Last update",
    "First seen",
    "Intents",
    "Intent",
]


def csv_text_to_markdown_notebooklm(
    csv_text: str,
    preamble: str = "",
    detail: str = "minimal",
    max_rows: int | None = None,
    *,
    parse: CsvParseOptions | None = None,
    positions: str | None = None,
    sort: SortKey | None = None,
) -> str:
    """
    NotebookLM-friendly output:
    - one heading per keyword
    - bullet list of key fields
    """
    if parse is None:
        parse = CsvParseOptions()
    headers, rows = _parse_csv(csv_text, parse)
    headers, rows = _apply_positions(headers, rows, _parse_positions(positions))
    if sort and headers:
        rows = _apply_sort(rows, sort, len(headers))

    header_map = {_clean_header(h).lower(): h for h in headers}
    # Choose which fields to include
    if detail.lower() == "full":
        fields = headers
    else:
        fields = [header_map[f.lower()] for f in NOTEBOOKLM_MIN_FIELDS if f.lower() in header_map]
        # Fallback if we didn't match anything OR we only matched the keyword column
        keyword_header_candidate = header_map.get("keyword")
        if (not fields) or (
            keyword_header_candidate in headers
            and all(h == keyword_header_candidate for h in fields)
        ):
            fields = headers[: min(len(headers), 8)]

    # Find keyword column
    keyword_key = "keyword"
    keyword_header = header_map.get(keyword_key)
    keyword_idx = headers.index(keyword_header) if keyword_header in headers else 0

    if max_rows is not None:
        rows = rows[: max_rows]

    out: List[str] = []
    out.append("## Export (NotebookLM)\n")
    out.append(f"- **Řádků**: {len(rows)}")
    out.append(f"- **Režim**: {'full' if detail.lower() == 'full' else 'minimal'}")
    out.append("")
    out.append("## Záznamy\n")

    for i, row in enumerate(rows, start=1):
        # Normalize row length
        values = list(itertools.islice(itertools.chain(row, itertools.repeat("")), len(headers)))
        kw = _escape_md_cell(values[keyword_idx]).strip()
        if not kw:
            kw = f"Řádek {i}"
        out.append(f"### {kw}")

        row_by_header = {headers[j]: values[j] for j in range(len(headers))}
        for h in fields:
            if h == keyword_header:
                continue
            v = _escape_md_cell(str(row_by_header.get(h, ""))).strip()
            if not v:
                continue
            # Make links more readable in NotebookLM
            if v.startswith("http://") or v.startswith("https://"):
                v = f"<{v}>"
            out.append(f"- **{h}**: {v}")
        out.append("")

    return _normalize_preamble(preamble) + "\n".join(out).rstrip() + "\n"


