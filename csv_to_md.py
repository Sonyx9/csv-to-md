from converter import csv_text_to_markdown_table, decode_csv_bytes
from datetime import date
from pathlib import Path

INPUT = "data.csv"
OUTPUT = Path(INPUT).with_suffix(".md").name

today = date.today()
cz_date = f"{today.day}. {today.month}. {today.year}"

CONTEXT = f"""# SEO data – organická klíčová slova
Zdroj: CSV export
Datum: {cz_date}

"""

raw = open(INPUT, "rb").read()
csv_text = decode_csv_bytes(raw)
table_md = csv_text_to_markdown_table(csv_text)

with open(OUTPUT, "w", encoding="utf-8") as f:
    f.write(CONTEXT)
    f.write(table_md)
print("✅ Hotovo: data.md")
