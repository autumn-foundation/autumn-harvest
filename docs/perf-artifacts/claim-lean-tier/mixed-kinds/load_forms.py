"""Write forms.sql: a table of the dumped claim texts, and one PREPARE per text.

Usage: load_forms.py <dir of <variant>-{lean,window}.sql> > forms.sql
Only the three plain variants (any, workflow, activity) are prepared.
"""
import pathlib
import sys

src = pathlib.Path(sys.argv[1])
print("\\set ON_ERROR_STOP on")
for kind in ("any", "workflow", "activity"):
    for form in ("lean", "window"):
        text = (src / f"{kind}-{form}.sql").read_text().strip().rstrip(";")
        assert "$claimtext$" not in text
        print(f"PREPARE {form}_{kind}(text, text[], text, bigint, text[], text[]) AS {text};")
