import os
import sys

def process_file(filepath):
    try:
        with open(filepath, 'r') as f:
            content = f.read()
    except:
        return

    if "saturating_add" in content:
        print(f"Modifying {filepath}")
        content = content.replace(".saturating_add(1)", ".checked_add(1).ok_or_else(|| crate::error::database_error(\"integer overflow\"))?")
        with open(filepath, 'w') as f:
            f.write(content)

for root, _, files in os.walk('./autumn-harvest/src'):
    for file in files:
        if file.endswith('.rs'):
            process_file(os.path.join(root, file))
