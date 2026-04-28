import sys

def main():
    content = ""
    with open("autumn-harvest/src/query.rs", "r") as f:
        content = f.read()

    old_import = "use crate::error::HarvestResult;\n"
    content = content.replace(old_import, "")

    with open("autumn-harvest/src/query.rs", "w") as f:
        f.write(content)

if __name__ == "__main__":
    main()
