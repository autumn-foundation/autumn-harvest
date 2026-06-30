import os

for filename in os.listdir("autumn-harvest/tests"):
    if filename.endswith(".rs"):
        path = os.path.join("autumn-harvest/tests", filename)
        with open(path, "r") as f:
            content = f.read()

        if "autumn_harvest::testing" in content and '#![cfg(feature = "testing")]' not in content:
            content = '#![cfg(feature = "testing")]\n' + content
            with open(path, "w") as f:
                f.write(content)
