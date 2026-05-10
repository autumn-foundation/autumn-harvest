import sys

with open("./autumn-harvest/src/store.rs", "r") as f:
    content = f.read()

search_str = """        println!("err={}", result.unwrap_err().to_string());
        assert!(false);"""

replace_str = """        assert!(result.unwrap_err().to_string().contains("Event ID overflow"));"""

content = content.replace(search_str, replace_str)

with open("./autumn-harvest/src/store.rs", "w") as f:
    f.write(content)
