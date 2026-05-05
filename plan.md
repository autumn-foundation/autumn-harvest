Wait, `assert_eq!(task_duration("000000000000s"), Some(Duration::from_secs(0)));` failed!
Why? Let's trace it.
`task_duration` is:
```rust
    for ch in s.chars() {
        if ch.is_ascii_digit() {
            if current_num == "0" {
                current_num.clear();
            }
            if current_num.len() > 20 {
                return None;
            }
            current_num.push(ch);
        } else if ch.is_ascii_alphabetic() {
            let num: u64 = current_num.parse().ok()?;
```
If `s` is "000000000000s", what happens?
1st char '0': `current_num` is "". Appends '0', `current_num`="0".
2nd char '0': `current_num` == "0". Clears it. `current_num` is "". Appends '0', `current_num`="0".
...
Last char '0': Appends '0', `current_num`="0".
Next char 's': Calls `current_num.parse().ok()?`. "0".parse() is `0`.
Wait, why did it return `None`?

Ah! Wait, `0000010m`.
1st '0': "0"
2nd '0': clears, "0"
...
5th '0': clears, "0"
Next '1': clears? No, `current_num` is "0", `ch` is '1'.
It clears! `current_num` becomes "". Appends '1'. `current_num`="1".
Next '0': `current_num` is "1", does not clear. Appends '0'. `current_num`="10".
So `"0000010m"` parses correctly? Let me check which assertion failed!

`left: None, right: Some(0ns)`
This corresponds to `assert_eq!(task_duration("000000000000s"), Some(Duration::from_secs(0)));`.
Wait!
"0".repeat(50) + "1s" -> `task_duration` returned `Some(Duration::from_secs(1))`. So the FIRST assertion passed!
Wait, why did `task_duration("000000000000s")` fail?
Ah! `current_num.parse().ok()?` succeeds. `total_secs = 0`.
At the end:
```rust
    if !current_num.is_empty() || total_secs == 0 {
        return None;
    }
```
Oh! `total_secs == 0` returns `None`!
Wait, `task_duration` ALWAYS returns `None` for `0` duration!
Let's look at `autumn-harvest/src/lib.rs`.
```rust
    if !current_num.is_empty() || total_secs == 0 {
        return None;
    }
```
Yes! It rejects 0!
So `assert_eq!(task_duration("000000000000s"), Some(Duration::from_secs(0)));` was WRONG to expect `Some(0)`!
Wait, the original code had:
```rust
    if !current_num.is_empty() || total_secs == 0 {
        return None;
    }
```
So `task_duration("0s")` returns `None`!
My test expects `Some(0)`.
