## 👺 Havoc: Prevent Integer Overflow in Pool Validation

🧨 **The Trigger:**
Passing large pool sizes (`worker_pool` + `web_pool` > `usize::MAX`) causes an integer overflow when calculating combined pool sizes in `PoolConfig::validate()`.

📉 **The Stack Trace:**
```
thread 'pool::pool_proptest::test_pool_validate_fuzz' panicked at autumn-harvest/src/pool.rs:67:24:
attempt to add with overflow.
minimal failing input: worker_pool = 1960644485038893467, web_pool = 16486099588670658149, max_total = 0
```

🧪 **Reproduction:**
Run the proptest in `autumn-harvest/src/pool/pool_proptest.rs`.

😈 **Comment:**
"You assumed people wouldn't try to instantiate pools larger than system memory. You were wrong. An attacker could trivially crash the setup routine by injecting large values into the configuration block."
