# Depth diagnostic, re-measured after the payload corrections

## harvest arms (registered payload)

### harvest depth 250 (registered payload)
## arm `postgres`
rep 0: 23.64 workflows/sec (250 completed in 10.58 s, 750 activity runs, correctness PASS)
## arm `redis_pg`
rep 0: 22.60 workflows/sec (250 completed in 11.06 s, 750 activity runs, correctness PASS, residue entries=0 pending=0 markers=0)
### harvest depth 500 (registered payload)
## arm `postgres`
rep 0: 23.90 workflows/sec (500 completed in 20.92 s, 1500 activity runs, correctness PASS)
## arm `redis_pg`
rep 0: 21.82 workflows/sec (500 completed in 22.92 s, 1500 activity runs, correctness PASS, residue entries=0 pending=0 markers=0)
### harvest depth 1000 (registered payload)
## arm `postgres`
rep 0: 13.93 workflows/sec (1000 completed in 71.77 s, 3000 activity runs, correctness PASS)
## arm `redis_pg`
rep 0: 22.58 workflows/sec (1000 completed in 44.28 s, 3000 activity runs, correctness PASS, residue entries=0 pending=0 markers=0)
### harvest depth 2000 (registered payload)
## arm `postgres`
rep 0: 5.60 workflows/sec (2000 completed in 356.91 s, 6000 activity runs, correctness PASS)
## arm `redis_pg`
rep 0: 21.84 workflows/sec (2000 completed in 91.59 s, 6000 activity runs, correctness PASS, residue entries=0 pending=0 markers=0)

## temporal arm (registered payload)

### temporal depth 250 (registered payload)
rep 0: 36.17 workflows/sec (250 completed in 6.91 s, 750 activity runs, 0 workflow task failures, 0 unread histories, correctness PASS)
### temporal depth 500 (registered payload)
rep 0: 34.56 workflows/sec (500 completed in 14.47 s, 1500 activity runs, 0 workflow task failures, 0 unread histories, correctness PASS)
### temporal depth 1000 (registered payload)
rep 0: 45.80 workflows/sec (1000 completed in 21.84 s, 3000 activity runs, 0 workflow task failures, 0 unread histories, correctness PASS)

The depth-2000 temporal cell is the registered sweep's mean, 43.2943.
