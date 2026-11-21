-- Production-shaped mutex waiter fixture for the terminal-sweep key read.
-- Usage: psql -v waiters=10000 -f fixture.sql
-- The harness replaces :waiters with the size before it runs the file.
--
-- Shape. 80% of the waiter rows queue on 20 hot keys, with a squared skew,
-- so key 0 holds the deepest queue. 20% are one-row keys. Each one-row key
-- has a lock row, because a grant keeps the waiter row of its holder.
-- A churn pass inserts and deletes as many rows again first. Autovacuum is
-- off for the table, so about half of the heap tuples are dead.
-- Every id is a hash of its row number, so each run seeds the same rows.
ALTER TABLE harvest_mutex_waiters SET (autovacuum_enabled = false);
TRUNCATE harvest_mutex_waiters, harvest_mutex_locks;
INSERT INTO harvest_mutex_waiters (lock_key, waiter_exec_id, requested_at)
SELECT 'churn:' || g, md5('churn-' || g)::uuid, now() - interval '1 hour'
FROM generate_series(1, :waiters) g;
INSERT INTO harvest_mutex_waiters (lock_key, waiter_exec_id, requested_at)
SELECT 'account:hot-' || floor(20 * power((hashint4(g)::bigint & 2147483647) / 2147483648.0, 2))::int,
       md5('hot-' || g)::uuid, now() - interval '10 minutes'
FROM generate_series(1, (:waiters * 8) / 10) g;
INSERT INTO harvest_mutex_waiters (lock_key, waiter_exec_id, requested_at)
SELECT 'order:' || g, md5('order-' || g)::uuid, now() - interval '1 minute'
FROM generate_series(1, (:waiters * 2) / 10) g;
DELETE FROM harvest_mutex_waiters WHERE lock_key LIKE 'churn:%';
INSERT INTO harvest_mutex_locks (lock_key, holder_exec_id, lock_seq, acquired_at, lease_expires_at)
SELECT lock_key, waiter_exec_id, id, now(), now() + interval '1 day'
FROM harvest_mutex_waiters WHERE lock_key LIKE 'order:%';
ANALYZE harvest_mutex_waiters;
ANALYZE harvest_mutex_locks;
