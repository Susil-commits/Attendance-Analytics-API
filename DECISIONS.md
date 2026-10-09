# DECISIONS.md

1. **Indexes.**
   I created eight indexes: on `employees`, `{emp_code: 1}` (unique duplicate guard, lookup key) and `{department: 1, emp_code: 1}` (sorted department directory); on `attendance_logs`, `{emp_code: 1, date: 1}` (unique race guard, natural key), `{date: -1, emp_code: 1}` (unfiltered ledger sort, date-range scans), `{status: 1, date: -1, emp_code: 1}` (status filtering without in-memory sort), and `{emp_code: 1, punch_in: -1}` (punch-out lookup). I rejected indexing `status` alone because low cardinality causes poor index selectivity, yielding high write overhead for minimal read benefit.

2. **Punch-in race.**
   Both requests validate input and compute the identical attendance date simultaneously. Without check-then-insert pre-checks, both issue `insert_one`. MongoDB's unique index on `(emp_code, date)` guarantees atomic serialization at the storage engine: one insert acquires the write lock and succeeds (returning 201), while the second immediately triggers a `DuplicateKeyError`, caught and translated directly into HTTP 409.

3. **Ties at the cutoff.**
   The leaderboard applies `$setWindowFields` using `$rank` (standard competition ranking: 1, 2, 2, 4) and matches `rank <= limit` *after* ranking. If employees tie at the cutoff rank, all tied records are returned, deterministically ordered by `total_late_minutes` descending, then `emp_code` ascending. Thus, the response row count may exceed `limit`.

4. **Headcount.**
   The department summary aggregation drives from the `employees` collection filtered by `joined_on <= month_end`, not from `attendance_logs`. A `$lookup` with an embedded sub-pipeline groups each employee's logs into a single summary document (or zero if no logs exist). Grouping by department then computes `headcount = $sum 1`, ensuring employees with zero attendance records are fully counted without row duplication.

5. **Scaling to 100x data.**
   At 100x data (~10 million logs), I would replace live date-range aggregations with pre-aggregated daily department rollups updated via Change Streams or out-of-band cron workers, switch from skip-based pagination to cursor pagination on `(date, emp_code)`, and shard `attendance_logs` on hashed `emp_code`.
