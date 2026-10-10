# DECISIONS.md

1. **Indexes.**
   I created compound indexes on `employees` (`emp_code` unique; `(department, emp_code)`) and `attendance_logs` (`(emp_code, date)` unique; `(date, emp_code)`; `(status, date, emp_code)`; `(emp_code, punch_in)`). I avoided indexing `status` alone because 5 low-cardinality values offer poor selectivity and add write overhead.

2. **Punch-in race.**
   Instead of checking before inserting, both concurrent requests run `insert_one`. MongoDB's unique index on `(emp_code, date)` serializes them: one insert succeeds (201), while the other hits a `DuplicateKeyError`, caught and returned as HTTP 409.

3. **Ties at the cutoff.**
   The late leaderboard uses `$setWindowFields` with `$rank` for competition ranking (1, 2, 2, 4) and filters `rank <= limit` *after* ranking, ordered by `emp_code`. Tied employees at the cutoff are all returned, so results can exceed `limit`.

4. **Headcount.**
   The department summary drives from `employees` joined by month-end. An embedded `$lookup` sub-pipeline summarizes each employee's logs into at most one document, followed by `$group` with `headcount = $sum 1`. Employees with zero logs are counted without duplicating rows.

5. **Scaling to 100x.**
   At 10M records, I would pre-aggregate daily department stats via Change Streams, switch pagination to keyset cursors on `(date, emp_code)`, shard logs by `emp_code`, and stop inlining employee `joined_on` arrays in trend queries.
