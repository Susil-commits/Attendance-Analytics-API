# DECISIONS.md

1. **Indexes.**
I added unique indexes on `emp_code` and `(emp_code, date)` to stop duplicate records. For queries, I indexed `(date, emp_code)` and `(status, date, emp_code)`. I skipped indexing `status` alone because five values give low selectivity and slow down writes.

2. **Punch-in race.**
Both requests call `insert_one` directly without pre-checking. MongoDB's unique index on `(emp_code, date)` serializes them: the first insert succeeds with 201, and the second triggers a `DuplicateKeyError`, which we catch and return as 409.

3. **Ties at the cutoff.**
We rank by `total_late_minutes` using `$rank`, so tied employees share the same rank. We filter `rank <= limit` afterward. If multiple people tie at the cutoff, all are returned, so results can exceed `limit`. We sort by `emp_code` only for deterministic order.

4. **Headcount.**
Department summary starts from `employees` filtered by `joined_on`, not attendance logs, so employees with zero logs are still counted. A `$lookup` summarizes logs per person before grouping by department, preventing row multiplication.

5. **Scaling to 100x.**
At 10M rows, I'd maintain daily department rollups using Change Streams instead of scanning raw logs. For lists, I'd replace offset skip with cursor pagination on `(date, emp_code)`, and shard logs by `emp_code`.
