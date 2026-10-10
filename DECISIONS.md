# DECISIONS.md

1. **Indexes.**
I added unique indexes on `emp_code` and `(emp_code, date)` to stop duplicates, plus `(emp_code, punch_in)` for punch-outs and two `joined_on` indexes for headcount scans. For queries, I indexed `(date, emp_code)` and `(status, date, emp_code)`. I skipped indexing `status` alone due to low selectivity.

2. **Punch-in race.**
Both requests call `insert_one` directly without pre-checking. MongoDB's unique index on `(emp_code, date)` serializes them: the first succeeds with 201, and the second raises `DuplicateKeyError`, which I catch and return as 409.

3. **Ties at the cutoff.**
I rank by `total_late_minutes` using `$rank`, so tied employees share ranks. I filter `rank <= limit` afterward. If employees tie at the cutoff, all are returned, so results can exceed `limit`. I sort by `emp_code` for deterministic order.

4. **Headcount.**
Department summary starts from `employees` filtered by `joined_on`, not attendance logs, so employees with zero logs are still counted. A `$lookup` summarizes logs per person before grouping by department, preventing row multiplication.

5. **Scaling to 100x.**
At 10M rows, I'd maintain daily department rollups using Change Streams instead of scanning raw logs. For lists, I'd replace offset skip with cursor pagination on `(date, emp_code)`, and shard logs by `emp_code`.
