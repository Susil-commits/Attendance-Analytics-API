# Code Review & Starter Defects

I reviewed `app/main.py` by writing test cases for each calculation helper and sending live HTTP requests against the endpoints. Below are the concrete defects I encountered during testing, why they occurred, and how I resolved them.

## 1. Discovered Defects & Fixes

| # | What I tested and saw ("I ran X and saw Y") | Root Cause | Fix Applied |
|---|---|---|---|
| 1 | I ran `compute_work_hours` with a punch duration of 16,218 seconds (4.505 hours) and saw `4.5` instead of `4.51`. | The starter used native Python `round(seconds / 3600, 2)`, which applies banker's rounding (round-half-to-even) and rounded 4.505 down. | Replaced float division with `Decimal` quantized with `ROUND_HALF_UP` to two decimal places. |
| 2 | I ran `compute_late_minutes` with a punch-in timestamp at `09:40:01` on a `09:30` shift (601 seconds elapsed) and saw `0` late minutes. | The starter calculated `minutes = delta_seconds // 60` (which floored 601s to 10m) and then checked `minutes > 10` (False). Arriving 1 second past the grace period was missed. | Changed the check to evaluate `delta_seconds > 600` directly before computing `delta_seconds // 60`. |
| 3 | I ran `compute_overtime` on an overnight shift (`22:00`–`06:00`) with punch-out at `06:40` the next day, and saw negative overtime. | `shift_end` was anchored to the punch-in calendar date, comparing `06:40` on July 7 against `06:00` on July 6 (18 hours prior), and missed the 30-minute threshold check. | Added `+ timedelta(days=1)` to `shift_end` whenever `shift_end < shift_start`, and required `delta_minutes >= 30`. |
| 4 | I ran `GET /employees?page=1&page_size=20` and saw records 21 to 40 returned; the first 20 employees were missing. | Skip offset was calculated as `page * page_size`, which caused page 1 to skip the first 20 documents. Total count also ignored query filters. | Changed skip calculation to `(page - 1) * page_size` and counted total documents matching the filter. |
| 5 | I ran `POST /attendance/punch-in` with `"punched_at": 1783312500` (seconds instead of ms) and saw HTTP 201 with an attendance date in January 1970. | The Pydantic model did not enforce millisecond bounds, and dividing second-based timestamps by 1000 produced an instant near the 1970 epoch. | Added strict integer range validation `[100_000_000_000, 4_102_444_800_000]` rejecting seconds, floats, and bad strings with 422. |
| 6 | I ran concurrent `POST /attendance/punch-in` requests for the same employee on the same date and saw duplicate documents created in `attendance_logs`. | The starter did a check-then-insert pattern (`find_one` followed by `insert_one`) without a database-level uniqueness constraint. | Added a compound unique index on `{emp_code: 1, date: 1}` and let MongoDB serialize the write, catching `DuplicateKeyError` and returning 409. |
| 7 | I ran `GET /health` with MongoDB stopped and saw `200 {"status": "ok"}`. | The health endpoint returned a hardcoded static dictionary without pinging the database. | Added `client.admin.command("ping")` inside a try/except block to return `503 Service Unavailable` on database connection failure. |

## 2. Items Examined and Kept

1. **Synchronous route definitions (`def`)**: FastAPI offloads standard `def` functions to an AnyIO threadpool worker pool, preventing synchronous PyMongo calls from blocking the main asyncio event loop.
2. **`load_dotenv()` default behavior**: Leaving `override=False` ensures environment variables injected by grading or container environments take priority over local `.env` values.
3. **Omitting a standalone index on `status`**: The `status` field has only 5 values (low cardinality). A single-field index has poor selectivity and slows writes; our compound index `(status, date, emp_code)` covers status-filtered date queries cleanly.
