# REVIEW.md

I reviewed the starter implementation in `app/main.py`. While basic flows worked, the code had critical bugs around time handling, race conditions, edge-case math, and MongoDB scale.

## 1. Starter Defects Found & Fixed

| # | Location | Defect | Concrete Symptom / Test Case | Fix |
|---|---|---|---|---|
| 1 | `MongoClient` initialization | Client didn't pass `tz_aware=True`, returning naive datetimes. | Comparing database timestamps with UTC datetimes raised `TypeError` or shifted epochs based on local server time. | Initialized `MongoClient(..., tz_aware=True, tzinfo=timezone.utc)`. |
| 2 | `/health` endpoint | Returned hardcoded `200` without checking database connectivity. | Stopping MongoDB still resulted in `200 OK`, masking database outages. | Added `client.admin.command("ping")`, returning 503 on connection failure. |
| 3 | `compute_late_minutes` | Checked `minutes > 10` after integer floor division. R2 states punch-in is late only when strictly > 10:00 (600s). | Punching in at 09:40:01 for a 09:30 shift is 601s; `601 // 60 = 10`, and `10 > 10` was False, reporting 0 minutes late instead of 10. | Evaluated `delta_seconds > 600`, returning `delta_seconds // 60`. |
| 4 | `compute_late_minutes` | Anchored shift start with naive `replace()`, ignoring IST timezone and overnight shifts. | Punching in at 22:05 IST on a 22:00 overnight shift was compared against UTC 22:00 on the same date. | Built shift start in IST using `shift_start_dt` anchored to the R1 attendance date. |
| 5 | `compute_late_minutes` | Milliseconds were not truncated to whole seconds. | Punching in at 09:40:00.900 counted as late because sub-seconds pushed duration past 600s. | Truncated epoch ms using `(ms // 1000) * 1000` in `ms_to_dt`. |
| 6 | `compute_work_hours` | Used Python's default `round()` (banker's rounding) on floats instead of half-up arithmetic. | 16,218 seconds (4.505 hours) rounded down to 4.50 instead of 4.51. | Converted seconds to `Decimal` and quantized with `ROUND_HALF_UP`. |
| 7 | `compute_overtime` | Assumed shift end was on the same calendar day, and missed the 30-minute minimum threshold. | A 22:00–06:00 overnight shift with punch-out at 06:40 gave negative time. Punching out 15 min late awarded 15 min overtime (spec requires >= 30). | Calculated `shift_end_dt` (+1 day for overnight shifts), returning whole minutes only if >= 30. |
| 8 | `EmployeeIn` model | Lacked format validation for employee code, email, shift times, and valid calendar dates. | Accepted invalid payloads like `{"emp_code": "bad", "joined_on": "2026-02-30", "shift_start": "09:00", "shift_end": "09:00"}`. | Added regex validations, verified `shift_start != shift_end`, and strictly validated calendar dates. |
| 9 | `create_employee` | Used check-then-insert vulnerable to race conditions; stored naive `created_at`. | Concurrent POST requests with the same `emp_code` created duplicate employees. | Added unique index on `emp_code`, caught `DuplicateKeyError` to return 409, and returned epoch ms. |
| 10 | `list_employees` | Off-by-one pagination bug (`skip = page * page_size`), and total ignored filters. | Requesting `page=1` skipped the first page; total count didn't reflect department filters. | Corrected `skip = (page - 1) * page_size`, added sort on `emp_code`, and counted with filter query. |
| 11 | `punch_in` | Did not verify employee existence before reading `emp["shift_start"]`. | Non-existent employee code caused `TypeError: 'NoneType' object is not subscriptable` (500). | Checked employee lookup first and returned 404 if not found. |
| 12 | `punch_in` | Used `datetime.fromtimestamp(ms / 1000)` in server-local time to determine the attendance date. | A server running in UTC evaluated late-night IST punches on the wrong calendar day. | Converted epoch ms to UTC datetime and evaluated attendance date in IST per R1. |
| 13 | `punch_in` | `status` was unvalidated, accepting `ABSENT`, `LEAVE`, or arbitrary strings. | Clients could punch in with `status="LEAVE"`. | Restricted punch-in status to presence values (`PRESENT`, `WFH`, `ON_DUTY`), returning 422 otherwise. |
| 14 | `punch_in` | Checked for existing records before inserting instead of atomic indexing. | Parallel punch-ins for the same employee on the same date created duplicate attendance logs. | Created compound unique index on `(emp_code, date)` and caught `DuplicateKeyError` to return 409. |
| 15 | `punch_in` response | Leaked internal MongoDB `_id`/`id` and returned datetime objects. | Response mismatched OpenAPI `AttendanceRecord` schema. | Stripped internal IDs and converted datetimes to epoch milliseconds. |
| 16 | `PunchInIn` timestamp | Pydantic lax mode coerced strings and floats into integers, accepting second-based timestamps. | Passing seconds like `1783312500` or `"1783312500000"` was accepted without validation. | Used strict integer typing with bounds `[100000000000, 4102444800000]` to enforce 422 on bad types. |
| 17 | `list_attendance` | Loaded the entire collection into memory using `list(find())` and sorted in Python. | High memory consumption and severe slowdown on 100k records; crashed on documents without `half_day`. | Replaced with indexed database-level sorting, skip, limit, and added field defaults during serialization. |
| 18 | `list_attendance` date range | Did not check that `date_from <= date_to`. | Passing `date_from=2026-07-10&date_to=2026-07-01` executed an inverted query silently. | Added validation raising 422 when `date_from > date_to`. |

## 2. Items Examined and Kept

1. **Synchronous endpoint handlers (`def` vs `async def`)**:
   FastAPI automatically runs synchronous `def` endpoints on an external threadpool (AnyIO), preventing synchronous PyMongo calls from blocking the main event loop.
2. **Default `load_dotenv()` without override**:
   `python-dotenv` has `override=False` by default, ensuring real injected environment variables (like `MONGO_URI`) take precedence over local `.env` files.
3. **No standalone index on `status`**:
   `status` has low cardinality (5 enum values). An individual index provides poor selectivity; compound index `{status: 1, date: -1, emp_code: 1}` provides covered sorting when filtered without write overhead.
