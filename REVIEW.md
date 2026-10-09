# REVIEW.md: Analysis of Starter Code Defects

The starter codebase in `app/main.py` contained multiple critical defects spanning timezone mismanagement, concurrency vulnerabilities, incorrect business rule calculations, data validation gaps, and aggregation missing logic. Below is the comprehensive review table documenting each identified defect, its concrete failure symptom, and the fix applied.

---

## 1. Identified Defects & Applied Fixes

| # | Where (function / line) | What is wrong | How you'd notice it (test, input, or symptom) | How you fixed it |
|---|---|---|---|---|
| 1 | `MongoClient(...)` (line 22) | Client is not `tz_aware`, returning naive datetimes from MongoDB. | Comparing or serializing stored datetimes causes `TypeError` or wrong epoch milliseconds due to server-local assumptions. | Initialized `MongoClient(..., tz_aware=True, tzinfo=timezone.utc, maxPoolSize=100)`. |
| 2 | `/health` (line 72-74) | Always returns `200 {"status":"ok"}` without pinging MongoDB or returning 503 on failure. | Stopping MongoDB still yields `200 OK`, masking database outages. | Added `client.admin.command("ping")`, catching exceptions and returning `503 Service Unavailable`. |
| 3 | `compute_late_minutes` (line 33-37) | Uses `minutes > 10` after integer floor division. Rule R2 requires late calculation when strictly more than 10:00 (600s) from shift start. | Punch-in at `09:40:01` on a `09:30` shift yields 601s; `int(601/60) = 10`, and `10 > 10` evaluates to False, reporting `0` instead of `10` minutes late. | Calculated `delta_seconds = int((pi - start).total_seconds())`. Checked `delta_seconds > 600`, returning `delta_seconds // 60`. |
| 4 | `compute_late_minutes` (line 35) | `punch_in.replace(hour=h, minute=m)` uses the datetime's existing tz (UTC or local) instead of IST, and ignores overnight shifts and attendance dates. | Punch-in at 22:05 IST on a 22:00 shift computes against UTC 22:00, causing massive negative/positive distortions. | Anchored shift start in IST using `shift_start_dt(date_str, shift_start)` on the R1 attendance date. |
| 5 | `compute_late_minutes` (line 33) | Does not truncate timestamps to whole seconds before computing elapsed duration. | Punch-in at `09:40:00.900` counts as late if sub-seconds push duration past 600s. | Truncated input milliseconds to whole seconds `(ms // 1000 * 1000)` in `ms_to_dt`. |
| 6 | `compute_work_hours` (line 40-41) | Uses Python's native `round()` (banker's rounding) on floats instead of half-up arithmetic. | Duration of 16,218 seconds (4.505 hours) rounds down to `4.5` instead of `4.51`. | Implemented `Decimal(delta_sec) / Decimal(3600)` quantized with `ROUND_HALF_UP` to 2 decimal places. |
| 7 | `compute_overtime` (line 44-47) | Anchors `shift_end` on the same calendar day (breaks overnight shifts), omits the 30-minute minimum threshold, and mixes aware/naive datetimes. | Shift 22:00-06:00 out at 06:40 next day produces negative delta or `TypeError`. Out 15 min past shift end awards 15 min overtime (spec requires >= 30). | Used `shift_end_dt` (+1 day for overnight), computed `delta // 60`, returning `mins` only if `mins >= 30` else `0`. |
| 8 | `EmployeeIn` (line 53-61) | Missing schema validation: no `emp_code` regex (`^EMP\d{4,6}$`), no email validation, accepts `shift_start == shift_end` and invalid calendar dates. | Accepts invalid payload like `{"emp_code": "abc", "shift_start": "25:99", "joined_on": "2026-02-30"}`. | Implemented Pydantic models with field regexes, `shift_start != shift_end` validator, and strict date parsing. |
| 9 | `create_employee` (line 78-85) | Check-then-insert race condition; `created_at` set to naive server-local `datetime.now()` and returned as datetime string instead of epoch ms. | Concurrent requests with same `emp_code` create duplicate records; response violates OpenAPI `EpochMillis` schema. | Enforced database unique index on `emp_code` (catching `DuplicateKeyError` -> 409) and serialized `created_at` as epoch ms. |
| 10 | `list_employees` (line 89-96) | Off-by-one pagination bug (`skip = page * page_size`), total counts entire collection ignoring filter, no deterministic sort. | `page=1` skips the entire first page; total ignores department filter. | Set `skip = (page - 1) * page_size`, added sort by `emp_code` ascending, and calculated `total = db.employees.count_documents(q)`. |
| 11 | `punch_in` (line 100-101) | Does not verify employee existence before accessing `emp["shift_start"]`, triggering `TypeError: 'NoneType' object is not subscriptable` (500). | Punch-in with non-existent `emp_code` returns HTTP 500. | Looked up employee first; if not found, raised `HTTPException(404, "Employee not found")`. |
| 12 | `punch_in` (line 102-103) | `fromtimestamp(ms / 1000)` uses server local time to determine attendance date; falsy check `if body.punched_at` fails for timestamp `0`. | Server in UTC assigns incorrect date to punches occurring after 00:00 IST but before 00:00 UTC. | Converted epoch ms to aware UTC datetime, evaluated attendance date in IST according to R1 (accounting for overnight shifts). |
| 13 | `punch_in` (line 109) | `status` accepts any arbitrary string without validation (e.g. `ABSENT`, `LEAVE`, or invalid strings). | Submitting `status="LEAVE"` to punch-in creates an illegal record. | Restricted accepted statuses to `PresenceStatus` (`PRESENT`, `WFH`, `ON_DUTY`); others return 422. |
| 14 | `punch_in` (line 104-106) | Check-then-insert race condition on `(emp_code, date)`. | 10 concurrent identical punch-ins yield multiple documents in `attendance_logs`. | Created unique compound index on `{"emp_code": 1, "date": 1}`, catching `DuplicateKeyError` to return 409. |
| 15 | `punch_in` (line 119) | Response leaks internal MongoDB `id` string and returns datetimes instead of epoch milliseconds. | Response schema mismatches OpenAPI `AttendanceRecord` specification. | Stripped `_id` and `id`, mapped `punch_in`/`punch_out` to integer epoch ms. |
| 16 | `PunchInIn` (line 63-67) | Pydantic lax mode coerces strings `"1783312500000"` and floats `1.0` into integers; permits second-level timestamps. | Timestamp in seconds (`1783312500`) is accepted and points to year 1970. | Used strict integer typing with bounded range `[100000000000, 4102444800000]` rejecting floats, strings, booleans, and seconds with 422. |
| 17 | `list_attendance` (line 124-150) | Loads entire collection into memory with `list(find())` and sorts in Python; `page < 1` gives invalid slice; leaks `id`; crashes on missing fields. | Severely degrades and runs out of memory on 100k documents; crashes on legacy documents missing `half_day`. | Replaced with cursor-level `.sort([("date", -1), ("emp_code", 1)]).skip().limit()` backed by index; added default fields in serializer. |
| 18 | `list_attendance` (line 137-141) | No validation that `date_from <= date_to`, accepting inverted date intervals. | Passing `date_from=2026-07-10&date_to=2026-07-01` executes without validation. | Added validation raising HTTP 422 if `date_from > date_to`. |

---

## 2. Items Examined and Determined Not to Be Defects

1. **Synchronous endpoint definitions (`def`) with PyMongo**:
   - *Observation*: Endpoints use standard `def` functions rather than `async def`.
   - *Conclusion*: Not a defect. FastAPI automatically offloads synchronous endpoint handlers to an external AnyIO threadpool worker pool, ensuring the asyncio event loop is never blocked by synchronous PyMongo network I/O.
2. **`load_dotenv()` default behavior**:
   - *Observation*: `load_dotenv()` is invoked without arguments.
   - *Conclusion*: Not a defect. By default in `python-dotenv`, `override=False`, meaning existing operating system environment variables (such as injected production `MONGO_URI`) take strict precedence over `.env` defaults.
3. **Absence of single-field index on `status`**:
   - *Observation*: `attendance_logs.status` is not individually indexed.
   - *Conclusion*: Intentional design decision. Status has very low cardinality (5 enum values); compound index `{status: 1, date: -1, emp_code: 1}` provides complete index-covered ordering for status-filtered queries without wasteful write overhead.
