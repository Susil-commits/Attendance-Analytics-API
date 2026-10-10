# HROne Attendance & Analytics Engine

FastAPI backend for employee attendance tracking and workforce analytics with MongoDB. Implements punch-in/punch-out rules, manual regularization audit trails, and aggregation pipelines.

---

## 1. Quick Start

### Prerequisites
- Python 3.11+
- MongoDB 6.0+ (Local instance or MongoDB Atlas)

### Setup & Run
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment (optional, defaults to localhost:27017 and attendance_db)
export MONGO_URI="mongodb://127.0.0.1:27017"
export MONGO_DB="attendance_db"

# 3. Start the application
uvicorn app.main:app --port 8000
```

- **Swagger Documentation:** `http://localhost:8000/docs`
- **OpenAPI Schema:** `http://localhost:8000/openapi.json`
- **Readiness Probe:** `http://localhost:8000/health`

---

## 2. Implementation Notes & Design Decisions

### Concurrency Handling
- **Punch-In:** Uses a compound unique index on `(emp_code, date)` and direct `insert_one`. Duplicate concurrent requests result in one HTTP 201 and remaining HTTP 409s without check-then-insert races.
- **Punch-Out:** Uses atomic `find_one_and_update` filtering on `{_id, punch_out: None, punch_in: pi}`. Parallel punch-outs yield one HTTP 200 and remaining HTTP 409s.
- **Regularization (`PATCH`):** Uses optimistic concurrency control checking the current length of the `history` array. Retries up to 5 times with randomized exponential backoff (jitter) before returning HTTP 409.

### Analytics & Business Rules
- **Employee Monthly (`/analytics/employees/{emp_code}/monthly`):** Aggregates presence on weekdays only, accounting for half-days (0.5 weight). For employees who joined mid-month, working days are computed from `joined_on`.
- **Department Summary (`/analytics/departments/summary`):** Queries from `employees` joined by month-end so employees with zero attendance records are included in headcount. Uses a `$lookup` sub-pipeline to aggregate individual record work hours.
- **Late Leaderboard (`/analytics/leaderboard/late`):** Uses `$setWindowFields` with `$rank` for standard competition ranking (1, 2, 2, 4). The `limit` filter is applied after ranking, so all employees tied at the cutoff rank are returned.
- **Department Trend (`/analytics/departments/{department}/trend`):** Uses `$densify` with date boundaries to ensure every calendar day is present. The 7-day moving average is computed over unrounded attendance rates in MongoDB's `$setWindowFields` stage to preserve floating-point precision across the window; both `attendance_rate` and `moving_avg_7d` are rounded to 4 decimals using half-up arithmetic upon serialization.

### Index Coverage & Explain
Indexes are created automatically at startup inside FastAPI's `lifespan` handler. `GET /admin/explain/{endpoint}` exposes MongoDB `explain` plans with `executionStats` for performance verification.

---

## 3. Verification & Testing

### Rule & Concurrency Verification
- **Business Rules:** Pure calculations (10-minute grace period boundary at 600s, Decimal round-half-up, 30-minute overtime threshold, overnight shift date attribution) verified with isolated unit tests.
- **Punch Concurrency:** 20 parallel punch-ins yield strictly `1 × 201` and `19 × 409`; 20 parallel punch-outs yield strictly `1 × 200` and `19 × 409`.
- **Optimistic Locking:** 10 parallel PATCH regularization requests result in audit history length strictly equal to the number of successful 200 responses.

### Analytics Mathematical Verification
- **Mid-Month Joiner & Zero-Log Employees:** Verified `working_days` calculation from `joined_on` (e.g. joining July 20 yields 10 working days; zero-log employees yield 23 working days and 0.0 attendance percentage without errors).
- **Weekend Records:** Verified that weekend punches are excluded from `present_days` and `working_days`, while their `late_minutes`, `overtime_minutes`, and `work_hours` are included in monthly totals.
- **Department Headcount:** Verified that department summary includes enrolled employees who have zero attendance records in the month.
- **Leaderboard Ties at Cutoff:** Verified standard competition ranking (1, 2, 2, 4) with ties at cutoff rank (e.g. totals 100, 80, 80, 50 with limit=2 returns 3 records with ranks 1, 2, 2).
- **Zero-Record Trend Gap Filling:** Verified that department trend across an interval with zero records returns every single calendar day using `$unionWith` + `$documents` + `$densify`.

### 120,000-Document Explain Plan Verification
Tested against live MongoDB with 120,000 unique `(emp_code, date)` documents and 2,000 employees. Every endpoint and query filter variant was scanned across the full explain JSON tree:
- `attendance_list` (date range, status + date, emp_code + date): **IXSCAN: YES | COLLSCAN: NO**
- `employee_monthly`: **IXSCAN: YES | COLLSCAN: NO**
- `department_summary` (with and without department filter): **IXSCAN: YES | COLLSCAN: NO**
- `late_leaderboard` (with and without department filter): **IXSCAN: YES | COLLSCAN: NO**
- `department_trend`: **IXSCAN: YES | COLLSCAN: NO**

