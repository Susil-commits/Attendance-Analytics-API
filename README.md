# HROne Attendance & Analytics Engine

A production-grade, high-performance Employee Attendance and Workforce Analytics API built with FastAPI and MongoDB 7.0. Fully compliant with `openapi.yaml`, `DATA_MODEL.md`, and business rules R1–R10.

---

## 1. Quick Start

### Prerequisites
- Python 3.11+
- MongoDB 6.0+ (MongoDB 7.0 recommended, or MongoDB Atlas M0 cluster)

### Installation & Run
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment (optional, defaults to localhost:27017 and attendance_db)
export MONGO_URI="mongodb://127.0.0.1:27017"
export MONGO_DB="attendance_db"

# 3. Start the application
uvicorn app.main:app --port 8000
```

Once running:
- **Interactive Minimalist Swiss Console:** `http://localhost:8000/` or `http://localhost:8000/dashboard`
- **Swagger Documentation:** `http://localhost:8000/docs`
- **OpenAPI Schema:** `http://localhost:8000/openapi.json`
- **Readiness Probe:** `http://localhost:8000/health`

---

## 2. Architecture & Design Highlights

### Concurrency Guarantees
- **Atomic Punch-In:** Uses a unique compound index on `(emp_code, date)` with direct `insert_one`. Duplicate concurrent requests yield exactly one HTTP 201 and remaining HTTP 409s without race-prone check-then-insert patterns.
- **Atomic Punch-Out:** Uses `find_one_and_update` filtering on `{_id, punch_out: None, punch_in: pi}`. Concurrent requests yield exactly one HTTP 200 and remaining HTTP 409s.
- **Optimistic Locking Regularization:** Concurrency on `PATCH /attendance/{emp_code}/{date}` is guarded via optimistic locking on history length `{"$size": {"$ifNull": ["$history", []]}}` with bounded retry and jitter.

### Analytics MongoDB Aggregation Pipelines
- **Monthly Employee Summary (`/analytics/employees/{emp_code}/monthly`):** Aggregates present days (Mon–Fri only, half-day weight 0.5), leave counts, and overtime. Accurately determines `working_days` from `joined_on` for mid-month joiners (R7).
- **Department Monthly Summary (`/analytics/departments/summary`):** Starts from `employees` joined on or before month end to count zero-log employees (R9). Uses an embedded `$lookup` sub-pipeline to compute record-level average work hours.
- **Late Leaderboard (`/analytics/leaderboard/late`):** Uses `$setWindowFields` with `$rank` for standard competition ranking (1, 2, 2, 4). The `limit` filter is applied *after* ranking so ties at the cutoff rank are preserved.
- **Department Daily Trend (`/analytics/departments/{department}/trend`):** Bounded `$densify` guarantees exactly one row per calendar day across weekends and days with no logs. Computes a 7-day moving average using windowed `$avg` across non-null attendance rates.

### Performance & Index Coverage
All queries and aggregation pipelines are backed by targeted compound indexes created automatically at startup. The explain endpoint `GET /admin/explain/{endpoint}` executes with `executionStats` verbosity, demonstrating index scan (`IXSCAN`) usage and zero collection scans (`COLLSCAN`).

---

## 3. Minimalist Swiss Enterprise Console

Accessible at `http://localhost:8000/`:
- **Executive Biometric Punch Kiosk:** Live simulation of arrival and departure events with real-time R1/R2 calculation preview and 10x concurrent race testing.
- **Attendance Master Ledger:** Searchable, status-filtered, paginated ledger with an integrated Regularization dialog and complete audit trail inspector.
- **Workforce Directory:** Live directory of enrolled personnel, assigned shifts, and departments.
- **Analytics Studio:** Real-time departmental metrics, competition-ranked late leaderboard with medal badges, and an interactive SVG 7-day moving average trend chart.
- **Performance Studio:** Direct visual interface to MongoDB's `explain` output inspecting `executionStats`, execution time, and `IXSCAN` index verification.

---

## 4. Running Tests

```bash
# Run the local regression and concurrency test suite
python -m pytest tests_local/test_suite.py -v
```
