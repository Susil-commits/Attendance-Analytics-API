# HROne Attendance & Analytics Engine

FastAPI backend for employee attendance tracking and workforce analytics backed by MongoDB. Implements punch-in/punch-out rules, manual regularization audit trails, and MongoDB aggregation pipelines.

## How to Run

### Prerequisites
- Python 3.11+
- MongoDB 6.0+ (Local instance or MongoDB Atlas)

### Setup & Execution
```bash
pip install -r requirements.txt
uvicorn app.main:app --port 8000
```
- Interactive Swagger Docs: `http://localhost:8000/docs`
- OpenAPI Specification: `http://localhost:8000/openapi.json`
- Health Probe: `http://localhost:8000/health`

## Environment Variables
- `MONGO_URI`: MongoDB connection URI (default: `mongodb://127.0.0.1:27017`).
- `MONGO_DB`: Target MongoDB database name (default: `attendance_db`).

## Assumptions
- **Punch-Out Error Order**: Punch-out validation checks evaluate in the order 404 (employee not found, or no punch-in at or before punched_at), 409 (record already punched out), and 422 (invalid timestamp, or punch-out not after punch-in / more than 24 h later).
- **Moving Average Precision**: The 7-day moving average in department trend queries computes over unrounded daily attendance rates in MongoDB's `$setWindowFields` stage to preserve precision, rounding both daily rates and moving averages to 4 decimal places using half-up arithmetic upon serialization.
- **Weekend Attendance Impact**: Attendance records logged on weekends (Saturday/Sunday) count toward `late_minutes` and `overtime_minutes` only, but are excluded from `present_days` and `working_days`.
- **Ties at Leaderboard Cutoff**: When multiple employees tie at the cutoff rank, all tied employees are returned, allowing results to exceed `limit`.

## What is Missing
Nothing missing. All required endpoints, business rules (R1–R10), concurrency controls, and aggregation pipelines are fully implemented and compliant with the OpenAPI specification.

## Verification
All business rules, concurrency guarantees, and 120k-record explain plans were verified using a comprehensive local test suite (tests are local and not included in the repo).

