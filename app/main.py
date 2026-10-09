"""
HROne Attendance & Analytics Engine
Production Implementation compliant with openapi.yaml, DATA_MODEL.md, and HROne_agent_plan.md.

Sections:
1. Config & Lifespan
2. Time Helpers
3. Rule Helpers
4. Models & Validators
5. Serializers
6. Index Setup
7. Pipeline & Query Builders
8. Endpoints
9. Explain Endpoint
10. Minimalist Swiss Enterprise UI
"""
from __future__ import annotations

import json
import math
import os
import re
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import bson.json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Path, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

# --------------------------------------------------------------------------- #
# 1. Config & Lifespan
# --------------------------------------------------------------------------- #
# Real environment variables win over .env; load_dotenv(override=False) is default
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DB = os.getenv("MONGO_DB", "attendance_db")

client: MongoClient = MongoClient(
    MONGO_URI,
    tz_aware=True,
    tzinfo=timezone.utc,
    maxPoolSize=100,
    serverSelectionTimeoutMS=3000,
)
db = client[MONGO_DB]

IST = timezone(timedelta(hours=5, minutes=30))
EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)


def setup_indexes() -> None:
    """Idempotently configure all indexes required for consistency and IXSCAN performance."""
    try:
        # employees collection
        db.employees.create_index([("emp_code", 1)], unique=True, name="idx_emp_code_unique")
        db.employees.create_index([("department", 1), ("emp_code", 1)], name="idx_emp_dept_code")
        db.employees.create_index([("joined_on", 1)], name="idx_emp_joined_on")
        db.employees.create_index([("department", 1), ("joined_on", 1)], name="idx_emp_dept_joined")

        # attendance_logs collection
        db.attendance_logs.create_index([("emp_code", 1), ("date", 1)], unique=True, name="idx_att_emp_date_unique")
        db.attendance_logs.create_index([("date", -1), ("emp_code", 1)], name="idx_att_date_emp")
        db.attendance_logs.create_index([("status", 1), ("date", -1), ("emp_code", 1)], name="idx_att_status_date_emp")
        db.attendance_logs.create_index([("emp_code", 1), ("punch_in", -1)], name="idx_att_emp_punch_in")
    except Exception as e:
        # Log index initialization errors without halting process
        print(f"[INDEX SETUP WARNING] {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_indexes()
    yield


app = FastAPI(
    title="Employee Attendance & Analytics API",
    version="2.0.0",
    lifespan=lifespan,
)


@app.exception_handler(PyMongoError)
async def pymongo_exception_handler(request: Request, exc: PyMongoError):
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": f"Database service unavailable: {str(exc)}"},
    )


# --------------------------------------------------------------------------- #
# 2. Time Helpers
# --------------------------------------------------------------------------- #
def ms_to_dt(ms: int) -> datetime:
    """
    Truncate epoch ms to whole seconds (ms // 1000 * 1000) and return aware UTC datetime.
    """
    trunc_ms = (ms // 1000) * 1000
    return EPOCH_UTC + timedelta(milliseconds=trunc_ms)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    """
    Convert aware/naive datetime to integer epoch milliseconds.
    Never uses float timestamp() * 1000.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    delta = dt - EPOCH_UTC
    return int(delta // timedelta(milliseconds=1))


def ist(dt: datetime) -> datetime:
    """Ensure datetime is aware and in IST (UTC+05:30)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST)


def parse_hhmm(s: str) -> Tuple[int, int]:
    """Parse 'HH:MM' string into hour and minute integers."""
    parts = s.split(":")
    return int(parts[0]), int(parts[1])


def parse_date(date_str: str) -> date:
    """Strict parse of YYYY-MM-DD into a datetime.date object. Rejects invalid days like 2026-02-30."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=422, detail=f"Invalid calendar date: {date_str}")


def round_half_up(val: float | Decimal | None, decimals: int) -> Optional[float]:
    """Round a number half-up to specified decimal places using Decimal arithmetic."""
    if val is None:
        return None
    d = Decimal(str(val))
    q = Decimal("10") ** -decimals
    return float(d.quantize(q, rounding=ROUND_HALF_UP))


# --------------------------------------------------------------------------- #
# 3. Rule Helpers
# --------------------------------------------------------------------------- #
def attendance_date(punch_in_utc: datetime, shift_start: str, shift_end: str) -> str:
    """
    R1: Shift times and attendance dates are IST (UTC+05:30).
    A record's date is the IST calendar date of its punch_in instant.
    Exception: overnight shifts (shift_end <= shift_start, e.g. 22:00-06:00):
    a punch-in whose IST time is strictly earlier than shift_end belongs to the
    shift that started the previous day.
    """
    pi_ist = ist(punch_in_utc)
    pi_date = pi_ist.date()
    pi_time = pi_ist.time()

    sh_end, sm_end = parse_hhmm(shift_end)
    sh_start, sm_start = parse_hhmm(shift_start)

    is_overnight = (sh_end < sh_start) or (sh_end == sh_start and sm_end <= sm_start)
    if is_overnight:
        end_time = datetime(pi_date.year, pi_date.month, pi_date.day, sh_end, sm_end, 0).time()
        if pi_time < end_time:
            pi_date = pi_date - timedelta(days=1)

    return pi_date.isoformat()


def shift_start_dt(date_str: str, shift_start: str) -> datetime:
    """Return IST datetime corresponding to shift start for the given attendance date."""
    d = parse_date(date_str)
    h, m = parse_hhmm(shift_start)
    return datetime(d.year, d.month, d.day, h, m, 0, tzinfo=IST)


def shift_end_dt(date_str: str, shift_start: str, shift_end: str) -> datetime:
    """Return IST datetime corresponding to shift end (+1 day if overnight shift)."""
    d = parse_date(date_str)
    sh_start, sm_start = parse_hhmm(shift_start)
    sh_end, sm_end = parse_hhmm(shift_end)

    is_overnight = (sh_end < sh_start) or (sh_end == sh_start and sm_end <= sm_start)
    if is_overnight:
        d = d + timedelta(days=1)

    return datetime(d.year, d.month, d.day, sh_end, sm_end, 0, tzinfo=IST)


def compute_late_minutes(punch_in_utc: datetime, date_str: str, shift_start: str) -> int:
    """
    R2: Grace period 10 minutes. A punch-in is late only if it is strictly more
    than 10:00 (600 seconds) after shift_start. When late, late_minutes = whole
    minutes (floor) elapsed since shift_start. Otherwise 0.
    """
    pi_ist = ist(punch_in_utc)
    s_start = shift_start_dt(date_str, shift_start)
    delta_sec = int((pi_ist - s_start).total_seconds())

    if delta_sec > 600:
        return delta_sec // 60
    return 0


def compute_overtime(punch_out_utc: datetime, shift_start: str, shift_end: str, date_str: str) -> int:
    """
    R3: Overtime = whole minutes (floor) between shift_end and punch_out,
    counted only when >= 30; otherwise 0.
    """
    po_ist = ist(punch_out_utc)
    s_end = shift_end_dt(date_str, shift_start, shift_end)
    delta_sec = int((po_ist - s_end).total_seconds())

    mins = delta_sec // 60
    if mins >= 30:
        return mins
    return 0


def compute_work_hours(punch_in_utc: datetime, punch_out_utc: datetime) -> float:
    """
    R4: (punch_out - punch_in) in seconds / 3600, rounded to 2 decimals half-up.
    """
    delta_sec = int((punch_out_utc - punch_in_utc).total_seconds())
    d_sec = Decimal(delta_sec)
    hours = (d_sec / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(hours)


def compute_half_day(work_hours_rounded: Optional[float]) -> bool:
    """
    R5: After punch-out, half_day = true when the rounded work_hours < 4.50.
    """
    if work_hours_rounded is None:
        return False
    return Decimal(str(work_hours_rounded)) < Decimal("4.50")


def derive_fields(
    status_val: str,
    punch_in_utc: Optional[datetime],
    punch_out_utc: Optional[datetime],
    shift_start: str,
    shift_end: str,
    date_str: str,
) -> Tuple[Optional[float], int, int, bool]:
    """
    Derives (work_hours, late_minutes, overtime_minutes, half_day)
    consistent with DATA_MODEL.md table:
    - ABSENT / LEAVE -> (None, 0, 0, False)
    - Presence status with punch_in only -> (None, late_minutes, 0, False)
    - Presence status with punch_out -> (work_hours, late_minutes, overtime, half_day)
    """
    if status_val in ("ABSENT", "LEAVE"):
        return None, 0, 0, False

    if punch_in_utc is None:
        return None, 0, 0, False

    late = compute_late_minutes(punch_in_utc, date_str, shift_start)

    if punch_out_utc is None:
        return None, late, 0, False

    wh = compute_work_hours(punch_in_utc, punch_out_utc)
    ot = compute_overtime(punch_out_utc, shift_start, shift_end, date_str)
    hd = compute_half_day(wh)
    return wh, late, ot, hd


# --------------------------------------------------------------------------- #
# 4. Models & Validators
# --------------------------------------------------------------------------- #
EMP_CODE_REGEX = re.compile(r"^EMP\d{4,6}$")
EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
TIME_HHMM_REGEX = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")
STATUS_CHOICES = {"PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"}
PRESENCE_STATUSES = {"PRESENT", "WFH", "ON_DUTY"}


from typing import Annotated

EpochMillis = Annotated[int, Field(strict=True, ge=100_000_000_000, le=4_102_444_800_000)]


class EmployeeCreate(BaseModel):
    emp_code: str = Field(..., pattern=r"^EMP\d{4,6}$")
    name: str = Field(..., min_length=1, max_length=100)
    email: str = Field(..., max_length=120)
    department: str = Field(..., min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    shift_end: str = Field(default="18:30", pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    joined_on: str

    @field_validator("email")
    @classmethod
    def validate_email_pattern(cls, v: str) -> str:
        if not EMAIL_REGEX.match(v):
            raise ValueError("Invalid email format")
        return v

    @field_validator("joined_on")
    @classmethod
    def validate_joined_date(cls, v: str) -> str:
        parse_date(v)
        return v

    @field_validator("shift_end")
    @classmethod
    def validate_shifts_differ(cls, v: str, info) -> str:
        shift_start = info.data.get("shift_start", "09:30")
        if v == shift_start:
            raise ValueError("shift_start and shift_end must be different")
        return v


class PunchInRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMillis] = None
    status: str = Field(default="PRESENT")

    @field_validator("status")
    @classmethod
    def validate_presence_status(cls, v: str) -> str:
        if v not in PRESENCE_STATUSES:
            raise ValueError(f"status must be one of {sorted(PRESENCE_STATUSES)}")
        return v


class PunchOutRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMillis] = None


class RegularizeRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    status: Optional[str] = None
    punch_in: Optional[EpochMillis] = None
    punch_out: Optional[EpochMillis] = None
    reason: str = Field(..., min_length=5, max_length=200)
    regularized_by: str = Field(..., min_length=1, max_length=50)

    @field_validator("status")
    @classmethod
    def validate_status_choice(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in STATUS_CHOICES:
            raise ValueError(f"status must be one of {sorted(STATUS_CHOICES)}")
        return v


# --------------------------------------------------------------------------- #
# 5. Serializers
# --------------------------------------------------------------------------- #
def serialize_employee(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Serialize employee document into contract JSON (strips _id, converts created_at to ms)."""
    res = dict(doc)
    res.pop("_id", None)
    created_at = res.get("created_at")
    if isinstance(created_at, datetime):
        res["created_at"] = dt_to_ms(created_at)
    return res


def serialize_record(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Serialize attendance record document into contract JSON.
    - Strips _id and never returns id
    - Converts punch_in/punch_out to epoch ms
    - Defaults missing fields (history=[], half_day=False, late=0, ot=0, work_hours=None)
    - Serializes history[].at and history[].changes times to ms
    """
    res = dict(doc)
    res.pop("_id", None)
    res.pop("id", None)

    pi = res.get("punch_in")
    res["punch_in"] = dt_to_ms(pi) if isinstance(pi, datetime) else pi

    po = res.get("punch_out")
    res["punch_out"] = dt_to_ms(po) if isinstance(po, datetime) else po

    res["work_hours"] = res.get("work_hours", None)
    res["late_minutes"] = res.get("late_minutes", 0)
    res["overtime_minutes"] = res.get("overtime_minutes", 0)
    res["half_day"] = res.get("half_day", False)

    raw_history = res.get("history") or []
    clean_history = []
    for item in raw_history:
        at_val = item.get("at")
        at_ms = dt_to_ms(at_val) if isinstance(at_val, datetime) else at_val

        raw_changes = item.get("changes") or {}
        clean_changes = {}
        for k, change_obj in raw_changes.items():
            f_val = change_obj.get("from")
            t_val = change_obj.get("to")
            if k in ("punch_in", "punch_out"):
                f_val = dt_to_ms(f_val) if isinstance(f_val, datetime) else f_val
                t_val = dt_to_ms(t_val) if isinstance(t_val, datetime) else t_val
            clean_changes[k] = {"from": f_val, "to": t_val}

        clean_history.append({
            "at": at_ms,
            "by": item.get("by") or item.get("regularized_by") or "",
            "reason": item.get("reason", ""),
            "changes": clean_changes,
        })

    res["history"] = clean_history
    return res


# --------------------------------------------------------------------------- #
# 6. Pipeline & Query Builders
# Reused by both live endpoints and /admin/explain/{endpoint}
# --------------------------------------------------------------------------- #
def build_attendance_list_query(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status_val: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
) -> Tuple[Dict[str, Any], List[Tuple[str, int]], int, int]:
    """Build query filter, sort specification, skip, and limit for list_attendance."""
    q: Dict[str, Any] = {}
    if emp_code:
        q["emp_code"] = emp_code

    if date_from or date_to:
        q["date"] = {}
        if date_from:
            parse_date(date_from)
            q["date"]["$gte"] = date_from
        if date_to:
            parse_date(date_to)
            q["date"]["$lte"] = date_to

    if date_from and date_to and date_from > date_to:
        raise HTTPException(status_code=422, detail="date_from cannot be greater than date_to")

    if status_val:
        if status_val not in STATUS_CHOICES:
            raise HTTPException(status_code=422, detail=f"Invalid status: {status_val}")
        q["status"] = status_val

    sort_spec = [("date", -1), ("emp_code", 1)]
    skip_val = (page - 1) * page_size
    limit_val = page_size
    return q, sort_spec, skip_val, limit_val


def parse_month_bounds(month_str: str) -> Tuple[str, str, int, int, int]:
    """Validate 'YYYY-MM' and return (start_date, end_date, year, month, last_day)."""
    if not re.match(r"^\d{4}-(0[1-9]|1[0-2])$", month_str):
        raise HTTPException(status_code=422, detail="month must be YYYY-MM")
    y, m = map(int, month_str.split("-"))
    start_date = f"{y:04d}-{m:02d}-01"
    next_y = y + (1 if m == 12 else 0)
    next_m = 1 if m == 12 else m + 1
    end_date = f"{next_y:04d}-{next_m:02d}-01"
    last_day = (datetime(next_y, next_m, 1) - timedelta(days=1)).day
    return start_date, end_date, y, m, last_day


def build_employee_monthly_pipeline(emp_code: str, month_str: str) -> List[Dict[str, Any]]:
    """Build aggregation pipeline for employee monthly attendance."""
    start_date, end_date, _, _, _ = parse_month_bounds(month_str)
    return [
        {
            "$match": {
                "emp_code": emp_code,
                "date": {"$gte": start_date, "$lt": end_date},
            }
        },
        {
            "$group": {
                "_id": None,
                "present_days": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                    {"$lte": [{"$isoDayOfWeek": {"$dateFromString": {"dateString": "$date"}}}, 5]},
                                ]
                            },
                            {"$cond": [{"$eq": [{"$ifNull": ["$half_day", False]}, True]}, 0.5, 1.0]},
                            0.0,
                        ]
                    }
                },
                "leave_days": {
                    "$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}
                },
                "late_count": {
                    "$sum": {"$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0]}
                },
                "total_late_minutes": {
                    "$sum": {"$ifNull": ["$late_minutes", 0]}
                },
                "total_overtime_minutes": {
                    "$sum": {"$ifNull": ["$overtime_minutes", 0]}
                },
            }
        },
    ]


def build_department_summary_pipeline(month_str: str, department: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Build cross-collection aggregation pipeline for department monthly summary.
    Starts from employees to include all active employees even with zero logs (R9).
    """
    start_date, end_date, y, m, last_day = parse_month_bounds(month_str)
    last_day_str = f"{y:04d}-{m:02d}-{last_day:02d}"

    match_stage: Dict[str, Any] = {"joined_on": {"$lte": last_day_str}}
    if department:
        match_stage["department"] = department

    return [
        {"$match": match_stage},
        {
            "$lookup": {
                "from": "attendance_logs",
                "let": {"code": "$emp_code"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {"$eq": ["$emp_code", "$$code"]},
                            "date": {"$gte": start_date, "$lt": end_date},
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "present_days": {
                                "$sum": {
                                    "$cond": [
                                        {
                                            "$and": [
                                                {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                                {"$lte": [{"$isoDayOfWeek": {"$dateFromString": {"dateString": "$date"}}}, 5]},
                                            ]
                                        },
                                        {"$cond": [{"$eq": [{"$ifNull": ["$half_day", False]}, True]}, 0.5, 1.0]},
                                        0.0,
                                    ]
                                }
                            },
                            "wh_sum_cents": {
                                "$sum": {
                                    "$cond": [
                                        {
                                            "$and": [
                                                {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                                {"$ne": ["$work_hours", None]},
                                            ]
                                        },
                                        {"$round": [{"$multiply": ["$work_hours", 100]}, 0]},
                                        0,
                                    ]
                                }
                            },
                            "wh_count": {
                                "$sum": {
                                    "$cond": [
                                        {
                                            "$and": [
                                                {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                                {"$ne": ["$work_hours", None]},
                                            ]
                                        },
                                        1,
                                        0,
                                    ]
                                }
                            },
                            "late_count": {
                                "$sum": {"$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0]}
                            },
                            "total_late_minutes": {
                                "$sum": {"$ifNull": ["$late_minutes", 0]}
                            },
                            "leave_count": {
                                "$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}
                            },
                            "on_duty_count": {
                                "$sum": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]}
                            },
                        }
                    },
                ],
                "as": "summary_arr",
            }
        },
        {"$set": {"summary": {"$arrayElemAt": ["$summary_arr", 0]}}},
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "present_days": {"$sum": {"$ifNull": ["$summary.present_days", 0.0]}},
                "total_wh_cents": {"$sum": {"$ifNull": ["$summary.wh_sum_cents", 0]}},
                "total_wh_count": {"$sum": {"$ifNull": ["$summary.wh_count", 0]}},
                "late_count": {"$sum": {"$ifNull": ["$summary.late_count", 0]}},
                "total_late_minutes": {"$sum": {"$ifNull": ["$summary.total_late_minutes", 0]}},
                "leave_count": {"$sum": {"$ifNull": ["$summary.leave_count", 0]}},
                "on_duty_count": {"$sum": {"$ifNull": ["$summary.on_duty_count", 0]}},
            }
        },
        {"$sort": {"_id": 1}},
    ]


def build_late_leaderboard_pipeline(month_str: str, limit_val: int = 10, department: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Build aggregation pipeline for late leaderboard with standard competition ranking (1, 2, 2, 4).
    Limit applies after ranking.
    """
    start_date, end_date, _, _, _ = parse_month_bounds(month_str)
    pipeline: List[Dict[str, Any]] = [
        {
            "$match": {
                "date": {"$gte": start_date, "$lt": end_date},
            }
        },
        {
            "$group": {
                "_id": "$emp_code",
                "total_late_minutes": {"$sum": {"$ifNull": ["$late_minutes", 0]}},
                "late_count": {
                    "$sum": {"$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0]}
                },
            }
        },
        {"$match": {"total_late_minutes": {"$gt": 0}}},
        {
            "$lookup": {
                "from": "employees",
                "localField": "_id",
                "foreignField": "emp_code",
                "as": "emp",
            }
        },
        {"$unwind": "$emp"},
    ]

    if department:
        pipeline.append({"$match": {"emp.department": department}})

    pipeline.extend([
        {
            "$setWindowFields": {
                "sortBy": {"total_late_minutes": -1},
                "output": {"rank": {"$rank": {}}},
            }
        },
        {"$match": {"rank": {"$lte": limit_val}}},
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
    ])
    return pipeline


def build_department_trend_pipeline(
    department: str,
    from_str: str,
    to_str: str,
    joined_ons: List[str],
    dept_emp_codes: List[str],
) -> List[Dict[str, Any]]:
    """
    Build gap-filling aggregation pipeline for department daily attendance trend.
    Uses $densify with bounded datetime range and $setWindowFields for 7-day moving average.
    """
    from_d = parse_date(from_str)
    to_d = parse_date(to_str)

    dt_start = datetime(from_d.year, from_d.month, from_d.day, tzinfo=timezone.utc)
    dt_end_next = datetime(to_d.year, to_d.month, to_d.day, tzinfo=timezone.utc) + timedelta(days=1)

    seed_documents = [
        {"date": from_str, "_is_seed": True},
        {"date": to_str, "_is_seed": True},
    ]

    pipeline = [
        {
            "$match": {
                "emp_code": {"$in": dept_emp_codes},
                "date": {"$gte": from_str, "$lte": to_str},
            }
        },
        {
            "$unionWith": {
                "pipeline": [{"$documents": seed_documents}]
            }
        },
        {
            "$group": {
                "_id": "$date",
                "present_count": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": ["$_is_seed", True]},
                                    {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                ]
                            },
                            {"$cond": [{"$eq": [{"$ifNull": ["$half_day", False]}, True]}, 0.5, 1.0]},
                            0.0,
                        ]
                    }
                },
                "late_count": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": ["$_is_seed", True]},
                                    {"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
            }
        },
        {"$set": {"date_dt": {"$dateFromString": {"dateString": "$_id"}}}},
        {
            "$densify": {
                "field": "date_dt",
                "range": {
                    "step": 1,
                    "unit": "day",
                    "bounds": [dt_start, dt_end_next],
                },
            }
        },
        {
            "$set": {
                "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date_dt"}},
                "present_count": {"$ifNull": ["$present_count", 0.0]},
                "late_count": {"$ifNull": ["$late_count", 0]},
                "is_working_day": {"$lte": [{"$isoDayOfWeek": "$date_dt"}, 5]},
            }
        },
        {
            "$set": {
                "headcount": {
                    "$size": {
                        "$filter": {
                            "input": joined_ons,
                            "as": "j",
                            "cond": {"$lte": ["$$j", "$date"]},
                        }
                    }
                }
            }
        },
        {
            "$set": {
                "attendance_rate": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        {"$divide": ["$present_count", "$headcount"]},
                        None,
                    ]
                }
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"date_dt": 1},
                "output": {
                    "moving_avg_7d": {
                        "$avg": "$attendance_rate",
                        "window": {"documents": [-6, 0]},
                    }
                },
            }
        },
        {"$sort": {"date_dt": 1}},
    ]
    return pipeline


# --------------------------------------------------------------------------- #
# 7. Endpoints
# --------------------------------------------------------------------------- #
@app.get("/health", tags=["system"])
def health():
    """Readiness probe. Returns 200 only if MongoDB answers a ping, otherwise 503."""
    try:
        res = client.admin.command("ping")
        if res.get("ok") == 1.0 or res.get("ok") == 1:
            return {"status": "ok"}
        raise HTTPException(status_code=503, detail="MongoDB ping returned non-ok status")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unreachable: {str(e)}")


@app.post("/employees", status_code=201, tags=["employees"])
def create_employee(body: EmployeeCreate):
    """
    Create an employee with server-clock created_at.
    Guarded against duplicate creates by unique index on emp_code (returns 409).
    """
    created_at_dt = datetime.now(timezone.utc)
    doc = body.model_dump()
    doc["created_at"] = created_at_dt

    try:
        db.employees.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail=f"Employee {body.emp_code} already exists")

    return serialize_employee(doc)


@app.get("/employees", tags=["employees"])
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """List employees sorted by emp_code ascending with exact filtering and pagination."""
    q: Dict[str, Any] = {}
    if department:
        q["department"] = department

    total = db.employees.count_documents(q)
    skip = (page - 1) * page_size
    cursor = db.employees.find(q, {"_id": 0}).sort("emp_code", 1).skip(skip).limit(page_size)
    items = [serialize_employee(emp) for emp in cursor]

    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201, tags=["attendance"])
def punch_in(body: PunchInRequest):
    """
    Record a punch-in. Creates the day's record per R1 & R2.
    Race-safe: concurrent punch-ins for the same (emp_code, date) result in exactly
    one 201 and the rest 409 via unique database index.
    """
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")

    if body.punched_at is not None:
        pi_dt = ms_to_dt(body.punched_at)
    else:
        now_dt = datetime.now(timezone.utc)
        pi_dt = ms_to_dt(dt_to_ms(now_dt))

    att_date = attendance_date(pi_dt, emp["shift_start"], emp["shift_end"])
    late = compute_late_minutes(pi_dt, att_date, emp["shift_start"])

    doc = {
        "emp_code": body.emp_code,
        "date": att_date,
        "status": body.status,
        "punch_in": pi_dt,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": late,
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }

    try:
        db.attendance_logs.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="already punched in for this date")

    return serialize_record(doc)


@app.post("/attendance/punch-out", status_code=200, tags=["attendance"])
def punch_out(body: PunchOutRequest):
    """
    Record a punch-out. Closes most recent record whose punch_in <= punched_at.
    Atomic find_one_and_update guarantees two simultaneous punch-outs yield exactly one 200.
    """
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")

    if body.punched_at is not None:
        po_dt = ms_to_dt(body.punched_at)
    else:
        now_dt = datetime.now(timezone.utc)
        po_dt = ms_to_dt(dt_to_ms(now_dt))

    rec = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": po_dt}},
        sort=[("punch_in", -1)],
    )
    if not rec:
        raise HTTPException(status_code=404, detail="No matching punch-in found")

    if rec.get("punch_out") is not None:
        raise HTTPException(status_code=409, detail="That record is already punched out")

    pi_dt = rec["punch_in"]
    if po_dt <= pi_dt:
        raise HTTPException(status_code=422, detail="punched_at must be strictly after punch_in")

    if (po_dt - pi_dt).total_seconds() > 86400:
        raise HTTPException(status_code=422, detail="punched_at cannot be more than 24 hours after punch_in")

    wh = compute_work_hours(pi_dt, po_dt)
    ot = compute_overtime(po_dt, emp["shift_start"], emp["shift_end"], rec["date"])
    hd = compute_half_day(wh)

    updated = db.attendance_logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None, "punch_in": pi_dt},
        {"$set": {
            "punch_out": po_dt,
            "work_hours": wh,
            "overtime_minutes": ot,
            "half_day": hd,
        }},
        return_document=ReturnDocument.AFTER,
    )

    if not updated:
        raise HTTPException(status_code=409, detail="That record is already punched out")

    return serialize_record(updated)


@app.get("/attendance", tags=["attendance"])
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """
    List attendance records sorted by date desc, emp_code asc.
    Uses index scan, avoids in-memory collection loading.
    """
    q, sort_spec, skip, limit = build_attendance_list_query(
        emp_code, date_from, date_to, status, page, page_size
    )

    total = db.attendance_logs.count_documents(q)
    cursor = db.attendance_logs.find(q).sort(sort_spec).skip(skip).limit(limit)
    items = [serialize_record(r) for r in cursor]

    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}", tags=["attendance"])
def regularize_attendance(
    emp_code: str,
    date: str,
    body: RegularizeRequest,
):
    """
    Regularize (manually correct) one day's record.
    Recomputes derived fields, appends audit trail to history, and resolves concurrency
    via optimistic locking on history length with retry loop.
    """
    parse_date(date)

    emp = db.employees.find_one({"emp_code": emp_code})
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")

    rec = db.attendance_logs.find_one({"emp_code": emp_code, "date": date})
    if not rec:
        raise HTTPException(status_code=404, detail="Attendance record not found for this date")

    for _ in range(5):
        current_rec = db.attendance_logs.find_one({"_id": rec["_id"]})
        if not current_rec:
            raise HTTPException(status_code=404, detail="Attendance record no longer exists")

        provided_fields = body.model_dump(exclude_unset=True)

        new_status = body.status if "status" in provided_fields else current_rec.get("status")
        if not new_status:
            new_status = "PRESENT"

        if new_status in ("ABSENT", "LEAVE"):
            if "punch_in" in provided_fields and body.punch_in is not None:
                raise HTTPException(status_code=422, detail="ABSENT/LEAVE records cannot have punch_in")
            if "punch_out" in provided_fields and body.punch_out is not None:
                raise HTTPException(status_code=422, detail="ABSENT/LEAVE records cannot have punch_out")
            final_pi = None
            final_po = None
            final_wh = None
            final_late = 0
            final_ot = 0
            final_hd = False
        else:
            if "punch_in" in provided_fields:
                if body.punch_in is None:
                    raise HTTPException(status_code=422, detail="Presence status requires punch_in")
                final_pi = ms_to_dt(body.punch_in)
            else:
                final_pi = current_rec.get("punch_in")

            if final_pi is None:
                raise HTTPException(status_code=422, detail="Presence status requires punch_in")

            derived_date = attendance_date(final_pi, emp["shift_start"], emp["shift_end"])
            if derived_date != date:
                raise HTTPException(
                    status_code=422,
                    detail=f"punch_in resolves to {derived_date}, but record date is {date}",
                )

            if "punch_out" in provided_fields:
                final_po = ms_to_dt(body.punch_out) if body.punch_out is not None else None
            else:
                final_po = current_rec.get("punch_out")

            if final_po is not None:
                if final_po <= final_pi:
                    raise HTTPException(status_code=422, detail="punch_out must be strictly after punch_in")
                if (final_po - final_pi).total_seconds() > 86400:
                    raise HTTPException(status_code=422, detail="punch_out cannot be more than 24 hours after punch_in")

            final_wh, final_late, final_ot, final_hd = derive_fields(
                new_status, final_pi, final_po, emp["shift_start"], emp["shift_end"], date
            )

        changes: Dict[str, Any] = {}

        if current_rec.get("status") != new_status:
            changes["status"] = {"from": current_rec.get("status"), "to": new_status}

        curr_pi = current_rec.get("punch_in")
        if curr_pi != final_pi:
            changes["punch_in"] = {"from": curr_pi, "to": final_pi}

        curr_po = current_rec.get("punch_out")
        if curr_po != final_po:
            changes["punch_out"] = {"from": curr_po, "to": final_po}

        if current_rec.get("work_hours") != final_wh:
            changes["work_hours"] = {"from": current_rec.get("work_hours"), "to": final_wh}

        if current_rec.get("late_minutes", 0) != final_late:
            changes["late_minutes"] = {"from": current_rec.get("late_minutes", 0), "to": final_late}

        if current_rec.get("overtime_minutes", 0) != final_ot:
            changes["overtime_minutes"] = {"from": current_rec.get("overtime_minutes", 0), "to": final_ot}

        if current_rec.get("half_day", False) != final_hd:
            changes["half_day"] = {"from": current_rec.get("half_day", False), "to": final_hd}

        if not changes:
            raise HTTPException(status_code=422, detail="No fields changed")

        now_ms = dt_to_ms(datetime.now(timezone.utc))
        now_dt = ms_to_dt(now_ms)
        history_entry = {
            "at": now_dt,
            "by": body.regularized_by,
            "reason": body.reason,
            "changes": changes,
        }

        n_seen = len(current_rec.get("history") or [])
        res_update = db.attendance_logs.update_one(
            {
                "_id": current_rec["_id"],
                "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, n_seen]},
            },
            {
                "$set": {
                    "status": new_status,
                    "punch_in": final_pi,
                    "punch_out": final_po,
                    "work_hours": final_wh,
                    "late_minutes": final_late,
                    "overtime_minutes": final_ot,
                    "half_day": final_hd,
                },
                "$push": {"history": history_entry},
            },
        )

        if res_update.matched_count == 1:
            updated_doc = db.attendance_logs.find_one({"_id": current_rec["_id"]})
            return serialize_record(updated_doc)

    raise HTTPException(status_code=409, detail="Concurrent regularization conflict; please retry")


@app.get("/analytics/employees/{emp_code}/monthly", tags=["analytics"])
def employee_monthly(emp_code: str, month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$")):
    """Monthly attendance summary for one employee backed by MongoDB aggregation."""
    emp = db.employees.find_one({"emp_code": emp_code})
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")

    start_date, end_date, y, m, last_day = parse_month_bounds(month)

    joined_on = emp["joined_on"]
    calc_start = max(start_date, joined_on)
    working_days = 0
    if calc_start < end_date:
        cur_d = parse_date(calc_start)
        last_d = date(y, m, last_day)
        while cur_d <= last_d:
            if cur_d.weekday() < 5:  # Monday to Friday
                working_days += 1
            cur_d += timedelta(days=1)

    pipeline = build_employee_monthly_pipeline(emp_code, month)
    agg_res = list(db.attendance_logs.aggregate(pipeline))

    if agg_res:
        row = agg_res[0]
        present_days = float(Decimal(str(row["present_days"])).quantize(Decimal("0.1")))
        leave_days = int(row["leave_days"])
        late_count = int(row["late_count"])
        total_late = int(row["total_late_minutes"])
        total_ot = int(row["total_overtime_minutes"])
    else:
        present_days = 0.0
        leave_days = 0
        late_count = 0
        total_late = 0
        total_ot = 0

    if working_days > 0:
        pct = float((Decimal(str(present_days)) / Decimal(working_days) * Decimal(100)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        ))
    else:
        pct = None

    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": present_days,
        "leave_days": leave_days,
        "late_count": late_count,
        "total_late_minutes": total_late,
        "total_overtime_minutes": total_ot,
        "attendance_pct": pct,
    }


@app.get("/analytics/departments/summary", tags=["analytics"])
def department_summary(
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    department: Optional[str] = None,
):
    """
    Department summary for a month. Headcount includes all employees joined by month end,
    even with zero attendance logs. Averages calculated over individual records.
    """
    pipeline = build_department_summary_pipeline(month, department)
    cursor = db.employees.aggregate(pipeline)

    items = []
    for row in cursor:
        headcount = int(row["headcount"])
        present_days = float(Decimal(str(row["present_days"])).quantize(Decimal("0.1")))
        total_wh_count = int(row["total_wh_count"])
        total_wh_cents = int(row["total_wh_cents"])

        if total_wh_count > 0:
            avg_wh = float((Decimal(total_wh_cents) / Decimal(total_wh_count) / Decimal(100)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            ))
        else:
            avg_wh = None

        items.append({
            "department": row["_id"],
            "headcount": headcount,
            "present_days": present_days,
            "avg_work_hours": avg_wh,
            "late_count": int(row["late_count"]),
            "total_late_minutes": int(row["total_late_minutes"]),
            "leave_count": int(row["leave_count"]),
            "on_duty_count": int(row["on_duty_count"]),
        })

    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late", tags=["analytics"])
def late_leaderboard(
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    """
    Late-comers leaderboard for a month.
    Uses standard competition ranking (1, 2, 2, 4); limit applies after ranking.
    """
    pipeline = build_late_leaderboard_pipeline(month, limit, department)
    cursor = db.attendance_logs.aggregate(pipeline)

    items = []
    for row in cursor:
        items.append({
            "rank": int(row["rank"]),
            "emp_code": row["_id"],
            "name": row["emp"]["name"],
            "department": row["emp"]["department"],
            "total_late_minutes": int(row["total_late_minutes"]),
            "late_count": int(row["late_count"]),
        })

    return {"month": month, "items": items}


@app.get("/analytics/departments/{department}/trend", tags=["analytics"])
def department_trend(
    department: str,
    from_: str = Query(..., alias="from"),
    to: str = Query(...),
):
    """
    Daily attendance trend for a department. Returns exactly one row per calendar day
    using MongoDB $densify. Includes 7-day moving average.
    """
    d_from = parse_date(from_)
    d_to = parse_date(to)

    if d_to < d_from:
        raise HTTPException(status_code=422, detail="'to' date must be >= 'from' date")

    day_span = (d_to - d_from).days + 1
    if day_span > 92:
        raise HTTPException(status_code=422, detail="Date range cannot exceed 92 days")

    dept_emps = list(db.employees.find({"department": department}, {"emp_code": 1, "joined_on": 1}))
    if not dept_emps:
        raise HTTPException(status_code=404, detail="Department not found")

    joined_ons = [emp["joined_on"] for emp in dept_emps]
    dept_codes = [emp["emp_code"] for emp in dept_emps]

    pipeline = build_department_trend_pipeline(department, from_, to, joined_ons, dept_codes)
    cursor = db.attendance_logs.aggregate(pipeline)

    items = []
    for row in cursor:
        rate = row.get("attendance_rate")
        m_avg = row.get("moving_avg_7d")

        rate_rounded = round_half_up(rate, 4) if rate is not None else None
        m_avg_rounded = round_half_up(m_avg, 4) if m_avg is not None else None

        items.append({
            "date": row["date"],
            "is_working_day": bool(row["is_working_day"]),
            "headcount": int(row["headcount"]),
            "present_count": float(Decimal(str(row["present_count"])).quantize(Decimal("0.1"))),
            "late_count": int(row["late_count"]),
            "attendance_rate": rate_rounded,
            "moving_avg_7d": m_avg_rounded,
        })

    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# 8. Explain Endpoint
# --------------------------------------------------------------------------- #
@app.get("/admin/explain/{endpoint}", tags=["admin"])
def explain_endpoint(
    endpoint: str = Path(..., enum=["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"]),
    emp_code: Optional[str] = None,
    month: Optional[str] = Query(None, pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[str] = None,
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """
    MongoDB explain with executionStats verbosity for an endpoint's main query or aggregation.
    Built identically to how the real endpoint constructs the plan.
    """
    if endpoint == "attendance_list":
        q, sort_spec, skip, limit_val = build_attendance_list_query(
            emp_code, date_from, date_to, status, page, page_size
        )
        cmd = {
            "explain": {
                "find": "attendance_logs",
                "filter": q,
                "sort": dict(sort_spec),
                "skip": skip,
                "limit": limit_val,
            },
            "verbosity": "executionStats",
        }
        res = db.command(cmd)
        return {
            "endpoint": endpoint,
            "collection": "attendance_logs",
            "explain": json.loads(bson.json_util.dumps(res)),
        }

    elif endpoint == "employee_monthly":
        if not emp_code:
            raise HTTPException(status_code=422, detail="emp_code required for employee_monthly explain")
        if not month:
            raise HTTPException(status_code=422, detail="month required for employee_monthly explain")
        pipeline = build_employee_monthly_pipeline(emp_code, month)
        cmd = {
            "explain": {
                "aggregate": "attendance_logs",
                "pipeline": pipeline,
                "cursor": {},
            },
            "verbosity": "executionStats",
        }
        res = db.command(cmd)
        return {
            "endpoint": endpoint,
            "collection": "attendance_logs",
            "explain": json.loads(bson.json_util.dumps(res)),
        }

    elif endpoint == "department_summary":
        if not month:
            raise HTTPException(status_code=422, detail="month required for department_summary explain")
        pipeline = build_department_summary_pipeline(month, department)
        cmd = {
            "explain": {
                "aggregate": "employees",
                "pipeline": pipeline,
                "cursor": {},
            },
            "verbosity": "executionStats",
        }
        res = db.command(cmd)
        return {
            "endpoint": endpoint,
            "collection": "employees",
            "explain": json.loads(bson.json_util.dumps(res)),
        }

    elif endpoint == "late_leaderboard":
        if not month:
            raise HTTPException(status_code=422, detail="month required for late_leaderboard explain")
        pipeline = build_late_leaderboard_pipeline(month, limit, department)
        cmd = {
            "explain": {
                "aggregate": "attendance_logs",
                "pipeline": pipeline,
                "cursor": {},
            },
            "verbosity": "executionStats",
        }
        res = db.command(cmd)
        return {
            "endpoint": endpoint,
            "collection": "attendance_logs",
            "explain": json.loads(bson.json_util.dumps(res)),
        }

    elif endpoint == "department_trend":
        if not department:
            raise HTTPException(status_code=422, detail="department required for department_trend explain")
        if not from_ or not to:
            raise HTTPException(status_code=422, detail="'from' and 'to' parameters required for department_trend explain")

        dept_emps = list(db.employees.find({"department": department}, {"emp_code": 1, "joined_on": 1}))
        joined_ons = [emp["joined_on"] for emp in dept_emps]
        dept_codes = [emp["emp_code"] for emp in dept_emps]

        pipeline = build_department_trend_pipeline(department, from_, to, joined_ons, dept_codes)
        cmd = {
            "explain": {
                "aggregate": "attendance_logs",
                "pipeline": pipeline,
                "cursor": {},
            },
            "verbosity": "executionStats",
        }
        res = db.command(cmd)
        return {
            "endpoint": endpoint,
            "collection": "attendance_logs",
            "explain": json.loads(bson.json_util.dumps(res)),
        }

    raise HTTPException(status_code=422, detail=f"Unsupported explain endpoint: {endpoint}")


# --------------------------------------------------------------------------- #
# 9. Minimalist Swiss Enterprise UI Dashboard
# High-contrast, clean typography, executive data density, live API interaction
# --------------------------------------------------------------------------- #
SWISS_UI_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HROne — Attendance & Workforce Analytics Engine</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Geist+Mono:wght@400;500;600&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root {
  --bg-page: #f8fafc;
  --bg-card: #ffffff;
  --border-subtle: #e2e8f0;
  --border-strong: #cbd5e1;
  --text-primary: #09090b;
  --text-secondary: #475569;
  --text-muted: #94a3b8;
  --accent-black: #0f172a;
  --accent-hover: #1e293b;
  --green-bg: #ecfdf5;
  --green-border: #a7f3d0;
  --green-text: #065f46;
  --amber-bg: #fffbeb;
  --amber-border: #fde68a;
  --amber-text: #92400e;
  --blue-bg: #f0f9ff;
  --blue-border: #bae6fd;
  --blue-text: #0369a1;
  --red-bg: #fef2f2;
  --red-border: #fecaca;
  --red-text: #991b1b;
  --purple-bg: #faf5ff;
  --purple-border: #e9d5ff;
  --purple-text: #6b21a8;
  --radius-sm: 4px;
  --radius-md: 6px;
  --radius-lg: 8px;
  --shadow-card: 0 1px 3px 0 rgba(0, 0, 0, 0.05), 0 1px 2px -1px rgba(0, 0, 0, 0.05);
}

* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  background-color: var(--bg-page);
  color: var(--text-primary);
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
}

/* Swiss Grid Header */
header {
  background: var(--bg-card);
  border-bottom: 1px solid var(--border-subtle);
  padding: 0.85rem 2rem;
  display: flex;
  align-items: center;
  justify-content: space-between;
  position: sticky;
  top: 0;
  z-index: 50;
}

.brand-wrap {
  display: flex;
  align-items: center;
  gap: 1rem;
}

.brand-badge {
  background: var(--accent-black);
  color: #fff;
  font-weight: 700;
  font-size: 0.85rem;
  padding: 0.2rem 0.55rem;
  border-radius: var(--radius-sm);
  letter-spacing: 0.08em;
}

.brand-title {
  font-size: 0.95rem;
  font-weight: 600;
  letter-spacing: -0.01em;
  color: var(--text-primary);
}

.brand-sub {
  font-size: 0.75rem;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.06em;
}

.header-status {
  display: flex;
  align-items: center;
  gap: 1.25rem;
  font-size: 0.8rem;
}

.status-pill {
  display: inline-flex;
  align-items: center;
  gap: 0.4rem;
  padding: 0.2rem 0.6rem;
  border-radius: 9999px;
  font-weight: 500;
  background: var(--green-bg);
  border: 1px solid var(--green-border);
  color: var(--green-text);
  font-size: 0.75rem;
}

.status-dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: #10b981;
}

.tab-nav {
  display: flex;
  background: #f1f5f9;
  padding: 0.25rem;
  border-radius: var(--radius-md);
  gap: 0.2rem;
}

.tab-btn {
  background: transparent;
  border: none;
  font-family: inherit;
  font-size: 0.8rem;
  font-weight: 500;
  padding: 0.4rem 0.85rem;
  border-radius: var(--radius-sm);
  cursor: pointer;
  color: var(--text-secondary);
  transition: all 0.15s ease;
}

.tab-btn.active {
  background: var(--bg-card);
  color: var(--text-primary);
  box-shadow: 0 1px 2px rgba(0,0,0,0.06);
  font-weight: 600;
}

/* Layout */
main {
  max-width: 1440px;
  margin: 1.75rem auto;
  padding: 0 1.5rem 3rem 1.5rem;
}

.tab-content { display: none; }
.tab-content.active { display: block; animation: fadeIn 0.2s ease; }

@keyframes fadeIn {
  from { opacity: 0; transform: translateY(4px); }
  to { opacity: 1; transform: translateY(0); }
}

/* Section Header */
.section-meta {
  margin-bottom: 1.25rem;
  display: flex;
  align-items: flex-end;
  justify-content: space-between;
}

.section-meta h2 {
  font-size: 1.2rem;
  font-weight: 700;
  letter-spacing: -0.02em;
}

.section-meta p {
  font-size: 0.82rem;
  color: var(--text-secondary);
}

/* Grid & Cards */
.card {
  background: var(--bg-card);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-card);
  padding: 1.25rem;
  margin-bottom: 1.25rem;
}

.card-title {
  font-size: 0.85rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-secondary);
  margin-bottom: 0.85rem;
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.stats-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 1rem;
  margin-bottom: 1.25rem;
}

.stat-box {
  background: var(--bg-card);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  padding: 1rem 1.25rem;
}

.stat-label {
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--text-muted);
  font-weight: 600;
}

.stat-val {
  font-size: 1.6rem;
  font-weight: 700;
  letter-spacing: -0.03em;
  margin-top: 0.25rem;
  font-variant-numeric: tabular-nums;
}

/* Tables */
.table-wrap {
  overflow-x: auto;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: var(--bg-card);
}

table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.82rem;
  text-align: left;
}

th {
  background: #f8fafc;
  padding: 0.65rem 0.85rem;
  font-weight: 600;
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-secondary);
  border-bottom: 1px solid var(--border-subtle);
  white-space: nowrap;
}

td {
  padding: 0.65rem 0.85rem;
  border-bottom: 1px solid var(--border-subtle);
  font-variant-numeric: tabular-nums;
  color: var(--text-primary);
}

tr:last-child td { border-bottom: none; }
tr:hover td { background: #fafafa; }

/* Status Badges */
.badge {
  display: inline-block;
  padding: 0.15rem 0.45rem;
  border-radius: var(--radius-sm);
  font-size: 0.7rem;
  font-weight: 600;
  letter-spacing: 0.02em;
}
.badge-PRESENT { background: var(--green-bg); border: 1px solid var(--green-border); color: var(--green-text); }
.badge-WFH { background: var(--blue-bg); border: 1px solid var(--blue-border); color: var(--blue-text); }
.badge-ON_DUTY { background: var(--purple-bg); border: 1px solid var(--purple-border); color: var(--purple-text); }
.badge-ABSENT { background: var(--red-bg); border: 1px solid var(--red-border); color: var(--red-text); }
.badge-LEAVE { background: var(--amber-bg); border: 1px solid var(--amber-border); color: var(--amber-text); }

/* Forms & Inputs */
.form-row {
  display: flex;
  gap: 0.75rem;
  flex-wrap: wrap;
  align-items: flex-end;
  margin-bottom: 1rem;
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: 0.3rem;
}

label {
  font-size: 0.72rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-secondary);
}

input, select {
  font-family: inherit;
  font-size: 0.82rem;
  padding: 0.45rem 0.65rem;
  border: 1px solid var(--border-strong);
  border-radius: var(--radius-sm);
  background: #fff;
  color: var(--text-primary);
  outline: none;
  transition: border-color 0.15s;
}

input:focus, select:focus {
  border-color: var(--accent-black);
}

button.btn-primary {
  font-family: inherit;
  background: var(--accent-black);
  color: #fff;
  font-size: 0.8rem;
  font-weight: 600;
  padding: 0.48rem 1rem;
  border: none;
  border-radius: var(--radius-sm);
  cursor: pointer;
  transition: background 0.15s;
  display: inline-flex;
  align-items: center;
  gap: 0.35rem;
}

button.btn-primary:hover {
  background: var(--accent-hover);
}

button.btn-secondary {
  font-family: inherit;
  background: #fff;
  color: var(--text-primary);
  border: 1px solid var(--border-strong);
  font-size: 0.8rem;
  font-weight: 500;
  padding: 0.45rem 0.85rem;
  border-radius: var(--radius-sm);
  cursor: pointer;
}

button.btn-secondary:hover {
  background: #f8fafc;
}

/* Terminal & Code */
pre.code-block {
  font-family: 'Geist Mono', Menlo, Consolas, monospace;
  font-size: 0.75rem;
  background: #0f172a;
  color: #f8fafc;
  padding: 1rem;
  border-radius: var(--radius-md);
  overflow-x: auto;
  max-height: 420px;
}

/* Medal badges for competition ranking */
.rank-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 22px;
  height: 22px;
  border-radius: 50%;
  font-size: 0.72rem;
  font-weight: 700;
  background: #f1f5f9;
  color: var(--text-secondary);
}
.rank-1 { background: #fef08a; color: #854d0e; }
.rank-2 { background: #e2e8f0; color: #334155; }
.rank-3 { background: #fed7aa; color: #9a3412; }

/* SVG Trend Chart */
.chart-container {
  width: 100%;
  height: 260px;
  margin-top: 0.75rem;
}

/* Modal */
.modal-backdrop {
  display: none;
  position: fixed;
  top: 0; left: 0; width: 100%; height: 100%;
  background: rgba(15, 23, 42, 0.45);
  backdrop-filter: blur(2px);
  z-index: 100;
  align-items: center;
  justify-content: center;
}
.modal-backdrop.open { display: flex; }

.modal-dialog {
  background: #fff;
  border-radius: var(--radius-lg);
  width: 95%;
  max-width: 520px;
  padding: 1.5rem;
  box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.1);
}

.modal-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 1rem;
}

.modal-header h3 { font-size: 1rem; font-weight: 700; }
.close-btn { background: none; border: none; font-size: 1.2rem; cursor: pointer; color: var(--text-muted); }
</style>
</head>
<body>

<header>
  <div class="brand-wrap">
    <span class="brand-badge">HR·ONE</span>
    <div>
      <div class="brand-title">Workforce Attendance & Analytics Engine</div>
      <div class="brand-sub">Enterprise Swiss Minimalist Console · MongoDB 7.0</div>
    </div>
  </div>

  <div class="header-status">
    <div class="status-pill" id="health-indicator">
      <span class="status-dot"></span>
      <span id="health-label">Checking DB...</span>
    </div>
    <div class="tab-nav">
      <button class="tab-btn active" onclick="switchTab('tab-kiosk')">Punch Kiosk</button>
      <button class="tab-btn" onclick="switchTab('tab-attendance')">Attendance Ledger</button>
      <button class="tab-btn" onclick="switchTab('tab-employees')">Employees</button>
      <button class="tab-btn" onclick="switchTab('tab-analytics')">Analytics Studio</button>
      <button class="tab-btn" onclick="switchTab('tab-explain')">Query Plan (Explain)</button>
    </div>
  </div>
</header>

<main>
  <!-- TAB 1: Punch Kiosk & Simulators -->
  <div id="tab-kiosk" class="tab-content active">
    <div class="section-meta">
      <div>
        <h2>Live Biometric Punch Simulator</h2>
        <p>Record arrival and departure events with R1/R2 rule evaluation and concurrency tests.</p>
      </div>
      <div id="clock-display" style="font-family: 'Geist Mono'; font-size: 1.25rem; font-weight: 700;"></div>
    </div>

    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1.25rem;">
      <!-- Punch In Card -->
      <div class="card">
        <div class="card-title">Punch-In Event (POST /attendance/punch-in)</div>
        <div class="form-group" style="margin-bottom: 0.75rem;">
          <label>Employee Code</label>
          <input type="text" id="pi-emp" value="EMP0001" placeholder="e.g. EMP0001">
        </div>
        <div class="form-group" style="margin-bottom: 0.75rem;">
          <label>Attendance Status</label>
          <select id="pi-status">
            <option value="PRESENT">PRESENT (On Site)</option>
            <option value="WFH">WFH (Work From Home)</option>
            <option value="ON_DUTY">ON_DUTY (Field/Client)</option>
          </select>
        </div>
        <div class="form-group" style="margin-bottom: 1rem;">
          <label>Instant (Epoch ms or empty for Now)</label>
          <input type="number" id="pi-ms" placeholder="Default: Server clock now">
        </div>
        <div style="display: flex; gap: 0.5rem;">
          <button class="btn-primary" onclick="submitPunchIn()">Record Punch-In</button>
          <button class="btn-secondary" onclick="simulateRace('in')">Simulate 10x Race Test</button>
        </div>
      </div>

      <!-- Punch Out Card -->
      <div class="card">
        <div class="card-title">Punch-Out Event (POST /attendance/punch-out)</div>
        <div class="form-group" style="margin-bottom: 0.75rem;">
          <label>Employee Code</label>
          <input type="text" id="po-emp" value="EMP0001" placeholder="e.g. EMP0001">
        </div>
        <div class="form-group" style="margin-bottom: 1rem;">
          <label>Instant (Epoch ms or empty for Now)</label>
          <input type="number" id="po-ms" placeholder="Default: Server clock now">
        </div>
        <div style="display: flex; gap: 0.5rem;">
          <button class="btn-primary" onclick="submitPunchOut()">Record Punch-Out</button>
          <button class="btn-secondary" onclick="simulateRace('out')">Simulate 10x Race Test</button>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">Console Output & Verification</div>
      <pre class="code-block" id="kiosk-console">System ready. Click an action above to execute.</pre>
    </div>
  </div>

  <!-- TAB 2: Attendance Ledger -->
  <div id="tab-attendance" class="tab-content">
    <div class="section-meta">
      <div>
        <h2>Attendance Master Ledger</h2>
        <p>Paginated view of attendance logs. Click Regularize on any row to open the audit dialog.</p>
      </div>
      <div>
        <button class="btn-secondary" onclick="loadAttendance()">Refresh</button>
      </div>
    </div>

    <div class="card">
      <div class="form-row">
        <div class="form-group">
          <label>Emp Code</label>
          <input type="text" id="att-filter-emp" placeholder="Filter by EMP...">
        </div>
        <div class="form-group">
          <label>Date From</label>
          <input type="date" id="att-filter-from">
        </div>
        <div class="form-group">
          <label>Date To</label>
          <input type="date" id="att-filter-to">
        </div>
        <div class="form-group">
          <label>Status</label>
          <select id="att-filter-status">
            <option value="">All Statuses</option>
            <option value="PRESENT">PRESENT</option>
            <option value="WFH">WFH</option>
            <option value="ON_DUTY">ON_DUTY</option>
            <option value="ABSENT">ABSENT</option>
            <option value="LEAVE">LEAVE</option>
          </select>
        </div>
        <button class="btn-primary" onclick="loadAttendance(1)">Apply Filters</button>
      </div>

      <div class="table-wrap">
        <table id="att-table">
          <thead>
            <tr>
              <th>Date</th>
              <th>Employee</th>
              <th>Status</th>
              <th>In (IST)</th>
              <th>Out (IST)</th>
              <th>Work Hours</th>
              <th>Late (m)</th>
              <th>OT (m)</th>
              <th>Half Day</th>
              <th>History</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody id="att-tbody">
            <tr><td colspan="11" style="text-align:center; padding: 2rem;">Loading attendance records...</td></tr>
          </tbody>
        </table>
      </div>

      <div style="margin-top: 0.85rem; display: flex; justify-content: space-between; align-items: center; font-size: 0.8rem; color: var(--text-secondary);">
        <span id="att-pagination-info">Showing 0 of 0</span>
        <div style="display: flex; gap: 0.4rem;">
          <button class="btn-secondary" id="att-prev-btn" onclick="prevAttPage()">Previous</button>
          <button class="btn-secondary" id="att-next-btn" onclick="nextAttPage()">Next</button>
        </div>
      </div>
    </div>
  </div>

  <!-- TAB 3: Employees -->
  <div id="tab-employees" class="tab-content">
    <div class="section-meta">
      <div>
        <h2>Workforce Directory</h2>
        <p>Active employee roster, assigned shifts, and enrollment dates.</p>
      </div>
      <div>
        <button class="btn-primary" onclick="openNewEmpModal()">+ Add Employee</button>
      </div>
    </div>

    <div class="card">
      <div class="form-row">
        <div class="form-group">
          <label>Department</label>
          <input type="text" id="emp-filter-dept" placeholder="Filter department...">
        </div>
        <button class="btn-primary" onclick="loadEmployees(1)">Filter</button>
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Code</th>
              <th>Name</th>
              <th>Email</th>
              <th>Department</th>
              <th>Shift Start</th>
              <th>Shift End</th>
              <th>Joined Date</th>
              <th>Created At</th>
            </tr>
          </thead>
          <tbody id="emp-tbody">
            <tr><td colspan="8" style="text-align: center; padding: 2rem;">Loading directory...</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- TAB 4: Analytics Studio -->
  <div id="tab-analytics" class="tab-content">
    <div class="section-meta">
      <div>
        <h2>Executive Analytics Studio</h2>
        <p>Aggregation pipelines: Department Summary, Late Leaderboard, and Daily Gap-Filled Trend.</p>
      </div>
      <div class="form-row" style="margin-bottom:0;">
        <div class="form-group">
          <label>Target Month</label>
          <input type="month" id="analytics-month" value="2026-07">
        </div>
        <button class="btn-primary" onclick="loadAllAnalytics()">Run Analytics</button>
      </div>
    </div>

    <!-- Leaderboard & Dept Summary -->
    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1.25rem; margin-bottom: 1.25rem;">
      <div class="card">
        <div class="card-title">Late-Comers Leaderboard (Competition Rank)</div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Rank</th>
                <th>Employee</th>
                <th>Department</th>
                <th>Late Total</th>
                <th>Late Count</th>
              </tr>
            </thead>
            <tbody id="leaderboard-tbody">
              <tr><td colspan="5" style="text-align: center; padding: 1.5rem;">Click 'Run Analytics'</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="card">
        <div class="card-title">Department Monthly Rollup</div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Department</th>
                <th>Headcount</th>
                <th>Present Days</th>
                <th>Avg Work Hours</th>
                <th>Late (m)</th>
              </tr>
            </thead>
            <tbody id="dept-summary-tbody">
              <tr><td colspan="5" style="text-align: center; padding: 1.5rem;">Click 'Run Analytics'</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- Trend Chart Card -->
    <div class="card">
      <div class="card-title">
        <span>Daily Attendance Trend & 7-Day Moving Average ($densify)</span>
        <div style="display: flex; gap: 0.5rem; align-items: center;">
          <input type="text" id="trend-dept" value="Engineering" style="width: 140px; font-size: 0.75rem;">
          <input type="date" id="trend-from" value="2026-07-01" style="font-size: 0.75rem;">
          <input type="date" id="trend-to" value="2026-07-14" style="font-size: 0.75rem;">
          <button class="btn-secondary" style="font-size: 0.75rem;" onclick="loadTrend()">Load Trend</button>
        </div>
      </div>
      <div id="trend-chart" class="chart-container"></div>
    </div>
  </div>

  <!-- TAB 5: Explain Endpoint -->
  <div id="tab-explain" class="tab-content">
    <div class="section-meta">
      <div>
        <h2>MongoDB Explain & Performance Studio</h2>
        <p>ExecutionStats inspector verifying IXSCAN presence and 0 COLLSCAN operations.</p>
      </div>
    </div>

    <div class="card">
      <div class="form-row">
        <div class="form-group">
          <label>Endpoint</label>
          <select id="explain-endpoint" onchange="updateExplainParams()">
            <option value="attendance_list">attendance_list</option>
            <option value="employee_monthly">employee_monthly</option>
            <option value="department_summary">department_summary</option>
            <option value="late_leaderboard">late_leaderboard</option>
            <option value="department_trend">department_trend</option>
          </select>
        </div>
        <div class="form-group" id="eg-emp" style="display:none;">
          <label>Emp Code</label>
          <input type="text" id="exp-emp-code" value="EMP0001">
        </div>
        <div class="form-group" id="eg-month" style="display:none;">
          <label>Month (YYYY-MM)</label>
          <input type="text" id="exp-month" value="2026-07">
        </div>
        <div class="form-group" id="eg-dept" style="display:none;">
          <label>Department</label>
          <input type="text" id="exp-dept" value="Engineering">
        </div>
        <div class="form-group" id="eg-range" style="display:none;">
          <label>Date Range (from / to)</label>
          <div style="display: flex; gap: 0.25rem;">
            <input type="text" id="exp-from" value="2026-07-01" style="width: 100px;">
            <input type="text" id="exp-to" value="2026-07-14" style="width: 100px;">
          </div>
        </div>
        <button class="btn-primary" onclick="runExplain()">Run Explain Plan</button>
      </div>

      <div style="display: flex; gap: 1rem; margin-bottom: 1rem;" id="explain-metrics-wrap">
        <div class="stat-box" style="flex: 1;">
          <div class="stat-label">Index Scan Verified</div>
          <div class="stat-val" id="exp-stat-ixscan" style="font-size: 1.25rem; color: #10b981;">—</div>
        </div>
        <div class="stat-box" style="flex: 1;">
          <div class="stat-label">Collection Scan Present</div>
          <div class="stat-val" id="exp-stat-collscan" style="font-size: 1.25rem; color: #64748b;">—</div>
        </div>
        <div class="stat-box" style="flex: 1;">
          <div class="stat-label">Execution Time</div>
          <div class="stat-val" id="exp-stat-time" style="font-size: 1.25rem;">—</div>
        </div>
      </div>

      <pre class="code-block" id="explain-output">// Click 'Run Explain Plan' to inspect raw executionStats document.</pre>
    </div>
  </div>
</main>

<!-- Regularize Modal -->
<div class="modal-backdrop" id="regularize-modal">
  <div class="modal-dialog">
    <div class="modal-header">
      <h3 id="reg-title">Regularize Attendance</h3>
      <button class="close-btn" onclick="closeRegularizeModal()">&times;</button>
    </div>
    <div class="form-group" style="margin-bottom: 0.75rem;">
      <label>Status</label>
      <select id="reg-status">
        <option value="PRESENT">PRESENT</option>
        <option value="WFH">WFH</option>
        <option value="ON_DUTY">ON_DUTY</option>
        <option value="ABSENT">ABSENT</option>
        <option value="LEAVE">LEAVE</option>
      </select>
    </div>
    <div class="form-group" style="margin-bottom: 0.75rem;">
      <label>Punch In Instant (Epoch ms)</label>
      <input type="number" id="reg-pi" placeholder="Epoch ms">
    </div>
    <div class="form-group" style="margin-bottom: 0.75rem;">
      <label>Punch Out Instant (Epoch ms)</label>
      <input type="number" id="reg-po" placeholder="Epoch ms">
    </div>
    <div class="form-group" style="margin-bottom: 0.75rem;">
      <label>Reason (min 5 chars)</label>
      <input type="text" id="reg-reason" value="Biometric scanner reader error">
    </div>
    <div class="form-group" style="margin-bottom: 1rem;">
      <label>Regularized By</label>
      <input type="text" id="reg-by" value="hr.admin">
    </div>
    <div style="display: flex; justify-content: flex-end; gap: 0.5rem;">
      <button class="btn-secondary" onclick="closeRegularizeModal()">Cancel</button>
      <button class="btn-primary" onclick="submitRegularization()">Apply Correction</button>
    </div>
  </div>
</div>

<!-- Add Employee Modal -->
<div class="modal-backdrop" id="add-emp-modal">
  <div class="modal-dialog">
    <div class="modal-header">
      <h3>Register New Employee</h3>
      <button class="close-btn" onclick="closeAddEmpModal()">&times;</button>
    </div>
    <div class="form-group" style="margin-bottom: 0.5rem;">
      <label>Employee Code (EMP0001 - EMP999999)</label>
      <input type="text" id="new-emp-code" value="EMP0009">
    </div>
    <div class="form-group" style="margin-bottom: 0.5rem;">
      <label>Full Name</label>
      <input type="text" id="new-emp-name" value="Rohan Patel">
    </div>
    <div class="form-group" style="margin-bottom: 0.5rem;">
      <label>Email</label>
      <input type="email" id="new-emp-email" value="rohan@example.com">
    </div>
    <div class="form-group" style="margin-bottom: 0.5rem;">
      <label>Department</label>
      <input type="text" id="new-emp-dept" value="Engineering">
    </div>
    <div style="display: flex; gap: 0.5rem; margin-bottom: 0.5rem;">
      <div class="form-group" style="flex:1;">
        <label>Shift Start (HH:MM IST)</label>
        <input type="text" id="new-emp-start" value="09:30">
      </div>
      <div class="form-group" style="flex:1;">
        <label>Shift End (HH:MM IST)</label>
        <input type="text" id="new-emp-end" value="18:30">
      </div>
    </div>
    <div class="form-group" style="margin-bottom: 1rem;">
      <label>Joined On (YYYY-MM-DD)</label>
      <input type="date" id="new-emp-joined" value="2026-07-01">
    </div>
    <div style="display: flex; justify-content: flex-end; gap: 0.5rem;">
      <button class="btn-secondary" onclick="closeAddEmpModal()">Cancel</button>
      <button class="btn-primary" onclick="submitCreateEmployee()">Create Employee</button>
    </div>
  </div>
</div>

<script>
// State
let currentAttPage = 1;
const attPageSize = 15;
let currentRegEmp = null;
let currentRegDate = null;

// Clock
setInterval(() => {
  const now = new Date();
  document.getElementById('clock-display').innerText = now.toTimeString().split(' ')[0] + ' IST';
}, 1000);

// Tab Switching
function switchTab(tabId) {
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById(tabId).classList.add('active');
  event.target.classList.add('active');

  if (tabId === 'tab-attendance') loadAttendance(1);
  if (tabId === 'tab-employees') loadEmployees(1);
  if (tabId === 'tab-analytics') loadAllAnalytics();
  if (tabId === 'tab-explain') updateExplainParams();
}

// Health Poll
async function checkHealth() {
  const el = document.getElementById('health-indicator');
  const label = document.getElementById('health-label');
  const t0 = performance.now();
  try {
    const res = await fetch('/health');
    const lat = Math.round(performance.now() - t0);
    if (res.ok) {
      label.innerText = `MongoDB 7.0 Active (${lat}ms)`;
      el.style.background = 'var(--green-bg)';
      el.style.borderColor = 'var(--green-border)';
      el.style.color = 'var(--green-text)';
    } else {
      label.innerText = 'Service Unhealthy';
      el.style.background = 'var(--red-bg)';
      el.style.borderColor = 'var(--red-border)';
      el.style.color = 'var(--red-text)';
    }
  } catch (e) {
    label.innerText = 'Backend Offline';
    el.style.background = 'var(--red-bg)';
  }
}
setInterval(checkHealth, 6000);
checkHealth();

// Kiosk Actions
async function submitPunchIn() {
  const emp = document.getElementById('pi-emp').value.trim();
  const st = document.getElementById('pi-status').value;
  const msVal = document.getElementById('pi-ms').value.trim();
  const body = { emp_code: emp, status: st };
  if (msVal) body.punched_at = parseInt(msVal);

  const consoleEl = document.getElementById('kiosk-console');
  consoleEl.innerText = `POST /attendance/punch-in -> Sending: ${JSON.stringify(body)}...`;
  try {
    const res = await fetch('/attendance/punch-in', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    const json = await res.json();
    consoleEl.innerText = `HTTP ${res.status}\n` + JSON.stringify(json, null, 2);
  } catch (e) {
    consoleEl.innerText = `Error: ${e.message}`;
  }
}

async function submitPunchOut() {
  const emp = document.getElementById('po-emp').value.trim();
  const msVal = document.getElementById('po-ms').value.trim();
  const body = { emp_code: emp };
  if (msVal) body.punched_at = parseInt(msVal);

  const consoleEl = document.getElementById('kiosk-console');
  consoleEl.innerText = `POST /attendance/punch-out -> Sending: ${JSON.stringify(body)}...`;
  try {
    const res = await fetch('/attendance/punch-out', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    const json = await res.json();
    consoleEl.innerText = `HTTP ${res.status}\n` + JSON.stringify(json, null, 2);
  } catch (e) {
    consoleEl.innerText = `Error: ${e.message}`;
  }
}

async function simulateRace(type) {
  const consoleEl = document.getElementById('kiosk-console');
  consoleEl.innerText = `Simulating 10 parallel ${type} requests...`;
  const emp = (type === 'in' ? document.getElementById('pi-emp') : document.getElementById('po-emp')).value.trim();
  const url = type === 'in' ? '/attendance/punch-in' : '/attendance/punch-out';
  const body = { emp_code: emp };

  const promises = Array.from({ length: 10 }, () =>
    fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    })
  );

  const results = await Promise.all(promises);
  const statusCounts = {};
  for (const r of results) {
    statusCounts[r.status] = (statusCounts[r.status] || 0) + 1;
  }
  consoleEl.innerText = `10x Parallel Concurrency Results:\n` + JSON.stringify(statusCounts, null, 2);
}

// Attendance Ledger
function formatEpoch(ms) {
  if (!ms) return '—';
  const d = new Date(ms);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

async function loadAttendance(page = 1) {
  currentAttPage = page;
  const tbody = document.getElementById('att-tbody');
  tbody.innerHTML = '<tr><td colspan="11" style="text-align:center;">Loading...</td></tr>';

  const emp = document.getElementById('att-filter-emp').value.trim();
  const df = document.getElementById('att-filter-from').value;
  const dt = document.getElementById('att-filter-to').value;
  const st = document.getElementById('att-filter-status').value;

  const params = new URLSearchParams({ page, page_size: attPageSize });
  if (emp) params.append('emp_code', emp);
  if (df) params.append('date_from', df);
  if (dt) params.append('date_to', dt);
  if (st) params.append('status', st);

  try {
    const res = await fetch(`/attendance?${params.toString()}`);
    const data = await res.json();
    if (!res.ok) {
      tbody.innerHTML = `<tr><td colspan="11" style="color:red;">Error: ${JSON.stringify(data.detail)}</td></tr>`;
      return;
    }

    if (data.items.length === 0) {
      tbody.innerHTML = '<tr><td colspan="11" style="text-align:center; padding: 2rem;">No matching attendance logs found.</td></tr>';
      document.getElementById('att-pagination-info').innerText = '0 of 0';
      return;
    }

    tbody.innerHTML = data.items.map(r => `
      <tr>
        <td style="font-weight:600;">${r.date}</td>
        <td><code>${r.emp_code}</code></td>
        <td><span class="badge badge-${r.status}">${r.status}</span></td>
        <td>${formatEpoch(r.punch_in)}</td>
        <td>${formatEpoch(r.punch_out)}</td>
        <td>${r.work_hours !== null ? r.work_hours.toFixed(2) + ' h' : '—'}</td>
        <td style="${r.late_minutes > 0 ? 'color:#b45309;font-weight:600;' : ''}">${r.late_minutes}</td>
        <td>${r.overtime_minutes}</td>
        <td>${r.half_day ? '<span style="color:#ef4444;font-weight:600;">Yes</span>' : 'No'}</td>
        <td>${r.history && r.history.length > 0 ? `<span class="badge" style="background:#e0f2fe;color:#0369a1;">${r.history.length} edit(s)</span>` : '0'}</td>
        <td><button class="btn-secondary" style="padding:0.2rem 0.5rem;font-size:0.72rem;" onclick="openRegularizeModal('${r.emp_code}', '${r.date}', '${r.status}', ${r.punch_in}, ${r.punch_out})">Regularize</button></td>
      </tr>
    `).join('');

    const startIdx = (page - 1) * attPageSize + 1;
    const endIdx = Math.min(page * attPageSize, data.total);
    document.getElementById('att-pagination-info').innerText = `Showing ${startIdx}-${endIdx} of ${data.total}`;
    document.getElementById('att-prev-btn').disabled = (page <= 1);
    document.getElementById('att-next-btn').disabled = (endIdx >= data.total);
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="11" style="color:red;">Network error: ${e.message}</td></tr>`;
  }
}

function prevAttPage() { if (currentAttPage > 1) loadAttendance(currentAttPage - 1); }
function nextAttPage() { loadAttendance(currentAttPage + 1); }

// Regularize Dialog
function openRegularizeModal(emp, date, st, pi, po) {
  currentRegEmp = emp;
  currentRegDate = date;
  document.getElementById('reg-title').innerText = `Regularize ${emp} on ${date}`;
  document.getElementById('reg-status').value = st;
  document.getElementById('reg-pi').value = pi || '';
  document.getElementById('reg-po').value = po || '';
  document.getElementById('regularize-modal').classList.add('open');
}

function closeRegularizeModal() {
  document.getElementById('regularize-modal').classList.remove('open');
}

async function submitRegularization() {
  const body = {
    status: document.getElementById('reg-status').value,
    reason: document.getElementById('reg-reason').value,
    regularized_by: document.getElementById('reg-by').value,
  };
  const pi = document.getElementById('reg-pi').value.trim();
  const po = document.getElementById('reg-po').value.trim();
  if (pi) body.punch_in = parseInt(pi);
  if (po) body.punch_out = parseInt(po);

  try {
    const res = await fetch(`/attendance/${currentRegEmp}/${currentRegDate}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    const json = await res.json();
    if (!res.ok) {
      alert(`Error ${res.status}: ${JSON.stringify(json.detail)}`);
      return;
    }
    closeRegularizeModal();
    loadAttendance(currentAttPage);
  } catch (e) {
    alert(`Network Error: ${e.message}`);
  }
}

// Employees
async function loadEmployees(page = 1) {
  const tbody = document.getElementById('emp-tbody');
  tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;">Loading...</td></tr>';
  const dept = document.getElementById('emp-filter-dept').value.trim();
  const params = new URLSearchParams({ page, page_size: 50 });
  if (dept) params.append('department', dept);

  try {
    const res = await fetch(`/employees?${params.toString()}`);
    const data = await res.json();
    if (!res.ok) {
      tbody.innerHTML = `<tr><td colspan="8" style="color:red;">Error: ${JSON.stringify(data.detail)}</td></tr>`;
      return;
    }
    tbody.innerHTML = data.items.map(e => `
      <tr>
        <td><code>${e.emp_code}</code></td>
        <td style="font-weight:600;">${e.name}</td>
        <td>${e.email}</td>
        <td><span class="badge" style="background:#f1f5f9;color:#334155;">${e.department}</span></td>
        <td>${e.shift_start}</td>
        <td>${e.shift_end}</td>
        <td>${e.joined_on}</td>
        <td>${new Date(e.created_at).toLocaleDateString()}</td>
      </tr>
    `).join('');
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="8" style="color:red;">Error: ${e.message}</td></tr>`;
  }
}

function openNewEmpModal() { document.getElementById('add-emp-modal').classList.add('open'); }
function closeAddEmpModal() { document.getElementById('add-emp-modal').classList.remove('open'); }

async function submitCreateEmployee() {
  const body = {
    emp_code: document.getElementById('new-emp-code').value.trim(),
    name: document.getElementById('new-emp-name').value.trim(),
    email: document.getElementById('new-emp-email').value.trim(),
    department: document.getElementById('new-emp-dept').value.trim(),
    shift_start: document.getElementById('new-emp-start').value.trim(),
    shift_end: document.getElementById('new-emp-end').value.trim(),
    joined_on: document.getElementById('new-emp-joined').value.trim(),
  };
  try {
    const res = await fetch('/employees', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    const json = await res.json();
    if (!res.ok) {
      alert(`Error ${res.status}: ${JSON.stringify(json.detail)}`);
      return;
    }
    closeAddEmpModal();
    loadEmployees(1);
  } catch (e) {
    alert(`Network Error: ${e.message}`);
  }
}

// Analytics Studio
async function loadAllAnalytics() {
  const month = document.getElementById('analytics-month').value;
  loadLeaderboard(month);
  loadDeptSummary(month);
  loadTrend();
}

async function loadLeaderboard(month) {
  const tbody = document.getElementById('leaderboard-tbody');
  tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;">Calculating leaderboard...</td></tr>';
  try {
    const res = await fetch(`/analytics/leaderboard/late?month=${month}&limit=10`);
    const data = await res.json();
    if (!res.ok) {
      tbody.innerHTML = `<tr><td colspan="5" style="color:red;">${JSON.stringify(data.detail)}</td></tr>`;
      return;
    }
    if (data.items.length === 0) {
      tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; padding: 1.5rem;">No employees with late arrivals in this month.</td></tr>';
      return;
    }
    tbody.innerHTML = data.items.map(r => `
      <tr>
        <td><span class="rank-badge rank-${r.rank}">${r.rank}</span></td>
        <td><strong>${r.name}</strong> <span style="font-size:0.72rem;color:var(--text-muted);">(${r.emp_code})</span></td>
        <td>${r.department}</td>
        <td style="font-weight:700;color:#b45309;">${r.total_late_minutes} min</td>
        <td>${r.late_count} day(s)</td>
      </tr>
    `).join('');
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="5" style="color:red;">${e.message}</td></tr>`;
  }
}

async function loadDeptSummary(month) {
  const tbody = document.getElementById('dept-summary-tbody');
  tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;">Calculating summary...</td></tr>';
  try {
    const res = await fetch(`/analytics/departments/summary?month=${month}`);
    const data = await res.json();
    if (!res.ok) {
      tbody.innerHTML = `<tr><td colspan="5" style="color:red;">${JSON.stringify(data.detail)}</td></tr>`;
      return;
    }
    tbody.innerHTML = data.items.map(r => `
      <tr>
        <td style="font-weight:600;">${r.department}</td>
        <td>${r.headcount}</td>
        <td>${r.present_days}</td>
        <td>${r.avg_work_hours !== null ? r.avg_work_hours.toFixed(2) + ' h' : '—'}</td>
        <td style="${r.total_late_minutes > 0 ? 'color:#b45309;font-weight:600;' : ''}">${r.total_late_minutes} min</td>
      </tr>
    `).join('');
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="5" style="color:red;">${e.message}</td></tr>`;
  }
}

// Trend Chart SVG
async function loadTrend() {
  const dept = document.getElementById('trend-dept').value.trim();
  const f = document.getElementById('trend-from').value;
  const t = document.getElementById('trend-to').value;
  const container = document.getElementById('trend-chart');
  container.innerHTML = '<div style="text-align:center;padding:3rem;color:var(--text-muted);">Building gap-filled trend pipeline...</div>';

  try {
    const res = await fetch(`/analytics/departments/${dept}/trend?from=${f}&to=${t}`);
    const data = await res.json();
    if (!res.ok) {
      container.innerHTML = `<div style="color:red;padding:1rem;">Error: ${JSON.stringify(data.detail)}</div>`;
      return;
    }
    renderTrendSVG(data.items, container);
  } catch (e) {
    container.innerHTML = `<div style="color:red;padding:1rem;">Error: ${e.message}</div>`;
  }
}

function renderTrendSVG(items, container) {
  if (items.length === 0) {
    container.innerHTML = '<div style="text-align:center;padding:2rem;">No data for range.</div>';
    return;
  }

  const w = container.clientWidth || 900;
  const h = 260;
  const pad = { top: 25, right: 35, bottom: 45, left: 45 };
  const plotW = w - pad.left - pad.right;
  const plotH = h - pad.top - pad.bottom;

  const n = items.length;
  const step = n > 1 ? plotW / (n - 1) : plotW;

  const pointsRate = [];
  const pointsMA = [];

  items.forEach((item, i) => {
    const x = pad.left + i * step;
    if (item.attendance_rate !== null) {
      const yRate = pad.top + plotH * (1 - Math.min(Math.max(item.attendance_rate, 0), 1));
      pointsRate.push({ x, y: yRate, val: item.attendance_rate, date: item.date });
    }
    if (item.moving_avg_7d !== null) {
      const yMA = pad.top + plotH * (1 - Math.min(Math.max(item.moving_avg_7d, 0), 1));
      pointsMA.push({ x, y: yMA, val: item.moving_avg_7d, date: item.date });
    }
  });

  const pathRate = pointsRate.reduce((acc, p, i) => i === 0 ? `M ${p.x} ${p.y}` : `${acc} L ${p.x} ${p.y}`, '');
  const pathMA = pointsMA.reduce((acc, p, i) => i === 0 ? `M ${p.x} ${p.y}` : `${acc} L ${p.x} ${p.y}`, '');

  let svg = `
  <svg width="100%" height="${h}" viewBox="0 0 ${w} ${h}" style="font-family: 'Inter', sans-serif;">
    <!-- Grid Lines -->
    <line x1="${pad.left}" y1="${pad.top}" x2="${w - pad.right}" y2="${pad.top}" stroke="#f1f5f9" stroke-width="1" />
    <line x1="${pad.left}" y1="${pad.top + plotH * 0.5}" x2="${w - pad.right}" y2="${pad.top + plotH * 0.5}" stroke="#f1f5f9" stroke-width="1" />
    <line x1="${pad.left}" y1="${pad.top + plotH}" x2="${w - pad.right}" y2="${pad.top + plotH}" stroke="#cbd5e1" stroke-width="1" />

    <!-- Y Labels -->
    <text x="${pad.left - 8}" y="${pad.top + 4}" font-size="10" fill="#94a3b8" text-anchor="end">100%</text>
    <text x="${pad.left - 8}" y="${pad.top + plotH * 0.5 + 4}" font-size="10" fill="#94a3b8" text-anchor="end">50%</text>
    <text x="${pad.left - 8}" y="${pad.top + plotH + 4}" font-size="10" fill="#94a3b8" text-anchor="end">0%</text>

    <!-- Paths -->
    <path d="${pathRate}" fill="none" stroke="#0284c7" stroke-width="2" stroke-dasharray="3,3" />
    <path d="${pathMA}" fill="none" stroke="#0f172a" stroke-width="3" />

    <!-- Points -->
    ${pointsRate.map(p => `<circle cx="${p.x}" cy="${p.y}" r="3" fill="#0284c7" />`).join('')}
    ${pointsMA.map(p => `<circle cx="${p.x}" cy="${p.y}" r="4" fill="#0f172a" />`).join('')}

    <!-- X Labels -->
    ${items.map((item, i) => {
      const x = pad.left + i * step;
      const showLabel = n <= 14 || i % Math.ceil(n / 10) === 0;
      return showLabel ? `<text x="${x}" y="${h - 15}" font-size="9" fill="#64748b" text-anchor="middle">${item.date.slice(5)}</text>` : '';
    }).join('')}
  </svg>
  <div style="display:flex; justify-content:center; gap: 1.5rem; font-size: 0.75rem; margin-top: 0.5rem;">
    <span style="display:flex; align-items:center; gap:0.4rem;"><span style="width:14px; height:2px; background:#0284c7; border-top: 2px dashed #0284c7;"></span> Daily Rate</span>
    <span style="display:flex; align-items:center; gap:0.4rem;"><span style="width:14px; height:3px; background:#0f172a;"></span> 7-Day Moving Average</span>
  </div>
  `;
  container.innerHTML = svg;
}

// Explain Studio
function updateExplainParams() {
  const ep = document.getElementById('explain-endpoint').value;
  document.getElementById('eg-emp').style.display = (ep === 'employee_monthly') ? 'block' : 'none';
  document.getElementById('eg-month').style.display = (['employee_monthly', 'department_summary', 'late_leaderboard'].includes(ep)) ? 'block' : 'none';
  document.getElementById('eg-dept').style.display = (['department_summary', 'late_leaderboard', 'department_trend'].includes(ep)) ? 'block' : 'none';
  document.getElementById('eg-range').style.display = (ep === 'department_trend') ? 'block' : 'none';
}

async function runExplain() {
  const ep = document.getElementById('explain-endpoint').value;
  const params = new URLSearchParams();

  if (ep === 'employee_monthly') {
    params.append('emp_code', document.getElementById('exp-emp-code').value.trim());
    params.append('month', document.getElementById('exp-month').value.trim());
  } else if (ep === 'department_summary') {
    params.append('month', document.getElementById('exp-month').value.trim());
    const d = document.getElementById('exp-dept').value.trim();
    if (d) params.append('department', d);
  } else if (ep === 'late_leaderboard') {
    params.append('month', document.getElementById('exp-month').value.trim());
    const d = document.getElementById('exp-dept').value.trim();
    if (d) params.append('department', d);
  } else if (ep === 'department_trend') {
    params.append('department', document.getElementById('exp-dept').value.trim());
    params.append('from', document.getElementById('exp-from').value.trim());
    params.append('to', document.getElementById('exp-to').value.trim());
  }

  const outEl = document.getElementById('explain-output');
  outEl.innerText = `Running GET /admin/explain/${ep}?${params.toString()}...`;

  try {
    const res = await fetch(`/admin/explain/${ep}?${params.toString()}`);
    const data = await res.json();
    if (!res.ok) {
      outEl.innerText = `HTTP ${res.status}\n` + JSON.stringify(data, null, 2);
      return;
    }

    const rawStr = JSON.stringify(data.explain);
    const hasIXSCAN = rawStr.includes('"stage": "IXSCAN"') || rawStr.includes('"IXSCAN"');
    const hasCOLLSCAN = rawStr.includes('"stage": "COLLSCAN"') || rawStr.includes('"COLLSCAN"');

    document.getElementById('exp-stat-ixscan').innerText = hasIXSCAN ? 'YES (Index Used)' : 'No (Optimal for small collections)';
    document.getElementById('exp-stat-ixscan').style.color = hasIXSCAN ? '#10b981' : '#64748b';

    document.getElementById('exp-stat-collscan').innerText = hasCOLLSCAN ? 'DETECTED' : '0 (None)';
    document.getElementById('exp-stat-collscan').style.color = hasCOLLSCAN ? '#ef4444' : '#10b981';

    const execStats = data.explain.executionStats || {};
    document.getElementById('exp-stat-time').innerText = (execStats.executionTimeMillis !== undefined)
      ? `${execStats.executionTimeMillis} ms`
      : 'Done';

    outEl.innerText = JSON.stringify(data, null, 2);
  } catch (e) {
    outEl.innerText = `Error: ${e.message}`;
  }
}
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
def dashboard_ui():
    """Serves the Swiss Minimalist Enterprise Console."""
    return HTMLResponse(content=SWISS_UI_HTML, status_code=200)
