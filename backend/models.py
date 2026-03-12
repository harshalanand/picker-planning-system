from pydantic import BaseModel
from typing import Optional, List

class ActualRecord(BaseModel):
    do_no: str
    actual_date: Optional[str] = None
    actual_start: Optional[str] = None
    actual_end: Optional[str] = None
    actual_qty: Optional[int] = 0
    notes: Optional[str] = ""

class StatusUpdateRequest(BaseModel):
    token: str
    do_no: str
    status: str
    cancel_reason: Optional[str] = ""

class BulkStatusRequest(BaseModel):
    token: str
    do_nos: List[str]
    status: str
    cancel_reason: Optional[str] = ""

class ConfigModel(BaseModel):
    start_hr: int = 8
    start_min: int = 0
    lunch_hr: int = 13
    lunch_min: int = 0
    lunch_dur: int = 45
    shift_hrs: float = 9.0
    bgt_picker: int = 3000
    fill_pct: int = 70

class PlanGenerateRequest(BaseModel):
    plan_date: str
    notes: Optional[str] = ""
    config: ConfigModel = ConfigModel()
