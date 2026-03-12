"""
Picker Planning System — FastAPI Backend
Run: uvicorn main:app --reload --port 8000
"""
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List
import io, json
from datetime import datetime

from database import init_db, migrate_db, get_db, get_connection
from allocator import allocate_plan, analyse_demand, auto_group, m2t, t2m
from models import (
    PlanGenerateRequest, ActualRecord, StatusUpdateRequest,
    BulkStatusRequest, ConfigModel
)
import excel_export as xl

app = FastAPI(title="Picker Planning API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup():
    init_db()
    migrate_db()


# ─── Config ──────────────────────────────────────────────────────────────────
@app.get("/api/config")
def get_config():
    return {
        "start_hr": 8, "start_min": 0,
        "lunch_hr": 13, "lunch_min": 0,
        "lunch_dur": 45, "shift_hrs": 9.0,
        "bgt_picker": 3000, "fill_pct": 70
    }


# ─── Plans ───────────────────────────────────────────────────────────────────
@app.get("/api/plans")
def list_plans(plan_date: Optional[str] = None, limit: int = 200):
    conn = get_connection()
    if plan_date:
        rows = conn.execute(
            "SELECT * FROM plans WHERE plan_date=? ORDER BY run_number DESC",
            (plan_date,)).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM plans ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/plans/{token}")
def get_plan(token: str):
    conn = get_connection()
    plan = conn.execute("SELECT * FROM plans WHERE token=?", (token,)).fetchone()
    if not plan:
        conn.close()
        raise HTTPException(404, "Plan not found")
    details = conn.execute("SELECT * FROM plan_details WHERE token=? ORDER BY floor,picker_no,start_time",
                           (token,)).fetchall()
    conn.close()
    return {"plan": dict(plan), "details": [dict(d) for d in details]}


@app.delete("/api/plans/{token}")
def cancel_plan(token: str):
    conn = get_connection()
    plan = conn.execute("SELECT token FROM plans WHERE token=?", (token,)).fetchone()
    if not plan:
        conn.close()
        raise HTTPException(404, "Plan not found")
    now = datetime.now().isoformat()
    conn.execute("""UPDATE plan_details SET status='Cancelled',
                    cancel_reason='Plan cancelled', cancelled_at=?
                    WHERE token=?""", (now, token))
    conn.execute("DELETE FROM plans WHERE token=?", (token,))
    conn.commit(); conn.close()
    return {"ok": True, "message": f"Plan {token} cancelled. DOs are free to re-plan."}


@app.post("/api/plans/generate")
async def generate_plan(
    file: UploadFile = File(...),
    plan_date: str = Query(...),
    notes: str = Query(""),
    start_hr: int = Query(8), start_min: int = Query(0),
    lunch_hr: int = Query(13), lunch_min: int = Query(0),
    lunch_dur: int = Query(45), shift_hrs: float = Query(9.0),
    bgt_picker: int = Query(3000), fill_pct: int = Query(70)
):
    import pandas as pd
    content = await file.read()
    try:
        xl_data = pd.read_excel(io.BytesIO(content), sheet_name=None)
    except Exception as e:
        raise HTTPException(400, f"Cannot read Excel: {e}")

    if "DO" not in xl_data or "MACHINE" not in xl_data:
        raise HTTPException(400, f"Need sheets: DO, MACHINE. Found: {list(xl_data.keys())}")

    do_df = xl_data["DO"].copy()
    machine_df = xl_data["MACHINE"].copy()
    if "PRIORITY" not in do_df.columns:
        do_df["PRIORITY"] = 1

    S = t2m(start_hr, start_min)
    LS = t2m(lunch_hr, lunch_min)
    LD = lunch_dur; LE = LS + LD
    TM = int(shift_hrs * 60); EFF = max(1, TM - LD); WH_END = S + TM

    cfg = dict(
        start_min=S, lunch_s=LS, lunch_e=LE, wh_end=WH_END,
        bgt=bgt_picker, eff_min=EFF, fill_pct=fill_pct,
        start_str=f"{start_hr:02d}:{start_min:02d}",
        wh_end_str=f"{int(WH_END)//60:02d}:{int(WH_END)%60:02d}",
        lunch_str=f"{lunch_hr:02d}:{lunch_min:02d}–{int(LE)//60:02d}:{int(LE)%60:02d}"
    )

    conn = get_connection()
    # Get run number
    row = conn.execute(
        "SELECT COALESCE(MAX(run_number),0)+1 FROM plans WHERE plan_date=?",
        (plan_date,)).fetchone()
    run_number = row[0]

    # Get locked DOs (Planned/Done/Delayed across ALL dates)
    locked = {r[0] for r in conn.execute("""
        SELECT DISTINCT do_no FROM plan_details
        WHERE COALESCE(status,'Planned') NOT IN ('Cancelled','Deleted')
    """).fetchall()}

    # Get today's already-planned DOs (excluding cancelled)
    today_planned = {r[0] for r in conn.execute("""
        SELECT DISTINCT do_no FROM plan_details
        WHERE plan_date=? AND COALESCE(status,'Planned') NOT IN ('Cancelled','Deleted')
    """, (plan_date,)).fetchall()}

    all_excluded = locked | today_planned

    # Load picker state for multi-run
    prev_state = {}
    ps_rows = conn.execute(
        "SELECT machine_no, floor, cap_used, avail_min FROM picker_day_state WHERE plan_date=?",
        (plan_date,)).fetchall()
    for r in ps_rows:
        prev_state[(str(r[0]), int(r[1]))] = {"cap_used": r[2], "avail_min": r[3]}
    conn.close()

    do_rem = do_df[~do_df["DO_NO"].astype(str).isin(all_excluded)].copy()
    if do_rem.empty:
        return {"error": "no_dos", "message": "No DOs available. All are locked or already planned.",
                "locked_count": len(all_excluded)}

    demand = analyse_demand(do_rem, cfg)
    plan_df, mdf, fp, skipped = allocate_plan(do_rem, machine_df, cfg, demand,
                                               prev_state=prev_state, skip_dos=all_excluded)

    token = f"PKP-{plan_date.replace('-','')}-R{run_number:02d}"

    # Save to DB
    conn2 = get_connection()
    avg_util = float(plan_df.groupby("machine_no")["util_pct"].max().mean()) if not plan_df.empty else 0
    conn2.execute("""
        INSERT INTO plans(token,plan_date,run_number,created_at,notes,config_json,
            demand_json,total_dos,total_qty,pickers_used,avg_util,skipped_dos)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""", (
        token, plan_date, run_number, datetime.now().isoformat(), notes,
        json.dumps(cfg), json.dumps(demand, default=str),
        len(plan_df), int(plan_df["do_qty"].sum()) if not plan_df.empty else 0,
        plan_df["machine_no"].nunique() if not plan_df.empty else 0,
        round(avg_util, 2), len(skipped)
    ))

    for _, r in plan_df.iterrows():
        conn2.execute("""
            INSERT INTO plan_details(token,plan_date,run_number,priority,do_no,sto_no,
                st_cd,st_nm,floor,sec,do_qty,picker_no,machine_no,scanner_name,grp,
                bgt_machine,start_time,end_time,duration_min,pcs_per_min,cap_used,
                util_pct,remaining,over_wh,avail_min,status)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            token, plan_date, run_number,
            int(r.get("priority", 1)), str(r["do_no"]),
            str(r.get("sto_no", "")), str(r.get("st_cd", "")), str(r.get("st_nm", "")),
            int(r["floor"]), str(r.get("sec", "")), int(r["do_qty"]),
            int(r["picker_no"]), str(r["machine_no"]), str(r.get("scanner_name", "")),
            str(r.get("grp", "G1")), int(r.get("bgt_machine", bgt_picker)),
            str(r["start_time"]), str(r["end_time"]),
            float(r.get("duration_min", 0)), float(r.get("pcs_per_min", 0)),
            int(r.get("cap_used", 0)), float(r.get("util_pct", 0)),
            int(r.get("remaining", 0)), int(r.get("over_wh", 0)),
            float(r.get("avail_min", 0)), "Planned"
        ))

    # Save picker state
    for _, r in plan_df.iterrows():
        conn2.execute("""
            INSERT INTO picker_day_state(plan_date,machine_no,floor,cap_used,avail_min,last_token)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(plan_date,machine_no,floor) DO UPDATE SET
                cap_used=MAX(excluded.cap_used,cap_used),
                avail_min=MAX(excluded.avail_min,avail_min),
                last_token=excluded.last_token""", (
            plan_date, str(r["machine_no"]), int(r["floor"]),
            int(r.get("cap_used", 0)), float(r.get("avail_min", 0)), token
        ))

    conn2.commit(); conn2.close()

    return {
        "token": token,
        "run_number": run_number,
        "plan_date": plan_date,
        "total_dos": len(plan_df),
        "total_qty": int(plan_df["do_qty"].sum()) if not plan_df.empty else 0,
        "pickers_used": plan_df["machine_no"].nunique() if not plan_df.empty else 0,
        "avg_util": round(avg_util, 2),
        "skipped": len(skipped),
        "skipped_list": skipped[:50],
        "locked_count": len(all_excluded),
        "demand": demand,
    }


# ─── Demand preview (before generating plan) ──────────────────────────────────
@app.post("/api/plans/preview")
async def preview_demand(
    file: UploadFile = File(...),
    plan_date: str = Query(...),
    bgt_picker: int = Query(3000),
    fill_pct: int = Query(70),
    start_hr: int = Query(8), start_min: int = Query(0),
    shift_hrs: float = Query(9.0), lunch_dur: int = Query(45)
):
    import pandas as pd
    content = await file.read()
    xl_data = pd.read_excel(io.BytesIO(content), sheet_name=None)
    if "DO" not in xl_data or "MACHINE" not in xl_data:
        raise HTTPException(400, "Need DO + MACHINE sheets")

    do_df = xl_data["DO"].copy()
    machine_df = xl_data["MACHINE"].copy()
    if "PRIORITY" not in do_df.columns: do_df["PRIORITY"] = 1

    S = t2m(start_hr, start_min); LD = lunch_dur
    TM = int(shift_hrs * 60); EFF = max(1, TM - LD)
    cfg = dict(start_min=S, wh_end=S+TM, bgt=bgt_picker, eff_min=EFF, fill_pct=fill_pct)

    # Locked DOs
    conn = get_connection()
    locked = {r[0] for r in conn.execute("""
        SELECT DISTINCT do_no FROM plan_details
        WHERE COALESCE(status,'Planned') NOT IN ('Cancelled','Deleted')
    """).fetchall()}
    conn.close()

    do_rem = do_df[~do_df["DO_NO"].astype(str).isin(locked)].copy()
    demand = analyse_demand(do_rem, cfg)

    mt = machine_df.copy()
    mt.columns = [c.strip().upper().replace(" ","_") for c in mt.columns]
    mt = mt.drop_duplicates(subset=["MACHINE_NO"], keep="first")
    mt["BGT_MACHINE"] = mt["BGT_PICKER"].fillna(bgt_picker).astype(int) if "BGT_PICKER" in mt.columns else bgt_picker
    mt["GROUP"], mt["GROUP_TIER"] = zip(*mt["BGT_MACHINE"].apply(auto_group))

    return {
        "total_dos": len(do_df),
        "available_dos": len(do_rem),
        "locked_dos": len(locked),
        "machines": {
            "total": len(mt),
            "G1": int((mt["GROUP"]=="G1").sum()),
            "G2": int((mt["GROUP"]=="G2").sum()),
            "G3": int((mt["GROUP"]=="G3").sum()),
        },
        "demand": demand,
    }


# ─── Actual Times ─────────────────────────────────────────────────────────────
@app.get("/api/actuals/{token}")
def get_actuals(token: str):
    conn = get_connection()
    rows = conn.execute("SELECT * FROM actual_times WHERE token=?", (token,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.post("/api/actuals/{token}")
def save_actuals(token: str, records: List[ActualRecord]):
    conn = get_connection()
    plan = conn.execute("SELECT plan_date FROM plans WHERE token=?", (token,)).fetchone()
    if not plan:
        conn.close()
        raise HTTPException(404, "Token not found")
    plan_date = plan[0]
    saved = 0
    for r in records:
        if not r.actual_start or not r.actual_end:
            continue
        ad = r.actual_date or plan_date
        status = "Delayed" if (ad and plan_date and ad > plan_date) else "Done"
        conn.execute("""
            INSERT INTO actual_times(token,do_no,plan_date,actual_date,actual_start,
                actual_end,actual_qty,notes,entered_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(token,do_no) DO UPDATE SET
                actual_start=excluded.actual_start, actual_end=excluded.actual_end,
                actual_date=excluded.actual_date, notes=excluded.notes,
                entered_at=excluded.entered_at""", (
            token, str(r.do_no), plan_date, ad,
            r.actual_start, r.actual_end, r.actual_qty or 0, r.notes or "",
            datetime.now().isoformat()
        ))
        conn.execute("""UPDATE plan_details SET status=?, cancelled_at=?
                        WHERE token=? AND do_no=?""",
                     (status, datetime.now().isoformat(), token, str(r.do_no)))
        saved += 1
    conn.commit(); conn.close()
    return {"saved": saved}


# ─── Status Updates ───────────────────────────────────────────────────────────
@app.post("/api/status/update")
def update_status(req: StatusUpdateRequest):
    conn = get_connection()
    now = datetime.now().isoformat()
    conn.execute("""UPDATE plan_details SET status=?, cancel_reason=?,
                    cancelled_at=CASE WHEN ?='Cancelled' THEN ? ELSE cancelled_at END
                    WHERE token=? AND do_no=?""",
                 (req.status, req.cancel_reason or "", req.status, now, req.token, str(req.do_no)))
    conn.commit(); conn.close()
    return {"ok": True}


@app.post("/api/status/bulk")
def bulk_status_update(req: BulkStatusRequest):
    conn = get_connection()
    now = datetime.now().isoformat()
    updated = 0
    for do_no in req.do_nos:
        conn.execute("""UPDATE plan_details SET status=?, cancel_reason=?,
                        cancelled_at=CASE WHEN ?='Cancelled' THEN ? ELSE cancelled_at END
                        WHERE token=? AND do_no=?""",
                     (req.status, req.cancel_reason or "", req.status, now, req.token, str(do_no)))
        updated += 1
    conn.commit(); conn.close()
    return {"updated": updated}


# ─── Analytics ────────────────────────────────────────────────────────────────
@app.get("/api/analytics/{token}")
def get_analytics(token: str):
    import pandas as pd
    conn = get_connection()
    plan = conn.execute("SELECT * FROM plans WHERE token=?", (token,)).fetchone()
    if not plan:
        conn.close()
        raise HTTPException(404, "Token not found")
    details = pd.DataFrame([dict(r) for r in conn.execute(
        "SELECT * FROM plan_details WHERE token=?", (token,)).fetchall()])
    actuals_rows = conn.execute(
        "SELECT * FROM actual_times WHERE token=?", (token,)).fetchall()
    actuals = pd.DataFrame([dict(r) for r in actuals_rows]) if actuals_rows else pd.DataFrame()
    conn.close()

    if details.empty:
        return {"error": "no_data"}

    details["do_no"] = details["do_no"].astype(str)
    if not actuals.empty:
        actuals["do_no"] = actuals["do_no"].astype(str)
        merged = details.merge(
            actuals[["do_no","actual_start","actual_end","actual_date","actual_qty","notes"]],
            on="do_no", how="left")
    else:
        merged = details.copy()
        for c in ["actual_start","actual_end","actual_date","actual_qty","notes"]:
            merged[c] = None

    def dur_min(s, e):
        try:
            sh,sm = str(s).split(":"); eh,em = str(e).split(":")
            return int(eh)*60+int(em) - (int(sh)*60+int(sm))
        except: return None

    plan_date = dict(plan).get("plan_date","")
    def derive_status(row):
        if row.get("status") == "Cancelled": return "Cancelled"
        if row.get("actual_start") and str(row.get("actual_start","")) not in ("","nan","None"):
            ad = str(row.get("actual_date",""))
            if ad and plan_date and ad > plan_date: return "Delayed"
            return "Done"
        if row.get("status") in ("Done","Delayed","Not Picked"): return row["status"]
        return "Not Picked" if row.get("status") == "Planned" else row.get("status","Planned")

    merged["derived_status"] = merged.apply(derive_status, axis=1)
    merged["plan_dur"] = merged.apply(lambda r: dur_min(r.get("start_time",""), r.get("end_time","")), axis=1)
    merged["act_dur"]  = merged.apply(lambda r: dur_min(r.get("actual_start",""), r.get("actual_end","")), axis=1)
    merged["var_min"]  = merged.apply(
        lambda r: (r["act_dur"] - r["plan_dur"]) if r["act_dur"] and r["plan_dur"] else None, axis=1)

    sc = merged["derived_status"].value_counts().to_dict()
    has_act = merged[merged["actual_start"].notna() & (merged["actual_start"].astype(str)!="")]
    avg_var = float(has_act["var_min"].mean()) if not has_act.empty and has_act["var_min"].notna().any() else 0

    # Floor breakdown
    floor_data = []
    for fl in sorted(merged["floor"].unique()):
        fm = merged[merged["floor"]==fl]
        fsc = fm["derived_status"].value_counts().to_dict()
        fm_has = fm[fm["actual_start"].notna() & (fm["actual_start"].astype(str)!="")]
        floor_data.append({
            "floor": int(fl),
            "total": len(fm),
            "qty": int(fm["do_qty"].sum()),
            "done": fsc.get("Done",0),
            "delayed": fsc.get("Delayed",0),
            "not_picked": fsc.get("Not Picked",0),
            "cancelled": fsc.get("Cancelled",0),
            "completion_pct": round((fsc.get("Done",0)+fsc.get("Delayed",0))/len(fm)*100,1),
            "avg_var": round(float(fm_has["var_min"].mean()),1) if not fm_has.empty and fm_has["var_min"].notna().any() else None,
            "pickers": int(fm["picker_no"].nunique()),
        })

    # Picker performance
    picker_data = []
    gc_p = [c for c in ["picker_no","machine_no","grp","floor"] if c in merged.columns]
    if not has_act.empty:
        for keys, grp in has_act.groupby(gc_p):
            kd = dict(zip(gc_p, keys if isinstance(keys, tuple) else [keys]))
            picker_data.append({
                **kd,
                "dos": len(grp),
                "plan_qty": int(grp["do_qty"].sum()),
                "plan_min": round(float(grp["plan_dur"].sum()),1) if grp["plan_dur"].notna().any() else 0,
                "actual_min": round(float(grp["act_dur"].sum()),1) if grp["act_dur"].notna().any() else 0,
                "avg_var": round(float(grp["var_min"].mean()),1) if grp["var_min"].notna().any() else 0,
                "done": int((grp["derived_status"]=="Done").sum()),
                "delayed": int((grp["derived_status"]=="Delayed").sum()),
                "slow": int((grp["var_min"]>5).sum()) if grp["var_min"].notna().any() else 0,
                "efficiency_pct": round(
                    float(grp["plan_dur"].sum() / grp["act_dur"].sum() * 100), 1)
                    if grp["act_dur"].notna().any() and float(grp["act_dur"].sum())>0 else 0,
            })

    total = len(merged)
    return {
        "plan": dict(plan),
        "summary": {
            "total": total, "done": sc.get("Done",0), "delayed": sc.get("Delayed",0),
            "not_picked": sc.get("Not Picked",0), "cancelled": sc.get("Cancelled",0),
            "planned": sc.get("Planned",0),
            "completion_pct": round((sc.get("Done",0)+sc.get("Delayed",0))/total*100,1) if total>0 else 0,
            "avg_var": round(avg_var,1),
            "on_time": int((has_act["var_min"].abs()<=5).sum()) if not has_act.empty and has_act["var_min"].notna().any() else 0,
            "slow": int((has_act["var_min"]>5).sum()) if not has_act.empty and has_act["var_min"].notna().any() else 0,
            "total_plan_qty": int(merged["do_qty"].sum()),
            "done_qty": int(merged[merged["derived_status"].isin(["Done","Delayed"])]["do_qty"].sum()),
        },
        "floor_data": floor_data,
        "picker_data": picker_data,
        "rows": merged.where(merged.notna(), None).to_dict(orient="records"),
    }


# ─── Templates ────────────────────────────────────────────────────────────────
@app.get("/api/templates/{token}/actuals")
def download_actuals_template(token: str):
    import pandas as pd
    conn = get_connection()
    plan = conn.execute("SELECT * FROM plans WHERE token=?", (token,)).fetchone()
    if not plan: conn.close(); raise HTTPException(404, "Token not found")
    details = pd.DataFrame([dict(r) for r in conn.execute(
        "SELECT * FROM plan_details WHERE token=? ORDER BY floor,picker_no,start_time",
        (token,)).fetchall()])
    conn.close()
    buf = xl.make_actuals_template(details, dict(plan))
    return StreamingResponse(io.BytesIO(buf),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="Actuals_Template_{token}.xlsx"'})


@app.get("/api/templates/{token}/status")
def download_status_template(token: str):
    import pandas as pd
    conn = get_connection()
    plan = conn.execute("SELECT * FROM plans WHERE token=?", (token,)).fetchone()
    if not plan: conn.close(); raise HTTPException(404, "Token not found")
    details = pd.DataFrame([dict(r) for r in conn.execute(
        "SELECT * FROM plan_details WHERE token=? ORDER BY floor,picker_no,start_time",
        (token,)).fetchall()])
    conn.close()
    buf = xl.make_status_template(details, dict(plan))
    return StreamingResponse(io.BytesIO(buf),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="Status_Template_{token}.xlsx"'})


@app.get("/api/templates/{token}/plan")
def download_plan_excel(token: str):
    import pandas as pd
    conn = get_connection()
    plan = conn.execute("SELECT * FROM plans WHERE token=?", (token,)).fetchone()
    if not plan: conn.close(); raise HTTPException(404, "Token not found")
    details = pd.DataFrame([dict(r) for r in conn.execute(
        "SELECT * FROM plan_details WHERE token=? ORDER BY floor,picker_no,start_time",
        (token,)).fetchall()])
    cfg = json.loads(dict(plan).get("config_json","{}"))
    conn.close()
    buf = xl.make_plan_excel(details, cfg, dict(plan))
    return StreamingResponse(io.BytesIO(buf),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="Picker_Plan_{token}.xlsx"'})


# ─── Bulk Upload ─────────────────────────────────────────────────────────────
@app.post("/api/upload/actuals/{token}")
async def upload_actuals_bulk(token: str, file: UploadFile = File(...)):
    import pandas as pd
    conn = get_connection()
    plan = conn.execute("SELECT plan_date FROM plans WHERE token=?", (token,)).fetchone()
    if not plan: conn.close(); raise HTTPException(404, "Token not found")
    plan_date = plan[0]; conn.close()

    content = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(content), sheet_name="ACTUALS")
        df.columns = [c.strip().upper() for c in df.columns]
    except Exception as e:
        raise HTTPException(400, f"Cannot read file: {e}")

    filled = df[df["ACTUAL_START"].notna() & (df["ACTUAL_START"].astype(str).str.strip()!="")]
    records = []
    for _,r in filled.iterrows():
        def fmt(v):
            s = str(v).strip()
            if ":" in s:
                p = s.split(":"); return f"{int(p[0]):02d}:{int(p[1]):02d}"
            return s
        records.append(ActualRecord(
            do_no=str(r["DO_NO"]),
            actual_date=str(r.get("ACTUAL_DATE",plan_date) or plan_date),
            actual_start=fmt(r["ACTUAL_START"]),
            actual_end=fmt(r["ACTUAL_END"]) if "ACTUAL_END" in r and str(r["ACTUAL_END"]).strip() else "",
            actual_qty=int(r["DO_QTY"]) if "DO_QTY" in r and r["DO_QTY"] else 0,
            notes=str(r.get("NOTES","") or ""),
        ))
    result = save_actuals(token, records)
    return {**result, "total_rows": len(filled)}


@app.post("/api/upload/status/{token}")
async def upload_status_bulk(token: str, file: UploadFile = File(...)):
    import pandas as pd
    conn = get_connection()
    if not conn.execute("SELECT 1 FROM plans WHERE token=?", (token,)).fetchone():
        conn.close(); raise HTTPException(404, "Token not found")
    conn.close()
    content = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(content), sheet_name="STATUS_UPDATE")
        df.columns = [c.strip().upper() for c in df.columns]
    except Exception as e:
        raise HTTPException(400, f"Cannot read file: {e}")
    VALID = ["Done","Cancelled","Not Picked","Delayed","Planned"]
    filled = df[df["NEW_STATUS"].notna() & (df["NEW_STATUS"].astype(str).str.strip()!="")]
    do_nos = [str(r["DO_NO"]) for _,r in filled.iterrows()]
    statuses = [str(r["NEW_STATUS"]).strip() for _,r in filled.iterrows()]
    reasons = [str(r.get("CANCEL_REASON","") or "") for _,r in filled.iterrows()]
    req = BulkStatusRequest(token=token, do_nos=do_nos, status=statuses[0] if statuses else "Planned",
                            cancel_reason=reasons[0] if reasons else "")
    # Actually do per-row since statuses may vary
    conn = get_connection()
    now = datetime.now().isoformat()
    updated = 0
    for do_no, status, reason in zip(do_nos, statuses, reasons):
        if status in VALID:
            if reason in ("nan","None"): reason=""
            conn.execute("""UPDATE plan_details SET status=?, cancel_reason=?,
                cancelled_at=CASE WHEN ?='Cancelled' THEN ? ELSE cancelled_at END
                WHERE token=? AND do_no=?""",
                (status, reason, status, now, token, do_no))
            updated += 1
    conn.commit(); conn.close()
    return {"updated": updated, "total_rows": len(filled)}
