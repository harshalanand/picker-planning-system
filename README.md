# Picker Planning System v9

Warehouse DO allocation and picker scheduling system — **FastAPI backend + React frontend**.

## Quick Start

```bash
pip install -r requirements.txt
uvicorn backend:app --host 0.0.0.0 --port 8000 --reload
# Open: http://localhost:8000
```

## Structure

```
picker_app/
├── backend.py          # FastAPI backend — all allocation logic + REST API
├── requirements.txt
└── static/
    └── index.html      # React SPA frontend
```

> `picker_planning.db` is auto-created on first run. Auto-migrates from v8.

## Features

- **DO Global Lock** — A DO planned on any date is locked until explicitly cancelled
- **Multi-run per day** — Run multiple allocation passes; each picks up remaining capacity
- **Cancel = Unlock** — Cancelling from History or Cancel/Status tab frees DOs immediately
- **Actual Times** — Blank by default; only explicitly filled rows are saved
- **Bulk Excel Templates** — Actuals + Status update templates for mass operations
- **G1/G2/G3 Groups** — Auto-grouped by BGT_PICKER; priority-ordered greedy allocation
- **Analytics** — Completion %, floor/picker/status breakdown

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/excel/parse` | Parse DO + Machine Excel |
| POST | `/api/plans/generate` | Run allocation → save plan |
| GET | `/api/plans` | List all plans |
| GET | `/api/plans/{token}` | Plan details + actuals |
| DELETE | `/api/plans/{token}` | Cancel plan (unlocks all DOs) |
| GET | `/api/plans/{token}/excel` | Download plan Excel |
| GET | `/api/plans/{token}/actuals-template` | Actuals fill template |
| GET | `/api/plans/{token}/status-template` | Status update template |
| POST | `/api/actuals/{token}` | Save actual times |
| POST | `/api/actuals/{token}/bulk-upload` | Bulk upload actuals Excel |
| POST | `/api/status/{token}` | Update DO statuses |
| POST | `/api/status/{token}/bulk-upload` | Bulk status Excel upload |
| GET | `/api/analytics/{token}` | Analytics data |
