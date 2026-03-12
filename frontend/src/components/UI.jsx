// ─── Shared UI Primitives ────────────────────────────────────────────────────

export function Spinner({ size = 18 }) {
  return <div className="spinner" style={{ width: size, height: size }} />;
}

export function LoadingCenter({ text = "Loading…" }) {
  return (
    <div className="loading-center">
      <Spinner size={24} />
      <span>{text}</span>
    </div>
  );
}

export function Alert({ type = "info", children }) {
  const icons = { info: "ℹ️", warn: "⚠️", success: "✅", error: "❌", lock: "🔒" };
  return (
    <div className={`alert alert-${type}`}>
      <span>{icons[type]}</span>
      <span>{children}</span>
    </div>
  );
}

export function KCard({ label, value, sub, color }) {
  return (
    <div className={`kcard${color ? " " + color : ""}`}>
      <div className="klbl">{label}</div>
      <div className="kval">{value ?? "—"}</div>
      {sub && <div className="ksub">{sub}</div>}
    </div>
  );
}

export function StatusPill({ status }) {
  const map = {
    Planned: "pill-planned", Done: "pill-done", Delayed: "pill-delayed",
    "Not Picked": "pill-not", Cancelled: "pill-cancelled",
  };
  return <span className={`pill ${map[status] || "pill-planned"}`}>{status}</span>;
}

export function GrpPill({ grp }) {
  const map = { G1: "pill-g1", G2: "pill-g2", G3: "pill-g3" };
  return <span className={`pill ${map[grp] || "pill-g1"}`}>{grp}</span>;
}

export function SectionHeader({ children }) {
  return <div className="sec-hdr">{children}</div>;
}

export function Modal({ title, onClose, children }) {
  return (
    <div className="overlay" onClick={e => e.target === e.currentTarget && onClose()}>
      <div className="modal">
        <div className="modal-header">
          <div className="modal-title">{title}</div>
          <button className="modal-close" onClick={onClose}>✕</button>
        </div>
        {children}
      </div>
    </div>
  );
}

export function FileDropZone({ onFile, file, accept = ".xlsx,.xls" }) {
  const [dragging, setDragging] = useState(false);
  const inputRef = useRef();

  function handle(f) {
    if (f && (f.name.endsWith(".xlsx") || f.name.endsWith(".xls"))) onFile(f);
  }

  return (
    <div
      className={`file-drop ${dragging ? "dragging" : ""}`}
      onClick={() => inputRef.current?.click()}
      onDragOver={e => { e.preventDefault(); setDragging(true); }}
      onDragLeave={() => setDragging(false)}
      onDrop={e => { e.preventDefault(); setDragging(false); handle(e.dataTransfer.files[0]); }}
    >
      <div className="fd-icon">📂</div>
      {file ? (
        <div className="fd-file">✓ {file.name}</div>
      ) : (
        <>
          <div className="fd-title">Drop Excel file here</div>
          <div className="fd-sub">or click to browse · .xlsx required</div>
        </>
      )}
      <input ref={inputRef} type="file" accept={accept} style={{ display: "none" }}
        onChange={e => handle(e.target.files[0])} />
    </div>
  );
}

export function TimePicker({ label, value, onChange, disabled }) {
  // value: "HH:MM" or ""
  const [h, m] = value ? value.split(":") : ["", ""];
  const hours = Array.from({ length: 24 }, (_, i) => String(i).padStart(2, "0"));
  const mins  = Array.from({ length: 12 }, (_, i) => String(i * 5).padStart(2, "0"));

  function update(nh, nm) {
    if (nh && nm !== undefined) onChange(`${nh}:${nm}`);
    else onChange("");
  }

  return (
    <div className="field">
      {label && <label>{label}</label>}
      <div className="time-picker">
        <select value={h || ""} disabled={disabled}
          onChange={e => update(e.target.value, m || "00")}
          style={{ width: 62 }}>
          <option value="">--</option>
          {hours.map(v => <option key={v}>{v}</option>)}
        </select>
        <span className="time-sep">:</span>
        <select value={m || ""} disabled={disabled}
          onChange={e => update(h || "08", e.target.value)}
          style={{ width: 62 }}>
          <option value="">--</option>
          {mins.map(v => <option key={v}>{v}</option>)}
        </select>
      </div>
    </div>
  );
}

export function TimePickerFree({ label, value, onChange, disabled }) {
  // 1-minute step variant for precise entry
  const [h, m] = value ? value.split(":") : ["", ""];
  const hours = Array.from({ length: 24 }, (_, i) => String(i).padStart(2, "0"));
  const mins  = Array.from({ length: 60 }, (_, i) => String(i).padStart(2, "0"));

  function update(nh, nm) {
    if (nh && nm !== undefined) onChange(`${nh}:${nm}`);
    else onChange("");
  }

  return (
    <div className="field">
      {label && <label>{label}</label>}
      <div className="time-picker">
        <select value={h || ""} disabled={disabled}
          onChange={e => update(e.target.value, m || "00")} style={{ width: 62 }}>
          <option value="">--</option>
          {hours.map(v => <option key={v}>{v}</option>)}
        </select>
        <span className="time-sep">:</span>
        <select value={m || ""} disabled={disabled}
          onChange={e => update(h || "08", e.target.value)} style={{ width: 62 }}>
          <option value="">--</option>
          {mins.map(v => <option key={v}>{v}</option>)}
        </select>
      </div>
    </div>
  );
}

export function ProgressBar({ pct, color }) {
  return (
    <div className="progress-bar">
      <div className={`fill ${color || ""}`} style={{ width: `${Math.min(100, pct || 0)}%` }} />
    </div>
  );
}

export function ConfirmModal({ title, message, onConfirm, onCancel, danger }) {
  return (
    <Modal title={title} onClose={onCancel}>
      <p style={{ color: "var(--text2)", fontSize: ".75rem", marginBottom: 20 }}>{message}</p>
      <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
        <button className="btn btn-outline btn-sm" onClick={onCancel}>Cancel</button>
        <button className={`btn ${danger ? "btn-danger" : "btn-primary"} btn-sm`}
                onClick={onConfirm}>Confirm</button>
      </div>
    </Modal>
  );
}

// ─── React imports needed by components above ────────────────────────────────
import { useState, useRef } from "react";
