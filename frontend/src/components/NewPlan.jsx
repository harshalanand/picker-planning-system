import { useState, useCallback } from "react";
import { api, downloadFile } from "../api/client";
import { Alert, KCard, SectionHeader, FileDropZone, Spinner, GrpPill, StatusPill } from "./UI";
import { Zap, Download } from "lucide-react";

const HOURS = Array.from({ length: 24 }, (_, i) => String(i).padStart(2, "0"));
const MINS  = Array.from({ length: 60 }, (_, i) => String(i).padStart(2, "0"));

function today() {
  return new Date().toISOString().slice(0, 10);
}

function GanttChart({ details }) {
  const floors = [...new Set(details.map(d => d.floor))].sort();
  const allTimes = details.flatMap(d => [d.start_time, d.end_time]).filter(Boolean);
  function toMin(t) { const [h, m] = t.split(":"); return +h * 60 + +m; }
  const minT = Math.min(...allTimes.map(toMin));
  const maxT = Math.max(...allTimes.map(toMin));
  const range = maxT - minT || 1;

  const COLORS = ["#3B82F6","#10B981","#F59E0B","#EF4444","#8B5CF6","#06B6D4","#EC4899","#84CC16"];

  return (
    <div className="gantt-wrap" style={{ maxHeight: 320, overflowY: "auto" }}>
      {floors.map(fl => {
        const pickers = [...new Set(details.filter(d => d.floor === fl).map(d => d.picker_no))].sort((a,b)=>a-b);
        return (
          <div key={fl} style={{ marginBottom: 8 }}>
            <div style={{ fontSize: ".6rem", color: "var(--accent)", fontWeight: 700, marginBottom: 3 }}>
              FLOOR {fl}
            </div>
            {pickers.slice(0, 20).map(pk => {
              const dos = details.filter(d => d.floor === fl && d.picker_no === pk);
              return (
                <div key={pk} className="gantt-row">
                  <div className="gantt-label">P{pk}</div>
                  <div className="gantt-track">
                    {dos.map((d, i) => {
                      const s = toMin(d.start_time), e = toMin(d.end_time);
                      const left = ((s - minT) / range) * 100;
                      const width = Math.max(0.4, ((e - s) / range) * 100);
                      const ci = (d.priority - 1) % COLORS.length;
                      return (
                        <div key={d.do_no} className="gantt-block"
                          style={{ left: `${left}%`, width: `${width}%`, background: COLORS[ci] }}
                          title={`${d.do_no} | P${d.priority} | ${d.start_time}–${d.end_time} | ${d.do_qty}pcs`}>
                          {width > 4 ? d.do_no : ""}
                        </div>
                      );
                    })}
                  </div>
                </div>
              );
            })}
          </div>
        );
      })}
    </div>
  );
}

export default function NewPlan({ config, onPlanSaved }) {
  const [file, setFile] = useState(null);
  const [planDate, setPlanDate] = useState(today());
  const [notes, setNotes] = useState("");
  const [preview, setPreview] = useState(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const [innerTab, setInnerTab] = useState("dos");

  async function loadPreview(f, d) {
    if (!f || !d) return;
    setPreviewLoading(true);
    setPreview(null); setError("");
    try {
      const p = await api.previewDemand(f, {
        plan_date: d,
        bgt_picker: config.bgt_picker, fill_pct: config.fill_pct,
        start_hr: config.start_hr, start_min: config.start_min,
        shift_hrs: config.shift_hrs, lunch_dur: config.lunch_dur,
      });
      setPreview(p);
    } catch (e) { setError(e.message); }
    setPreviewLoading(false);
  }

  function handleFile(f) {
    setFile(f); setResult(null);
    loadPreview(f, planDate);
  }

  function handleDateChange(d) {
    setPlanDate(d);
    if (file) loadPreview(file, d);
  }

  async function generate() {
    if (!file || !planDate) return;
    setGenerating(true); setError(""); setResult(null);
    try {
      const r = await api.generatePlan(file, {
        plan_date: planDate, notes,
        start_hr: config.start_hr, start_min: config.start_min,
        lunch_hr: config.lunch_hr, lunch_min: config.lunch_min,
        lunch_dur: config.lunch_dur, shift_hrs: config.shift_hrs,
        bgt_picker: config.bgt_picker, fill_pct: config.fill_pct,
      });
      if (r.error === "no_dos") {
        setError(`No DOs available for planning. ${r.locked_count} DOs are locked (Planned/Done/Delayed). Cancel existing plans first to re-use those DOs.`);
      } else {
        setResult(r); onPlanSaved();
        // Load full plan details for display
        const full = await api.getPlan(r.token);
        setResult(prev => ({ ...prev, ...full }));
      }
    } catch (e) { setError(e.message); }
    setGenerating(false);
  }

  const details = result?.details || [];
  const plan = result?.plan || result;

  return (
    <div>
      {/* ── File + Date ── */}
      <SectionHeader>Upload & Configure</SectionHeader>
      <div className="row">
        <div style={{ flex: 2 }}>
          <FileDropZone file={file} onFile={handleFile} />
        </div>
        <div style={{ flex: 1 }}>
          <div className="field">
            <label>Plan Date</label>
            <input type="date" value={planDate} onChange={e => handleDateChange(e.target.value)} />
          </div>
          <div className="field">
            <label>Notes (optional)</label>
            <input type="text" value={notes} onChange={e => setNotes(e.target.value)}
              placeholder="e.g. Morning batch" />
          </div>
          <button className="btn btn-primary btn-full" onClick={generate}
            disabled={!file || !planDate || generating}>
            {generating ? <><Spinner size={14} /> Generating…</> : <><Zap size={14} /> Generate Plan</>}
          </button>
        </div>
      </div>

      {error && <Alert type="error">{error}</Alert>}

      {/* ── Demand Preview ── */}
      {previewLoading && <div style={{ color: "var(--text3)", fontSize: ".7rem", margin: "8px 0" }}>
        <Spinner size={13} style={{ display: "inline-block", marginRight: 6 }} /> Analysing file…
      </div>}

      {preview && (
        <>
          <SectionHeader>Demand Preview — {planDate}</SectionHeader>
          <div className="kgrid kgrid-4" style={{ marginBottom: 10 }}>
            <KCard label="Total DOs" value={preview.total_dos} color="blue" />
            <KCard label="Available" value={preview.available_dos} color="green"
              sub={preview.locked_dos > 0 ? `${preview.locked_dos} locked` : "all free"} />
            <KCard label="Pickers" value={preview.machines?.total} />
            <KCard label="G1/G2/G3" value={`${preview.machines?.G1}/${preview.machines?.G2}/${preview.machines?.G3}`} />
          </div>
          {preview.locked_dos > 0 && (
            <Alert type="lock">
              {preview.locked_dos} DOs locked (Planned/Done/Delayed on other dates). 
              Only <strong> {preview.available_dos}</strong> available for new plan. 
              Cancel existing plans from History or Status tab to unlock DOs.
            </Alert>
          )}
          {preview.demand && (
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 8 }}>
              {Object.entries(preview.demand).filter(([k]) => k.startsWith("f")).map(([k, v]) => (
                <div key={k} className="card" style={{ padding: "8px 12px", minWidth: 90, flex: "0 0 auto" }}>
                  <div style={{ fontSize: ".58rem", color: "var(--text3)", fontWeight: 700 }}>FLOOR {k.slice(1)}</div>
                  <div style={{ fontFamily: "var(--mono)", fontSize: ".8rem", color: "var(--accent2)" }}>
                    {typeof v === "object" ? `${v.dos || 0} DOs / ${(v.qty||0).toLocaleString()} pcs` : v}
                  </div>
                </div>
              ))}
            </div>
          )}
        </>
      )}

      {/* ── Plan Result ── */}
      {result && details.length > 0 && (
        <>
          <SectionHeader>Plan Generated</SectionHeader>
          <div className="card" style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
            <div className="token-badge">🎫 {plan.token || result.token}</div>
            <div style={{ display: "flex", gap: 6 }}>
              <a href={api.downloadPlan(plan.token || result.token)}
                className="btn btn-outline btn-sm" download>
                <Download size={12} /> Excel
              </a>
            </div>
          </div>
          <div className="kgrid kgrid-4" style={{ marginBottom: 12 }}>
            <KCard label="DOs Planned" value={plan.total_dos || result.total_dos} color="green" />
            <KCard label="Total Qty" value={(plan.total_qty || result.total_qty || 0).toLocaleString()} color="blue" />
            <KCard label="Pickers Used" value={plan.pickers_used || result.pickers_used} />
            <KCard label="Avg Util" value={`${plan.avg_util || result.avg_util || 0}%`} color="yellow" />
          </div>
          {(result.skipped > 0) && (
            <Alert type="warn">{result.skipped} DOs could not be allocated (capacity exhausted).</Alert>
          )}

          {/* Inner tabs */}
          <div className="inner-tabs">
            {["dos", "gantt", "unallocated"].map(t => (
              <button key={t} className={`inner-tab ${innerTab === t ? "active" : ""}`}
                onClick={() => setInnerTab(t)}>
                {t === "dos" ? "DO List" : t === "gantt" ? "Gantt" : "Unallocated"}
              </button>
            ))}
          </div>

          {innerTab === "dos" && (
            <div className="tbl-wrap">
              <table>
                <thead>
                  <tr>
                    {["DO No","Floor","SEC","P","Qty","Picker","Grp","BGT","Start","End","Util%"].map(h =>
                      <th key={h}>{h}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {details.map(d => (
                    <tr key={d.do_no}>
                      <td className="primary">{d.do_no}</td>
                      <td>{d.floor}</td>
                      <td>{d.sec}</td>
                      <td>{d.priority}</td>
                      <td>{d.do_qty?.toLocaleString()}</td>
                      <td>{d.picker_no}</td>
                      <td><GrpPill grp={d.grp} /></td>
                      <td>{d.bgt_machine?.toLocaleString()}</td>
                      <td style={{ color: "var(--accent2)" }}>{d.start_time}</td>
                      <td style={{ color: "var(--text3)" }}>{d.end_time}</td>
                      <td style={{ color: d.util_pct > 90 ? "var(--red)" : "var(--green)" }}>
                        {d.util_pct?.toFixed(1)}%
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {innerTab === "gantt" && <GanttChart details={details} />}

          {innerTab === "unallocated" && (
            result.skipped_list?.length > 0 ? (
              <div>
                <Alert type="warn">These DOs could not be allocated. Increase Fill % or add pickers.</Alert>
                <div className="tbl-wrap">
                  <table>
                    <thead><tr><th>DO No</th><th>Reason</th></tr></thead>
                    <tbody>
                      {result.skipped_list.map(([dn, reason]) => (
                        <tr key={dn}><td className="primary">{dn}</td><td>{reason}</td></tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ) : <Alert type="success">All DOs were successfully allocated!</Alert>
          )}
        </>
      )}
    </div>
  );
}
