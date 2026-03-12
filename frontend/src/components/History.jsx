import { useState, useEffect } from "react";
import { api } from "../api/client";
import { LoadingCenter, Alert, SectionHeader, StatusPill, ConfirmModal, KCard } from "./UI";
import { Download, Trash2, RefreshCw } from "lucide-react";

export default function History({ refreshKey }) {
  const [plans, setPlans] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filterDate, setFilterDate] = useState("");
  const [selected, setSelected] = useState(null);
  const [details, setDetails] = useState([]);
  const [detailLoading, setDetailLoading] = useState(false);
  const [confirmCancel, setConfirmCancel] = useState(null);
  const [msg, setMsg] = useState(null);

  async function load() {
    setLoading(true);
    const rows = await api.listPlans(filterDate || undefined);
    setPlans(rows);
    setLoading(false);
  }

  useEffect(() => { load(); }, [refreshKey, filterDate]);

  async function loadDetails(token) {
    if (selected === token) { setSelected(null); setDetails([]); return; }
    setSelected(token); setDetailLoading(true);
    const r = await api.getPlan(token);
    setDetails(r.details || []);
    setDetailLoading(false);
  }

  async function cancelPlan(token) {
    try {
      await api.cancelPlan(token);
      setMsg({ type: "success", text: `Plan ${token} cancelled. All DOs are now free to re-plan.` });
      setConfirmCancel(null);
      setSelected(null); setDetails([]);
      load();
    } catch (e) {
      setMsg({ type: "error", text: e.message });
    }
  }

  // Group by date for multi-run display
  const byDate = plans.reduce((acc, p) => {
    (acc[p.plan_date] = acc[p.plan_date] || []).push(p);
    return acc;
  }, {});

  const statusCount = (rows) => rows.reduce((a, r) => {
    a[r.status || "Planned"] = (a[r.status || "Planned"] || 0) + 1; return a;
  }, {});

  if (loading) return <LoadingCenter text="Loading history…" />;

  return (
    <div>
      <SectionHeader>Plan History</SectionHeader>

      {msg && <Alert type={msg.type}>{msg.text}</Alert>}

      <div style={{ display: "flex", gap: 10, marginBottom: 14, alignItems: "flex-end" }}>
        <div className="field" style={{ margin: 0 }}>
          <label>Filter by Date</label>
          <input type="date" value={filterDate} onChange={e => setFilterDate(e.target.value)}
            style={{ width: 160 }} />
        </div>
        {filterDate && (
          <button className="btn btn-ghost btn-sm" onClick={() => setFilterDate("")}>
            <RefreshCw size={12} /> Clear
          </button>
        )}
        <div style={{ marginLeft: "auto", color: "var(--text3)", fontSize: ".68rem" }}>
          {plans.length} plan{plans.length !== 1 ? "s" : ""}
        </div>
      </div>

      {plans.length === 0 && <Alert type="info">No plans found. Generate a plan first.</Alert>}

      {Object.entries(byDate).sort(([a],[b]) => b.localeCompare(a)).map(([date, dayPlans]) => (
        <div key={date} style={{ marginBottom: 18 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
            <div style={{ fontFamily: "var(--mono)", fontSize: ".72rem", fontWeight: 700, color: "var(--text2)" }}>
              📅 {date}
            </div>
            {dayPlans.length > 1 && (
              <span style={{ fontSize: ".6rem", background: "var(--surface)", padding: "2px 7px", borderRadius: 10, color: "var(--text3)" }}>
                {dayPlans.length} runs · {dayPlans.reduce((s,p) => s + p.total_dos, 0)} DOs · {dayPlans.reduce((s,p) => s + p.total_qty, 0).toLocaleString()} pcs
              </span>
            )}
          </div>

          {dayPlans.map(p => (
            <div key={p.token} className="card" style={{ marginBottom: 6 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
                <div className="token-badge" style={{ cursor: "pointer" }}
                     onClick={() => loadDetails(p.token)}>
                  {p.token}
                </div>
                <span style={{ fontSize: ".6rem", color: "var(--text3)" }}>R{p.run_number}</span>
                <span style={{ fontSize: ".68rem", color: "var(--text2)" }}>{p.total_dos} DOs · {p.total_qty?.toLocaleString()} pcs · {p.pickers_used} pickers</span>
                <span style={{ fontSize: ".65rem", color: "var(--text3)" }}>{p.created_at?.slice(0,16)}</span>
                {p.notes && <span style={{ fontSize: ".62rem", color: "var(--text3)", fontStyle: "italic" }}>{p.notes}</span>}
                <div style={{ marginLeft: "auto", display: "flex", gap: 6 }}>
                  <a href={api.downloadPlan(p.token)} className="btn btn-outline btn-sm" download>
                    <Download size={11} /> Plan
                  </a>
                  <a href={api.downloadActualsTemplate(p.token)} className="btn btn-outline btn-sm" download>
                    <Download size={11} /> Actuals Tpl
                  </a>
                  <button className="btn btn-danger btn-sm"
                    onClick={() => setConfirmCancel(p.token)}>
                    <Trash2 size={11} /> Cancel Plan
                  </button>
                </div>
              </div>

              {/* Expanded detail */}
              {selected === p.token && (
                <div style={{ marginTop: 12, borderTop: "1px solid var(--border)", paddingTop: 10 }}>
                  {detailLoading ? <LoadingCenter text="Loading details…" /> : (
                    <>
                      <div style={{ display: "flex", gap: 8, marginBottom: 8, flexWrap: "wrap" }}>
                        {Object.entries(statusCount(details)).map(([st, cnt]) => (
                          <span key={st} style={{ fontSize: ".62rem" }}>
                            <StatusPill status={st} /> ×{cnt}
                          </span>
                        ))}
                      </div>
                      <div className="tbl-wrap" style={{ maxHeight: 240 }}>
                        <table>
                          <thead>
                            <tr>{["DO No","Floor","P","Qty","Picker","Start","End","Status"].map(h =>
                              <th key={h}>{h}</th>)}</tr>
                          </thead>
                          <tbody>
                            {details.slice(0, 100).map(d => (
                              <tr key={d.do_no}>
                                <td className="primary">{d.do_no}</td>
                                <td>{d.floor}</td>
                                <td>{d.priority}</td>
                                <td>{d.do_qty?.toLocaleString()}</td>
                                <td>{d.picker_no}</td>
                                <td style={{ color: "var(--accent2)" }}>{d.start_time}</td>
                                <td style={{ color: "var(--text3)" }}>{d.end_time}</td>
                                <td><StatusPill status={d.status || "Planned"} /></td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                      {details.length > 100 && (
                        <div style={{ fontSize: ".62rem", color: "var(--text3)", textAlign: "center", marginTop: 6 }}>
                          Showing first 100 of {details.length} DOs. Download Excel for full list.
                        </div>
                      )}
                    </>
                  )}
                </div>
              )}
            </div>
          ))}
        </div>
      ))}

      {confirmCancel && (
        <ConfirmModal
          title="Cancel Plan"
          danger
          message={`Cancel plan ${confirmCancel}? All DOs in this plan will be marked Cancelled and become available for re-planning on any date.`}
          onConfirm={() => cancelPlan(confirmCancel)}
          onCancel={() => setConfirmCancel(null)}
        />
      )}
    </div>
  );
}
