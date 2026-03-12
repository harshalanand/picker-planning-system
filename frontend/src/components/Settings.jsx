import { X } from "lucide-react";

const FIELD = ({ label, children }) => (
  <div className="field"><label>{label}</label>{children}</div>
);

export default function Settings({ config, setConfig, onClose }) {
  const C = config;
  const set = (k, v) => setConfig(p => ({ ...p, [k]: v }));

  return (
    <>
      <div className="settings-overlay" onClick={onClose} />
      <div className="settings-drawer">
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 20 }}>
          <div style={{ fontSize: ".85rem", fontWeight: 700 }}>⚙️ Business Rules</div>
          <button onClick={onClose} style={{ background: "none", border: "none", color: "var(--text2)", cursor: "pointer" }}>
            <X size={18} />
          </button>
        </div>

        <div className="sec-hdr">Shift Timing</div>
        <div className="row">
          <FIELD label="Start Hour">
            <input type="number" min={0} max={23} value={C.start_hr}
              onChange={e => set("start_hr", +e.target.value)} />
          </FIELD>
          <FIELD label="Start Min">
            <input type="number" min={0} max={59} value={C.start_min}
              onChange={e => set("start_min", +e.target.value)} />
          </FIELD>
        </div>
        <div className="row">
          <FIELD label="Shift Hours">
            <input type="number" min={1} max={24} step={0.5} value={C.shift_hrs}
              onChange={e => set("shift_hrs", +e.target.value)} />
          </FIELD>
          <FIELD label="Lunch Duration (min)">
            <input type="number" min={0} max={120} value={C.lunch_dur}
              onChange={e => set("lunch_dur", +e.target.value)} />
          </FIELD>
        </div>
        <div className="row">
          <FIELD label="Lunch Hour">
            <input type="number" min={0} max={23} value={C.lunch_hr}
              onChange={e => set("lunch_hr", +e.target.value)} />
          </FIELD>
          <FIELD label="Lunch Min">
            <input type="number" min={0} max={59} value={C.lunch_min}
              onChange={e => set("lunch_min", +e.target.value)} />
          </FIELD>
        </div>

        <div className="sec-hdr" style={{ marginTop: 18 }}>Machine Config</div>
        <FIELD label="Default BGT Picker (pcs/shift)">
          <input type="number" min={500} max={9999} value={C.bgt_picker}
            onChange={e => set("bgt_picker", +e.target.value)} />
        </FIELD>
        <FIELD label="Fill % (capacity target)">
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <input type="range" min={50} max={100} value={C.fill_pct}
              onChange={e => set("fill_pct", +e.target.value)}
              style={{ flex: 1 }} />
            <span style={{ fontFamily: "var(--mono)", fontSize: ".75rem", color: "var(--accent)", minWidth: 34 }}>{C.fill_pct}%</span>
          </div>
        </FIELD>

        <div className="alert alert-info" style={{ marginTop: 16, fontSize: ".65rem" }}>
          <span>ℹ️</span>
          <span>
            Effective min = Shift×60 − Lunch<br />
            PCS/min = BGT ÷ Effective min<br />
            G1≥3000 · G2≥2000 · G3&lt;2000
          </span>
        </div>

        <button className="btn btn-primary btn-full" style={{ marginTop: 16 }} onClick={onClose}>
          ✓ Apply Settings
        </button>
      </div>
    </>
  );
}
