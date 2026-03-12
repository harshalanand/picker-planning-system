import { useState, useEffect } from "react";
import NewPlan from "./components/NewPlan";
import History from "./components/History";
import ActualTimes from "./components/ActualTimes";
import Analytics from "./components/Analytics";
import CancelStatus from "./components/CancelStatus";
import Settings from "./components/Settings";
import { SettingsIcon, BarChart2, Clock, History as HistoryIcon, PlusCircle, XCircle } from "lucide-react";

const TABS = [
  { id: "plan",     label: "New Plan",     icon: PlusCircle },
  { id: "history",  label: "History",      icon: HistoryIcon },
  { id: "actual",   label: "Actual Times", icon: Clock },
  { id: "analytics",label: "Analytics",    icon: BarChart2 },
  { id: "cancel",   label: "Cancel/Status",icon: XCircle },
];

export const DEFAULT_CONFIG = {
  start_hr: 8, start_min: 0,
  lunch_hr: 13, lunch_min: 0,
  lunch_dur: 45, shift_hrs: 9.0,
  bgt_picker: 3000, fill_pct: 70,
};

export default function App() {
  const [tab, setTab] = useState("plan");
  const [config, setConfig] = useState(DEFAULT_CONFIG);
  const [showSettings, setShowSettings] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);

  const refresh = () => setRefreshKey(k => k+1);

  return (
    <div className="app-root">
      {/* ── Top Bar ── */}
      <header className="topbar">
        <div className="topbar-brand">
          <span className="brand-icon">🏭</span>
          <div>
            <div className="brand-title">Picker Planning System</div>
            <div className="brand-sub">G1/G2/G3 · Multi-run · SQLite · Status tracking</div>
          </div>
        </div>
        <nav className="topbar-tabs">
          {TABS.map(t => {
            const Icon = t.icon;
            return (
              <button
                key={t.id}
                className={`tab-btn ${tab === t.id ? "active" : ""}`}
                onClick={() => setTab(t.id)}
              >
                <Icon size={13} />
                <span>{t.label}</span>
              </button>
            );
          })}
        </nav>
        <button className="settings-btn" onClick={() => setShowSettings(true)}
                title="Business Rules">
          <SettingsIcon size={16} />
        </button>
      </header>

      {/* ── Settings Drawer ── */}
      {showSettings && (
        <Settings config={config} setConfig={setConfig} onClose={() => setShowSettings(false)} />
      )}

      {/* ── Content ── */}
      <main className="main-content">
        {tab === "plan"      && <NewPlan      config={config} onPlanSaved={refresh} />}
        {tab === "history"   && <History      refreshKey={refreshKey} />}
        {tab === "actual"    && <ActualTimes  refreshKey={refreshKey} />}
        {tab === "analytics" && <Analytics    refreshKey={refreshKey} />}
        {tab === "cancel"    && <CancelStatus refreshKey={refreshKey} onUpdated={refresh} />}
      </main>
    </div>
  );
}
