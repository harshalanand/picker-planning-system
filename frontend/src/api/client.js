const BASE = import.meta.env.VITE_API_URL || "http://localhost:8000";

async function req(path, opts = {}) {
  const res = await fetch(`${BASE}${path}`, opts);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || err.message || `HTTP ${res.status}`);
  }
  return res.json();
}

export const api = {
  // Plans
  listPlans: (plan_date) =>
    req(`/api/plans${plan_date ? `?plan_date=${plan_date}` : ""}`),
  getPlan: (token) => req(`/api/plans/${token}`),
  cancelPlan: (token) => req(`/api/plans/${token}`, { method: "DELETE" }),

  generatePlan: (file, params) => {
    const fd = new FormData();
    fd.append("file", file);
    const qs = new URLSearchParams(params).toString();
    return req(`/api/plans/generate?${qs}`, { method: "POST", body: fd });
  },

  previewDemand: (file, params) => {
    const fd = new FormData();
    fd.append("file", file);
    const qs = new URLSearchParams(params).toString();
    return req(`/api/plans/preview?${qs}`, { method: "POST", body: fd });
  },

  // Actuals
  getActuals: (token) => req(`/api/actuals/${token}`),
  saveActuals: (token, records) =>
    req(`/api/actuals/${token}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(records),
    }),

  // Status
  updateStatus: (body) =>
    req("/api/status/update", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
  bulkStatus: (body) =>
    req("/api/status/bulk", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),

  // Analytics
  getAnalytics: (token) => req(`/api/analytics/${token}`),

  // Downloads (return URL strings for anchor clicks)
  downloadPlan: (token) => `${BASE}/api/templates/${token}/plan`,
  downloadActualsTemplate: (token) => `${BASE}/api/templates/${token}/actuals`,
  downloadStatusTemplate: (token) => `${BASE}/api/templates/${token}/status`,

  // Bulk uploads
  uploadActuals: (token, file) => {
    const fd = new FormData(); fd.append("file", file);
    return req(`/api/upload/actuals/${token}`, { method: "POST", body: fd });
  },
  uploadStatus: (token, file) => {
    const fd = new FormData(); fd.append("file", file);
    return req(`/api/upload/status/${token}`, { method: "POST", body: fd });
  },
};

export function downloadFile(url, filename) {
  const a = document.createElement("a");
  a.href = url; a.download = filename; a.click();
}
