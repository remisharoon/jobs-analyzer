const Q = (s, r = document) => r.querySelector(s);
const QA = (s, r = document) => [...r.querySelectorAll(s)];
let DATA = [], VIEW = [];
let sortKey = "created_at_epoch_iso", sortDir = "desc";
let chart;

async function load() {
  const url = window.R2_JSON_URL;
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`Fetch failed: ${res.status}`);
  DATA = await res.json();
  // normalize types
  DATA.forEach(d => {
    d.price = num(d.price);
    d.details_kilometers = num(d.details_kilometers);
    d.details_year = num(d.details_year);
  });
  hydrateFilters();
  applyFilters();
}

function hydrateFilters() {
  const citySel = Q("#city");
  const bodySel = Q("#body");
  const cities = uniq(DATA.map(d => d.city_inferred).filter(Boolean)).sort();
  const bodies = uniq(DATA.map(d => d.details_body_type).filter(Boolean)).sort();
  citySel.innerHTML = `<option value="">All cities</option>` + cities.map(c => `<option>${esc(c)}</option>`).join("");
  bodySel.innerHTML = `<option value="">All bodies</option>` + bodies.map(c => `<option>${esc(c)}</option>`).join("");
}

function applyFilters() {
  const q = Q("#q").value.trim().toLowerCase();
  const city = Q("#city").value;
  const body = Q("#body").value;

  VIEW = DATA.filter(d => {
    if (city && d.city_inferred !== city) return false;
    if (body && d.details_body_type !== body) return false;
    if (q) {
      const blob = [
        d.title_en, d.city_inferred, d.details_body_type
      ].join(" ").toLowerCase();
      if (!blob.includes(q)) return false;
    }
    return true;
  });

  sortAndRender();
  renderStats();
  renderChart();
}

function sortAndRender() {
  const dir = sortDir === "asc" ? 1 : -1;
  VIEW.sort((a,b) => cmp(a[sortKey], b[sortKey]) * dir);
  renderTable();
}

function renderTable() {
  const tbody = Q("#tbl tbody");
  tbody.innerHTML = VIEW.map(d => {
    const link = d.url ? `<a href="${esc(d.url)}" target="_blank">Listing</a>` : "";
    const plink = d.permalink ? ` | <a href="${esc(d.permalink)}" target="_blank">Contact</a>` : "";
    return `<tr>
      <td>${d.id ?? ""}</td>
      <td>${esc(d.title_en ?? "")}</td>
      <td>${fmtPrice(d.price)}</td>
      <td>${esc(d.city_inferred ?? "")}</td>
      <td>${esc(d.details_body_type ?? "")}</td>
      <td>${d.details_year ?? ""}</td>
      <td>${fmtKM(d.details_kilometers)}</td>
      <td>${link}${plink}</td>
    </tr>`;
  }).join("");
}

function renderStats() {
  const s = Q("#stats");
  const count = VIEW.length;
  const avg = safeAvg(VIEW.map(d => d.price));
  const cities = uniq(VIEW.map(d => d.city_inferred).filter(Boolean)).length;
  s.innerHTML = `
    <div><b>Count:</b> ${count}</div>
    <div><b>Avg Price:</b> ${fmtPrice(avg)}</div>
    <div><b>Cities:</b> ${cities}</div>
  `;
}

function renderChart() {
  const ctx = Q("#priceByCity");
  const groups = groupBy(VIEW, d => d.city_inferred || "Unknown");
  const labels = Object.keys(groups);
  const values = labels.map(k => safeAvg(groups[k].map(x => x.price)));
  if (chart) chart.destroy();
  chart = new Chart(ctx, {
    type: "bar",
    data: { labels, datasets: [{ label: "Avg Price", data: values }] },
    options: { responsive: true, maintainAspectRatio: false }
  });
}

function attachEvents() {
  Q("#q").addEventListener("input", applyFilters);
  Q("#city").addEventListener("change", applyFilters);
  Q("#body").addEventListener("change", applyFilters);
  Q("#reset").addEventListener("click", () => {
    Q("#q").value = ""; Q("#city").value = ""; Q("#body").value = "";
    applyFilters();
  });

  // sortable headers
  QA("#tbl thead th[data-key]").forEach(th => {
    th.addEventListener("click", () => {
      const key = th.dataset.key;
      if (sortKey === key) sortDir = sortDir === "asc" ? "desc" : "asc";
      else { sortKey = key; sortDir = "asc"; }
      sortAndRender();
    });
  });
}

function num(v){ const n = Number(v); return Number.isFinite(n) ? n : null; }
function cmp(a,b){ if(a==null && b==null) return 0; if(a==null) return 1; if(b==null) return -1;
  return (a>b)-(a<b); }
function uniq(arr){ return [...new Set(arr)]; }
function esc(s){ return String(s).replace(/[&<>"]/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[m])); }
function fmtPrice(v){ return v==null ? "" : `AED ${v.toLocaleString('en-US')}`; }
function fmtKM(v){ return v==null ? "" : `${v.toLocaleString('en-US')} km`; }
function groupBy(arr, fn){ return arr.reduce((m,x)=>{ const k=fn(x); (m[k]=m[k]||[]).push(x); return m; },{}); }
function safeAvg(arr){ const xs = arr.filter(v => typeof v === 'number' && Number.isFinite(v)); return xs.length? Math.round(xs.reduce((a,b)=>a+b,0)/xs.length) : null; }

attachEvents();
load().catch(err => {
  console.error(err);
  alert("Failed to load data. Check R2 public URL / CORS.");
});
