const $ = (q, root = document) => root.querySelector(q);
const $$ = (q, root = document) => Array.from(root.querySelectorAll(q));

const API_BASE = "/api";

function fmtJSON(obj) {
  return JSON.stringify(obj, null, 2);
}

function escapeHTML(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function toast(text) {
  let box = $("#toast");
  if (!box) {
    box = document.createElement("div");
    box.id = "toast";
    box.style.position = "fixed";
    box.style.left = "50%";
    box.style.bottom = "20px";
    box.style.transform = "translateX(-50%)";
    box.style.background = "#111827";
    box.style.color = "#fff";
    box.style.padding = "10px 14px";
    box.style.borderRadius = "12px";
    box.style.boxShadow = "0 10px 26px rgba(0,0,0,.25)";
    box.style.fontWeight = "700";
    box.style.zIndex = "9999";
    box.style.opacity = "0";
    box.style.transition = "opacity .15s ease";
    document.body.appendChild(box);
  }
  box.textContent = text;
  box.style.opacity = "1";
  clearTimeout(window.__toastTimer);
  window.__toastTimer = setTimeout(() => (box.style.opacity = "0"), 1800);
}

function openModal(id) {
  const back = $(id);
  if (!back) return;
  back.classList.add("open");
}

function closeModal(id) {
  const back = $(id);
  if (!back) return;
  back.classList.remove("open");
}

function setActiveNav() {
  const page = document.body.dataset.page;
  $$(".nav a").forEach((a) => {
    if (a.dataset.nav === page) a.classList.add("active");
  });
}

async function apiFetch(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, options);
  const contentType = response.headers.get("content-type") || "";

  if (!response.ok) {
    let message = `HTTP ${response.status}`;
    try {
      if (contentType.includes("application/json")) {
        const payload = await response.json();
        message = payload.detail || payload.message || fmtJSON(payload);
      } else {
        message = (await response.text()) || message;
      }
    } catch (_) {}
    throw new Error(message);
  }

  if (response.status === 204) return null;
  if (contentType.includes("application/json")) return response.json();
  return response.text();
}

function parsePeriodInput(s) {
  const m = String(s || "").match(/(\d{4}-\d{2}-\d{2})/g);
  if (!m || m.length === 0) return null;
  if (m.length === 1) return { from: m[0], to: m[0] };
  return { from: m[0], to: m[1] };
}

function toIsoDateStart(date) {
  return `${date}T00:00:00Z`;
}

function toIsoDateEnd(date) {
  return `${date}T23:59:59Z`;
}

function normalizeLog(raw) {
  const parsed = raw.parsed || {};
  const timestamp = raw.timestamp ? new Date(raw.timestamp) : null;
  const isValidTs = timestamp && !Number.isNaN(timestamp.getTime());

  const yyyy = isValidTs ? timestamp.getUTCFullYear() : "";
  const mm = isValidTs ? String(timestamp.getUTCMonth() + 1).padStart(2, "0") : "";
  const dd = isValidTs ? String(timestamp.getUTCDate()).padStart(2, "0") : "";
  const hh = isValidTs ? String(timestamp.getUTCHours()).padStart(2, "0") : "";
  const mi = isValidTs ? String(timestamp.getUTCMinutes()).padStart(2, "0") : "";
  const ss = isValidTs ? String(timestamp.getUTCSeconds()).padStart(2, "0") : "";

  const ip = parsed.ip || parsed.remote_addr || parsed.client || "";
  const method = parsed.method || parsed.request_method || "";
  const endpoint = parsed.uri || parsed.request_path || "";
  const status = parsed.status ?? null;
  const bytes = parsed.body_bytes_sent ?? parsed.bytes ?? 0;
  const ua = parsed.user_agent || parsed.http_user_agent || "";
  const ref = parsed.referer || parsed.http_referer || "";
  const message = parsed.message || "";

  return {
    id: raw.id || raw._id,
    mongoId: raw._id || raw.id,
    type: raw.log_type,
    ts: isValidTs ? `${hh}:${mi}:${ss}` : "",
    date: isValidTs ? `${yyyy}-${mm}-${dd}` : "",
    status,
    ip,
    method,
    endpoint,
    bytes,
    ua,
    ref,
    message,
    raw: raw.raw || "",
    parse_error: Boolean(raw.parse_error),
    normalized: raw.normalized || null,
    parsed,
    source: raw.source || {},
    timestamp: raw.timestamp || null,
    original: raw,
  };
}

async function fetchLogsFromApi(filters = {}) {
  const params = new URLSearchParams();

  if (filters.type && filters.type !== "all") params.set("type", filters.type);
  if (filters.search) params.set("search", filters.search);
  if (filters.status != null && filters.status !== "") params.set("status", String(filters.status));
  if (filters.method) params.set("method", filters.method);
  if (filters.from_date) params.set("from_date", filters.from_date);
  if (filters.to_date) params.set("to_date", filters.to_date);
  params.set("limit", String(filters.limit ?? 200));
  params.set("offset", String(filters.offset ?? 0));

  const result = await apiFetch(`/logs?${params.toString()}`);
  return {
    total: result.total || 0,
    items: Array.isArray(result.items) ? result.items.map(normalizeLog) : [],
  };
}

async function fetchLogDetails(logId) {
  const result = await apiFetch(`/logs/${encodeURIComponent(logId)}`);
  return normalizeLog(result);
}

async function fetchRawLog(logId) {
  return apiFetch(`/logs/${encodeURIComponent(logId)}/raw`);
}

async function importLogs(file, type, mode) {
  const formData = new FormData();
  formData.append("file", file);
  formData.append("type", type);
  formData.append("mode", mode);

  return apiFetch("/import", {
    method: "POST",
    body: formData,
  });
}

async function exportLogsFromApi(filters = {}) {
  const params = new URLSearchParams();
  if (filters.type && filters.type !== "all") params.set("type", filters.type);
  if (filters.limit != null) params.set("limit", String(filters.limit));
  if (filters.offset != null) params.set("offset", String(filters.offset));

  return apiFetch(`/export?${params.toString()}`);
}

function downloadJson(filename, payload) {
  const blob = new Blob([fmtJSON(payload)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function seedDemoRunsOnce() {
  if (localStorage.getItem("apacheLogs.seededRuns")) return;

  const runs = [
    { id: "r1", created: "10:05", createdDate: "2026-02-24", method: "endpoint+status", filters: "type=access | period", status: "finished", clusters: 124, logsProcessed: 12480, updated: "10:06", comment: "" },
    { id: "r2", created: "09:40", createdDate: "2026-02-24", method: "error_template", filters: "type=error", status: "finished", clusters: 18, logsProcessed: 640, updated: "09:41", comment: "" },
    { id: "r3", created: "вчера", createdDate: "2026-02-23", method: "endpoint", filters: "access | errors-only", status: "finished", clusters: 77, logsProcessed: 9050, updated: "вчера", comment: "" },
    { id: "r4", created: "вчера", createdDate: "2026-02-23", method: "status", filters: "access", status: "failed", clusters: 0, logsProcessed: 0, updated: "вчера", comment: "timeout while embedding" },
  ];

  const clustersByRun = {
    r1: [
      { id: "c1", key: "GET /api/user/<ID>/profile#200", cnt: 420, hint: "endpoint+status" },
      { id: "c2", key: "GET /api/item/<ID>#404", cnt: 95, hint: "endpoint+status" },
      { id: "c3", key: "POST /api/login#500", cnt: 18, hint: "endpoint+status" },
      { id: "c4", key: "GET /health#200", cnt: 510, hint: "endpoint+status" },
    ],
    r2: [
      { id: "c5", key: "File does not exist: favicon.ico", cnt: 128, hint: "error_template" },
      { id: "c6", key: "Upstream timed out", cnt: 34, hint: "error_template" },
    ],
    r3: [
      { id: "c7", key: "GET /api/search#200", cnt: 300, hint: "endpoint" },
      { id: "c8", key: "GET /api/search#500", cnt: 22, hint: "endpoint" },
    ],
    r4: [],
  };

  localStorage.setItem("apacheLogs.data.runs", JSON.stringify(runs));
  localStorage.setItem("apacheLogs.data.clustersByRun", JSON.stringify(clustersByRun));
  localStorage.setItem("apacheLogs.seededRuns", "1");
}

function loadJSON(key, fallback) {
  try {
    const raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : fallback;
  } catch (_) {
    return fallback;
  }
}

function saveJSON(key, value) {
  localStorage.setItem(key, JSON.stringify(value));
}

function getImportModalControls() {
  const modal = $("#importModal");
  if (!modal) return null;

  const fileInput = $("#importFile", modal);
  const typeSelect = $("#importType", modal);
  const modeSelect = $("#importMode", modal);

  if (fileInput) {
    fileInput.disabled = false;
    fileInput.removeAttribute("disabled");
    fileInput.multiple = false;
  }

  const importButton = $("#doImport");
  if (importButton) importButton.textContent = "Импортировать";

  return { modal, fileInput, typeSelect, modeSelect };
}

function bindGlobal() {
  $$("#openImport, #openImport2").forEach((btn) =>
    btn?.addEventListener("click", () => {
      getImportModalControls();
      openModal("#importModal");
    })
  );
  $$("#openExport, #openExport2").forEach((btn) =>
    btn?.addEventListener("click", () => openModal("#exportModal"))
  );

  $$("#closeImport, #cancelImport").forEach((btn) =>
    btn?.addEventListener("click", () => closeModal("#importModal"))
  );
  $$("#closeExport, #cancelExport").forEach((btn) =>
    btn?.addEventListener("click", () => closeModal("#exportModal"))
  );

  ["#importModal", "#exportModal", "#rawModal"].forEach((id) => {
    const back = $(id);
    if (!back) return;
    back.addEventListener("click", (e) => {
      if (e.target === back) closeModal(id);
    });
  });

  $("#doImport")?.addEventListener("click", async () => {
    const controls = getImportModalControls();
    const file = controls?.fileInput?.files?.[0];
    const type = controls?.typeSelect?.value || "access";
    const mode = controls?.modeSelect?.value || "append";

    if (!file) {
      toast("Выберите файл для импорта");
      return;
    }

    const btn = $("#doImport");
    const prevText = btn?.textContent || "Импорт";
    if (btn) {
      btn.disabled = true;
      btn.textContent = "Импорт...";
    }

    try {
      const result = await importLogs(file, type, mode);
      closeModal("#importModal");
      if (controls?.fileInput) controls.fileInput.value = "";
      toast(`Импорт завершён: ${result.inserted}/${result.total}, ошибок: ${result.errors}`);
      if (document.body.dataset.page === "logs" && window.__refreshLogsPage) {
        await window.__refreshLogsPage();
      }
    } catch (error) {
      toast(`Ошибка импорта: ${error.message}`);
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.textContent = prevText;
      }
    }
  });

  $("#doExport")?.addEventListener("click", async () => {
    const btn = $("#doExport");
    const prevText = btn?.textContent || "Экспорт";
    if (btn) {
      btn.disabled = true;
      btn.textContent = "Экспорт...";
    }

    try {
      if (document.body.dataset.page === "logs" && window.__exportCurrentLogs) {
        await window.__exportCurrentLogs();
      } else {
        const payload = await exportLogsFromApi({ limit: 1000, offset: 0 });
        downloadJson("apache_logs_export.json", payload);
        toast("Экспорт логов сформирован");
      }
      closeModal("#exportModal");
    } catch (error) {
      toast(`Ошибка экспорта: ${error.message}`);
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.textContent = prevText;
      }
    }
  });
}

function initLogsPage() {
  const params = new URLSearchParams(location.search);
  const initialCluster = params.get("cluster");
  const pageSize = 50;

  let activeFilters = {
    period: "",
    type: "all",
    result: "all",
    statusGroup: "all",
    ip: "",
    cluster: initialCluster ? decodeURIComponent(initialCluster) : null,
    q: "",
  };

  let currentLogs = [];
  let currentFilteredLogs = [];
  let currentPage = 1;
  let requestId = 0;

  const chipsEl = $("#chips");
  const tableBody = $("#logsTbody");
  const detail = $("#logDetail");
  const pager = $("#logsPager");

  const typeSelect = $("#typeSelect");
  const resultSelect = $("#resultSelect");
  const statusSelect = $("#statusSelect");
  const ipInput = $("#ipInput");
  const periodInput = $("#periodInput");
  const resetBtn = $("#resetFilters");
  const searchInput = $("#searchInput");

  function syncControlsFromState() {
    if (typeSelect) typeSelect.value = activeFilters.type || "all";
    if (resultSelect) resultSelect.value = activeFilters.result || "all";
    if (statusSelect) statusSelect.value = activeFilters.statusGroup || "all";
    if (ipInput) ipInput.value = activeFilters.ip || "";
    if (periodInput) periodInput.value = activeFilters.period || "";
    if (searchInput) searchInput.value = activeFilters.q || "";
  }

  function renderChips() {
    if (!chipsEl) return;
    chipsEl.innerHTML = "";
    const defs = [];

    if (activeFilters.period) defs.push({ k: "period", v: `period: ${activeFilters.period}` });
    if (activeFilters.type && activeFilters.type !== "all") defs.push({ k: "type", v: `type: ${activeFilters.type}`, cls: activeFilters.type === "access" ? "good" : "" });
    if (activeFilters.result && activeFilters.result !== "all") defs.push({ k: "result", v: `result: ${activeFilters.result}` });
    if (activeFilters.statusGroup && activeFilters.statusGroup !== "all") defs.push({ k: "statusGroup", v: `status: ${activeFilters.statusGroup}` });
    if (activeFilters.ip) defs.push({ k: "ip", v: `ip: ${activeFilters.ip}` });
    if (activeFilters.cluster) defs.push({ k: "cluster", v: `cluster: ${activeFilters.cluster}` });
    if (activeFilters.q) defs.push({ k: "q", v: `q: ${activeFilters.q}` });

    defs.forEach((d) => {
      const chip = document.createElement("span");
      chip.className = "chip " + (d.cls || "");
      chip.innerHTML = `<span>${escapeHTML(d.v)}</span><span class="x" title="Убрать">×</span>`;
      chip.querySelector(".x")?.addEventListener("click", () => {
        activeFilters[d.k] = d.k === "period" ? "" : null;
        if (d.k === "type") activeFilters.type = "all";
        if (d.k === "result") activeFilters.result = "all";
        if (d.k === "statusGroup") activeFilters.statusGroup = "all";
        if (d.k === "q") activeFilters.q = "";
        if (d.k === "cluster") {
          const p = new URLSearchParams(location.search);
          p.delete("cluster");
          history.replaceState({}, "", location.pathname + (p.toString() ? `?${p}` : ""));
        }
        currentPage = 1;
        syncControlsFromState();
        render();
      });
      chipsEl.appendChild(chip);
    });
  }

  function inPeriod(log, period) {
    if (!period) return true;
    const d = log.date;
    if (!d) return true;
    return d >= period.from && d <= period.to;
  }

  function statusMatches(log) {
    const g = activeFilters.statusGroup || "all";
    if (g === "all") return true;
    if (log.type !== "access") return false;
    const s = Number(log.status || 0);
    if (g === "4xx5xx") return s >= 400 && s < 600;
    if (g === "2xx") return s >= 200 && s < 300;
    if (g === "3xx") return s >= 300 && s < 400;
    if (g === "4xx") return s >= 400 && s < 500;
    if (g === "5xx") return s >= 500 && s < 600;
    return true;
  }

  function resultMatches(log) {
    const r = activeFilters.result || "all";
    if (r === "all") return true;
    const isFailed = log.type === "error" || (log.type === "access" && Number(log.status || 0) >= 400);
    if (r === "failed") return isFailed;
    if (r === "success") return log.type === "access" && Number(log.status || 0) < 400;
    return true;
  }

  function typeMatches(log) {
    const t = activeFilters.type || "all";
    if (t === "all") return true;
    return log.type === t;
  }

  function ipMatches(log) {
    const ipf = (activeFilters.ip || "").trim();
    if (!ipf) return true;
    const ip = String(log.ip || "");
    if (ipf.includes("*")) {
      const pref = ipf.split("*")[0];
      return ip.startsWith(pref);
    }
    if (ipf.endsWith(".")) return ip.startsWith(ipf);
    return ip.startsWith(ipf);
  }

  function clusterMatches(log) {
    if (!activeFilters.cluster) return true;
    const hay = `${log.method || ""} ${log.endpoint || ""}#${log.status || ""} ${log.message || ""}`.toLowerCase();
    const needle = activeFilters.cluster.toLowerCase().replace("<id>", "").replace("<ID>", "");
    return hay.includes(needle);
  }

  function queryMatches(log) {
    const q = (activeFilters.q || "").trim().toLowerCase();
    if (!q) return true;
    const hay = `${log.raw} ${log.endpoint || ""} ${log.message || ""}`.toLowerCase();
    return hay.includes(q);
  }

  function matches(log) {
    const period = parsePeriodInput(activeFilters.period);
    return (
      inPeriod(log, period) &&
      typeMatches(log) &&
      resultMatches(log) &&
      statusMatches(log) &&
      ipMatches(log) &&
      clusterMatches(log) &&
      queryMatches(log)
    );
  }

  async function renderDetailById(logId) {
    if (!detail) return;
    detail.innerHTML = `<div class="muted">Загрузка...</div>`;

    try {
      const l = await fetchLogDetails(logId);
      const parsedText = l.type === "access"
        ? [
            `endpoint/template: ${(l.endpoint || "").replace(/\d+/g, "<ID>")}`,
            `method: ${l.method || "—"}`,
            `status: ${l.status ?? "—"}`,
            `ip: ${l.ip || "—"}`,
            `bytes: ${l.bytes ?? "—"}`,
            `user-agent: ${l.ua || "—"}`,
            `referer: ${l.ref || "—"}`,
            l.parse_error ? "parse_error: true" : "parse_error: false",
          ].join("\n")
        : [
            `level: ${l.parsed.level || "—"}`,
            `ip: ${l.ip || "—"}`,
            `message: ${l.message || "—"}`,
            l.parse_error ? "parse_error: true" : "parse_error: false",
          ].join("\n");

      detail.innerHTML = `
        <div style="font-weight:800; margin-bottom:6px">Детали выбранного лога</div>
        <div class="small muted" style="white-space:pre-line; margin-bottom:10px">${escapeHTML(parsedText)}</div>
        <div class="small" style="white-space:pre-wrap; margin-bottom:10px">${escapeHTML(fmtJSON(l.original))}</div>
        <div class="row">
          <button class="btn small" id="openRaw">Открыть raw</button>
          <button class="btn small" id="copyRaw">Копировать</button>
        </div>
      `;

      $("#openRaw")?.addEventListener("click", async () => {
        try {
          const rawPayload = await fetchRawLog(logId);
          $("#rawText").textContent = rawPayload.raw || "";
          openModal("#rawModal");
        } catch (error) {
          toast(`Не удалось загрузить raw: ${error.message}`);
        }
      });

      $("#copyRaw")?.addEventListener("click", async () => {
        try {
          const rawPayload = await fetchRawLog(logId);
          await navigator.clipboard.writeText(rawPayload.raw || "");
          toast("Скопировано");
        } catch (_) {
          toast("Не удалось скопировать");
        }
      });
    } catch (error) {
      detail.innerHTML = `<div class="muted">Ошибка загрузки: ${escapeHTML(error.message)}</div>`;
    }
  }

  function renderPagination(totalItems) {
    if (!pager) return;

    if (totalItems <= 0) {
      pager.innerHTML = "";
      return;
    }

    const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));
    currentPage = Math.min(Math.max(currentPage, 1), totalPages);
    const start = (currentPage - 1) * pageSize + 1;
    const end = Math.min(start + pageSize - 1, totalItems);

    const pageItems = [];
    const addPage = (page) => {
      if (!pageItems.includes(page)) {
        pageItems.push(page);
      }
    };

    if (totalPages <= 7) {
      for (let page = 1; page <= totalPages; page += 1) {
        addPage(page);
      }
    } else {
      addPage(1);
      addPage(2);

      for (let page = currentPage - 1; page <= currentPage + 1; page += 1) {
        if (page > 2 && page < totalPages - 1) {
          addPage(page);
        }
      }

      addPage(totalPages - 1);
      addPage(totalPages);
    }

    pageItems.sort((a, b) => a - b);

    const paginationMarkup = [];
    pageItems.forEach((page, index) => {
      const prevPage = pageItems[index - 1];
      if (prevPage && page - prevPage > 1) {
        paginationMarkup.push(`<span class="pagerEllipsis">...</span>`);
      }
      paginationMarkup.push(
        `<button class="btn small${page === currentPage ? " active" : ""}" data-page="${page}" type="button">${page}</button>`
      );
    });

    pager.innerHTML = `
      <div class="pagerInfo">Показано ${start}-${end} из ${totalItems}</div>
      <div class="pagerControls">
        <button class="btn small" id="pagerPrev" type="button"${currentPage === 1 ? " disabled" : ""}>Назад</button>
        <div class="pagerPages">
          ${paginationMarkup.join("")}
        </div>
        <button class="btn small" id="pagerNext" type="button"${currentPage === totalPages ? " disabled" : ""}>Дальше</button>
      </div>
    `;

    $("#pagerPrev", pager)?.addEventListener("click", async () => {
      if (currentPage <= 1) return;
      currentPage -= 1;
      renderTable(currentFilteredLogs);
    });

    $("#pagerNext", pager)?.addEventListener("click", async () => {
      if (currentPage >= totalPages) return;
      currentPage += 1;
      renderTable(currentFilteredLogs);
    });

    $$("[data-page]", pager).forEach((button) => {
      button.addEventListener("click", async () => {
        const nextPage = Number(button.dataset.page || currentPage);
        if (!Number.isFinite(nextPage) || nextPage === currentPage) return;
        currentPage = nextPage;
        renderTable(currentFilteredLogs);
      });
    });
  }

  function renderTable(list) {
    if (!tableBody) return;
    tableBody.innerHTML = "";

    const totalPages = Math.max(1, Math.ceil(list.length / pageSize));
    currentPage = Math.min(Math.max(currentPage, 1), totalPages);
    const startIndex = (currentPage - 1) * pageSize;
    const pageItems = list.slice(startIndex, startIndex + pageSize);

    pageItems.forEach((l, idx) => {
      const tr = document.createElement("tr");
      tr.dataset.logId = l.id;
      tr.innerHTML = `
        <td>${escapeHTML(l.ts || "—")}</td>
        <td>${escapeHTML(l.type || "—")}</td>
        <td>${escapeHTML(l.type === "access" && l.status != null ? String(l.status) : "—")}</td>
      `;
      tr.addEventListener("click", async () => {
        $$("#logsTbody tr").forEach((x) => x.classList.remove("selected"));
        tr.classList.add("selected");
        await renderDetailById(l.id);
      });
      tableBody.appendChild(tr);

      if (idx === 0) {
        tr.classList.add("selected");
        renderDetailById(l.id);
      }
    });

    if (list.length === 0) {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td colspan="3" class="muted">Нет данных под фильтры</td>`;
      tableBody.appendChild(tr);
      if (detail) detail.innerHTML = `<div class="muted">Выберите запись</div>`;
    }

    renderPagination(list.length);
  }

  async function loadFromServer() {
    const currentRequest = ++requestId;
    const period = parsePeriodInput(activeFilters.period);
    const serverFilters = {
      type: activeFilters.type !== "all" ? activeFilters.type : undefined,
      search: activeFilters.q || undefined,
      from_date: period?.from ? toIsoDateStart(period.from) : undefined,
      to_date: period?.to ? toIsoDateEnd(period.to) : undefined,
      limit: 5000,
      offset: 0,
    };

    const result = await fetchLogsFromApi(serverFilters);
    if (currentRequest !== requestId) return;

    currentLogs = result.items;
    currentFilteredLogs = currentLogs.filter(matches);
  }

  async function render() {
    syncControlsFromState();
    renderChips();

    if (tableBody) {
      tableBody.innerHTML = `<tr><td colspan="3" class="muted">Загрузка...</td></tr>`;
    }

    try {
      await loadFromServer();
      currentPage = Math.min(currentPage, Math.max(1, Math.ceil(currentFilteredLogs.length / pageSize)));
      renderTable(currentFilteredLogs);
    } catch (error) {
      if (tableBody) {
        tableBody.innerHTML = `<tr><td colspan="3" class="muted">Ошибка загрузки: ${escapeHTML(error.message)}</td></tr>`;
      }
      if (pager) pager.innerHTML = "";
      if (detail) detail.innerHTML = `<div class="muted">Не удалось загрузить детали</div>`;
      toast(`Ошибка загрузки логов: ${error.message}`);
    }
  }

  window.__refreshLogsPage = render;
  window.__exportCurrentLogs = async () => {
    const payload = currentFilteredLogs.map((log) => log.original);
    downloadJson("apache_logs_export.json", payload);
    toast("Экспорт логов сформирован");
  };

  typeSelect?.addEventListener("change", async (e) => {
    activeFilters.type = e.target.value;
    currentPage = 1;
    await render();
  });

  resultSelect?.addEventListener("change", async (e) => {
    activeFilters.result = e.target.value;
    currentPage = 1;
    await render();
  });

  statusSelect?.addEventListener("change", async (e) => {
    activeFilters.statusGroup = e.target.value;
    currentPage = 1;
    await render();
  });

  ipInput?.addEventListener("input", async (e) => {
    activeFilters.ip = e.target.value.trim();
    currentPage = 1;
    await render();
  });

  periodInput?.addEventListener("input", async (e) => {
    activeFilters.period = e.target.value.trim();
    currentPage = 1;
    await render();
  });

  searchInput?.addEventListener("input", async (e) => {
    activeFilters.q = e.target.value.trim();
    currentPage = 1;
    await render();
  });

  resetBtn?.addEventListener("click", async () => {
    activeFilters = {
      period: "",
      type: "all",
      result: "all",
      statusGroup: "all",
      ip: "",
      cluster: null,
      q: "",
    };
    currentPage = 1;
    const p = new URLSearchParams(location.search);
    p.delete("cluster");
    history.replaceState({}, "", location.pathname + (p.toString() ? `?${p}` : ""));
    await render();
  });

  render();
}

function initClusteringPage() {
  const runs = loadJSON("apacheLogs.data.runs", []);
  const clustersByRun = loadJSON("apacheLogs.data.clustersByRun", {});

  function renderLast() {
    const last = runs[0];
    if (!last) return;
    $("#lastClusters").textContent = last.clusters || "—";
    $("#lastProcessed").textContent = last.logsProcessed || "—";
    $("#lastTop").textContent = clustersByRun[last.id]?.[0]?.key || "—";
  }

  function matchesRun(r) {
    const m = ($("#runMethodFilter")?.value || "").trim().toLowerCase();
    const s = ($("#runStatusFilter")?.value || "all").trim().toLowerCase();
    if (m && !String(r.method || "").toLowerCase().includes(m)) return false;
    if (s !== "all" && String(r.status || "").toLowerCase() !== s) return false;
    return true;
  }

  function renderRunsTable() {
    const body = $("#runsTbody");
    if (!body) return;
    body.innerHTML = "";
    const list = runs.filter(matchesRun);

    list.forEach((r) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${escapeHTML(r.created)}</td>
        <td>${escapeHTML(r.method)}</td>
        <td>${escapeHTML(r.status)}</td>
        <td>${escapeHTML(String(r.clusters ?? "—"))}</td>
        <td>${escapeHTML(String(r.logsProcessed ?? "—"))}</td>
      `;
      tr.addEventListener("click", () => {
        location.href = `runs.html?run=${encodeURIComponent(r.id)}`;
      });
      body.appendChild(tr);
    });

    if (list.length === 0) {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td colspan="5" class="muted">Нет запусков под фильтры</td>`;
      body.appendChild(tr);
    }
  }

  $("#runBtn")?.addEventListener("click", () => {
    toast("Кластеризация будет добавлена в следующей итерации");
  });

  $("#savePreset")?.addEventListener("click", () => {
    toast("Preset будет добавлен в следующей итерации");
  });

  $("#runMethodFilter")?.addEventListener("input", renderRunsTable);
  $("#runStatusFilter")?.addEventListener("change", renderRunsTable);

  renderLast();
  renderRunsTable();
}

function initRunsPage() {
  const runs = loadJSON("apacheLogs.data.runs", []);
  const clustersByRun = loadJSON("apacheLogs.data.clustersByRun", {});
  const params = new URLSearchParams(location.search);
  const runId = params.get("run") || runs[0]?.id;

  const run = runs.find((r) => r.id === runId) || runs[0];
  const clusters = clustersByRun[run?.id] || [];

  const meta = $("#runMeta");
  const clustersBody = $("#clustersTbody");
  const showBtn = $("#showClusterLogs");

  let selectedClusterKey = null;

  function setKPI() {
    $("#kpiClusters").textContent = String(run?.clusters ?? clusters.length ?? "—");
    $("#kpiProcessed").textContent = String(run?.logsProcessed ?? "—");
    $("#kpiStatus").textContent = String(run?.status ?? "—");
  }

  function renderMeta() {
    if (!run) {
      meta.textContent = "Запуск не найден (выберите на экране «Кластеризация»)";
      return;
    }
    meta.textContent = `run_id: ${run.id} · created: ${run.createdDate || "—"} ${run.created || ""} · method: ${run.method} · filters: ${run.filters}`;
  }

  function renderClusters() {
    const q = ($("#clusterSearch")?.value || "").trim().toLowerCase();
    const list = q ? clusters.filter((c) => c.key.toLowerCase().includes(q)) : clusters;

    clustersBody.innerHTML = "";
    list.forEach((c) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${escapeHTML(c.key)}</td><td>${escapeHTML(String(c.cnt))}</td>`;
      if (c.key === selectedClusterKey) tr.classList.add("selected");
      tr.addEventListener("click", () => {
        selectedClusterKey = c.key;
        renderClusters();
        showBtn.disabled = !selectedClusterKey;
      });
      clustersBody.appendChild(tr);
    });

    if (list.length === 0) {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td colspan="2" class="muted">Нет кластеров</td>`;
      clustersBody.appendChild(tr);
    }

    showBtn.disabled = !selectedClusterKey;
  }

  function drawClusterChart() {
    const canvas = $("#clusterChart");
    if (!canvas) return;
    const ctx = canvas.getContext("2d");

    const items = [...clusters]
      .sort((a, b) => (b.cnt || 0) - (a.cnt || 0))
      .slice(0, 10)
      .map((c) => ({ x: c.key, y: Number(c.cnt || 0) }));

    const w = (canvas.width = canvas.clientWidth * devicePixelRatio);
    const h = (canvas.height = 240 * devicePixelRatio);
    ctx.clearRect(0, 0, w, h);

    const padding = 24 * devicePixelRatio;
    const maxY = Math.max(1, ...items.map((i) => i.y));
    const barW = (w - padding * 2) / Math.max(1, items.length);
    const base = h - padding;

    ctx.strokeStyle = "rgba(17,24,39,.18)";
    ctx.lineWidth = 1 * devicePixelRatio;
    ctx.beginPath();
    ctx.moveTo(padding, padding);
    ctx.lineTo(padding, base);
    ctx.lineTo(w - padding, base);
    ctx.stroke();

    ctx.fillStyle = "rgba(43,102,246,.55)";
    items.forEach((it, i) => {
      const bh = (base - padding) * (it.y / maxY);
      const x = padding + i * barW + barW * 0.18;
      const y = base - bh;
      const bw = barW * 0.64;
      roundRect(ctx, x, y, bw, bh, 8 * devicePixelRatio);
      ctx.fill();
    });

    function roundRect(ctx, x, y, w, h, r) {
      const rr = Math.min(r, w / 2, h / 2);
      ctx.beginPath();
      ctx.moveTo(x + rr, y);
      ctx.arcTo(x + w, y, x + w, y + h, rr);
      ctx.arcTo(x + w, y + h, x, y + h, rr);
      ctx.arcTo(x, y + h, x, y, rr);
      ctx.arcTo(x, y, x + w, y, rr);
      ctx.closePath();
    }
  }

  $("#clusterSearch")?.addEventListener("input", () => {
    renderClusters();
    drawClusterChart();
  });

  showBtn?.addEventListener("click", () => {
    if (!selectedClusterKey) return;
    location.href = `logs.html?cluster=${encodeURIComponent(selectedClusterKey)}`;
  });

  renderMeta();
  setKPI();
  renderClusters();
  drawClusterChart();
}

document.addEventListener("DOMContentLoaded", () => {
  seedDemoRunsOnce();
  setActiveNav();
  bindGlobal();

  const page = document.body.dataset.page;
  if (page === "logs") initLogsPage();
  if (page === "clustering") initClusteringPage();
  if (page === "runs") initRunsPage();
});
