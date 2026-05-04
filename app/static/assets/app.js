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

function datetimeLocalToIso(value) {
  if (!value) return undefined;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return undefined;
  return date.toISOString();
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

async function importLogs(file, mode) {
  const formData = new FormData();
  formData.append("file", file);
  formData.append("mode", mode);

  return apiFetch("/import", {
    method: "POST",
    body: formData,
  });
}

async function exportApplicationFromApi() {
  return apiFetch("/export");
}

async function createClusterRun(payload = {}) {
  return apiFetch("/cluster-runs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

async function fetchClusterRuns(filters = {}) {
  const params = new URLSearchParams();
  if (filters.method) params.set("method", filters.method);
  if (filters.status && filters.status !== "all") params.set("status", filters.status);
  params.set("limit", String(filters.limit ?? 100));
  params.set("offset", String(filters.offset ?? 0));
  return apiFetch(`/cluster-runs?${params.toString()}`);
}

async function fetchClusterRun(runId) {
  return apiFetch(`/cluster-runs/${encodeURIComponent(runId)}`);
}

async function fetchClusters(runId, filters = {}) {
  const params = new URLSearchParams();
  if (filters.search) params.set("search", filters.search);
  params.set("limit", String(filters.limit ?? 500));
  params.set("offset", String(filters.offset ?? 0));
  return apiFetch(`/cluster-runs/${encodeURIComponent(runId)}/clusters?${params.toString()}`);
}

async function fetchClusterRunStats(runId) {
  return apiFetch(`/cluster-runs/${encodeURIComponent(runId)}/stats`);
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

function renderPager(container, { total, page, pageSize, onChange }) {
  if (!container) return;

  if (total <= 0) {
    container.innerHTML = "";
    return;
  }

  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const currentPage = Math.min(Math.max(page, 1), totalPages);
  const start = (currentPage - 1) * pageSize + 1;
  const end = Math.min(start + pageSize - 1, total);

  const pageItems = [];
  const addPage = (candidate) => {
    if (candidate >= 1 && candidate <= totalPages && !pageItems.includes(candidate)) {
      pageItems.push(candidate);
    }
  };

  if (totalPages <= 7) {
    for (let candidate = 1; candidate <= totalPages; candidate += 1) addPage(candidate);
  } else {
    addPage(1);
    addPage(2);
    for (let candidate = currentPage - 1; candidate <= currentPage + 1; candidate += 1) {
      addPage(candidate);
    }
    addPage(totalPages - 1);
    addPage(totalPages);
  }

  pageItems.sort((a, b) => a - b);

  const paginationMarkup = [];
  pageItems.forEach((pageItem, index) => {
    const prevPage = pageItems[index - 1];
    if (prevPage && pageItem - prevPage > 1) {
      paginationMarkup.push(`<span class="pagerEllipsis">...</span>`);
    }
    paginationMarkup.push(
      `<button class="btn small${pageItem === currentPage ? " active" : ""}" data-page="${pageItem}" type="button">${pageItem}</button>`
    );
  });

  container.innerHTML = `
    <div class="pagerInfo">Показано ${start}-${end} из ${total}</div>
    <div class="pagerControls">
      <button class="btn small" data-page-prev type="button"${currentPage === 1 ? " disabled" : ""}>Назад</button>
      <div class="pagerPages">${paginationMarkup.join("")}</div>
      <button class="btn small" data-page-next type="button"${currentPage === totalPages ? " disabled" : ""}>Дальше</button>
    </div>
  `;

  $("[data-page-prev]", container)?.addEventListener("click", () => {
    if (currentPage > 1) onChange(currentPage - 1);
  });

  $("[data-page-next]", container)?.addEventListener("click", () => {
    if (currentPage < totalPages) onChange(currentPage + 1);
  });

  $$("[data-page]", container).forEach((button) => {
    button.addEventListener("click", () => {
      const nextPage = Number(button.dataset.page || currentPage);
      if (Number.isFinite(nextPage) && nextPage !== currentPage) onChange(nextPage);
    });
  });
}

function getImportModalControls() {
  const modal = $("#importModal");
  if (!modal) return null;

  const fileInput = $("#importFile", modal);
  const modeSelect = $("#importMode", modal);

  if (fileInput) {
    fileInput.disabled = false;
    fileInput.removeAttribute("disabled");
    fileInput.multiple = false;
  }

  const importButton = $("#doImport");
  if (importButton) importButton.textContent = "Импортировать";

  return { modal, fileInput, modeSelect };
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
      const result = await importLogs(file, mode);
      closeModal("#importModal");
      if (controls?.fileInput) controls.fileInput.value = "";
      const analysisText = result.cluster_runs || result.clusters
        ? `, runs: ${result.cluster_runs || 0}, clusters: ${result.clusters || 0}`
        : "";
      toast(`Импорт завершён: ${result.inserted}/${result.total}, access: ${result.access || 0}, error: ${result.error || 0}${analysisText}, ошибок: ${result.errors}`);
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
      const payload = await exportApplicationFromApi();
      downloadJson("apache_logs_application_export.json", payload);
      toast("Экспорт приложения сформирован");
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
    from_date: "",
    to_date: "",
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
  const fromDateInput = $("#fromDateInput");
  const toDateInput = $("#toDateInput");
  const resetBtn = $("#resetFilters");
  const searchInput = $("#searchInput");

  function syncControlsFromState() {
    if (typeSelect) typeSelect.value = activeFilters.type || "all";
    if (resultSelect) resultSelect.value = activeFilters.result || "all";
    if (statusSelect) statusSelect.value = activeFilters.statusGroup || "all";
    if (ipInput) ipInput.value = activeFilters.ip || "";
    if (fromDateInput) fromDateInput.value = activeFilters.from_date || "";
    if (toDateInput) toDateInput.value = activeFilters.to_date || "";
    if (searchInput) searchInput.value = activeFilters.q || "";
  }

  function renderChips() {
    if (!chipsEl) return;
    chipsEl.innerHTML = "";
    const defs = [];

    if (activeFilters.from_date) defs.push({ k: "from_date", v: `from: ${activeFilters.from_date}` });
    if (activeFilters.to_date) defs.push({ k: "to_date", v: `to: ${activeFilters.to_date}` });
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
        activeFilters[d.k] = ["from_date", "to_date"].includes(d.k) ? "" : null;
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

  function inPeriod(log) {
    if (!activeFilters.from_date && !activeFilters.to_date) return true;
    const ts = log.timestamp ? new Date(log.timestamp) : null;
    if (!ts || Number.isNaN(ts.getTime())) return true;
    const from = activeFilters.from_date ? new Date(activeFilters.from_date) : null;
    const to = activeFilters.to_date ? new Date(activeFilters.to_date) : null;
    if (from && ts < from) return false;
    if (to && ts > to) return false;
    return true;
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
    const key = normalizeClusterKey(activeFilters.cluster);
    const candidates = [];

    if (log.type === "access") {
      candidates.push(normalizeClusterKey(`${log.method || "UNKNOWN"} ${templateUri(log.endpoint || "-")}#${log.status ?? "-"}`));
    }

    if (log.type === "error") {
      candidates.push(normalizeClusterKey(`${log.parsed.level || "unknown"}: ${templateMessage(log.message || log.raw || "")}`));
    }

    return candidates.some((candidate) => candidate === key || candidate.includes(key) || key.includes(candidate));
  }

  function normalizeClusterKey(value) {
    return String(value || "")
      .replaceAll("<ID>", "<id>")
      .replaceAll("<IP>", "<ip>")
      .replaceAll("<PATH_ID>", "<path_id>")
      .toLowerCase()
      .replace(/\s+/g, " ")
      .trim();
  }

  function templateUri(uri) {
    return String(uri || "-")
      .split("?")[0]
      .split("/")
      .map((part) => (/^\d+$/.test(part) ? "<id>" : part))
      .join("/") || "/";
  }

  function templateMessage(message) {
    return String(message || "")
      .replace(/\b\d{1,3}(?:\.\d{1,3}){3}\b/g, "<ip>")
      .replace(/\b\d+\b/g, "<id>")
      .replace(/\/[A-Za-z0-9._/-]*<id>[A-Za-z0-9._/-]*/g, "/<path_id>")
      .trim();
  }

  function queryMatches(log) {
    const q = (activeFilters.q || "").trim().toLowerCase();
    if (!q) return true;
    const hay = `${log.raw} ${log.endpoint || ""} ${log.message || ""}`.toLowerCase();
    return hay.includes(q);
  }

  function matches(log) {
    return (
      inPeriod(log) &&
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
    renderPager(pager, {
      total: totalItems,
      page: currentPage,
      pageSize,
      onChange: (nextPage) => {
        currentPage = nextPage;
        renderTable(currentFilteredLogs);
      },
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
    const serverFilters = {
      type: activeFilters.type !== "all" ? activeFilters.type : undefined,
      search: activeFilters.q || undefined,
      from_date: datetimeLocalToIso(activeFilters.from_date),
      to_date: datetimeLocalToIso(activeFilters.to_date),
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

  fromDateInput?.addEventListener("input", async (e) => {
    activeFilters.from_date = e.target.value;
    currentPage = 1;
    await render();
  });

  toDateInput?.addEventListener("input", async (e) => {
    activeFilters.to_date = e.target.value;
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
      from_date: "",
      to_date: "",
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

function normalizeRun(raw) {
  const createdAt = raw.created_at ? new Date(raw.created_at) : null;
  const validCreated = createdAt && !Number.isNaN(createdAt.getTime());
  const summary = raw.summary || {};
  return {
    id: raw.id || raw._id,
    created: validCreated ? createdAt.toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" }) : "—",
    createdDate: validCreated ? createdAt.toLocaleDateString("ru-RU") : "—",
    method: raw.method || "rule_based",
    filters: raw.filters || {},
    filtersText: Object.keys(raw.filters || {}).length ? fmtJSON(raw.filters) : "без фильтров",
    status: raw.status || "finished",
    clusters: summary.clusters_total ?? 0,
    logsProcessed: summary.clustered_logs_total ?? summary.logs_total ?? 0,
    original: raw,
  };
}

function normalizeCluster(raw) {
  return {
    id: raw.id || raw._id,
    runId: raw.run_id,
    key: raw.cluster_key || "—",
    cnt: raw.size ?? 0,
    stats: raw.stats || {},
    samples: raw.samples || [],
    description: raw.description || "",
    original: raw,
  };
}

function initClusteringPage() {
  let runs = [];
  let clustersByRun = {};
  let currentRunsPage = 1;
  let runsTotal = 0;
  const runsPageSize = 10;
  const runsPager = $("#runsPager");
  const clusterLogTypeSelect = $("#clusterLogTypeSelect");
  const clusterMethodSelect = $("#clusterMethodSelect");

  function syncClusterControls() {
    const selectedOption = clusterMethodSelect?.selectedOptions?.[0];
    const forcedType = selectedOption?.dataset?.logType || "";
    if (clusterLogTypeSelect) {
      if (forcedType) clusterLogTypeSelect.value = forcedType;
      clusterLogTypeSelect.disabled = Boolean(forcedType);
    }
  }

  function getClusterRunPayload() {
    const selectedOption = clusterMethodSelect?.selectedOptions?.[0];
    const forcedType = selectedOption?.dataset?.logType || "";
    const selectedType = clusterLogTypeSelect?.value || "";
    const filters = {};
    if (forcedType || selectedType) filters.type = forcedType || selectedType;
    return {
      method: clusterMethodSelect?.value || "rule_based",
      filters,
    };
  }

  function renderLast() {
    const last = runs[0];
    if (!last) {
      $("#lastClusters").textContent = "—";
      $("#lastProcessed").textContent = "—";
      $("#lastTop").textContent = "—";
      return;
    }
    $("#lastClusters").textContent = last.clusters || "—";
    $("#lastProcessed").textContent = last.logsProcessed || "—";
    $("#lastTop").textContent = clustersByRun[last.id]?.[0]?.key || "—";
  }

  function renderRunsTable() {
    const body = $("#runsTbody");
    if (!body) return;
    body.innerHTML = "";

    runs.forEach((r) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${escapeHTML(`${r.createdDate} ${r.created}`)}</td>
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

    if (runs.length === 0) {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td colspan="5" class="muted">Нет запусков под фильтры</td>`;
      body.appendChild(tr);
    }

    renderPager(runsPager, {
      total: runsTotal,
      page: currentRunsPage,
      pageSize: runsPageSize,
      onChange: async (nextPage) => {
        currentRunsPage = nextPage;
        await refresh();
      },
    });
  }

  async function loadRuns() {
    const result = await fetchClusterRuns({
      method: ($("#runMethodFilter")?.value || "").trim(),
      status: $("#runStatusFilter")?.value || "all",
      limit: runsPageSize,
      offset: (currentRunsPage - 1) * runsPageSize,
    });
    runs = (result.items || []).map(normalizeRun);
    runsTotal = Number(result.total || 0);
    clustersByRun = {};
    if (runs[0]) {
      const clustersResult = await fetchClusters(runs[0].id, { limit: 1, offset: 0 });
      clustersByRun[runs[0].id] = (clustersResult.items || []).map(normalizeCluster);
    }
  }

  async function refresh() {
    const body = $("#runsTbody");
    if (body) body.innerHTML = `<tr><td colspan="5" class="muted">Загрузка...</td></tr>`;
    try {
      await loadRuns();
      renderLast();
      renderRunsTable();
    } catch (error) {
      if (body) body.innerHTML = `<tr><td colspan="5" class="muted">Ошибка загрузки: ${escapeHTML(error.message)}</td></tr>`;
      if (runsPager) runsPager.innerHTML = "";
      toast(`Ошибка загрузки запусков: ${error.message}`);
    }
  }

  $("#runBtn")?.addEventListener("click", async () => {
    const btn = $("#runBtn");
    const prevText = btn?.textContent || "Запустить";
    if (btn) {
      btn.disabled = true;
      btn.textContent = "Запуск...";
    }
    try {
      const run = normalizeRun(await createClusterRun(getClusterRunPayload()));
      toast(`Кластеризация завершена: ${run.clusters} кластеров`);
      await refresh();
    } catch (error) {
      toast(`Ошибка кластеризации: ${error.message}`);
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.textContent = prevText;
      }
    }
  });

  clusterMethodSelect?.addEventListener("change", syncClusterControls);
  $("#runMethodFilter")?.addEventListener("input", async () => {
    currentRunsPage = 1;
    await refresh();
  });
  $("#runStatusFilter")?.addEventListener("change", async () => {
    currentRunsPage = 1;
    await refresh();
  });

  syncClusterControls();
  refresh();
}

function initRunsPage() {
  const params = new URLSearchParams(location.search);
  let runId = params.get("run");
  let run = null;
  let clusters = [];
  let clustersTotal = 0;
  let currentClustersPage = 1;
  let statsPayload = null;
  const clustersPageSize = 10;

  const meta = $("#runMeta");
  const clustersBody = $("#clustersTbody");
  const clustersPager = $("#clustersPager");
  const showBtn = $("#showClusterLogs");

  let selectedClusterKey = null;

  function setKPI() {
    $("#kpiClusters").textContent = String(run?.clusters ?? clusters.length ?? "—");
    $("#kpiProcessed").textContent = String(run?.logsProcessed ?? "—");
    $("#kpiStatus").textContent = String(run?.status ?? "—");

    const statusCounts = statsPayload?.status_counts || {};
    const methodCounts = statsPayload?.method_counts || {};
    const logTypeCounts = statsPayload?.log_type_counts || {};
    $("#statsStatusGroups").textContent = Object.keys(statusCounts).length ? Object.keys(statusCounts).length : "—";
    $("#statsMethods").textContent = Object.keys(methodCounts).length ? Object.keys(methodCounts).join(", ") : "—";
    $("#statsLogTypes").textContent = Object.keys(logTypeCounts).length ? Object.keys(logTypeCounts).join(", ") : "—";
  }

  function renderMeta() {
    if (!run) {
      meta.textContent = "Запуск не найден (выберите на экране «Кластеризация»)";
      return;
    }
    meta.textContent = `run_id: ${run.id} · created: ${run.createdDate || "—"} ${run.created || ""} · method: ${run.method} · filters: ${run.filtersText}`;
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

    renderPager(clustersPager, {
      total: clustersTotal,
      page: currentClustersPage,
      pageSize: clustersPageSize,
      onChange: async (nextPage) => {
        currentClustersPage = nextPage;
        selectedClusterKey = null;
        await loadRunDetails();
      },
    });
  }

  function drawClusterChart() {
    const canvas = $("#clusterChart");
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    const legend = $("#clusterChartLegend");

    const items = [...(statsPayload?.top_clusters || clusters)]
      .map(normalizeCluster)
      .slice(0, 10)
      .map((c, index) => ({ label: `#${index + 1}`, key: c.key, y: Number(c.cnt || 0) }));

    const w = (canvas.width = canvas.clientWidth * devicePixelRatio);
    const h = (canvas.height = 240 * devicePixelRatio);
    ctx.clearRect(0, 0, w, h);

    const padding = 28 * devicePixelRatio;
    const bottomPadding = 42 * devicePixelRatio;
    const maxY = Math.max(1, ...items.map((i) => i.y));
    const barW = (w - padding * 2) / Math.max(1, items.length);
    const base = h - bottomPadding;

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

    ctx.fillStyle = "rgba(17,24,39,.72)";
    ctx.font = `${12 * devicePixelRatio}px system-ui, -apple-system, Segoe UI, sans-serif`;
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    items.forEach((it, i) => {
      const x = padding + i * barW + barW / 2;
      ctx.fillText(it.label, x, base + 10 * devicePixelRatio);
    });

    if (legend) {
      legend.innerHTML = items.length
        ? items.map((it) => `
            <div class="legendItem">
              <span class="legendIndex">${escapeHTML(it.label)}</span>
              <span class="legendKey">${escapeHTML(it.key)}</span>
              <span class="legendCount">${escapeHTML(String(it.y))}</span>
            </div>
          `).join("")
        : `<div class="muted small">Нет данных для графика</div>`;
    }

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
    currentClustersPage = 1;
    selectedClusterKey = null;
    loadRunDetails();
  });

  showBtn?.addEventListener("click", () => {
    if (!selectedClusterKey) return;
    location.href = `logs.html?cluster=${encodeURIComponent(selectedClusterKey)}`;
  });

  async function loadRunDetails() {
    try {
      if (!runId) {
        const runsResult = await fetchClusterRuns({ limit: 1, offset: 0 });
        runId = runsResult.items?.[0]?.id || runsResult.items?.[0]?._id || null;
      }

      if (!runId) {
        run = null;
        clusters = [];
        clustersTotal = 0;
        statsPayload = null;
        renderMeta();
        setKPI();
        renderClusters();
        drawClusterChart();
        return;
      }

      const [runPayload, clustersPayload, statsResult] = await Promise.all([
        fetchClusterRun(runId),
        fetchClusters(runId, {
          search: ($("#clusterSearch")?.value || "").trim(),
          limit: clustersPageSize,
          offset: (currentClustersPage - 1) * clustersPageSize,
        }),
        fetchClusterRunStats(runId),
      ]);
      run = normalizeRun(runPayload);
      clusters = (clustersPayload.items || []).map(normalizeCluster);
      clustersTotal = Number(clustersPayload.total || 0);
      statsPayload = statsResult;
      if (statsPayload?.run) run = normalizeRun(statsPayload.run);
      renderMeta();
      setKPI();
      renderClusters();
      drawClusterChart();
    } catch (error) {
      if (meta) meta.textContent = `Ошибка загрузки: ${error.message}`;
      if (clustersBody) clustersBody.innerHTML = `<tr><td colspan="2" class="muted">Ошибка загрузки</td></tr>`;
      if (clustersPager) clustersPager.innerHTML = "";
      toast(`Ошибка загрузки запуска: ${error.message}`);
    }
  }

  loadRunDetails();
}

document.addEventListener("DOMContentLoaded", () => {
  setActiveNav();
  bindGlobal();

  const page = document.body.dataset.page;
  if (page === "logs") initLogsPage();
  if (page === "clustering") initClusteringPage();
  if (page === "runs") initRunsPage();
});
