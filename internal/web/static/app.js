let syncConfig = null;
let currentSyncState = null;
let historyData = { runs: [], summary: {} };
let activeHistoryFilter = "all";
let historySearchQuery = "";
const expandedRunIds = new Set();
let runFileSearchQueries = {};

document.addEventListener("DOMContentLoaded", () => {
  setupNavigation();
  setupSSE();
  fetchStateAndConfig();
  setupDashboardActions();
  setupHistoryUI();
  setupBrowserModalEvents();
  fetchHistory();
});

// -------------------------------------------------------------
// Multi-Page Navigation
// -------------------------------------------------------------
function setupNavigation() {
  const navBtns = document.querySelectorAll(".nav-btn");
  navBtns.forEach((btn) => {
    btn.addEventListener("click", () => {
      const targetPageId = btn.getAttribute("data-page");

      // Switch active tab
      navBtns.forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");

      // Switch page views
      document.querySelectorAll(".page-view").forEach((p) => p.classList.remove("active"));
      const target = document.getElementById(targetPageId);
      if (target) target.classList.add("active");

      if (targetPageId === "page-history") {
        fetchHistory();
      }
    });
  });
}

// -------------------------------------------------------------
// Server-Sent Events (SSE) for Real-Time Updates (Zero Polling)
// -------------------------------------------------------------
function setupSSE() {
  const evtSource = new EventSource("/api/events");

  evtSource.addEventListener("state", (e) => {
    try {
      const state = JSON.parse(e.data);
      const prevState = currentSyncState;
      updateDashboardUI(state);
      if (state.status === "completed" || state.status === "failed" || state.status === "aborted") {
        if (!prevState || prevState.status !== state.status) {
          fetchHistory();
        }
      }
    } catch (err) {
      console.error("Error parsing state event:", err);
    }
  });

  evtSource.onerror = (err) => {
    console.warn("SSE connection retry...", err);
  };
}

async function fetchStateAndConfig() {
  try {
    const [cfgRes, stateRes] = await Promise.all([
      fetch("/api/config"),
      fetch("/api/sync/state")
    ]);

    if (cfgRes.ok) {
      syncConfig = await cfgRes.json();
      renderSourcesPage();
      renderDestinationPage();
      renderSettingsPage();
    }

    if (stateRes.ok) {
      currentSyncState = await stateRes.json();
      updateDashboardUI(currentSyncState);
    }
  } catch (err) {
    console.error("Failed to load initial data:", err);
  }
}

// -------------------------------------------------------------
// Dashboard View & Real-Time Controls
// -------------------------------------------------------------
function updateDashboardUI(state) {
  currentSyncState = state;

  // Update header status pill
  const headerPill = document.getElementById("header-status-pill");
  const headerText = document.getElementById("header-status-text");
  if (headerPill && headerText) {
    const rawStatus = (state && state.status) ? state.status.toLowerCase() : "idle";
    headerPill.className = "header-status-pill " + rawStatus;
    const labelMap = {
      idle: "IDLE",
      gathering: "GATHERING",
      sending: "SENDING",
      completed: "COMPLETED",
      failed: "FAILED",
      aborted: "ABORTED"
    };
    headerText.textContent = labelMap[rawStatus] || rawStatus.toUpperCase();
  }

  const badge = document.getElementById("hero-badge");
  const heroPhase = document.getElementById("hero-phase-text");
  const heroSub = document.getElementById("hero-subtext");
  const actionBtn = document.getElementById("btn-sync-action");

  const step1 = document.getElementById("flow-step-1");
  const stepStaging = document.getElementById("flow-step-staging");
  const step2 = document.getElementById("flow-step-2");

  // Reset steps
  step1.className = "flow-step";
  stepStaging.className = "flow-step";
  step2.className = "flow-step";

  const isRunning = state.status === "gathering" || state.status === "sending";

  if (state.status === "gathering") {
    badge.className = "status-badge gathering";
    badge.innerHTML = `<span class="pulse-dot"></span> GATHERING (STEP 1 OF 2)`;
    heroPhase.textContent = "Gathering Remote Files...";
    heroSub.textContent = "Pulling sources in parallel into central local staging";
    step1.classList.add("active");

    actionBtn.className = "btn-cta btn-stop";
    actionBtn.innerHTML = `<span>✖</span> Stop Sync`;
    actionBtn.disabled = false;
  } else if (state.status === "sending") {
    badge.className = "status-badge sending";
    badge.innerHTML = `<span class="pulse-dot"></span> SENDING (STEP 2 OF 2)`;
    heroPhase.textContent = "Sending Snapshot to Destination...";
    heroSub.textContent = "Pushing gathered files to the intermittent remote PC";
    step1.classList.add("completed");
    stepStaging.classList.add("completed");
    step2.classList.add("active");

    actionBtn.className = "btn-cta btn-stop";
    actionBtn.innerHTML = `<span>✖</span> Stop Sync`;
    actionBtn.disabled = false;
  } else if (state.status === "completed") {
    badge.className = "status-badge completed";
    badge.innerHTML = `<span>✓</span> COMPLETED`;
    heroPhase.textContent = "Sync Complete";
    heroSub.textContent = "All files successfully gathered and sent with delta verification.";
    step1.classList.add("completed");
    stepStaging.classList.add("completed");
    step2.classList.add("completed");

    actionBtn.className = "btn-cta btn-start";
    actionBtn.innerHTML = `<span>▶</span> Start Sync Now`;
    actionBtn.disabled = false;
  } else if (state.status === "failed") {
    badge.className = "status-badge failed";
    badge.innerHTML = `<span>✗</span> FAILED`;
    heroPhase.textContent = "Sync Interrupted";
    heroSub.textContent = state.last_error || "An error occurred during transfer.";

    actionBtn.className = "btn-cta btn-start";
    actionBtn.innerHTML = `<span>↻</span> Retry Sync`;
    actionBtn.disabled = false;
  } else {
    // Idle or aborted
    badge.className = "status-badge idle";
    badge.innerHTML = `<span>●</span> IDLE`;
    heroPhase.textContent = "Ready to Sync";
    heroSub.textContent = "Pipeline is idle. Zero background processes active.";

    actionBtn.className = "btn-cta btn-start";
    actionBtn.innerHTML = `<span>▶</span> Start Sync Now`;
    actionBtn.disabled = false;
  }

  // Progress Bar & Stats
  const pct = state.overall_percent || 0;
  document.getElementById("stat-percent").textContent = pct + "%";
  document.getElementById("progress-bar-fill").style.width = pct + "%";

  document.getElementById("stat-speed").textContent = state.current_speed || "0 B/s";
  document.getElementById("stat-elapsed").textContent = (state.elapsed_sec || 0) + "s";

  if (state.last_success_time) {
    const d = new Date(state.last_success_time);
    document.getElementById("stat-last-sync").textContent = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  }

  // Error alert
  const alertBox = document.getElementById("alert-error");
  const alertMsg = document.getElementById("alert-error-msg");
  if (state.status === "failed" && state.last_error) {
    alertBox.style.display = "flex";
    alertMsg.textContent = state.last_error;
  } else {
    alertBox.style.display = "none";
  }

  // Render active tasks during transfer
  renderActiveTasks(state.tasks || {});
}

function renderActiveTasks(tasks) {
  const card = document.getElementById("card-active-tasks");
  const list = document.getElementById("active-tasks-list");
  const keys = Object.keys(tasks);

  if (keys.length === 0) {
    card.style.display = "none";
    return;
  }

  card.style.display = "block";
  list.innerHTML = "";

  keys.forEach((k) => {
    const t = tasks[k];
    const div = document.createElement("div");
    div.style.cssText = "background: var(--bg-card-sub); padding: 0.85rem 1rem; border-radius: 8px; margin-bottom: 0.5rem; border: 1px solid var(--border-color);";

    let statusPill = `<span style="color:#60a5fa; font-weight:600; font-size:0.8rem;">Transferring</span>`;
    if (t.status === "completed") statusPill = `<span style="color:#34d399; font-weight:600; font-size:0.8rem;">✓ Complete</span>`;
    if (t.status === "failed") statusPill = `<span style="color:#f87171; font-weight:600; font-size:0.8rem;">✗ Failed</span>`;

    div.innerHTML = `
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom: 0.35rem;">
        <div>
          <strong style="font-size:0.95rem;">${escapeHtml(t.name)}</strong>
          <span style="font-size:0.75rem; color:var(--text-muted); margin-left:0.5rem;">[${t.phase}]</span>
        </div>
        <div>${statusPill}</div>
      </div>
      <div style="display:flex; justify-content:space-between; font-size:0.8rem; color:var(--text-muted); margin-bottom:0.25rem;">
        <span>Speed: ${t.speed || "-"}</span>
        <span>${t.percent}% ${t.eta ? "(ETA: " + t.eta + ")" : ""}</span>
      </div>
      <div class="progress-track" style="height:6px;">
        <div class="progress-fill" style="width: ${t.percent}%;"></div>
      </div>
    `;
    list.appendChild(div);
  });
}

function setupDashboardActions() {
  const actionBtn = document.getElementById("btn-sync-action");
  actionBtn.addEventListener("click", async () => {
    if (!currentSyncState) return;

    if (currentSyncState.status === "gathering" || currentSyncState.status === "sending") {
      // Abort
      actionBtn.disabled = true;
      try {
        await fetch("/api/sync/stop", { method: "POST" });
      } catch (err) {
        alert("Error stopping sync: " + err);
      }
    } else {
      // Start
      actionBtn.disabled = true;
      try {
        const res = await fetch("/api/sync/start", { method: "POST" });
        if (!res.ok) {
          const txt = await res.text();
          alert("Could not start sync: " + txt);
        }
      } catch (err) {
        alert("Network error: " + err);
      }
    }
  });
}

// -------------------------------------------------------------
// Sources Page (Gather Phase)
// -------------------------------------------------------------
function renderSourcesPage() {
  const container = document.getElementById("sources-list-container");
  if (!syncConfig || !syncConfig.sources) return;

  container.innerHTML = "";

  if (syncConfig.sources.length === 0) {
    container.innerHTML = `<div style="color:var(--text-muted); padding:1rem 0;">No sources configured yet. Click "+ Add Source" to add one.</div>`;
  }

  syncConfig.sources.forEach((src, idx) => {
    const card = document.createElement("div");
    card.className = "endpoint-item";

    card.innerHTML = `
      <div class="endpoint-item-header">
        <div class="endpoint-header-left">
          <select class="form-control endpoint-type-select" onchange="updateSourceType(${idx}, this.value)">
            <option value="ssh" ${src.type === "ssh" ? "selected" : ""}>SSH Remote Server</option>
            <option value="webdav" ${src.type === "webdav" ? "selected" : ""}>WebDAV (HTTPS / Nextcloud)</option>
            <option value="smb" ${src.type === "smb" ? "selected" : ""}>SMB / NAS Share</option>
            <option value="local" ${src.type === "local" ? "selected" : ""}>Local Directory</option>
          </select>
          <input type="text" class="form-control endpoint-name-input" placeholder="Source Name *" value="${escapeHtml(src.name || "")}" oninput="syncConfig.sources[${idx}].name = this.value">
        </div>
        <div class="endpoint-header-actions">
          <span id="src-badge-${idx}"></span>
          <button type="button" class="btn btn-secondary" onclick="testSourceConn(${idx})">Test Connection</button>
          <button type="button" class="btn btn-danger" onclick="removeSource(${idx})">✕</button>
        </div>
      </div>

      ${src.type === "ssh" ? `
        <div class="form-grid">
          <div class="form-group">
            <label>Server Host / IP <span class="req-star">*</span></label>
            <input type="text" class="form-control" placeholder="192.168.1.100 or server.com" value="${escapeHtml(src.host || "")}" oninput="syncConfig.sources[${idx}].host = this.value">
          </div>
          <div class="form-group">
            <label>Port <span class="req-star">*</span></label>
            <input type="number" class="form-control" value="${src.port || 22}" oninput="syncConfig.sources[${idx}].port = parseInt(this.value, 10)">
          </div>
          <div class="form-group">
            <label>SSH Username <span class="req-star">*</span></label>
            <input type="text" class="form-control" placeholder="username" value="${escapeHtml(src.user || "")}" oninput="syncConfig.sources[${idx}].user = this.value">
          </div>
          <div class="form-group">
            <label>SSH Password <span class="optional-tag">(Optional if using SSH key)</span></label>
            <div class="password-wrapper">
              <input type="password" id="src-pass-${idx}" class="form-control" placeholder="••••••••" value="${escapeHtml(src.password || "")}" oninput="syncConfig.sources[${idx}].password = this.value">
              <button type="button" class="password-toggle" onclick="togglePasswordVisibility('src-pass-${idx}')">👁</button>
            </div>
          </div>
        </div>
      ` : ""}

      ${src.type === "smb" ? `
        <div class="form-grid">
          <div class="form-group">
            <label>NAS Server / IP <span class="req-star">*</span></label>
            <input type="text" class="form-control" placeholder="192.168.1.100 or nas.local" value="${escapeHtml(src.host || "")}" oninput="syncConfig.sources[${idx}].host = this.value">
          </div>
          <div class="form-group">
            <label>Port <span class="req-star">*</span></label>
            <input type="number" class="form-control" value="${src.port || 445}" oninput="syncConfig.sources[${idx}].port = parseInt(this.value, 10)">
          </div>
          <div class="form-group">
            <label>Share Name <span class="req-star">*</span></label>
            <input type="text" class="form-control" placeholder="e.g. backups or Public" value="${escapeHtml(src.share || "")}" oninput="syncConfig.sources[${idx}].share = this.value">
          </div>
          <div class="form-group">
            <label>Workgroup / Domain <span class="optional-tag">(Optional, default WORKGROUP)</span></label>
            <input type="text" class="form-control" placeholder="WORKGROUP" value="${escapeHtml(src.domain || "")}" oninput="syncConfig.sources[${idx}].domain = this.value">
          </div>
          <div class="form-group">
            <label>Username <span class="optional-tag">(Optional if guest)</span></label>
            <input type="text" class="form-control" placeholder="admin or guest" value="${escapeHtml(src.user || "")}" oninput="syncConfig.sources[${idx}].user = this.value">
          </div>
          <div class="form-group">
            <label>Password <span class="optional-tag">(Optional if guest)</span></label>
            <div class="password-wrapper">
              <input type="password" id="src-smb-pass-${idx}" class="form-control" placeholder="••••••••" value="${escapeHtml(src.password || "")}" oninput="syncConfig.sources[${idx}].password = this.value">
              <button type="button" class="password-toggle" onclick="togglePasswordVisibility('src-smb-pass-${idx}')">👁</button>
            </div>
          </div>
        </div>
      ` : ""}

      ${src.type === "webdav" ? `
        <div class="form-grid">
          <div class="form-group" style="grid-column: span 2;">
            <label>WebDAV Server URL <span class="req-star">*</span></label>
            <input type="text" class="form-control" placeholder="https://cloud.example.com/remote.php/dav/files/user" value="${escapeHtml(src.url || "")}" oninput="syncConfig.sources[${idx}].url = this.value">
          </div>
          <div class="form-group">
            <label>Username <span class="optional-tag">(Optional)</span></label>
            <input type="text" class="form-control" placeholder="username" value="${escapeHtml(src.user || "")}" oninput="syncConfig.sources[${idx}].user = this.value">
          </div>
          <div class="form-group">
            <label>Password / App Token <span class="optional-tag">(Optional)</span></label>
            <div class="password-wrapper">
              <input type="password" id="src-dav-pass-${idx}" class="form-control" placeholder="••••••••" value="${escapeHtml(src.password || "")}" oninput="syncConfig.sources[${idx}].password = this.value">
              <button type="button" class="password-toggle" onclick="togglePasswordVisibility('src-dav-pass-${idx}')">👁</button>
            </div>
          </div>
        </div>
      ` : ""}

      <div style="margin-top:0.5rem;">
        <div class="form-group">
          <label>${src.type === "ssh" ? "Remote Folder Path" : (src.type === "smb" ? "Folder Path inside Share (Leave empty for entire share)" : (src.type === "webdav" ? "Folder Path on WebDAV (Leave empty for root)" : "Local Folder Path"))} <span class="req-star">*</span></label>
          <div class="input-with-button">
            <input type="text" id="src-path-${idx}" class="form-control" placeholder="${src.type === "smb" ? "/photos or /" : (src.type === "webdav" ? "/backups or /" : "/path/to/source/folder")}" value="${escapeHtml(src.remote_path || "")}" oninput="syncConfig.sources[${idx}].remote_path = this.value">
            <button type="button" class="btn btn-secondary btn-browse" onclick="openFileBrowser('source', ${idx})">
              <span>📁</span> Browse
            </button>
          </div>
        </div>

        <!-- File Filter Mode & Patterns -->
        <div class="form-group" style="margin-top:0.75rem;">
          <div class="form-grid" style="grid-template-columns: 1fr 2fr;">
            <div>
              <label>File Filter Option</label>
              <select class="form-control" onchange="updateFilterMode(${idx}, this.value)">
                <option value="all" ${(src.filter_mode || "all") === "all" ? "selected" : ""}>Sync All Files (No Filter)</option>
                <option value="include_only" ${src.filter_mode === "include_only" ? "selected" : ""}>Only Include Matching Files</option>
                <option value="exclude" ${src.filter_mode === "exclude" ? "selected" : ""}>Exclude Matching Files</option>
              </select>
            </div>
            <div id="filter-box-${idx}" style="${(src.filter_mode || "all") === "all" ? "display:none;" : ""}">
              <label id="filter-label-${idx}">
                ${src.filter_mode === "include_only" ? "Files to Include (e.g. *.txt, *.pdf)" : "Files to Exclude (e.g. *.tmp, cache/)"}
              </label>
              <input type="text" class="form-control" id="filter-input-${idx}"
                placeholder="${src.filter_mode === "include_only" ? "e.g. *.txt, *.pdf" : "e.g. *.tmp, cache/, .git/"}" 
                value="${escapeHtml(src.filter_patterns || "")}" 
                oninput="syncConfig.sources[${idx}].filter_patterns = this.value">
            </div>
          </div>
        </div>
      </div>
    `;

    container.appendChild(card);
  });

  document.getElementById("btn-add-source").onclick = () => {
    syncConfig.sources.push({
      id: "src-" + Date.now(),
      name: "",
      type: "ssh",
      host: "",
      port: 22,
      user: "",
      password: "",
      remote_path: "",
      filter_mode: "all",
      filter_patterns: ""
    });
    renderSourcesPage();
  };

  document.getElementById("btn-save-sources").onclick = saveConfigToServer;
}

window.updateFilterMode = function(idx, mode) {
  syncConfig.sources[idx].filter_mode = mode;
  const box = document.getElementById(`filter-box-${idx}`);
  const label = document.getElementById(`filter-label-${idx}`);
  const input = document.getElementById(`filter-input-${idx}`);
  if (box && label && input) {
    if (mode === "all") {
      box.style.display = "none";
    } else {
      box.style.display = "block";
      if (mode === "include_only") {
        label.textContent = "Files to Include (e.g. *.txt, *.pdf)";
        input.placeholder = "e.g. *.txt, *.pdf";
      } else {
        label.textContent = "Files to Exclude (e.g. *.tmp, cache/)";
        input.placeholder = "e.g. *.tmp, cache/, .git/";
      }
    }
  }
};

window.updateSourceType = function(idx, type) {
  syncConfig.sources[idx].type = type;
  renderSourcesPage();
};

window.removeSource = function(idx) {
  syncConfig.sources.splice(idx, 1);
  renderSourcesPage();
};

window.testSourceConn = async function(idx) {
  const s = syncConfig.sources[idx];
  const badge = document.getElementById(`src-badge-${idx}`);
  badge.className = "test-result-badge test-pending";
  badge.textContent = "Testing...";

  try {
    const res = await fetch("/api/test-connection", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        type: s.type,
        host: s.host,
        port: s.port || (s.type === "smb" ? 445 : 22),
        share: s.share,
        url: s.url,
        user: s.user,
        password: s.password,
        domain: s.domain,
        path: s.remote_path
      })
    });
    const data = await res.json();
    if (data.ok) {
      badge.className = "test-result-badge test-success";
      badge.textContent = "✓ " + data.message;
    } else {
      badge.className = "test-result-badge test-error";
      badge.textContent = "✗ " + data.message;
    }
  } catch (err) {
    badge.className = "test-result-badge test-error";
    badge.textContent = "✗ " + err;
  }
};

// -------------------------------------------------------------
// Destination Page (Send Phase)
// -------------------------------------------------------------
function renderDestinationPage() {
  const container = document.getElementById("dest-card-container");
  if (!syncConfig || !syncConfig.destination) return;

  const dest = syncConfig.destination;
  container.innerHTML = `
    <div class="endpoint-item">
      <div class="endpoint-item-header">
        <div class="endpoint-header-left">
          <select class="form-control endpoint-type-select" onchange="updateDestType(this.value)">
            <option value="ssh" ${dest.type === "ssh" ? "selected" : ""}>SSH Remote PC / Server</option>
            <option value="webdav" ${dest.type === "webdav" ? "selected" : ""}>WebDAV (HTTPS / Nextcloud)</option>
            <option value="smb" ${dest.type === "smb" ? "selected" : ""}>SMB / NAS Share</option>
            <option value="local" ${dest.type === "local" ? "selected" : ""}>Local Directory</option>
          </select>
          <input type="text" class="form-control endpoint-name-input" placeholder="Destination Name *" value="${escapeHtml(dest.name || "")}" oninput="syncConfig.destination.name = this.value">
        </div>
        <div class="endpoint-header-actions">
          <span id="dest-test-badge"></span>
          <button type="button" class="btn btn-secondary" onclick="testDestConn()">Test Connection</button>
        </div>
      </div>

      ${dest.type === "ssh" ? `
        <div class="form-grid">
          <div class="form-group">
            <label>Remote PC Server / IP <span class="req-star">*</span></label>
            <input type="text" class="form-control" placeholder="192.168.1.150" value="${escapeHtml(dest.host || "")}" oninput="syncConfig.destination.host = this.value">
          </div>
          <div class="form-group">
            <label>Port <span class="req-star">*</span></label>
            <input type="number" class="form-control" value="${dest.port || 22}" oninput="syncConfig.destination.port = parseInt(this.value, 10)">
          </div>
          <div class="form-group">
            <label>SSH Username <span class="req-star">*</span></label>
            <input type="text" class="form-control" placeholder="syncuser" value="${escapeHtml(dest.user || "")}" oninput="syncConfig.destination.user = this.value">
          </div>
          <div class="form-group">
            <label>SSH Password <span class="optional-tag">(Optional if using SSH key)</span></label>
            <div class="password-wrapper">
              <input type="password" id="dest-pass" class="form-control" placeholder="••••••••" value="${escapeHtml(dest.password || "")}" oninput="syncConfig.destination.password = this.value">
              <button type="button" class="password-toggle" onclick="togglePasswordVisibility('dest-pass')">👁</button>
            </div>
          </div>
        </div>
      ` : ""}

      ${dest.type === "smb" ? `
        <div class="form-grid">
          <div class="form-group">
            <label>NAS Server / IP <span class="req-star">*</span></label>
            <input type="text" class="form-control" placeholder="192.168.1.150 or nas.local" value="${escapeHtml(dest.host || "")}" oninput="syncConfig.destination.host = this.value">
          </div>
          <div class="form-group">
            <label>Port <span class="req-star">*</span></label>
            <input type="number" class="form-control" value="${dest.port || 445}" oninput="syncConfig.destination.port = parseInt(this.value, 10)">
          </div>
          <div class="form-group">
            <label>Share Name <span class="req-star">*</span></label>
            <input type="text" class="form-control" placeholder="e.g. mirror or Shared" value="${escapeHtml(dest.share || "")}" oninput="syncConfig.destination.share = this.value">
          </div>
          <div class="form-group">
            <label>Workgroup / Domain <span class="optional-tag">(Optional, default WORKGROUP)</span></label>
            <input type="text" class="form-control" placeholder="WORKGROUP" value="${escapeHtml(dest.domain || "")}" oninput="syncConfig.destination.domain = this.value">
          </div>
          <div class="form-group">
            <label>Username <span class="optional-tag">(Optional if guest)</span></label>
            <input type="text" class="form-control" placeholder="admin or guest" value="${escapeHtml(dest.user || "")}" oninput="syncConfig.destination.user = this.value">
          </div>
          <div class="form-group">
            <label>Password <span class="optional-tag">(Optional if guest)</span></label>
            <div class="password-wrapper">
              <input type="password" id="dest-smb-pass" class="form-control" placeholder="••••••••" value="${escapeHtml(dest.password || "")}" oninput="syncConfig.destination.password = this.value">
              <button type="button" class="password-toggle" onclick="togglePasswordVisibility('dest-smb-pass')">👁</button>
            </div>
          </div>
        </div>
      ` : ""}

      ${dest.type === "webdav" ? `
        <div class="form-grid">
          <div class="form-group" style="grid-column: span 2;">
            <label>WebDAV Server URL <span class="req-star">*</span></label>
            <input type="text" class="form-control" placeholder="https://cloud.example.com/remote.php/dav/files/user" value="${escapeHtml(dest.url || "")}" oninput="syncConfig.destination.url = this.value">
          </div>
          <div class="form-group">
            <label>Username <span class="optional-tag">(Optional)</span></label>
            <input type="text" class="form-control" placeholder="username" value="${escapeHtml(dest.user || "")}" oninput="syncConfig.destination.user = this.value">
          </div>
          <div class="form-group">
            <label>Password / App Token <span class="optional-tag">(Optional)</span></label>
            <div class="password-wrapper">
              <input type="password" id="dest-dav-pass" class="form-control" placeholder="••••••••" value="${escapeHtml(dest.password || "")}" oninput="syncConfig.destination.password = this.value">
              <button type="button" class="password-toggle" onclick="togglePasswordVisibility('dest-dav-pass')">👁</button>
            </div>
          </div>
        </div>
      ` : ""}

      <div style="margin-top:0.5rem;">
        <div class="form-group">
          <label>${dest.type === "ssh" ? "Remote Destination Path" : (dest.type === "smb" ? "Folder Path inside Share" : (dest.type === "webdav" ? "Destination Folder on WebDAV (Leave empty for root)" : "Local Destination Path"))} <span class="req-star">*</span></label>
          <div class="input-with-button">
            <input type="text" id="dest-path" class="form-control" placeholder="${dest.type === "smb" ? "/mirror or /" : (dest.type === "webdav" ? "/mirror or /" : "/data/mirror")}" value="${escapeHtml(dest.remote_path || "")}" oninput="syncConfig.destination.remote_path = this.value">
            <button type="button" class="btn btn-secondary btn-browse" onclick="openFileBrowser('destination')">
              <span>📁</span> Browse
            </button>
          </div>
        </div>
        ${dest.type === "ssh" ? `
          <div class="form-group" style="display:flex; align-items:center; margin-top:0.75rem;">
            <label style="display:flex; align-items:center; gap:0.5rem; cursor:pointer; text-transform:none; font-size:0.85rem;">
              <input type="checkbox" ${dest.preflight_check ? "checked" : ""} onchange="syncConfig.destination.preflight_check = this.checked">
              Probe if device is online before pushing
            </label>
          </div>
        ` : ""}
      </div>
    </div>
  `;

  document.getElementById("btn-save-destination").onclick = saveConfigToServer;
}

window.updateDestType = function(type) {
  syncConfig.destination.type = type;
  renderDestinationPage();
};

window.testDestConn = async function() {
  const d = syncConfig.destination;
  const badge = document.getElementById("dest-test-badge");
  badge.className = "test-result-badge test-pending";
  badge.textContent = "Testing...";

  try {
    const res = await fetch("/api/test-connection", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        type: d.type,
        host: d.host,
        port: d.port || (d.type === "smb" ? 445 : 22),
        share: d.share,
        url: d.url,
        user: d.user,
        password: d.password,
        domain: d.domain,
        path: d.remote_path
      })
    });
    const data = await res.json();
    if (data.ok) {
      badge.className = "test-result-badge test-success";
      badge.textContent = "✓ " + data.message;
    } else {
      badge.className = "test-result-badge test-error";
      badge.textContent = "✗ " + data.message;
    }
  } catch (err) {
    badge.className = "test-result-badge test-error";
    badge.textContent = "✗ " + err;
  }
};

// -------------------------------------------------------------
// Schedule & Settings Page
// -------------------------------------------------------------
function renderSettingsPage() {
  if (!syncConfig) return;

  const currentPreset = syncConfig.schedule_preset || "manual";
  const cards = document.querySelectorAll("#schedule-cards .radio-card");

  cards.forEach((c) => {
    if (c.getAttribute("data-preset") === currentPreset) {
      c.classList.add("active");
    } else {
      c.classList.remove("active");
    }

    c.onclick = () => {
      cards.forEach((b) => b.classList.remove("active"));
      c.classList.add("active");
      const preset = c.getAttribute("data-preset");
      syncConfig.schedule_preset = preset;

      const presetMap = { "manual": 0, "15m": 15, "60m": 60, "360m": 360, "1440m": 1440 };
      syncConfig.interval_minutes = presetMap[preset] || 0;
    };
  });

  document.getElementById("cfg-workers").value = syncConfig.parallel_workers || 4;
  document.getElementById("cfg-net-timeout").value = syncConfig.network_timeout_sec || 15;
  document.getElementById("cfg-staging-dir").value = syncConfig.local_staging_dir || "";

  // Backup & Retention
  const backupEnabledEl = document.getElementById("cfg-backup-enabled");
  const backupOptionsContainer = document.getElementById("backup-options-container");
  const backupCountEl = document.getElementById("cfg-backup-count");
  const backupDirNameEl = document.getElementById("cfg-backup-dirname");

  if (backupEnabledEl) {
    backupEnabledEl.checked = !!syncConfig.backup_enabled;
    if (backupOptionsContainer) {
      backupOptionsContainer.style.display = backupEnabledEl.checked ? "block" : "none";
    }
    backupEnabledEl.onchange = () => {
      if (backupOptionsContainer) {
        backupOptionsContainer.style.display = backupEnabledEl.checked ? "block" : "none";
      }
    };
  }

  if (backupCountEl) {
    backupCountEl.value = syncConfig.backup_retention_count || 5;
  }
  if (backupDirNameEl) {
    backupDirNameEl.value = syncConfig.backup_dir_name || ".synchop-backups";
  }

  document.getElementById("btn-save-settings").onclick = () => {
    syncConfig.parallel_workers = parseInt(document.getElementById("cfg-workers").value, 10) || 4;
    syncConfig.network_timeout_sec = parseInt(document.getElementById("cfg-net-timeout").value, 10) || 15;
    syncConfig.local_staging_dir = document.getElementById("cfg-staging-dir").value;
    syncConfig.backup_enabled = !!(backupEnabledEl && backupEnabledEl.checked);
    syncConfig.backup_retention_count = parseInt(backupCountEl ? backupCountEl.value : 5, 10) || 5;
    syncConfig.backup_dir_name = (backupDirNameEl ? backupDirNameEl.value.trim() : "") || ".synchop-backups";
    saveConfigToServer();
  };
}

async function saveConfigToServer() {
  // Validation of Sources
  for (let i = 0; i < (syncConfig.sources || []).length; i++) {
    const s = syncConfig.sources[i];
    if (!s.name || !s.name.trim()) {
      alert(`Source #${i + 1}: Name is required.`);
      return;
    }
    if ((s.type === "ssh" || s.type === "smb") && (!s.host || !s.host.trim())) {
      alert(`Source "${s.name}": Server Host / IP is required.`);
      return;
    }
    if (s.type === "webdav" && (!s.url || !s.url.trim())) {
      alert(`Source "${s.name}": WebDAV Server URL is required.`);
      return;
    }
    if (s.type === "ssh" && (!s.user || !s.user.trim())) {
      alert(`Source "${s.name}": SSH Username is required.`);
      return;
    }
    if (s.type === "smb" && (!s.share || !s.share.trim())) {
      alert(`Source "${s.name}": Share Name is required.`);
      return;
    }
    if (s.type !== "smb" && (!s.remote_path || !s.remote_path.trim())) {
      alert(`Source "${s.name}": Folder Path is required.`);
      return;
    }
  }

  // Validation of Destination
  const d = syncConfig.destination;
  if (d && d.name && d.name.trim()) {
    if ((d.type === "ssh" || d.type === "smb") && (!d.host || !d.host.trim())) {
      alert(`Destination "${d.name}": Server Host / IP is required.`);
      return;
    }
    if (d.type === "webdav" && (!d.url || !d.url.trim())) {
      alert(`Destination "${d.name}": WebDAV Server URL is required.`);
      return;
    }
    if (d.type === "ssh" && (!d.user || !d.user.trim())) {
      alert(`Destination "${d.name}": SSH Username is required.`);
      return;
    }
    if (d.type === "smb" && (!d.share || !d.share.trim())) {
      alert(`Destination "${d.name}": Share Name is required.`);
      return;
    }
    if (d.type !== "smb" && (!d.remote_path || !d.remote_path.trim())) {
      alert(`Destination "${d.name}": Destination Folder Path is required.`);
      return;
    }
  }

  try {
    const res = await fetch("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(syncConfig)
    });

    if (res.ok) {
      alert("Settings saved successfully!");
      fetchStateAndConfig();
    } else {
      alert("Failed to save settings: " + (await res.text()));
    }
  } catch (err) {
    alert("Network error: " + err);
  }
}

// -------------------------------------------------------------
// Utilities
// -------------------------------------------------------------
window.togglePasswordVisibility = function(id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.type = el.type === "password" ? "text" : "password";
};

function escapeHtml(text) {
  return String(text || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

// -------------------------------------------------------------
// Execution History & Audit Log
// -------------------------------------------------------------
function setupHistoryUI() {
  document.getElementById("btn-refresh-history")?.addEventListener("click", () => fetchHistory(true));
  document.getElementById("btn-clear-history")?.addEventListener("click", clearHistory);

  document.getElementById("hist-search-input")?.addEventListener("input", (e) => {
    historySearchQuery = e.target.value.toLowerCase().trim();
    renderHistoryList();
  });

  document.querySelectorAll(".history-filter-pills .filter-pill").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".history-filter-pills .filter-pill").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      activeHistoryFilter = btn.getAttribute("data-filter");
      renderHistoryList();
    });
  });

  document.getElementById("btn-dash-view-history")?.addEventListener("click", () => {
    document.querySelectorAll(".nav-btn").forEach((b) => b.classList.remove("active"));
    const histBtn = document.querySelector('.nav-btn[data-page="page-history"]');
    if (histBtn) histBtn.classList.add("active");

    document.querySelectorAll(".page-view").forEach((p) => p.classList.remove("active"));
    const histPage = document.getElementById("page-history");
    if (histPage) histPage.classList.add("active");

    fetchHistory().then(() => {
      if (historyData.runs && historyData.runs.length > 0) {
        expandedRunIds.add(historyData.runs[0].id);
        renderHistoryList();
      }
      window.scrollTo({ top: 0, behavior: "smooth" });
    });
  });
}

async function fetchHistory(showToast = false) {
  try {
    const res = await fetch("/api/history");
    if (!res.ok) return;
    historyData = await res.json();

    const summary = historyData.summary || {};
    const totalEl = document.getElementById("hist-total-runs");
    const rateEl = document.getElementById("hist-success-rate");
    const sentEl = document.getElementById("hist-total-sent");

    if (totalEl) totalEl.textContent = summary.total_runs || 0;
    if (rateEl) {
      let rate = 100;
      if (summary.total_runs > 0) {
        rate = Math.round(((summary.successful_runs || 0) / summary.total_runs) * 100);
      }
      rateEl.textContent = rate + "%";
    }
    if (sentEl) sentEl.textContent = formatBytes(summary.total_bytes_sent || 0);

    // Update Dashboard Latest Run Box
    updateDashboardLastRun(summary.last_run);

    renderHistoryList();

    if (showToast) {
      const refBtn = document.getElementById("btn-refresh-history");
      if (refBtn) {
        const orig = refBtn.innerHTML;
        refBtn.innerHTML = `<span>✓</span> Updated`;
        setTimeout(() => { refBtn.innerHTML = orig; }, 1200);
      }
    }
  } catch (err) {
    console.error("Failed to fetch history:", err);
  }
}

function updateDashboardLastRun(lastRun) {
  const card = document.getElementById("dash-last-run-card");
  if (!card) return;

  if (!lastRun) {
    card.style.display = "none";
    return;
  }

  card.style.display = "flex";
  const badge = document.getElementById("dash-last-run-badge");
  const timeEl = document.getElementById("dash-last-run-time");
  const statsEl = document.getElementById("dash-last-run-stats");

  badge.className = `badge-mini ${lastRun.status}`;
  if (lastRun.status === "completed") {
    badge.textContent = "✓ Completed";
  } else if (lastRun.status === "failed") {
    badge.textContent = "✗ Failed";
  } else {
    badge.textContent = "● " + lastRun.status.toUpperCase();
  }

  timeEl.textContent = formatRelativeTime(lastRun.end_time || lastRun.start_time);

  let detailsText = `<strong>${lastRun.files_changed || 0}</strong> files changed • <strong>${formatBytes(lastRun.send_size || 0)}</strong> sent (${formatBytes(lastRun.total_size || 0)} total)`;
  if (lastRun.status === "failed" && lastRun.error_message) {
    detailsText = `<span style="color:var(--accent-rose); font-weight:600;">Error: ${escapeHtml(lastRun.error_message)}</span>`;
  }
  statsEl.innerHTML = detailsText;
}

async function clearHistory() {
  if (!confirm("Are you sure you want to clear all sync history records? This cannot be undone.")) {
    return;
  }

  try {
    const res = await fetch("/api/history", { method: "DELETE" });
    if (res.ok) {
      historyData = { runs: [], summary: {} };
      expandedRunIds.clear();
      fetchHistory();
    } else {
      alert("Failed to clear history: " + (await res.text()));
    }
  } catch (err) {
    alert("Network error: " + err);
  }
}

function toggleRunExpand(runId) {
  if (expandedRunIds.has(runId)) {
    expandedRunIds.delete(runId);
  } else {
    expandedRunIds.add(runId);
  }
  renderHistoryList();
}

function renderHistoryList() {
  const container = document.getElementById("history-list-container");
  if (!container) return;

  let runs = historyData.runs || [];

  // Filter by status
  if (activeHistoryFilter !== "all") {
    runs = runs.filter((r) => r.status === activeHistoryFilter);
  }

  // Filter by search query
  if (historySearchQuery) {
    runs = runs.filter((r) => {
      if ((r.destination_name || "").toLowerCase().includes(historySearchQuery)) return true;
      if ((r.trigger || "").toLowerCase().includes(historySearchQuery)) return true;
      if (r.files && r.files.some((f) => f.path.toLowerCase().includes(historySearchQuery))) return true;
      return false;
    });
  }

  if (runs.length === 0) {
    container.innerHTML = `
      <div class="empty-history">
        <div style="font-size: 2.2rem; margin-bottom: 0.5rem; opacity: 0.7;">📜</div>
        <h4 style="font-size: 1rem; color: var(--text-main); margin-bottom: 0.25rem;">No History Records Found</h4>
        <p style="font-size: 0.85rem; color: var(--text-muted);">
          ${historyData.runs && historyData.runs.length > 0 ? "No runs match your search or filter." : "Sync execution records will appear here automatically when the sync runs."}
        </p>
      </div>
    `;
    return;
  }

  container.innerHTML = "";

  runs.forEach((run) => {
    const isExpanded = expandedRunIds.has(run.id);
    const card = document.createElement("div");
    card.className = `history-run-card ${isExpanded ? "expanded" : ""}`;

    // Status Badge
    let statusBadge = `<span class="badge-mini completed">✓ Completed</span>`;
    if (run.status === "failed") {
      statusBadge = `<span class="badge-mini failed">✗ Failed</span>`;
    } else if (run.status === "aborted") {
      statusBadge = `<span class="badge-mini aborted">● Aborted</span>`;
    }

    // Bandwidth Savings Tag
    let savingsTag = "";
    if (run.total_size > run.send_size && run.send_size > 0 && run.total_size > 0) {
      const pct = Math.round((1 - run.send_size / run.total_size) * 100);
      if (pct > 0) {
        savingsTag = `<span class="savings-tag" title="Delta bandwidth saved vs full snapshot">${pct}% saved</span>`;
      }
    }

    // Build Header
    const cardHeader = document.createElement("div");
    cardHeader.className = "run-card-header";
    const backupBadge = run.backup_dir ? `<span class="trigger-pill" style="background: rgba(16, 185, 129, 0.15); border-color: rgba(16, 185, 129, 0.3); color: #34d399;" title="Destination backup preserved">🛡️ Backup</span>` : "";
    cardHeader.innerHTML = `
      <div class="run-header-left">
        ${statusBadge}
        <span class="trigger-pill">${escapeHtml(run.trigger || "manual")}</span>
        ${backupBadge}
        <span class="run-time-main">${formatDateTime(run.start_time)}</span>
        <span class="run-time-relative">(${formatRelativeTime(run.start_time)})</span>
      </div>
      <div class="run-header-right">
        <span class="run-duration-badge">⏱ ${formatDuration(run.duration_sec)}</span>
        <button type="button" class="run-toggle-btn">
          ${isExpanded ? "▲ Hide Details" : "▼ Details & Files (" + (run.files ? run.files.length : 0) + ")"}
        </button>
      </div>
    `;

    cardHeader.addEventListener("click", () => toggleRunExpand(run.id));
    card.appendChild(cardHeader);

    // Build Metrics Row
    const metricsGrid = document.createElement("div");
    metricsGrid.className = "run-metrics-grid";
    metricsGrid.innerHTML = `
      <div class="run-metric-cell">
        <span class="run-metric-title">Files Changed</span>
        <span class="run-metric-val">${run.files_changed || 0}</span>
      </div>
      <div class="run-metric-cell">
        <span class="run-metric-title">Send Size (Payload)</span>
        <span class="run-metric-val">${formatBytes(run.send_size || 0)} ${savingsTag}</span>
      </div>
      <div class="run-metric-cell">
        <span class="run-metric-title">Total Snapshot Size</span>
        <span class="run-metric-val">${formatBytes(run.total_size || 0)}</span>
      </div>
      <div class="run-metric-cell">
        <span class="run-metric-title">Destination</span>
        <span class="run-metric-val" style="font-size:0.85rem; font-weight:600;">
          ${escapeHtml(run.destination_name || "Destination")} <span style="font-size:0.75rem; color:var(--text-muted); font-weight:normal;">[${escapeHtml(run.destination_type || "SSH")}]</span>
        </span>
      </div>
    `;
    card.appendChild(metricsGrid);

    // Build Expandable Drawer
    if (isExpanded) {
      const drawer = document.createElement("div");
      drawer.className = "run-details-drawer";

      if (run.backup_dir) {
        const backupBanner = document.createElement("div");
        backupBanner.style.cssText = "background: rgba(59, 130, 246, 0.1); border: 1px solid rgba(59, 130, 246, 0.25); border-radius: 8px; padding: 0.6rem 0.85rem; margin-bottom: 1.25rem; display: flex; align-items: center; gap: 0.5rem; font-size: 0.82rem; color: #93c5fd;";
        backupBanner.innerHTML = `<span>🛡️</span> <span>Versioned backup preserved in: <code>${escapeHtml(run.backup_dir)}</code></span>`;
        drawer.appendChild(backupBanner);
      }

      // If failed, show error box
      if (run.status === "failed" && run.error_message) {
        const errBox = document.createElement("div");
        errBox.className = "alert-box";
        errBox.style.cssText = "margin-bottom: 1.25rem; display: flex;";
        errBox.innerHTML = `
          <span style="font-size: 1.2rem;">⚠️</span>
          <div>
            <strong style="display: block; margin-bottom: 0.25rem;">Failure Details</strong>
            <span>${escapeHtml(run.error_message)}</span>
          </div>
        `;
        drawer.appendChild(errBox);
      }

      // Changed Files Section
      const filesSection = document.createElement("div");
      filesSection.style.marginBottom = "1.5rem";

      const filesCount = run.files ? run.files.length : 0;
      filesSection.innerHTML = `
        <div class="run-section-title">
          <span>Files Actually Sent to Destination (${filesCount})</span>
          ${filesCount > 5 ? `<input type="text" class="form-control file-filter-input" data-run-id="${run.id}" placeholder="Filter changed files...">` : ""}
        </div>
      `;

      if (filesCount === 0) {
        const noFiles = document.createElement("div");
        noFiles.style.cssText = "padding: 0.85rem; background: rgba(255,255,255,0.02); border-radius: 8px; border: 1px dashed var(--border-color); text-align: center; color: var(--text-muted); font-size: 0.85rem;";
        noFiles.innerHTML = "✨ 1:1 Mirror was already up-to-date. Zero file modifications were needed.";
        filesSection.appendChild(noFiles);
      } else {
        const filterVal = (runFileSearchQueries[run.id] || "").toLowerCase();
        let displayFiles = run.files;
        if (filterVal) {
          displayFiles = displayFiles.filter((f) => f.path.toLowerCase().includes(filterVal));
        }

        const tableContainer = document.createElement("div");
        tableContainer.style.cssText = "max-height: 280px; overflow-y: auto; border: 1px solid var(--border-color); border-radius: 8px;";

        const table = document.createElement("table");
        table.className = "run-files-table";
        table.innerHTML = `
          <thead>
            <tr>
              <th style="width: 90px;">Action</th>
              <th>File Path</th>
              <th style="width: 100px; text-align: right;">Size</th>
            </tr>
          </thead>
          <tbody></tbody>
        `;

        const tbody = table.querySelector("tbody");
        displayFiles.forEach((file) => {
          let badgeClass = "modified";
          let actionText = "~ Modified";
          if (file.action === "created") {
            badgeClass = "created";
            actionText = "+ Added";
          } else if (file.action === "deleted") {
            badgeClass = "deleted";
            actionText = "- Deleted";
          }

          const tr = document.createElement("tr");
          tr.innerHTML = `
            <td><span class="file-badge ${badgeClass}">${actionText}</span></td>
            <td style="font-family: monospace; word-break: break-all;">${escapeHtml(file.path)}</td>
            <td style="text-align: right; color: var(--text-muted);">${formatBytes(file.size)}</td>
          `;
          tbody.appendChild(tr);
        });

        tableContainer.appendChild(table);
        filesSection.appendChild(tableContainer);
      }

      drawer.appendChild(filesSection);

      // Tasks Breakdown Section
      if (run.tasks && run.tasks.length > 0) {
        const tasksSection = document.createElement("div");
        tasksSection.innerHTML = `
          <div class="run-section-title">
            <span>Pipeline Tasks Breakdown (${run.tasks.length})</span>
          </div>
        `;

        const tasksList = document.createElement("div");
        tasksList.style.cssText = "display: flex; flex-direction: column; gap: 0.5rem;";

        run.tasks.forEach((t) => {
          const tDiv = document.createElement("div");
          tDiv.style.cssText = "padding: 0.65rem 0.85rem; background: rgba(255,255,255,0.03); border-radius: 6px; border: 1px solid rgba(255,255,255,0.06); display: flex; justify-content: space-between; align-items: center; font-size: 0.82rem; flex-wrap: wrap; gap: 0.5rem;";

          let tStatus = `<span style="color:#34d399; font-weight:600;">✓ Done</span>`;
          if (t.status === "failed") tStatus = `<span style="color:#f87171; font-weight:600;">✗ Failed</span>`;
          if (t.status === "aborted") tStatus = `<span style="color:#fbbf24; font-weight:600;">● Aborted</span>`;

          tDiv.innerHTML = `
            <div>
              <strong>${escapeHtml(t.name)}</strong>
              <span style="color:var(--text-muted); font-size:0.75rem; margin-left:0.4rem;">[Phase: ${escapeHtml(t.phase)}]</span>
            </div>
            <div style="display: flex; gap: 1rem; align-items: center;">
              <span style="color:var(--text-muted);">${t.files_changed || 0} changed • ${formatBytes(t.bytes_transferred || 0)} transferred</span>
              ${tStatus}
            </div>
          `;
          tasksList.appendChild(tDiv);
        });

        tasksSection.appendChild(tasksList);
        drawer.appendChild(tasksSection);
      }

      card.appendChild(drawer);

      // Wire up file filter input inside drawer if present
      const filterInput = drawer.querySelector(".file-filter-input");
      if (filterInput) {
        filterInput.value = runFileSearchQueries[run.id] || "";
        filterInput.addEventListener("input", (e) => {
          runFileSearchQueries[run.id] = e.target.value;
          renderHistoryList();
          // Restore focus
          const updatedInput = document.querySelector(`.file-filter-input[data-run-id="${run.id}"]`);
          if (updatedInput) {
            updatedInput.focus();
            updatedInput.selectionStart = updatedInput.selectionEnd = updatedInput.value.length;
          }
        });
      }
    }

    container.appendChild(card);
  });
}

function formatBytes(bytes) {
  if (bytes === 0) return "0 B";
  if (!bytes || isNaN(bytes)) return "-";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  if (i < 0) return bytes + " B";
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
}

function formatDuration(sec) {
  if (!sec || sec <= 0) return "0s";
  if (sec < 60) return sec + "s";
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return m + "m " + s + "s";
}

function formatRelativeTime(dateStr) {
  if (!dateStr) return "-";
  const d = new Date(dateStr);
  const diffSec = Math.max(0, Math.floor((new Date() - d) / 1000));
  if (diffSec < 10) return "Just now";
  if (diffSec < 60) return `${diffSec}s ago`;
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHours = Math.floor(diffMin / 60);
  if (diffHours < 24) return `${diffHours}h ago`;
  const diffDays = Math.floor(diffHours / 24);
  return `${diffDays}d ago`;
}

function formatDateTime(dateStr) {
  if (!dateStr) return "-";
  const d = new Date(dateStr);
  return d.toLocaleString([], {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  });
}

// -------------------------------------------------------------
// Interactive Directory Browser Modal
// -------------------------------------------------------------
let browserTarget = null; // { type: 'source'|'destination'|'staging', index?: number }
let browserCurrentPath = "";
let browserParentPath = "";
let browserEndpoint = null;

function setupBrowserModalEvents() {
  document.getElementById("btn-browser-up")?.addEventListener("click", () => {
    if (browserParentPath) {
      loadDirectory(browserParentPath);
    }
  });

  document.getElementById("btn-browser-refresh")?.addEventListener("click", () => {
    loadDirectory(browserCurrentPath);
  });

  document.getElementById("btn-browser-go")?.addEventListener("click", () => {
    const val = document.getElementById("browser-current-path-input")?.value.trim();
    if (val) loadDirectory(val);
  });

  document.getElementById("browser-current-path-input")?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      const val = e.target.value.trim();
      if (val) loadDirectory(val);
    }
  });

  document.getElementById("btn-browser-select")?.addEventListener("click", () => {
    if (browserCurrentPath) {
      selectAndClose(browserCurrentPath);
    }
  });

  // Close modal when clicking backdrop outside dialog
  const modal = document.getElementById("browser-modal");
  modal?.addEventListener("click", (e) => {
    if (e.target === modal) closeFileBrowser();
  });
}

window.openFileBrowser = function(targetType, idx) {
  browserTarget = { type: targetType, index: idx };
  browserEndpoint = {};

  if (targetType === "source") {
    const s = syncConfig.sources[idx];
    browserEndpoint.type = s.type;
    browserEndpoint.host = s.host;
    browserEndpoint.port = s.port || (s.type === "smb" ? 445 : 22);
    browserEndpoint.share = s.share;
    browserEndpoint.url = s.url;
    browserEndpoint.user = s.user;
    browserEndpoint.password = s.password;
    browserEndpoint.domain = s.domain;
    browserEndpoint.path = s.remote_path;

    // Check if user recently typed in the input fields before saving
    if (s.type === "ssh") {
      const passEl = document.getElementById(`src-pass-${idx}`);
      if (passEl && passEl.value) browserEndpoint.password = passEl.value;
    } else if (s.type === "smb") {
      const passEl = document.getElementById(`src-smb-pass-${idx}`);
      if (passEl && passEl.value) browserEndpoint.password = passEl.value;
    } else if (s.type === "webdav") {
      const passEl = document.getElementById(`src-dav-pass-${idx}`);
      if (passEl && passEl.value) browserEndpoint.password = passEl.value;
    }

    // Validation
    if (s.type === "ssh" && !browserEndpoint.host) {
      alert("Please enter the Server Host / IP first before browsing.");
      return;
    }
    if (s.type === "smb" && (!browserEndpoint.host || !browserEndpoint.share)) {
      alert("Please enter the NAS Server Host and Share Name first before browsing.");
      return;
    }
    if (s.type === "webdav" && !browserEndpoint.url) {
      alert("Please enter the WebDAV Server URL first before browsing.");
      return;
    }
  } else if (targetType === "destination") {
    const d = syncConfig.destination;
    browserEndpoint.type = d.type;
    browserEndpoint.host = d.host;
    browserEndpoint.port = d.port || (d.type === "smb" ? 445 : 22);
    browserEndpoint.share = d.share;
    browserEndpoint.url = d.url;
    browserEndpoint.user = d.user;
    browserEndpoint.password = d.password;
    browserEndpoint.domain = d.domain;
    browserEndpoint.path = d.remote_path;

    if (d.type === "ssh") {
      const passEl = document.getElementById("dest-pass");
      if (passEl && passEl.value) browserEndpoint.password = passEl.value;
    } else if (d.type === "smb") {
      const passEl = document.getElementById("dest-smb-pass");
      if (passEl && passEl.value) browserEndpoint.password = passEl.value;
    } else if (d.type === "webdav") {
      const passEl = document.getElementById("dest-dav-pass");
      if (passEl && passEl.value) browserEndpoint.password = passEl.value;
    }

    if (d.type === "ssh" && !browserEndpoint.host) {
      alert("Please enter the Destination Host / IP first before browsing.");
      return;
    }
    if (d.type === "smb" && (!browserEndpoint.host || !browserEndpoint.share)) {
      alert("Please enter the Destination NAS Host and Share Name first before browsing.");
      return;
    }
    if (d.type === "webdav" && !browserEndpoint.url) {
      alert("Please enter the Destination WebDAV Server URL first before browsing.");
      return;
    }
  } else if (targetType === "staging") {
    browserEndpoint.type = "local";
    const stagingInput = document.getElementById("cfg-staging-dir");
    browserEndpoint.path = stagingInput ? stagingInput.value : (syncConfig.local_staging_dir || "");
  }

  // Set modal header text
  const titleEl = document.getElementById("browser-modal-title");
  const subEl = document.getElementById("browser-modal-subtitle");
  if (browserEndpoint.type === "local") {
    titleEl.innerHTML = "📁 Browse Local Filesystem";
    subEl.textContent = "Local Machine";
  } else if (browserEndpoint.type === "ssh") {
    titleEl.innerHTML = "📁 Browse Remote SSH Server";
    subEl.textContent = `${browserEndpoint.user ? browserEndpoint.user + "@" : ""}${browserEndpoint.host}:${browserEndpoint.port}`;
  } else if (browserEndpoint.type === "smb") {
    titleEl.innerHTML = "📁 Browse SMB / NAS Share";
    subEl.textContent = `//${browserEndpoint.host}/${browserEndpoint.share}`;
  } else if (browserEndpoint.type === "webdav") {
    titleEl.innerHTML = "📁 Browse WebDAV Cloud";
    subEl.textContent = browserEndpoint.url;
  }

  const modal = document.getElementById("browser-modal");
  if (modal) modal.style.display = "flex";

  loadDirectory(browserEndpoint.path || "");
};

window.closeFileBrowser = function() {
  const modal = document.getElementById("browser-modal");
  if (modal) modal.style.display = "none";
  browserTarget = null;
  browserEndpoint = null;
};

async function loadDirectory(targetPath) {
  const content = document.getElementById("browser-content");
  const pathInput = document.getElementById("browser-current-path-input");
  const upBtn = document.getElementById("btn-browser-up");
  const selectBtn = document.getElementById("btn-browser-select");
  const displaySelected = document.getElementById("browser-selected-path-display");

  if (content) {
    content.innerHTML = `
      <div class="browser-spinner">
        <span class="pulse-dot"></span> Connecting &amp; fetching directory listing...
      </div>
    `;
  }
  if (upBtn) upBtn.disabled = true;
  if (selectBtn) selectBtn.disabled = true;

  try {
    const res = await fetch("/api/browse", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ...browserEndpoint,
        path: targetPath
      })
    });

    const data = await res.json();
    if (!data.ok) {
      if (content) {
        content.innerHTML = `
          <div class="alert-box" style="margin: 1.5rem 0.5rem; display: flex;">
            <span style="font-size: 1.3rem;">⚠️</span>
            <div>
              <strong style="display: block; margin-bottom: 0.25rem;">Browsing Error</strong>
              <span>${escapeHtml(data.message || "Failed to list directory contents")}</span>
              <div style="margin-top: 0.75rem;">
                <button type="button" class="btn btn-secondary btn-sm" onclick="loadDirectory('${escapeHtml(browserParentPath || "")}')">Go to Parent Folder</button>
              </div>
            </div>
          </div>
        `;
      }
      return;
    }

    browserCurrentPath = data.current_path || "/";
    browserParentPath = data.parent_path || "";

    if (pathInput) pathInput.value = browserCurrentPath;
    if (displaySelected) displaySelected.textContent = browserCurrentPath;
    if (upBtn) upBtn.disabled = !browserParentPath;
    if (selectBtn) selectBtn.disabled = false;

    renderBrowserItems(data.items || []);
  } catch (err) {
    if (content) {
      content.innerHTML = `
        <div class="alert-box" style="margin: 1.5rem 0.5rem; display: flex;">
          <span style="font-size: 1.3rem;">⚠️</span>
          <div>
            <strong style="display: block; margin-bottom: 0.25rem;">Network Error</strong>
            <span>${escapeHtml(err.message || String(err))}</span>
          </div>
        </div>
      `;
    }
  }
}

function renderBrowserItems(items) {
  const content = document.getElementById("browser-content");
  if (!content) return;

  if (items.length === 0) {
    content.innerHTML = `
      <div class="empty-browser">
        <div style="font-size: 1.8rem; margin-bottom: 0.4rem; opacity: 0.7;">📂</div>
        <span>This directory is empty.</span>
      </div>
    `;
    return;
  }

  content.innerHTML = "";

  items.forEach((item) => {
    const row = document.createElement("div");
    row.className = `browser-item ${item.is_dir ? "is-dir" : "is-file"}`;

    const icon = item.is_dir ? "📁" : "📄";
    const sizeText = item.is_dir ? "" : formatBytes(item.size);

    row.innerHTML = `
      <div class="browser-item-left">
        <span class="browser-item-icon">${icon}</span>
        <span class="browser-item-name" title="${escapeHtml(item.name)}">${escapeHtml(item.name)}</span>
      </div>
      <div class="browser-item-right">
        ${sizeText ? `<span class="browser-item-size">${sizeText}</span>` : ""}
        ${item.is_dir ? `<button type="button" class="btn-pick-folder" title="Select this directory">Select</button>` : ""}
      </div>
    `;

    // Click on directory row navigates inside
    if (item.is_dir) {
      row.querySelector(".browser-item-left").addEventListener("click", () => {
        loadDirectory(item.path);
      });

      // Quick select button on the right
      row.querySelector(".btn-pick-folder")?.addEventListener("click", (e) => {
        e.stopPropagation();
        selectAndClose(item.path);
      });
    }

    content.appendChild(row);
  });
}

window.selectAndClose = function(chosenPath) {
  if (!browserTarget || !chosenPath) return;

  if (browserTarget.type === "source") {
    syncConfig.sources[browserTarget.index].remote_path = chosenPath;
    const inputEl = document.getElementById(`src-path-${browserTarget.index}`);
    if (inputEl) inputEl.value = chosenPath;
  } else if (browserTarget.type === "destination") {
    syncConfig.destination.remote_path = chosenPath;
    const inputEl = document.getElementById("dest-path");
    if (inputEl) inputEl.value = chosenPath;
  } else if (browserTarget.type === "staging") {
    syncConfig.local_staging_dir = chosenPath;
    const inputEl = document.getElementById("cfg-staging-dir");
    if (inputEl) inputEl.value = chosenPath;
  }

  closeFileBrowser();
};
