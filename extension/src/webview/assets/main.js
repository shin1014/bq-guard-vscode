(function () {
  const vscode = acquireVsCodeApi();
  let debounceMs = 900;
  let debounceHandle = null;
  let latestEstimate = null;
  let latestHuman = '';
  let pageToken = null;

  const root = document.getElementById('root');

  function render() {
    root.innerHTML = `
      <div class="container">
        <header class="header">
          <div class="header-row">
            <h1>BQ Guard</h1>
            <button id="reviewButton" class="primary">Review</button>
          </div>
          <div class="header-grid">
            <div>Project: <span id="project">-</span></div>
            <div>Location: <span id="location">-</span></div>
            <div>Bytes: <span id="bytes">-</span></div>
            <div>WARN: <span id="warnCount">0</span> / ERROR: <span id="errorCount">0</span></div>
            <div>Status: <span id="state">Idle</span></div>
          </div>
        </header>
        <div class="body">
          <section class="editor">
            <textarea id="sqlInput" placeholder="Write SQL here..."></textarea>
          </section>
          <aside class="side">
            <h2>Guardrails</h2>
            <div id="findings"></div>
            <h3>Partition Summary</h3>
            <div id="partitionSummary"></div>
            <h3>Referenced Tables</h3>
            <ul id="tables"></ul>
          </aside>
        </div>
        <div class="tabs">
          <button data-tab="preview" class="tab active">Preview</button>
          <button data-tab="all" class="tab">All</button>
          <button data-tab="logs" class="tab">Logs</button>
        </div>
        <div class="tab-content" id="tab-preview">
          <div id="previewTable" class="table"></div>
        </div>
        <div class="tab-content hidden" id="tab-all">
          <div class="paging">
            <button id="prevPage">Prev</button>
            <button id="nextPage">Next</button>
            <span id="pageInfo"></span>
          </div>
          <div id="allTable" class="table"></div>
        </div>
        <div class="tab-content hidden" id="tab-logs">
          <div id="logs"></div>
        </div>
        <div class="modal hidden" id="reviewModal">
          <div class="modal-content">
            <h2>Review</h2>
            <div id="reviewDetails"></div>
            <div class="confirm">
              <label>Type to confirm: <strong id="confirmText"></strong></label>
              <input id="confirmInput" type="text" />
            </div>
            <div class="modal-actions">
              <button id="cancelReview">Cancel</button>
              <button id="executeButton" class="primary" disabled>Execute</button>
            </div>
          </div>
        </div>
        <div class="modal hidden" id="exportModal">
          <div class="modal-content">
            <h2>Export</h2>
            <div class="modal-actions">
              <button id="exportPreview">Export Preview</button>
              <button id="exportAll" class="primary">Export All</button>
              <button id="cancelExport">Cancel</button>
            </div>
          </div>
        </div>
      </div>
    `;

    document.getElementById('reviewButton').addEventListener('click', () => {
      vscode.postMessage({ type: 'review' });
    });

    const sqlInput = document.getElementById('sqlInput');
    sqlInput.addEventListener('input', () => {
      const sql = sqlInput.value;
      vscode.postMessage({ type: 'sqlChanged', sql });
      if (debounceHandle) {
        clearTimeout(debounceHandle);
      }
      debounceHandle = setTimeout(() => {
        vscode.postMessage({ type: 'estimate' });
      }, debounceMs);
    });

    document.querySelectorAll('.tab').forEach((tab) => {
      tab.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach((t) => t.classList.remove('active'));
        tab.classList.add('active');
        const target = tab.getAttribute('data-tab');
        document.querySelectorAll('.tab-content').forEach((content) => {
          content.classList.add('hidden');
        });
        document.getElementById(`tab-${target}`).classList.remove('hidden');
      });
    });

    document.getElementById('cancelReview').addEventListener('click', closeReview);
    document.getElementById('executeButton').addEventListener('click', () => {
      vscode.postMessage({ type: 'execute' });
      closeReview();
    });
    document.getElementById('confirmInput').addEventListener('input', handleConfirmInput);

    document.getElementById('prevPage').addEventListener('click', () => {
      vscode.postMessage({ type: 'fetchPage', pageToken: null });
    });
    document.getElementById('nextPage').addEventListener('click', () => {
      vscode.postMessage({ type: 'fetchPage', pageToken });
    });

    document.getElementById('exportPreview').addEventListener('click', () => {
      vscode.postMessage({ type: 'export', mode: 'preview' });
      closeExport();
    });
    document.getElementById('exportAll').addEventListener('click', () => {
      vscode.postMessage({ type: 'export', mode: 'all' });
      closeExport();
    });
    document.getElementById('cancelExport').addEventListener('click', closeExport);
  }

  function updateEstimate(estimate, project, location) {
    latestEstimate = estimate;
    latestHuman = estimate.bytes_human || '';
    document.getElementById('bytes').textContent = estimate.bytes_human || '-';
    document.getElementById('project').textContent = project || '-';
    document.getElementById('location').textContent = location || '-';

    const warnCount = (estimate.findings || []).filter((f) => f.severity === 'WARN').length;
    const errorCount = (estimate.findings || []).filter((f) => f.severity === 'ERROR').length;
    document.getElementById('warnCount').textContent = String(warnCount);
    document.getElementById('errorCount').textContent = String(errorCount);

    const findingsEl = document.getElementById('findings');
    findingsEl.innerHTML = '';
    (estimate.findings || []).forEach((finding) => {
      const div = document.createElement('div');
      div.className = `finding ${finding.severity.toLowerCase()}`;
      div.textContent = `[${finding.severity}] ${finding.code}: ${finding.message}`;
      findingsEl.appendChild(div);
    });

    const summaryEl = document.getElementById('partitionSummary');
    summaryEl.innerHTML = '';
    (estimate.partition_summary || []).forEach((row) => {
      const div = document.createElement('div');
      div.className = row.ok ? 'ok' : 'error';
      div.textContent = `${row.table}: ${row.ok ? 'OK' : 'NG'} (${(row.required_keys || []).join(', ')})`;
      summaryEl.appendChild(div);
    });

    const tablesEl = document.getElementById('tables');
    tablesEl.innerHTML = '';
    (estimate.referenced_tables || []).forEach((table) => {
      const li = document.createElement('li');
      li.textContent = table;
      tablesEl.appendChild(li);
    });
  }

  function updateState(state) {
    document.getElementById('state').textContent = state;
  }

  function openReview() {
    const modal = document.getElementById('reviewModal');
    const details = document.getElementById('reviewDetails');
    details.innerHTML = '';
    if (latestEstimate) {
      details.textContent = `Bytes: ${latestEstimate.bytes_human} | Tables: ${(latestEstimate.referenced_tables || []).length}`;
    }
    document.getElementById('confirmText').textContent = `RUN ${latestHuman}`;
    document.getElementById('confirmInput').value = '';
    document.getElementById('executeButton').disabled = true;
    modal.classList.remove('hidden');
  }

  function closeReview() {
    document.getElementById('reviewModal').classList.add('hidden');
  }

  function handleConfirmInput() {
    const input = document.getElementById('confirmInput').value;
    const expected = `RUN ${latestHuman}`;
    const hasError = (latestEstimate?.findings || []).some((f) => f.severity === 'ERROR');
    document.getElementById('executeButton').disabled = input !== expected || hasError;
  }

  function openExport() {
    document.getElementById('exportModal').classList.remove('hidden');
  }

  function closeExport() {
    document.getElementById('exportModal').classList.add('hidden');
  }

  function renderTable(target, data) {
    const container = document.getElementById(target);
    container.innerHTML = '';
    if (!data || !data.columns) {
      return;
    }
    const table = document.createElement('table');
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    data.columns.forEach((col) => {
      const th = document.createElement('th');
      th.textContent = col;
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);
    const tbody = document.createElement('tbody');
    data.rows.forEach((row) => {
      const tr = document.createElement('tr');
      row.forEach((cell) => {
        const td = document.createElement('td');
        td.textContent = cell === null || cell === undefined ? '' : String(cell);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    container.appendChild(table);
  }

  function appendLog(message, ts) {
    const logs = document.getElementById('logs');
    const div = document.createElement('div');
    div.textContent = `[${ts}] ${message}`;
    logs.prepend(div);
  }

  window.addEventListener('message', (event) => {
    const message = event.data;
    switch (message.type) {
      case 'estimate':
        updateEstimate(message.estimate, message.project, message.location);
        updateState(message.state || 'Idle');
        if (message.review) {
          openReview();
        }
        break;
      case 'state':
        updateState(message.state);
        break;
      case 'preview':
        renderTable('previewTable', message.preview);
        break;
      case 'page':
        pageToken = message.page.page_token || null;
        renderTable('allTable', message.page);
        document.getElementById('pageInfo').textContent = pageToken ? 'More pages available' : 'End of pages';
        break;
      case 'execute':
        appendLog(`Executed job ${message.execute.job_id}`, new Date().toISOString());
        break;
      case 'log':
        appendLog(message.message, message.ts);
        break;
      case 'openExport':
        openExport();
        break;
      case 'config':
        debounceMs = message.config?.app?.ui?.auto_estimate_debounce_ms || 900;
        break;
      default:
        break;
    }
  });

  render();
})();
