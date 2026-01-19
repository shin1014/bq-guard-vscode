import * as path from 'path';
import * as vscode from 'vscode';
import { PythonBridge } from '../pythonBridge';
import { GuardStateMachine } from '../state';
import { DiagnosticsManager } from '../diagnostics';

export class GuardPanel {
  private panel: vscode.WebviewPanel;
  private bridge: PythonBridge;
  private state: GuardStateMachine;
  private diagnostics: DiagnosticsManager;
  private extensionUri: vscode.Uri;
  private latestEstimate: any = null;
  private latestRevision = 0;
  private config: any = null;

  constructor(
    extensionUri: vscode.Uri,
    bridge: PythonBridge,
    state: GuardStateMachine,
    diagnostics: DiagnosticsManager
  ) {
    this.extensionUri = extensionUri;
    this.bridge = bridge;
    this.state = state;
    this.diagnostics = diagnostics;
    this.panel = vscode.window.createWebviewPanel(
      'bqGuardPanel',
      'BQ Guard',
      vscode.ViewColumn.One,
      {
        enableScripts: true,
        localResourceRoots: [vscode.Uri.joinPath(extensionUri, 'src', 'webview', 'assets')],
      }
    );
    this.panel.webview.html = this.getHtml();
    this.panel.webview.onDidReceiveMessage((message) => this.handleMessage(message));
    this.panel.onDidDispose(() => this.dispose());
    void this.loadConfig();
  }

  reveal(): void {
    this.panel.reveal();
  }

  postMessage(message: any): void {
    this.panel.webview.postMessage(message);
  }

  private getHtml(): string {
    const webview = this.panel.webview;
    const scriptUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this.extensionUri, 'src', 'webview', 'assets', 'main.js')
    );
    const styleUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this.extensionUri, 'src', 'webview', 'assets', 'styles.css')
    );
    const htmlUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this.extensionUri, 'src', 'webview', 'assets', 'index.html')
    );

    return `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource}; script-src ${webview.cspSource};" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <link href="${styleUri}" rel="stylesheet" />
</head>
<body>
  <div id="root"></div>
  <script src="${scriptUri}"></script>
</body>
</html>`;
  }

  private log(message: string): void {
    this.panel.webview.postMessage({ type: 'log', message, ts: new Date().toISOString() });
  }

  private async loadConfig(): Promise<void> {
    const response = await this.bridge.sendRequest({ op: 'get_effective_config' });
    if (response.ok) {
      this.config = response.config;
      this.panel.webview.postMessage({
        type: 'config',
        config: response.config,
        paths: response.paths,
      });
    }
  }

  private async handleMessage(message: any): Promise<void> {
    switch (message.type) {
      case 'sqlChanged':
        this.state.updateSql(message.sql || '');
        this.latestRevision = this.state.revision;
        return;
      case 'estimate':
        await this.runEstimate(this.state.currentSql, this.state.revision, false);
        return;
      case 'review':
        await this.runEstimate(this.state.currentSql, this.state.revision, true);
        return;
      case 'execute':
        await this.executeQuery();
        return;
      case 'fetchPreview':
        await this.fetchPreview();
        return;
      case 'fetchPage':
        await this.fetchPage(message.pageToken || null);
        return;
      case 'export':
        await this.exportResults(message.mode);
        return;
      case 'refreshMetadata':
        await this.refreshMetadata();
        return;
      default:
        return;
    }
  }

  private async runEstimate(sql: string, revision: number, forReview: boolean): Promise<void> {
    if (!sql.trim()) {
      return;
    }
    this.state.setState('Estimating');
    this.panel.webview.postMessage({ type: 'state', state: this.state.state });
    const response = await this.bridge.sendRequest({ op: forReview ? 'review' : 'estimate', sql });
    if (!response.ok) {
      this.state.setState('Error');
      this.panel.webview.postMessage({ type: 'state', state: this.state.state, error: response.error });
      this.diagnostics.update([
        { severity: 'ERROR', message: response.error?.message || 'Dry run failed', code: 'DRYRUN_FAILED' },
      ]);
      this.log(response.error?.detail || response.error?.message || 'Dry run failed');
      return;
    }

    if (revision !== this.state.revision) {
      return;
    }
    this.latestEstimate = response.estimate;
    this.state.setState('Idle');
    if (forReview) {
      this.state.markReviewReady(revision);
    }
    this.panel.webview.postMessage({
      type: 'estimate',
      estimate: response.estimate,
      project: response.project,
      location: response.location,
      state: this.state.state,
      review: forReview,
    });
    this.diagnostics.update(response.estimate.findings || []);
  }

  private async executeQuery(): Promise<void> {
    const hasError = (this.latestEstimate?.findings || []).some((f: any) => f.severity === 'ERROR');
    if (!this.state.canExecute()) {
      this.log('Execute blocked: review required.');
      return;
    }
    if (hasError) {
      this.log('Execute blocked: error findings present.');
      return;
    }
    this.state.setState('Executing');
    this.panel.webview.postMessage({ type: 'state', state: this.state.state });
    const response = await this.bridge.sendRequest({ op: 'execute', sql: this.state.currentSql });
    if (!response.ok) {
      this.state.setState('Error');
      this.panel.webview.postMessage({ type: 'state', state: this.state.state, error: response.error });
      this.log(response.error?.detail || response.error?.message || 'Execute failed');
      return;
    }
    this.state.setJob(response.execute.job_id);
    this.state.setState('Idle');
    this.panel.webview.postMessage({
      type: 'execute',
      execute: response.execute,
      state: this.state.state,
    });
    await this.fetchPreview();
  }

  private async fetchPreview(): Promise<void> {
    if (!this.state.jobId) {
      return;
    }
    const response = await this.bridge.sendRequest({ op: 'fetch_preview', job_id: this.state.jobId });
    if (response.ok) {
      this.panel.webview.postMessage({ type: 'preview', preview: response.preview });
    } else {
      this.log(response.error?.detail || response.error?.message || 'Preview failed');
    }
  }

  private async fetchPage(pageToken: string | null): Promise<void> {
    if (!this.state.jobId) {
      return;
    }
    const response = await this.bridge.sendRequest({
      op: 'fetch_page',
      job_id: this.state.jobId,
      page_token: pageToken,
    });
    if (response.ok) {
      this.panel.webview.postMessage({ type: 'page', page: response.page });
    } else {
      this.log(response.error?.detail || response.error?.message || 'Page fetch failed');
    }
  }

  private async exportResults(mode: string): Promise<void> {
    if (!this.state.jobId) {
      this.log('Export blocked: no job id.');
      return;
    }
    const timestamp = new Date().toISOString().replace(/[-:]/g, '').replace('T', '_').slice(0, 15);
    const filename = `${timestamp}_${this.state.jobId}_${mode}.csv`;
    const exportDir = path.join(vscode.workspace.workspaceFolders?.[0]?.uri.fsPath || '.', 'exports');
    await vscode.workspace.fs.createDirectory(vscode.Uri.file(exportDir));
    const outPath = path.join(exportDir, filename);
    const response = await this.bridge.sendRequest({
      op: 'export',
      job_id: this.state.jobId,
      mode,
      out_path: outPath,
    });
    if (response.ok) {
      this.log(`Exported ${response.export.rows} rows to ${response.export.path}`);
    } else {
      this.log(response.error?.detail || response.error?.message || 'Export failed');
    }
  }

  private async refreshMetadata(): Promise<void> {
    const tables = this.latestEstimate?.referenced_tables || [];
    const response = await this.bridge.sendRequest({ op: 'refresh_metadata', tables });
    if (response.ok) {
      this.log(`Refreshed metadata for ${response.refreshed.length} tables.`);
    } else {
      this.log(response.error?.detail || response.error?.message || 'Metadata refresh failed');
    }
  }

  dispose(): void {
    this.diagnostics.clear();
    this.panel.dispose();
  }
}
