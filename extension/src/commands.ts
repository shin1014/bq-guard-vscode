import * as vscode from 'vscode';
import { GuardPanel } from './webview/panel';

export class CommandRegistry {
  private panel: GuardPanel | null = null;

  constructor(private context: vscode.ExtensionContext, private panelFactory: () => GuardPanel) {}

  setPanel(panel: GuardPanel): void {
    this.panel = panel;
  }

  register(): void {
    this.context.subscriptions.push(
      vscode.commands.registerCommand('bqGuard.openPanel', () => this.openPanel()),
      vscode.commands.registerCommand('bqGuard.estimate', () => this.panel?.postMessage({ type: 'estimate' })),
      vscode.commands.registerCommand('bqGuard.review', () => this.panel?.postMessage({ type: 'review' })),
      vscode.commands.registerCommand('bqGuard.execute', () => this.panel?.postMessage({ type: 'execute' })),
      vscode.commands.registerCommand('bqGuard.export', () => this.panel?.postMessage({ type: 'openExport' })),
      vscode.commands.registerCommand('bqGuard.settings', () => this.openSettings()),
      vscode.commands.registerCommand('bqGuard.refreshMetadata', () => this.panel?.postMessage({ type: 'refreshMetadata' })),
      vscode.commands.registerCommand('bqGuard.showHistory', () => this.openHistory())
    );
  }

  private openPanel(): void {
    if (this.panel) {
      this.panel.reveal();
      return;
    }
    this.panel = this.panelFactory();
  }

  private async openSettings(): Promise<void> {
    const configPath = vscode.Uri.file(`${vscode.env.homeDir.fsPath}/.config/bq_guard/config.yaml`);
    const doc = await vscode.workspace.openTextDocument(configPath);
    await vscode.window.showTextDocument(doc, { preview: false });
  }

  private async openHistory(): Promise<void> {
    const historyPath = vscode.Uri.file(`${vscode.env.homeDir.fsPath}/.local/state/bq_guard/history.jsonl`);
    const doc = await vscode.workspace.openTextDocument(historyPath);
    await vscode.window.showTextDocument(doc, { preview: false });
  }
}
