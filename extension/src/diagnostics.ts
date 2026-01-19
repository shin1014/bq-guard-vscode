import * as vscode from 'vscode';

export class DiagnosticsManager {
  private collection: vscode.DiagnosticCollection;
  private uri: vscode.Uri;

  constructor() {
    this.collection = vscode.languages.createDiagnosticCollection('bq-guard');
    this.uri = vscode.Uri.parse('bqguard:/query.sql');
  }

  update(findings: Array<{ severity: string; message: string; code: string }>): void {
    const diagnostics: vscode.Diagnostic[] = [];
    findings.forEach((finding) => {
      const range = new vscode.Range(new vscode.Position(0, 0), new vscode.Position(0, 1));
      const severity = finding.severity === 'ERROR'
        ? vscode.DiagnosticSeverity.Error
        : vscode.DiagnosticSeverity.Warning;
      const diagnostic = new vscode.Diagnostic(range, `[${finding.code}] ${finding.message}`, severity);
      diagnostics.push(diagnostic);
    });
    this.collection.set(this.uri, diagnostics);
  }

  clear(): void {
    this.collection.set(this.uri, []);
  }

  dispose(): void {
    this.collection.dispose();
  }
}
