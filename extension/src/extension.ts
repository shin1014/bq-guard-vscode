import * as vscode from 'vscode';
import { PythonBridge } from './pythonBridge';
import { GuardStateMachine } from './state';
import { DiagnosticsManager } from './diagnostics';
import { GuardPanel } from './webview/panel';
import { CommandRegistry } from './commands';

export function activate(context: vscode.ExtensionContext) {
  const bridge = new PythonBridge();
  const state = new GuardStateMachine();
  const diagnostics = new DiagnosticsManager();

  const registry = new CommandRegistry(context, () => {
    const panel = new GuardPanel(context.extensionUri, bridge, state, diagnostics);
    registry.setPanel(panel);
    return panel;
  });
  registry.register();

  context.subscriptions.push(diagnostics, { dispose: () => bridge.dispose() });
}

export function deactivate() {}
