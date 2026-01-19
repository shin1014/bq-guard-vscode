import { ChildProcessWithoutNullStreams, spawn } from 'child_process';
import * as readline from 'readline';

interface PendingRequest {
  resolve: (value: any) => void;
  reject: (err: Error) => void;
}

export class PythonBridge {
  private process: ChildProcessWithoutNullStreams | null = null;
  private pending: PendingRequest[] = [];

  start(): void {
    if (this.process) {
      return;
    }
    this.process = spawn('python', ['-m', 'bq_guard.cli'], { stdio: 'pipe' });
    const rl = readline.createInterface({ input: this.process.stdout });
    rl.on('line', (line) => {
      const pending = this.pending.shift();
      if (!pending) {
        return;
      }
      try {
        const parsed = JSON.parse(line);
        pending.resolve(parsed);
      } catch (err) {
        pending.reject(err as Error);
      }
    });
    this.process.on('error', (err) => {
      while (this.pending.length) {
        this.pending.shift()?.reject(err);
      }
    });
  }

  sendRequest(payload: Record<string, any>): Promise<any> {
    this.start();
    return new Promise((resolve, reject) => {
      this.pending.push({ resolve, reject });
      this.process?.stdin.write(`${JSON.stringify(payload)}\n`);
    });
  }

  dispose(): void {
    this.process?.kill();
    this.process = null;
  }
}
