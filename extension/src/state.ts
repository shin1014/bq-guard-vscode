export type GuardState = 'Idle' | 'Estimating' | 'ReviewReady' | 'Executing' | 'Error';

export class GuardStateMachine {
  private _state: GuardState = 'Idle';
  private _revision = 0;
  private _reviewRevision: number | null = null;
  private _currentSql = '';
  private _jobId: string | null = null;

  get state(): GuardState {
    return this._state;
  }

  get revision(): number {
    return this._revision;
  }

  get reviewRevision(): number | null {
    return this._reviewRevision;
  }

  get currentSql(): string {
    return this._currentSql;
  }

  get jobId(): string | null {
    return this._jobId;
  }

  updateSql(sql: string): void {
    this._currentSql = sql;
    this._revision += 1;
    this._reviewRevision = null;
  }

  setState(state: GuardState): void {
    this._state = state;
  }

  markReviewReady(revision: number): void {
    this._reviewRevision = revision;
    this._state = 'ReviewReady';
  }

  canExecute(): boolean {
    return this._reviewRevision !== null && this._reviewRevision === this._revision;
  }

  setJob(jobId: string | null): void {
    this._jobId = jobId;
  }
}
