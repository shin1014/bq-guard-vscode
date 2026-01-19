# BQ Guard VS Code Extension

BQ Guard is a VS Code extension plus Python core that enforces guardrails for safe BigQuery execution in Remote-SSH environments. It ensures every query is reviewed with a dry-run estimate and requires typed confirmation before execution.

## Requirements

- VS Code Remote-SSH (Linux)
- Python 3.10+
- ADC credentials

### Authenticate ADC

```bash
gcloud auth application-default login
```

## Python setup (uv)

```bash
uv venv
uv pip install -e .
```

Install from a tag:

```bash
uv tool install "git+ssh://git@github.com/<OWNER>/<REPO>.git@vX.Y.Z"
```

## VS Code extension setup

```bash
cd extension
npm install
npm run compile
```

Press `F5` to launch an Extension Development Host.

## Main operations

- **Ctrl+E**: Estimate (dry-run)
- **Ctrl+Enter**: Review (dry-run + typed confirmation)
- **Ctrl+S**: Export (preview/all)
- **Ctrl+,**: Settings
- **Ctrl+M**: Refresh metadata cache
- **BQ Guard: Open Panel**: Open the UI
- **BQ Guard: Show History**: Open history.jsonl

## Files and paths

- Config: `~/.config/bq_guard/config.yaml`
- History: `~/.local/state/bq_guard/history.jsonl`
- Cache: `~/.cache/bq_guard/table_meta_cache.json`

## Common errors

- **Location mismatch**: ensure config location matches the dataset region.
- **ADC not configured**: run `gcloud auth application-default login`.
- **gcloud missing**: defaults will fall back to `asia-northeast1`.
