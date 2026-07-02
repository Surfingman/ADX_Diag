[한국어](README.md) | **English**

# adx_diagnose

![status](https://img.shields.io/badge/status-active-107C10)
![depth](https://img.shields.io/badge/depth-Full%20%2B%20Regression-0078D4)
![target](https://img.shields.io/badge/target-Azure%20Data%20Explorer%20(Kusto)-0078D4)
![focus](https://img.shields.io/badge/focus-query%20performance-0a6cbd)
![readonly](https://img.shields.io/badge/access-read--only-555)

A diagnostic tool for **Azure Data Explorer (ADX / Kusto)**. It collects the clues behind query slowdowns into a single self-contained HTML report. Its design philosophy matches `pg_diagnose` / `aks_diagnose` (read-only multi-tier collection → heuristic analysis → report).

> [!NOTE]
> Core diagnostic principle: **eliminating hot-cache misses improves query speed more than changing the SKU type.** When slow queries cluster around cold (disk) shard access accompanied by cache pressure/throttling, the tool steers you to fix the cache policy first.

---

## Collection Tiers (Full + Regression)

| Tier | Source | Contents |
|---|---|---|
| **1** | Azure Monitor | CacheUtilization · CPU · QueryDuration · IngestionLatency · IngestionUtilization · ThrottledQueries trends |
| **2** | Engine (KQL / management commands) | `.show queries` (duration · CPU · memory · hot/cold bytes · scanned extents) · `.show capacity` (throttling) · caching policy · extents · `.show tables` · ingestion failures |
| **3** | Correlation + Regression | slow query ↔ cold cache ↔ cache pressure/throttling correlation, trend vs. JSON baseline |

Findings are classified as critical / warning / info and presented with a **Health Score (100 − weights)** and recommended actions.

### `.show queries` Parsing (based on the actual schema)
- Server-side **`| top N by Duration desc`** retrieves only the top N (handles payload/retention limits)
- CPU is **`TotalCpu`** (timespan); memory is **`MemoryPeak`** (long, bytes)
- Cache: **`CacheStatistics.Shards.Hot/Cold.{HitBytes,MissBytes}`** → cold (disk) bytes = `Cold.HitBytes + Cold.MissBytes + `**`Hot.MissBytes`** (should have been in hot cache but missed → disk re-fetch); cold ratio = cold bytes / (hot + cold)
- Scan: **`ScannedExtentsStatistics.{ScannedExtentsCount,TotalExtentsCount,ScannedRowsCount}`** → scan ratio (filtering quality)
- dynamic columns are parsed defensively as either dict or JSON string

---

## Authentication — Entra ID and App Registration

| Method | Option | Internals (KustoConnectionStringBuilder) |
|---|---|---|
| Entra ID (default) | `--auth default` | `with_azure_token_credential(DefaultAzureCredential)` |
| Entra ID (az CLI) | `--auth cli` | `with_az_cli_authentication` |
| **App registration (service principal)** | `--auth app --app-id <id> --tenant <t>` + env `ADX_APP_KEY` | `with_aad_application_key_authentication` |
| Managed Identity | `--auth msi [--client-id <id>]` | `with_aad_managed_service_identity_authentication` |

> [!IMPORTANT]
> The metrics (Azure Monitor) tier **reuses the same `--auth`** credential as the engine (`build_token_credential` — e.g. `--auth app` → `ClientSecretCredential`). However, the two tiers **run independently**: if the metrics tier fails due to import/auth/permission/unresolved region, it does not crash the program — only that tier is skipped (engine diagnostics proceed normally).

**Permissions**: the engine needs **Viewer** on the target DB (full query visibility requires **Database Admin** or **AllDatabasesViewer**); metrics need **Monitoring Reader** on the cluster.

---

## Quick Start

```bash
pip install -r requirements.txt

# Preview (no connection needed)
python adx_diagnose.py --demo --out report.html

# Entra ID (az login) — engine + metrics
az login
python adx_diagnose.py --cluster https://<name>.<region>.kusto.windows.net \
  --database <db> --auth default \
  --resource-id "/subscriptions/.../providers/Microsoft.Kusto/clusters/<name>" \
  --region koreacentral --hours 24 --out report.html

# App registration (service principal)
export ADX_APP_KEY='<secret>'      # PowerShell: $env:ADX_APP_KEY="<secret>"
python adx_diagnose.py --cluster https://<name>.<region>.kusto.windows.net \
  --database <db> --auth app --app-id <appId> --tenant <tenantId> --out report.html
```

---

## Key Options

| Option | Description | Default |
|---|---|---|
| `--cluster` | `https://<name>.<region>.kusto.windows.net` | — |
| `--database` | Target DB for query/cache/extents analysis | query tier skipped if omitted |
| `--auth` | `default\|cli\|app\|msi` | `default` |
| `--app-id` `--tenant` `--client-id` | app/msi auth parameters | — |
| `--resource-id` `--region` | Azure Monitor target / region | metrics skipped if omitted |
| `--hours` `--granularity-min` | query window / interval | 24h / 15 min |
| `--top` | Top N slow queries | 15 |
| `--history` `--history-dir` | Baseline history / regression | on |
| `--demo` `--out` | Sample rendering / output | off / `adx_report.html` |

---

## What It Detects
Slow queries (duration/CPU/memory), **cold-cache dependence** (hot-cache misses), **excessive extent scanning** (weak filtering), cache-utilization saturation, **query throttling** · capacity consumption, **high query duration (QueryDuration)** (cross-validated with engine-tier slow queries — reported as informational when both fire, to avoid double-penalizing the score), **ingestion-utilization (IngestionUtilization) saturation**, ingestion latency/failures, too many small extents (merge delay), and the **root-cause correlation** that ties these together.

---

## Safety
Read-only. It only calls query commands (`.show ...`) and Azure Monitor reads, and never modifies the cluster. Each collection is an independent try/except (partial failure tolerated).

---

## Troubleshooting

| Symptom | Cause / Fix |
|---|---|
| `Forbidden` / permission error (.show queries) | Viewer on the target DB; full visibility needs Database Admin |
| Metrics section "skipped/failed" | Verify the chosen `--auth` credential (default/cli: `az login`) + cluster **Monitoring Reader**, and check `--resource-id`/`--region` |
| `cannot import name 'MetricsQueryClient'` | `azure-monitor-query` 2.x → `pip install azure-monitor-querymetrics` (2.x preferred / 1.x fallback) |
| App registration auth failure | `--app-id`, `--tenant`, and env `ADX_APP_KEY` are all required |
| Cold % is empty | Cold shards may be unused (all hot) — this is normal |

---

## Notes
- Thresholds are general heuristics. Adjust them to your environment.
- Always prefer the authoritative **Microsoft Learn** official docs as the basis.
- Accumulating a baseline via regular runs makes regression detection powerful. For ADX sizing, "start small + Azure Advisor" is recommended.
