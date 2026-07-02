**한국어** | [English](README.en.md)

# adx_diagnose

![status](https://img.shields.io/badge/status-active-107C10)
![depth](https://img.shields.io/badge/depth-Full%20%2B%20Regression-0078D4)
![target](https://img.shields.io/badge/target-Azure%20Data%20Explorer%20(Kusto)-0078D4)
![focus](https://img.shields.io/badge/focus-query%20performance-0a6cbd)
![readonly](https://img.shields.io/badge/access-read--only-555)

**Azure Data Explorer(ADX / Kusto)** 진단 도구. 쿼리 속도 저하의 단서를 한 장의 자체 완결형 HTML 리포트로 모읍니다. 설계 철학은 `pg_diagnose`·`aks_diagnose` 와 동일(읽기 전용 다계층 수집 → 휴리스틱 분석 → 리포트).

> [!NOTE]
> 핵심 진단 원칙: **핫캐시 미스 제거가 SKU 종류 변경보다 쿼리 속도 개선 효과가 크다.** 느린 쿼리가 콜드(디스크) 셰이드 접근에 몰려 있고 캐시 압박/스로틀이 동반되면, 캐시 정책부터 손보도록 유도합니다.

---

## 수집 계층 (전체 + 회귀)

| 계층 | 소스 | 내용 |
|---|---|---|
| **1** | Azure Monitor | CacheUtilization·CPU·QueryDuration·IngestionLatency·IngestionUtilization·ThrottledQueries 추세 |
| **2** | 엔진 (KQL/관리명령) | `.show queries`(지속·CPU·메모리·핫/콜드 바이트·스캔 익스텐트)·`.show capacity`(스로틀)·캐시 정책·extents·`.show tables`·ingestion failures |
| **3** | 상관 + 회귀 | 느린 쿼리 ↔ 콜드 캐시 ↔ 캐시 압박/스로틀 상관, JSON baseline 대비 추세 |

발견사항은 위험/주의/정보로 분류하고 **Health Score(100 − 가중치)** 와 권장 조치를 함께 제시합니다.

### `.show queries` 파싱 (실제 스키마 기준)
- 서버측에서 **`| top N by Duration desc`** 로 상위 N만 조회(payload·보존 한계 대응)
- CPU 는 **`TotalCpu`**(timespan), 메모리는 **`MemoryPeak`**(long, bytes)
- 캐시: **`CacheStatistics.Shards.Hot/Cold.{HitBytes,MissBytes}`** → 콜드(디스크) 바이트 = `Cold.HitBytes + Cold.MissBytes + `**`Hot.MissBytes`**(핫에 있어야 하나 미스 → 디스크 재조회), 콜드 비율 = 콜드 바이트 / (핫 + 콜드)
- 스캔: **`ScannedExtentsStatistics.{ScannedExtentsCount,TotalExtentsCount,ScannedRowsCount}`** → 스캔 비율(필터링 품질)
- dynamic 컬럼은 dict/JSON 문자열 모두 방어적으로 파싱

---

## 인증 — Entra ID / 앱 등록 둘 다

| 방식 | 옵션 | 내부(KustoConnectionStringBuilder) |
|---|---|---|
| Entra ID (기본) | `--auth default` | `with_azure_token_credential(DefaultAzureCredential)` |
| Entra ID (az CLI) | `--auth cli` | `with_az_cli_authentication` |
| **앱 등록(서비스 주체)** | `--auth app --app-id <id> --tenant <t>` + env `ADX_APP_KEY` | `with_aad_application_key_authentication` |
| 관리 ID | `--auth msi [--client-id <id>]` | `with_aad_managed_service_identity_authentication` |

> [!IMPORTANT]
> 메트릭(Azure Monitor) 계층은 엔진과 **동일한 `--auth`** 자격을 재사용합니다(`build_token_credential` — 예: `--auth app` → `ClientSecretCredential`). 다만 두 계층은 **독립적으로 실행**되어, 메트릭 계층이 import/인증/권한/리전 미해결로 실패해도 프로그램을 죽이지 않고 해당 계층만 생략합니다(엔진 진단은 그대로 진행).

**권한**: 엔진은 대상 DB **Viewer**(전체 쿼리 조회는 **Database Admin** 또는 **AllDatabasesViewer**), 메트릭은 클러스터 **Monitoring Reader**.

---

## 빠른 시작

```bash
pip install -r requirements.txt

# 미리보기 (연결 불필요)
python adx_diagnose.py --demo --out report.html

# Entra ID (az login) — 엔진 + 메트릭
az login
python adx_diagnose.py --cluster https://<name>.<region>.kusto.windows.net \
  --database <db> --auth default \
  --resource-id "/subscriptions/.../providers/Microsoft.Kusto/clusters/<name>" \
  --region koreacentral --hours 24 --out report.html

# 앱 등록(서비스 주체)
export ADX_APP_KEY='<secret>'      # PowerShell: $env:ADX_APP_KEY="<secret>"
python adx_diagnose.py --cluster https://<name>.<region>.kusto.windows.net \
  --database <db> --auth app --app-id <appId> --tenant <tenantId> --out report.html
```

---

## 주요 옵션

| 옵션 | 설명 | 기본값 |
|---|---|---|
| `--cluster` | `https://<name>.<region>.kusto.windows.net` | — |
| `--database` | 쿼리/캐시/extents 분석 대상 DB | 미지정 시 쿼리 계층 생략 |
| `--auth` | `default\|cli\|app\|msi` | `default` |
| `--app-id` `--tenant` `--client-id` | app/msi 인증 파라미터 | — |
| `--resource-id` `--region` | Azure Monitor 대상/리전 | 미지정 시 메트릭 생략 |
| `--hours` `--granularity-min` | 조회 범위/간격 | 24h / 15분 |
| `--top` | 느린 쿼리 상위 N | 15 |
| `--history` `--history-dir` | Baseline 이력/회귀 | on |
| `--demo` `--out` | 샘플 렌더링 / 출력 | off / `adx_report.html` |

---

## 무엇을 잡아내나
느린 쿼리(지속/CPU/메모리), **콜드 캐시 의존**(핫캐시 미스), **익스텐트 과다 스캔**(약한 필터링), 캐시 사용률 포화, **쿼리 스로틀**·용량 소모, **쿼리 지속시간(QueryDuration) 높음**(엔진 계층 느린 쿼리와 교차 검증 — 함께 뜨면 이중 감점 방지를 위해 정보성으로 표기), **인제스트 사용률(IngestionUtilization) 포화**, 인제스트 지연/실패, 작은 익스텐트 과다(머지 지연), 그리고 이들을 묶는 **근본원인 상관**.

---

## 안전성
읽기 전용입니다. 조회 명령(`.show ...`)과 Azure Monitor read 만 호출하며 클러스터를 변경하지 않습니다. 각 수집은 독립 try/except(부분 실패 허용).

---

## 트러블슈팅

| 증상 | 원인 / 해결 |
|---|---|
| `Forbidden`/권한 오류 (.show queries) | 대상 DB Viewer, 전체 조회는 Database Admin 필요 |
| 메트릭 섹션 "생략/실패" | 지정한 `--auth` 자격 유효성 확인(default/cli는 `az login`) + 클러스터 **Monitoring Reader**, `--resource-id`·`--region` 확인 |
| `cannot import name 'MetricsQueryClient'` | `azure-monitor-query` 2.x → `pip install azure-monitor-querymetrics` (2.x 우선/1.x 폴백) |
| 앱 등록 인증 실패 | `--app-id`·`--tenant`·환경변수 `ADX_APP_KEY` 모두 필요 |
| 콜드%가 비어 있음 | 콜드 셰이드 미사용(전부 핫)일 수 있음 — 정상 |

---

## 비고
- 임계치는 일반 휴리스틱입니다. 환경에 맞게 조정하세요.
- 권위 있는 근거는 항상 **Microsoft Learn** 공식 문서를 우선합니다.
- 정기 실행으로 baseline 을 누적하면 회귀 탐지가 강력해집니다. ADX 사이징은 "작게 시작 + Azure Advisor" 권장.
