#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
adx_diagnose — Azure Data Explorer(ADX) / Kusto 진단 도구

pg_diagnose 철학(읽기 전용 다계층 수집 → 휴리스틱 분석 → 자체 완결형 HTML 리포트)과 동일.
쿼리 속도 저하의 단서를 한 장으로 모은다.
  · 계층 1  Azure Monitor      — CacheUtilization·CPU·QueryDuration·Ingestion 지연/사용률·스로틀
  · 계층 2  엔진(KQL/관리명령) — .show queries(느린 쿼리·CPU·메모리·핫/콜드 스캔)·.show capacity
                                  ·캐시 정책·extents(머지)·ingestion failures
  · 계층 3  상관 + 회귀         — 느린 쿼리 ↔ 콜드 캐시/스로틀 상관, baseline 대비 추세
설계 원칙: 핫캐시 미스 제거가 SKU 변경보다 쿼리 속도 영향이 크다는 점을 진단에 반영.

인증(둘 다 지원):
  · Entra ID:  --auth default(DefaultAzureCredential) | cli(az) | msi
  · 앱 등록:   --auth app  --app-id <id> --tenant <tenant>  (키는 ADX_APP_KEY 환경변수)

읽기 전용: 쿼리/관리 조회 명령(.show ...)만 실행하며 클러스터를 변경하지 않는다.
부분 실패 허용: 각 수집은 독립 try/except.

실행:
  python adx_diagnose.py --demo --out report.html
  python adx_diagnose.py --cluster https://<name>.<region>.kusto.windows.net \
     --database <db> --auth default \
     --resource-id "/subscriptions/.../Microsoft.Kusto/clusters/<name>" --region koreacentral \
     --out report.html
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import html
import json
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class Config:
    cluster: Optional[str] = None
    database: Optional[str] = None
    # 인증 — Entra ID / 앱 등록 둘 다
    auth: str = "default"               # default | cli | app | msi
    app_id: Optional[str] = None        # --auth app
    tenant: Optional[str] = None        # --auth app
    client_id: Optional[str] = None     # --auth msi (사용자 할당)
    # 계층 1 (Azure Monitor)
    resource_id: Optional[str] = None
    region: Optional[str] = None
    hours: int = 24
    granularity_min: int = 15
    # 엔진
    top: int = 15
    history: bool = True
    history_dir: str = "./adx_diagnose_history"
    out: str = "adx_report.html"
    demo: bool = False


SEV_CRIT, SEV_WARN, SEV_INFO, SEV_OK = "critical", "warning", "info", "ok"
SEV_WEIGHT = {SEV_CRIT: 25, SEV_WARN: 10, SEV_INFO: 2, SEV_OK: 0}
SEV_LABEL = {SEV_CRIT: "위험", SEV_WARN: "주의", SEV_INFO: "정보", SEV_OK: "양호"}
ADX_METRICS = [("CacheUtilization", "%"), ("CPU", "%"), ("QueryDuration", "ms"),
               ("IngestionLatencyInSeconds", "s"), ("IngestionUtilization", "%"),
               ("TotalNumberOfThrottledQueries", "count")]


@dataclass
class Finding:
    severity: str
    category: str
    title: str
    detail: str
    recommendation: str


@dataclass
class MetricSeries:
    name: str
    unit: str
    timestamps: list = field(default_factory=list)
    raw_ts: list = field(default_factory=list)
    avg: list = field(default_factory=list)
    mx: list = field(default_factory=list)
    error: Optional[str] = None

    def _vals(self, arr):
        return [x for x in arr if x is not None]

    @property
    def avg_v(self):
        v = self._vals(self.avg)
        return round(sum(v) / len(v), 1) if v else None

    @property
    def max_v(self):
        v = self._vals(self.mx)
        return round(max(v), 1) if v else None


@dataclass
class SlowQuery:
    text: str
    duration_s: Optional[float]
    cpu_s: Optional[float]
    mem_mb: Optional[float]
    hot_bytes: Optional[float] = None      # CacheStatistics.Shards.Hot.HitBytes
    cold_bytes: Optional[float] = None     # Cold.HitBytes + Cold.MissBytes + Hot.MissBytes (디스크 접근)
    scanned_extents: Optional[float] = None
    total_extents: Optional[float] = None
    scanned_rows: Optional[float] = None
    user: str = ""
    app: str = ""

    @property
    def cold_ratio(self):
        h, c = self.hot_bytes or 0, self.cold_bytes or 0
        return (c / (h + c)) if (h + c) > 0 else None

    @property
    def scanned_ratio(self):
        t = self.total_extents or 0
        return (self.scanned_extents / t) if (t and self.scanned_extents is not None) else None


@dataclass
class EngineData:
    error: Optional[str] = None
    slow: list[SlowQuery] = field(default_factory=list)
    capacity: list[dict] = field(default_factory=list)     # resource/total/consumed
    cache_policy: Optional[str] = None                     # hot cache window
    extents: Optional[dict] = None                         # {count, size_gb, merged}
    ingestion_failures: int = 0
    tables: Optional[int] = None
    notes: list[str] = field(default_factory=list)


# ──────────────────────────────────────────────────────────────────────────
# 인증 — KustoClient 생성 (Entra ID / 앱 등록 둘 다)
# ──────────────────────────────────────────────────────────────────────────
def build_kusto_client(cfg: Config):
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder as KCSB
    c = cfg.cluster
    if cfg.auth == "app":
        key = os.environ.get("ADX_APP_KEY")
        if not (cfg.app_id and cfg.tenant and key):
            raise RuntimeError("앱 등록 인증에는 --app-id, --tenant, 환경변수 ADX_APP_KEY 가 필요합니다.")
        kcsb = KCSB.with_aad_application_key_authentication(c, cfg.app_id, key, cfg.tenant)
    elif cfg.auth == "cli":
        kcsb = KCSB.with_az_cli_authentication(c)
    elif cfg.auth == "msi":
        kcsb = KCSB.with_aad_managed_service_identity_authentication(c, client_id=cfg.client_id)
    else:  # default → DefaultAzureCredential (az login / env / WI / MSI 자동)
        from azure.identity import DefaultAzureCredential
        kcsb = KCSB.with_azure_token_credential(c, credential=DefaultAzureCredential())
    return KustoClient(kcsb)


def build_token_credential(cfg: Config):
    """cfg.auth에 맞는 azure.identity 자격 (메트릭/ARM 계층용).
    엔진(build_kusto_client)과 동일 인증을 재사용해 --auth 일관성을 보장한다.
    특히 --auth app(서비스 주체) 환경에서 메트릭 계층이 SP 자격을 그대로 쓴다."""
    from azure.identity import (DefaultAzureCredential, AzureCliCredential,
                                ManagedIdentityCredential, ClientSecretCredential)
    if cfg.auth == "app":
        key = os.environ.get("ADX_APP_KEY")
        if not (cfg.app_id and cfg.tenant and key):
            raise RuntimeError("앱 등록 인증에는 --app-id, --tenant, 환경변수 ADX_APP_KEY 가 필요합니다.")
        return ClientSecretCredential(cfg.tenant, cfg.app_id, key)
    if cfg.auth == "cli":
        return AzureCliCredential()
    if cfg.auth == "msi":
        return (ManagedIdentityCredential(client_id=cfg.client_id)
                if cfg.client_id else ManagedIdentityCredential())
    return DefaultAzureCredential()


def _rows(resp) -> list[dict]:
    """KustoResponseDataSet.primary_results[0] → list[dict]."""
    if not resp.primary_results:
        return []
    t = resp.primary_results[0]
    cols = [c.column_name for c in t.columns]
    out = []
    for r in t:
        try:
            out.append(dict(zip(cols, list(r))))
        except Exception:  # noqa: BLE001
            out.append({c: r[i] for i, c in enumerate(cols)})
    return out


def _secs(v) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, dt.timedelta):
        return round(v.total_seconds(), 3)
    if isinstance(v, (int, float)):
        return float(v)
    # "hh:mm:ss.fffffff" 형태 문자열
    m = re.match(r"(?:(\d+)\.)?(\d{1,2}):(\d{2}):(\d{2}(?:\.\d+)?)", str(v))
    if m:
        d = int(m.group(1) or 0); h = int(m.group(2)); mi = int(m.group(3)); s = float(m.group(4))
        return round(d * 86400 + h * 3600 + mi * 60 + s, 3)
    try:
        return float(v)
    except Exception:  # noqa: BLE001
        return None


def _num(v) -> Optional[float]:
    try:
        return float(v)
    except Exception:  # noqa: BLE001
        return None


def _dig(d, *path):
    """중첩 dynamic(dict) 안전 탐색."""
    cur = d
    for k in path:
        if isinstance(cur, dict):
            cur = cur.get(k)
        else:
            return None
    return cur


# ──────────────────────────────────────────────────────────────────────────
# 계층 2 — 엔진 (KQL / 관리 명령)
# ──────────────────────────────────────────────────────────────────────────
class EngineCollector:
    def __init__(self, cfg: Config, client=None):
        self.cfg = cfg
        self.client = client

    def _mgmt(self, db, cmd):
        return _rows(self.client.execute_mgmt(db, cmd))

    def _context_db(self, db):
        """cluster-scoped 명령(.show capacity/ingestion failures 등)용 컨텍스트 DB.
        --database 우선. 없으면 .show databases 로 실존 DB 하나를 탐색해 캐시한다.
        하드코딩 NetDefaultDB(삭제됐을 수 있음) 대신 실 DB를 쓴다.
        주의: 이 탐색 자체도 컨텍스트가 필요해 흔한 기본값으로 프로브하므로,
        실제 클러스터에서 한 번 동작 검증 권장."""
        if db:
            return db
        cached = getattr(self, "_ctx_db_cache", None)
        if cached is not None:
            return cached
        for probe in ("NetDefaultDB", ""):
            try:
                rows = _rows(self.client.execute_mgmt(
                    probe, ".show databases | project DatabaseName | take 1"))
                if rows and rows[0].get("DatabaseName"):
                    self._ctx_db_cache = rows[0]["DatabaseName"]
                    return self._ctx_db_cache
            except Exception:  # noqa: BLE001
                continue
        self._ctx_db_cache = "NetDefaultDB"  # 최후 폴백 (기존 동작과 동일)
        return self._ctx_db_cache

    def collect(self) -> EngineData:
        d = EngineData()
        if not self.cfg.cluster:
            d.error = "--cluster 미지정 → 엔진 계층 생략."
            return d
        try:
            if self.client is None:
                self.client = build_kusto_client(self.cfg)
        except Exception as e:  # noqa: BLE001
            d.error = str(e).strip().splitlines()[0]
            return d
        db = self.cfg.database

        # 느린 쿼리 (.show queries) — DB 범위
        if db:
            dbq = db.replace("'", "''")  # ④ bracketed-name escape (견고성)
            try:
                # 2① 서버측에서 top N만 가져와 payload/보존 한계 대응 (int로 주입 방어)
                rows = self._mgmt(db, f".show queries | top {int(self.cfg.top)} by Duration desc")
                rows.sort(key=lambda r: _secs(r.get("Duration")) or 0, reverse=True)
                for r in rows[: self.cfg.top]:
                    cache = r.get("CacheStatistics") or {}
                    scan = r.get("ScannedExtentsStatistics") or {}
                    if isinstance(cache, str):
                        try: cache = json.loads(cache)
                        except Exception: cache = {}  # noqa: BLE001,E701
                    if isinstance(scan, str):
                        try: scan = json.loads(scan)
                        except Exception: scan = {}  # noqa: BLE001,E701
                    # 2③ 디스크 접근 = 콜드 전체 + Hot.MissBytes.
                    #   Hot.MissBytes = 핫 캐시에 있어야 하나 미스 → 디스크 재조회이므로
                    #   '디스크 접근 비중'을 볼 때 콜드(디스크) 측에 합산한다.
                    #   (환경별 해석이 갈리는 지점 — 순수 콜드만 보려면 hot_miss 항 제거)
                    hot = _num(_dig(cache, "Shards", "Hot", "HitBytes"))
                    hot_miss = _num(_dig(cache, "Shards", "Hot", "MissBytes")) or 0
                    cold = ((_num(_dig(cache, "Shards", "Cold", "HitBytes")) or 0)
                            + (_num(_dig(cache, "Shards", "Cold", "MissBytes")) or 0)
                            + hot_miss) or None
                    d.slow.append(SlowQuery(
                        text=str(r.get("Text", ""))[:300],
                        duration_s=_secs(r.get("Duration")),
                        cpu_s=_secs(r.get("TotalCpu")),
                        mem_mb=(lambda m: round(m / 1048576, 1) if m else None)(_num(r.get("MemoryPeak"))),
                        hot_bytes=hot, cold_bytes=cold,
                        scanned_extents=_num(_dig(scan, "ScannedExtentsCount")),
                        total_extents=_num(_dig(scan, "TotalExtentsCount")),
                        scanned_rows=_num(_dig(scan, "ScannedRowsCount")),
                        user=str(r.get("User", "")), app=str(r.get("Application", ""))))
            except Exception as e:  # noqa: BLE001
                d.notes.append(f".show queries 실패: {str(e).splitlines()[0]}")

            try:
                cp = self._mgmt(db, f".show database ['{dbq}'] policy caching")
                if cp:
                    d.cache_policy = str(cp[0].get("Policy") or cp[0])[:200]
            except Exception:  # noqa: BLE001
                pass

            try:
                ex = self._mgmt(db, f".show database ['{dbq}'] extents | summarize Extents=count(), "
                                    f"Size=sum(ExtentSize)")
                if ex:
                    d.extents = {"count": _num(ex[0].get("Extents")),
                                 "size_gb": (lambda s: round(s / 1073741824, 1) if s else None)(_num(ex[0].get("Size")))}
            except Exception:  # noqa: BLE001
                pass

            try:
                tb = self._mgmt(db, ".show tables")
                d.tables = len(tb)
            except Exception:  # noqa: BLE001
                pass

        # 용량/스로틀 (.show capacity) — 클러스터 범위
        try:
            cap = self._mgmt(self._context_db(db), ".show capacity")
            for r in cap:
                d.capacity.append({"resource": r.get("Resource"), "total": _num(r.get("Total")),
                                   "consumed": _num(r.get("Consumed")), "remaining": _num(r.get("Remaining"))})
        except Exception as e:  # noqa: BLE001
            d.notes.append(f".show capacity 실패: {str(e).splitlines()[0]}")

        # 인제스트 실패
        try:
            inf = self._mgmt(self._context_db(db),
                             ".show ingestion failures | where FailedOn > ago(1d) | count")
            if inf:
                d.ingestion_failures = int(_num(inf[0].get("Count")) or 0)
        except Exception:  # noqa: BLE001
            pass
        return d


# ──────────────────────────────────────────────────────────────────────────
# 계층 1 — Azure Monitor (MetricsClient 2.x 우선, 1.x 폴백; pg_diagnose 와 동일 패턴)
# ──────────────────────────────────────────────────────────────────────────
def _ns_from_resource_id(rid: str) -> str:
    m = re.search(r"/providers/([^/]+)/([^/]+)/", rid or "")
    return f"{m.group(1)}/{m.group(2)}" if m else "Microsoft.Kusto/clusters"


def _resolve_region(cfg: Config, cred) -> Optional[str]:
    if cfg.region:
        return cfg.region
    try:
        import json as _json
        import urllib.request
        tok = cred.get_token("https://management.azure.com/.default").token
        url = f"https://management.azure.com{cfg.resource_id}?api-version=2023-08-15"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {tok}"})
        with urllib.request.urlopen(req, timeout=15) as r:  # noqa: S310
            return _json.load(r).get("location")
    except Exception:  # noqa: BLE001
        return None


class MetricsCollector:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def collect(self) -> dict[str, MetricSeries]:
        out: dict[str, MetricSeries] = {}
        names = [n for n, _ in ADX_METRICS]
        timespan = dt.timedelta(hours=self.cfg.hours)
        gran = dt.timedelta(minutes=self.cfg.granularity_min)
        try:
            cred = build_token_credential(self.cfg)
            resp_metrics = None
            try:
                from azure.monitor.querymetrics import MetricsClient, MetricAggregationType
                region = _resolve_region(self.cfg, cred)
                if not region:
                    raise RuntimeError("메트릭에 리전이 필요합니다 — --region <예: koreacentral> 지정")
                client = MetricsClient(f"https://{region}.metrics.monitor.azure.com", cred)
                results = client.query_resources(
                    resource_ids=[self.cfg.resource_id],
                    metric_namespace=_ns_from_resource_id(self.cfg.resource_id),
                    metric_names=names, timespan=timespan, granularity=gran,
                    aggregations=[MetricAggregationType.AVERAGE, MetricAggregationType.MAXIMUM])
                resp_metrics = results[0].metrics if results else []
            except ImportError:
                from azure.monitor.query import MetricsQueryClient, MetricAggregationType
                client = MetricsQueryClient(cred)
                resp = client.query_resource(
                    self.cfg.resource_id, metric_names=names, timespan=timespan, granularity=gran,
                    aggregations=[MetricAggregationType.AVERAGE, MetricAggregationType.MAXIMUM])
                resp_metrics = resp.metrics
        except Exception as e:  # noqa: BLE001
            for n, u in ADX_METRICS:
                out[n] = MetricSeries(n, u, error=str(e).strip().splitlines()[0])
            return out

        um = dict(ADX_METRICS)
        for m in resp_metrics or []:
            mname = getattr(m, "name", None)
            if not isinstance(mname, str):
                mname = getattr(mname, "value", None) or str(mname)
            ts, raw, avg, mx = [], [], [], []
            for series in getattr(m, "timeseries", None) or []:
                for dp in getattr(series, "data", None) or []:
                    t = getattr(dp, "timestamp", None)
                    ts.append(t.strftime("%m-%d %H:%M") if t else "")
                    raw.append(t); avg.append(getattr(dp, "average", None)); mx.append(getattr(dp, "maximum", None))
            out[mname] = MetricSeries(mname, um.get(mname, ""), ts, raw, avg, mx)
        return out


# ──────────────────────────────────────────────────────────────────────────
# 분석기
# ──────────────────────────────────────────────────────────────────────────
class Analyzer:
    def __init__(self, eng: EngineData, metrics: dict[str, MetricSeries], cfg: Config):
        self.e = eng
        self.m = metrics
        self.cfg = cfg
        self.findings: list[Finding] = []

    def _metric(self, name):
        return self.m.get(name)

    def run(self) -> list[Finding]:
        self._slow_queries()
        self._cache()
        self._capacity()
        self._ingestion()
        self._ingestion_utilization()
        self._query_duration()
        self._extents()
        self._cpu()
        self._correlate()
        if not self.findings:
            self.findings.append(Finding(SEV_OK, "전반", "특이 위험 없음",
                "수집된 신호에서 즉각적 임계치 초과가 없습니다.",
                "정기 진단으로 추세를 추적하세요."))
        order = {SEV_CRIT: 0, SEV_WARN: 1, SEV_INFO: 2, SEV_OK: 3}
        self.findings.sort(key=lambda f: order[f.severity])
        return self.findings

    def _slow_queries(self):
        if not self.e.slow:
            return
        top = self.e.slow[0]
        if top.duration_s and top.duration_s >= 10:
            sev = SEV_WARN if top.duration_s < 60 else SEV_CRIT
            self.findings.append(Finding(sev, "쿼리",
                f"느린 쿼리 — 최대 {top.duration_s:.1f}s",
                f"상위 쿼리 지속시간이 큽니다(CPU {top.cpu_s or '?'}s, 메모리 {top.mem_mb or '?'}MB).",
                "시간 필터 추가, 불필요한 join/소트 축소, materialized view·요약 테이블 검토. "
                "콜드 스캔이 크면 캐시 정책부터(아래 상관 참고)."))
        cold_heavy = [q for q in self.e.slow if (q.cold_ratio or 0) >= 0.5]
        if len(cold_heavy) >= max(2, len(self.e.slow) // 3):
            worst = max(cold_heavy, key=lambda q: q.cold_ratio or 0)
            self.findings.append(Finding(SEV_WARN, "쿼리 · 캐시",
                f"콜드(디스크) 캐시 의존 쿼리 {len(cold_heavy)}개",
                f"콜드 셰이드 접근 비중이 큰 쿼리가 많습니다(최대 콜드 {worst.cold_ratio*100:.0f}%) — "
                "핫캐시 적중이 낮아 쿼리가 느려집니다.",
                "자주 조회하는 기간을 포함하도록 캐시(핫) 정책을 넓히세요. "
                "핫캐시 미스 제거가 SKU 변경보다 속도 개선 효과가 큽니다."))
        scan_heavy = [q for q in self.e.slow if (q.scanned_ratio or 0) >= 0.8]
        if scan_heavy:
            w = max(scan_heavy, key=lambda q: q.scanned_ratio or 0)
            self.findings.append(Finding(SEV_INFO, "쿼리 · 필터링",
                f"익스텐트 과다 스캔 쿼리 {len(scan_heavy)}개",
                f"전체 익스텐트의 대부분을 스캔합니다(최대 {w.scanned_ratio*100:.0f}%) — 필터가 약하거나 "
                "인덱싱/파티셔닝이 쿼리 패턴과 맞지 않습니다.",
                "시간·고카디널리티 컬럼 필터를 앞단에 두고, partitioning/row-order 정책을 조회 패턴에 맞추세요."))

    def _cache(self):
        cu = self._metric("CacheUtilization")
        v = cu.max_v if cu else None
        if v is not None and v >= 90:
            self.findings.append(Finding(SEV_WARN, "캐시",
                f"캐시 사용률 {v}% (최대)",
                "핫캐시가 가득 차 콜드 스캔이 늘 수 있습니다.",
                "캐시 정책 기간을 데이터 양에 맞게 조정하거나 인스턴스/SKU의 캐시 용량을 늘리세요. "
                "우선순위는 캐시 적중률 개선입니다."))

    def _capacity(self):
        tq = self._metric("TotalNumberOfThrottledQueries")
        if tq and (tq.max_v or 0) > 0:
            self.findings.append(Finding(SEV_WARN, "용량",
                f"스로틀된 쿼리 발생 (최대 {tq.max_v})",
                "동시성/리소스 한계로 쿼리가 스로틀되고 있습니다.",
                "워크로드 그룹·요청 분류로 동시성 관리, 피크 분산, 필요 시 스케일아웃."))
        for c in self.e.capacity:
            tot, con = c.get("total"), c.get("consumed")
            if tot and con and tot > 0 and con / tot >= 0.9:
                self.findings.append(Finding(SEV_INFO, "용량",
                    f"{c.get('resource')} 용량 {con/tot*100:.0f}% 소모",
                    "특정 리소스(인제스트/머지/쿼리 등) 용량이 거의 찼습니다.",
                    "해당 작업의 동시성 정책·인스턴스 수를 검토하세요."))

    def _ingestion(self):
        il = self._metric("IngestionLatencyInSeconds")
        v = il.avg_v if il else None
        if v is not None and v >= 60:
            self.findings.append(Finding(SEV_WARN, "인제스트",
                f"인제스트 지연 평균 {v:.0f}s",
                "데이터 적재 지연이 큽니다 — 최신 데이터 조회가 늦어집니다.",
                "배치 정책(크기/시간), 인제스트 동시성, 스트리밍 인제스트 적용 여부를 검토하세요."))
        if self.e.ingestion_failures > 0:
            self.findings.append(Finding(SEV_WARN, "인제스트",
                f"최근 24h 인제스트 실패 {self.e.ingestion_failures}건",
                "적재 실패가 발생했습니다.",
                ".show ingestion failures 로 원인(매핑/스키마/권한) 확인."))

    def _extents(self):
        ex = self.e.extents
        if ex and ex.get("count") and ex["count"] >= 10000:
            self.findings.append(Finding(SEV_INFO, "스토리지",
                f"익스텐트 {int(ex['count']):,}개",
                "작은 익스텐트가 많으면 머지 지연·쿼리 오버헤드가 생길 수 있습니다.",
                "머지 정책을 확인하고 인제스트 배치 크기를 키워 익스텐트 수를 줄이세요."))

    def _cpu(self):
        cpu = self._metric("CPU")
        v = cpu.max_v if cpu else None
        if v is not None and v >= 90:
            self.findings.append(Finding(SEV_INFO, "리소스",
                f"CPU 최대 {v}%",
                "CPU 포화 구간이 있습니다.",
                "피크 시간 워크로드 분산 또는 스케일업/아웃 검토."))

    def _query_duration(self):
        # 2② 플랫폼 메트릭 QueryDuration(ms)을 판정에 사용 + 엔진 계층 느린 쿼리와 교차 검증
        qd = self._metric("QueryDuration")
        v = qd.max_v if qd else None  # ms
        if v is None:
            return
        sec = v / 1000.0
        if sec < 10:
            return
        engine_slow = [q for q in self.e.slow if (q.duration_s or 0) >= 10]
        if engine_slow:
            # 동일 느림을 _slow_queries가 이미 감점하므로 여기선 교차검증(info)로 두어
            # Health Score 이중 감점을 방지한다.
            sev = SEV_INFO
            worst = max(q.duration_s or 0 for q in engine_slow)
            detail = (f"플랫폼 메트릭 QueryDuration 최대 {sec:.1f}s — 엔진 계층 느린 쿼리 "
                      f"{len(engine_slow)}건과 교차 검증됩니다(엔진 최대 {worst:.1f}s).")
            rec = ("상위 느린 쿼리부터 최적화(시간 필터·조인/소트 축소·캐시 정책). "
                   "아래 느린 쿼리·상관 섹션과 함께 판단하세요.")
        else:
            # 엔진 상세가 없을 땐 이 메트릭이 유일한 느림 신호 → severity 유지.
            sev = SEV_WARN if sec < 60 else SEV_CRIT
            detail = (f"플랫폼 메트릭 QueryDuration 최대 {sec:.1f}s — 다만 엔진 계층(.show queries) "
                      f"상세가 비어 교차 검증이 불가합니다(--database 미지정 또는 권한).")
            rec = "--database 를 지정해 재실행하면 어떤 쿼리가 느린지 엔진 계층에서 확인됩니다."
        self.findings.append(Finding(sev, "쿼리 · 메트릭",
            f"쿼리 지속시간 높음 (최대 {sec:.1f}s)", detail, rec))

    def _ingestion_utilization(self):
        # 2② 수집만 하던 IngestionUtilization(%)을 판정에 사용
        iu = self._metric("IngestionUtilization")
        v = iu.max_v if iu else None  # %
        if v is not None and v >= 80:
            sev = SEV_WARN if v < 95 else SEV_CRIT
            self.findings.append(Finding(sev, "인제스트 · 메트릭",
                f"인제스트 사용률 {v}% (최대)",
                "인제스트 용량이 포화에 가깝습니다 — 적재 지연·실패로 이어질 수 있습니다.",
                "인제스트 동시성·배치 정책 조정, 스트리밍 인제스트 검토, 필요 시 스케일아웃. "
                "인제스트 지연/실패 finding과 함께 판단하세요."))

    def _correlate(self):
        cu = self._metric("CacheUtilization")
        cache_high = cu and (cu.max_v or 0) >= 85
        tq = self._metric("TotalNumberOfThrottledQueries")
        throttled = tq and (tq.max_v or 0) > 0
        cold_heavy = [q for q in self.e.slow if (q.cold_ratio or 0) >= 0.5]
        slow_big = any((q.duration_s or 0) >= 10 for q in self.e.slow)
        if slow_big and cold_heavy and (cache_high or throttled):
            self.findings.append(Finding(SEV_CRIT, "상관 · 근본원인",
                "느린 쿼리 + 콜드 캐시 스캔 + 캐시 압박/스로틀 동반",
                "느린 쿼리들이 콜드(디스크) 스캔에 몰려 있고, 동시에 캐시 사용률이 높거나 스로틀이 발생합니다 "
                "— 핫캐시 미스가 쿼리 지연의 주범일 가능성이 큽니다.",
                "① 캐시(핫) 정책을 조회 패턴에 맞게 넓히기 → ② 효과 부족 시 캐시 용량 큰 인스턴스로 스케일. "
                "핫캐시 미스 제거가 SKU 종류 변경보다 우선입니다."))


def health_score(findings):
    return max(0, min(100, 100 - sum(SEV_WEIGHT.get(f.severity, 0) for f in findings)))


# ──────────────────────────────────────────────────────────────────────────
# Baseline 이력 / 회귀
# ──────────────────────────────────────────────────────────────────────────
def _safe(s):
    return re.sub(r"[^A-Za-z0-9_.-]", "_", s or "demo")


def load_baseline(cfg: Config):
    if not cfg.history:
        return None
    key = _safe((cfg.cluster or "demo") + "__" + (cfg.database or ""))
    files = sorted(glob.glob(os.path.join(cfg.history_dir, f"{key}__*.json")))
    if not files:
        return None
    try:
        with open(files[-1], encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:  # noqa: BLE001
        return None


def save_snapshot(cfg: Config, score, eng: EngineData, metrics):
    if not cfg.history or cfg.demo:
        return
    os.makedirs(cfg.history_dir, exist_ok=True)
    cu = metrics.get("CacheUtilization")
    snap = {"schema_version": 1, "cluster": cfg.cluster, "database": cfg.database,
            "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "health_score": score,
            "max_query_s": max([q.duration_s or 0 for q in eng.slow], default=0),
            "cache_util_max": (cu.max_v if cu else None)}
    key = _safe((cfg.cluster or "demo") + "__" + (cfg.database or ""))
    fn = os.path.join(cfg.history_dir, f"{key}__{dt.datetime.now():%Y%m%d_%H%M%S}.json")
    try:
        with open(fn, "w", encoding="utf-8") as fh:
            json.dump(snap, fh, ensure_ascii=False, indent=2)
    except Exception:  # noqa: BLE001
        pass


# ──────────────────────────────────────────────────────────────────────────
# HTML 리포터 (pg_diagnose 와 동일 디자인 언어)
# ──────────────────────────────────────────────────────────────────────────
def _esc(v):
    return html.escape("" if v is None else str(v))


def _spark(values, color, w=240, h=40):
    pts = [v for v in values if v is not None]
    if len(pts) < 2:
        return '<span class="muted">데이터 없음</span>'
    lo, hi = min(pts), max(pts); span = (hi - lo) or 1; n = len(values); co = []
    for i, v in enumerate(values):
        if v is None:
            continue
        x = i / (n - 1) * (w - 4) + 2; y = h - 2 - (v - lo) / span * (h - 6)
        co.append(f"{x:.1f},{y:.1f}")
    return (f'<svg class="spark" viewBox="0 0 {w} {h}" preserveAspectRatio="none">'
            f'<polyline points="{" ".join(co)}" fill="none" stroke="{color}" stroke-width="1.6"/></svg>')


def render_html(cfg, eng: EngineData, metrics, findings, baseline, generated_at):
    score = health_score(findings)
    score_label = "양호" if score >= 85 else ("주의" if score >= 60 else "위험")
    score_cls = SEV_OK if score >= 85 else (SEV_WARN if score >= 60 else SEV_CRIT)
    nc = sum(1 for f in findings if f.severity == SEV_CRIT)
    nw = sum(1 for f in findings if f.severity == SEV_WARN)
    ni = sum(1 for f in findings if f.severity == SEV_INFO)

    fcards = "".join(f"""
      <article class="finding sev-{f.severity}">
        <div class="finding-head"><span class="chip chip-{f.severity}">{SEV_LABEL[f.severity]}</span>
          <span class="cat">{_esc(f.category)}</span><h3>{_esc(f.title)}</h3></div>
        <p class="detail">{_esc(f.detail)}</p>
        <div class="reco"><span class="reco-label">권장 조치</span>{_esc(f.recommendation)}</div>
      </article>""" for f in findings)

    # 메트릭 스파크라인
    mhtml = ""
    colors = {"CacheUtilization": "#8661c5", "CPU": "#c8362f", "QueryDuration": "#0078D4",
              "IngestionLatencyInSeconds": "#107c10", "IngestionUtilization": "#0a6cbd",
              "TotalNumberOfThrottledQueries": "#b07b00"}
    any_metric = any(not s.error and s.avg for s in metrics.values()) if metrics else False
    if metrics and any_metric:
        for n, _ in ADX_METRICS:
            s = metrics.get(n)
            if not s or s.error or not s.avg:
                continue
            mhtml += (f'<div class="metric-card"><div class="metric-name">'
                      f'<span class="dot" style="background:{colors.get(n,"#0078D4")}"></span>{_esc(n)} '
                      f'<span class="unit">({_esc(s.unit)})</span></div>'
                      f'<div class="metric-vals"><span>평균 <b>{s.avg_v}</b></span>'
                      f'<span>최대 <b>{s.max_v}</b></span></div>{_spark(s.avg, colors.get(n,"#0078D4"))}</div>')
        mhtml = f'<div class="metrics">{mhtml}</div>'
    else:
        err = next((s.error for s in (metrics or {}).values() if s.error), None)
        mhtml = f'<p class="muted">Azure Monitor 메트릭 생략/실패: {_esc(err or "--resource-id 미지정")}</p>'

    # 느린 쿼리 테이블
    if eng.error:
        slow_html = f'<p class="err">엔진 수집 실패: {_esc(eng.error)}</p>'
    elif not eng.slow:
        slow_html = '<p class="muted">느린 쿼리 데이터 없음 (--database 필요 또는 권한 확인)</p>'
    else:
        rows = ""
        for q in eng.slow[:cfg.top]:
            cold = f"{q.cold_ratio*100:.0f}%" if q.cold_ratio is not None else "—"
            scanned = (f"{int(q.scanned_extents):,}/{int(q.total_extents):,}"
                       if (q.scanned_extents is not None and q.total_extents) else "—")
            rows += (f"<tr><td class='mono'>{_esc(q.text[:90])}</td>"
                     f"<td>{('%.1f'%q.duration_s) if q.duration_s is not None else '—'}</td>"
                     f"<td>{('%.1f'%q.cpu_s) if q.cpu_s is not None else '—'}</td>"
                     f"<td>{q.mem_mb if q.mem_mb is not None else '—'}</td>"
                     f"<td>{cold}</td><td>{scanned}</td>"
                     f"<td>{_esc(q.app or q.user)}</td></tr>")
        slow_html = (f'<table><thead><tr><th>쿼리(요약)</th><th>지속(s)</th><th>CPU(s)</th>'
                     f'<th>메모리(MB)</th><th>콜드%</th><th>스캔/전체 익스텐트</th><th>앱/사용자</th></tr></thead>'
                     f'<tbody>{rows}</tbody></table>')

    # 용량 테이블
    if eng.capacity:
        cr = ""
        for c in eng.capacity:
            tot, con = c.get("total"), c.get("consumed")
            pct = f"{con/tot*100:.0f}%" if (tot and con and tot > 0) else "—"
            cr += (f"<tr><td>{_esc(c.get('resource'))}</td><td>{con if con is not None else '—'}</td>"
                   f"<td>{tot if tot is not None else '—'}</td><td>{pct}</td></tr>")
        cap_html = (f'<table><thead><tr><th>resource</th><th>consumed</th><th>total</th>'
                    f'<th>사용률</th></tr></thead><tbody>{cr}</tbody></table>')
    else:
        cap_html = '<p class="muted">.show capacity 데이터 없음</p>'

    # 구성 요약
    cfg_items = []
    if eng.tables is not None:
        cfg_items.append(f"테이블 {eng.tables}개")
    if eng.extents:
        cfg_items.append(f"익스텐트 {int(eng.extents.get('count') or 0):,}개"
                         + (f" · {eng.extents.get('size_gb')}GB" if eng.extents.get('size_gb') else ""))
    if eng.cache_policy:
        cfg_items.append(f"캐시 정책: {_esc(eng.cache_policy[:120])}")
    cfg_items.append(f"인제스트 실패(24h): {eng.ingestion_failures}건")
    cfg_html = "<ul class='kvlist'>" + "".join(f"<li>{_esc(x)}</li>" for x in cfg_items) + "</ul>"

    if baseline:
        prev = baseline.get("health_score"); delta = score - (prev or 0)
        dcls = SEV_OK if delta >= 0 else SEV_CRIT
        base_html = (f'<p class="kv">Baseline <b>{_esc(baseline.get("generated_at"))}</b> 대비 · '
                     f'Health Score <b>{prev} → {score}</b> '
                     f'(<span style="color:var(--{dcls})">{"+" if delta>=0 else ""}{delta}</span>) · '
                     f'최대 쿼리 <b>{baseline.get("max_query_s")}s → {max([q.duration_s or 0 for q in eng.slow], default=0):.1f}s</b></p>')
    else:
        base_html = '<p class="muted">이전 스냅샷 없음 (다음 실행부터 추세 비교).</p>'

    notes = ("<p class='muted'>" + " · ".join(_esc(n) for n in eng.notes) + "</p>") if eng.notes else ""

    return f"""<!DOCTYPE html><html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ADX 진단 리포트 — {_esc(cfg.cluster or 'demo')}</title>
<style>
 :root{{--ink:#15202b;--bg:#eef1f5;--card:#fff;--text:#1f2933;--muted:#64748b;--border:#e2e8f0;
  --azure:#0078D4;--crit:#c8362f;--warn:#b07b00;--info:#0a6cbd;--ok:#107c10;}}
 *{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--text);line-height:1.5;
  font-family:"Segoe UI",system-ui,-apple-system,"Malgun Gothic",sans-serif}}
 .mono,code,table{{font-family:"Cascadia Code","Consolas",ui-monospace,monospace}}
 .wrap{{max-width:1080px;margin:0 auto;padding:0 20px 64px}}
 header{{background:var(--ink);color:#e8eef4;padding:28px 0}}
 .hd{{max-width:1080px;margin:0 auto;padding:0 20px;display:flex;justify-content:space-between;
  align-items:center;gap:24px;flex-wrap:wrap}}
 header h1{{font-size:20px;margin:0 0 6px;font-weight:650}}
 .lvl{{display:inline-block;font-size:10.5px;letter-spacing:1px;background:rgba(0,120,212,.25);
  border:1px solid rgba(120,180,230,.4);color:#cfe6fb;border-radius:20px;padding:1px 9px;margin-left:8px}}
 header .meta{{font-size:12.5px;color:#9fb2c4}} header .meta b{{color:#cfe0ee}}
 .score{{text-align:center;padding:10px 22px;border-radius:12px;background:rgba(255,255,255,.06);
  border:1px solid rgba(255,255,255,.12)}}
 .score .num{{font-size:38px;font-weight:700;line-height:1}}
 .score .lab{{font-size:11px;letter-spacing:2px;text-transform:uppercase;margin-top:4px}}
 .score.ok .num{{color:#5dd55d}}.score.warning .num{{color:#ffcf4d}}.score.critical .num{{color:#ff7a72}}
 .summary{{display:flex;gap:12px;margin:22px 0 8px;flex-wrap:wrap}}
 .stat{{flex:1;min-width:120px;background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px 16px}}
 .stat .n{{font-size:26px;font-weight:700}}.stat .l{{font-size:12px;color:var(--muted)}}
 .stat.crit .n{{color:var(--crit)}}.stat.warn .n{{color:var(--warn)}}.stat.info .n{{color:var(--info)}}
 h2{{font-size:15px;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);
  margin:34px 0 14px;padding-bottom:8px;border-bottom:1px solid var(--border)}}
 .finding{{background:var(--card);border:1px solid var(--border);border-left:4px solid var(--muted);
  border-radius:10px;padding:16px 18px;margin-bottom:12px}}
 .finding.sev-critical{{border-left-color:var(--crit)}}.finding.sev-warning{{border-left-color:var(--warn)}}
 .finding.sev-info{{border-left-color:var(--info)}}.finding.sev-ok{{border-left-color:var(--ok)}}
 .finding-head{{display:flex;align-items:center;gap:10px;flex-wrap:wrap}}
 .finding-head h3{{font-size:15px;margin:0;flex-basis:100%}}
 .chip{{font-size:11px;font-weight:700;padding:2px 9px;border-radius:20px;color:#fff}}
 .chip-critical{{background:var(--crit)}}.chip-warning{{background:var(--warn)}}
 .chip-info{{background:var(--info)}}.chip-ok{{background:var(--ok)}}
 .cat{{font-size:11.5px;color:var(--muted);border:1px solid var(--border);padding:1px 8px;border-radius:20px}}
 .detail{{margin:10px 0;font-size:14px}}
 .reco{{background:#f1f6fb;border:1px solid #d8e7f4;border-radius:8px;padding:10px 12px;font-size:13.5px}}
 .reco-label{{display:inline-block;font-size:11px;font-weight:700;color:var(--azure);margin-right:8px;
  text-transform:uppercase;letter-spacing:.5px}}
 .panel{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:16px 18px}}
 .kv{{font-size:14px;margin:0}}.kv b{{color:var(--azure)}}
 .kvlist{{margin:0;padding-left:18px;font-size:13.5px}} .kvlist li{{margin:3px 0}}
 .metrics{{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:12px}}
 .metric-card{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px}}
 .metric-name{{font-size:13px;font-weight:600;display:flex;align-items:center;gap:7px}}
 .metric-name .unit{{color:var(--muted);font-weight:400}}
 .dot{{width:9px;height:9px;border-radius:50%;display:inline-block}}
 .metric-vals{{display:flex;justify-content:space-between;font-size:12.5px;color:var(--muted);margin:8px 0 4px}}
 .metric-vals b{{color:var(--text);font-size:15px}} svg.spark{{width:100%;height:40px;display:block}}
 table{{width:100%;border-collapse:collapse;font-size:12px;background:var(--card);
  border:1px solid var(--border);border-radius:8px;overflow:hidden}}
 th{{background:#f3f6f9;text-align:left;padding:7px 9px;font-weight:600;color:#42526b;
  border-bottom:1px solid var(--border);white-space:nowrap}}
 td{{padding:6px 9px;border-bottom:1px solid #eef2f6;vertical-align:top;overflow-wrap:anywhere}}
 td.mono{{font-size:11px}} tbody tr:hover{{background:#f8fbff}}
 .muted{{color:var(--muted);font-size:13px}}.err{{color:var(--crit);font-size:12.5px}}
 footer{{color:var(--muted);font-size:12px;margin-top:40px;padding-top:16px;border-top:1px solid var(--border)}}
 @media (max-width:640px){{.hd{{flex-direction:column;align-items:flex-start}}}}
</style></head><body>
<header><div class="hd">
  <div><h1>ADX 진단 리포트 — Azure Data Explorer<span class="lvl">FULL + REGRESSION</span></h1>
    <div class="meta">cluster <b>{_esc(cfg.cluster or 'demo')}</b>
      {('· db <b>'+_esc(cfg.database)+'</b>') if cfg.database else ''}
      · 신호원 <b>KQL/관리명령 + Azure Monitor</b> · 생성 {_esc(generated_at)}</div></div>
  <div class="score {score_cls}"><div class="num">{score}</div><div class="lab">{score_label}</div></div>
</div></header>
<div class="wrap">
  <div class="summary">
    <div class="stat crit"><div class="n">{nc}</div><div class="l">위험 (Critical)</div></div>
    <div class="stat warn"><div class="n">{nw}</div><div class="l">주의 (Warning)</div></div>
    <div class="stat info"><div class="n">{ni}</div><div class="l">정보 (Info)</div></div>
  </div>

  <h2>발견사항 및 권장 조치</h2>
  {fcards}

  <h2>Baseline 추세 (회귀)</h2>
  <div class="panel">{base_html}</div>

  <h2>리소스 추세 · Azure Monitor (계층 1)</h2>
  {mhtml}

  <h2>느린 쿼리 (.show queries · 계층 2)</h2>
  {slow_html}

  <h2>용량 · 스로틀 (.show capacity)</h2>
  {cap_html}

  <h2>클러스터/DB 구성</h2>
  <div class="panel">{cfg_html}{notes}</div>

  <footer>
    <p>읽기 전용 — 쿼리/관리 조회 명령(.show ...)과 Azure Monitor read 만 호출하며 클러스터를 변경하지 않습니다.</p>
    <p>핵심 원칙: 핫캐시 미스 제거가 SKU 종류 변경보다 쿼리 속도 영향이 큽니다. 임계치는 일반 휴리스틱이며 환경에 맞게 조정하세요.</p>
  </footer>
</div></body></html>"""


# ──────────────────────────────────────────────────────────────────────────
# 데모
# ──────────────────────────────────────────────────────────────────────────
def demo_engine() -> EngineData:
    d = EngineData(tables=42, ingestion_failures=2,
                   cache_policy="hot=7d (DataHotSpan)", extents={"count": 18400, "size_gb": 920.0})
    d.slow = [
        SlowQuery("Events | where Timestamp > ago(90d) | summarize count() by bin(Timestamp,1h), Region",
                  78.4, 61.0, 4200.0, hot_bytes=2.0e9, cold_bytes=8.0e9,
                  scanned_extents=1850, total_extents=2000, scanned_rows=5.2e8, app="Dashboard"),
        SlowQuery("Logs | join kind=inner (Ref) on Id | where Level=='Error'",
                  41.2, 33.0, 3100.0, hot_bytes=1.2e9, cold_bytes=3.4e9,
                  scanned_extents=1400, total_extents=2000, scanned_rows=2.1e8, app="ETL"),
        SlowQuery("Metrics | where Name=='cpu' | summarize avg(Value) by Host",
                  22.8, 12.0, 900.0, hot_bytes=2.6e9, cold_bytes=0.2e9,
                  scanned_extents=300, total_extents=2000, scanned_rows=4.0e7, app="API"),
        SlowQuery("Traces | where ts > ago(30d) | take 1000",
                  14.1, 6.0, 400.0, hot_bytes=1.0e9, cold_bytes=0.0,
                  scanned_extents=120, total_extents=2000, scanned_rows=1.0e6, app="Ad-hoc"),
    ]
    d.capacity = [{"resource": "Queries", "total": 100.0, "consumed": 96.0, "remaining": 4.0},
                  {"resource": "Ingestion", "total": 60.0, "consumed": 38.0, "remaining": 22.0},
                  {"resource": "ExtentsMerge", "total": 20.0, "consumed": 19.0, "remaining": 1.0}]
    return d


def demo_metrics() -> dict[str, MetricSeries]:
    import math
    out = {}
    base = {"CacheUtilization": 92, "CPU": 74, "QueryDuration": 8200,
            "IngestionLatencyInSeconds": 85, "IngestionUtilization": 55, "TotalNumberOfThrottledQueries": 3}
    for n, u in ADX_METRICS:
        b = base[n]
        avg = [round(b * (0.8 + 0.2 * abs(math.sin(i / 3.0))), 1) for i in range(24)]
        mx = [round(a * 1.1, 1) for a in avg]
        if u == "%":   # 퍼센트 지표는 100 으로 클램프
            avg = [min(a, 100.0) for a in avg]; mx = [min(x, 100.0) for x in mx]
        out[n] = MetricSeries(n, u, [f"{i:02d}:00" for i in range(24)], [], avg, mx)
    return out


def demo_baseline():
    return {"schema_version": 1, "cluster": "demo", "generated_at": "2026-06-03 09:00:00",
            "health_score": 78, "max_query_s": 40.0, "cache_util_max": 80.0}


# ──────────────────────────────────────────────────────────────────────────
def parse_args(argv):
    ba = argparse.BooleanOptionalAction
    p = argparse.ArgumentParser(description="ADX(Azure Data Explorer) 진단 → HTML (KQL+Monitor, 회귀)")
    p.add_argument("--cluster", help="https://<name>.<region>.kusto.windows.net")
    p.add_argument("--database")
    p.add_argument("--auth", default="default", choices=["default", "cli", "app", "msi"],
                   help="default(DefaultAzureCredential)/cli(az)/app(서비스주체)/msi")
    p.add_argument("--app-id"); p.add_argument("--tenant"); p.add_argument("--client-id")
    p.add_argument("--resource-id", help="Azure Monitor 대상 ADX 클러스터 리소스 ID")
    p.add_argument("--region", help="메트릭 엔드포인트 리전 (예: koreacentral)")
    p.add_argument("--hours", type=int, default=24)
    p.add_argument("--granularity-min", type=int, default=15)
    p.add_argument("--top", type=int, default=15)
    p.add_argument("--history", action=ba, default=True)
    p.add_argument("--history-dir", default="./adx_diagnose_history")
    p.add_argument("--out", default="adx_report.html")
    p.add_argument("--demo", action="store_true")
    a = p.parse_args(argv)
    return Config(cluster=a.cluster, database=a.database, auth=a.auth, app_id=a.app_id, tenant=a.tenant,
                  client_id=a.client_id, resource_id=a.resource_id, region=a.region, hours=a.hours,
                  granularity_min=a.granularity_min, top=a.top, history=a.history,
                  history_dir=a.history_dir, out=a.out, demo=a.demo)


def main(argv=None):
    cfg = parse_args(argv if argv is not None else sys.argv[1:])
    generated_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M")

    if cfg.demo:
        eng, metrics, baseline = demo_engine(), demo_metrics(), demo_baseline()
    else:
        eng = EngineCollector(cfg).collect()
        metrics = MetricsCollector(cfg).collect() if cfg.resource_id else {}
        baseline = load_baseline(cfg)
        if eng.error:
            print(f"참고: 엔진 수집 실패(연결/인증 확인) — {eng.error}", file=sys.stderr)
        if not cfg.resource_id:
            print("참고: --resource-id 미지정 → Azure Monitor 메트릭 생략.", file=sys.stderr)

    findings = Analyzer(eng, metrics, cfg).run()
    score = health_score(findings)
    save_snapshot(cfg, score, eng, metrics)

    out_html = render_html(cfg, eng, metrics, findings, baseline, generated_at)
    with open(cfg.out, "w", encoding="utf-8") as fh:
        fh.write(out_html)
    print(f"리포트 생성 완료: {cfg.out}  (Health Score: {score}, "
          f"위험 {sum(1 for f in findings if f.severity==SEV_CRIT)}건)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
