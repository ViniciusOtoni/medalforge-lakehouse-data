"""
Monitoramento de execuções em Azure Table Storage.

Objetivo
--------
Registrar início/fim de um run (pipeline) com status e metadados mínimos,
usando autenticação AAD com SPN (DefaultAzureCredential).

Design
------
- Soft-dependency: se as libs Azure *ou* ENVs não estiverem presentes, vira no-op.
- Particionamento pensado para leitura eficiente (PK por dia/env/pipeline/schema).
- Upsert idempotente por (PartitionKey, RowKey).

Requisitos de RBAC (data plane)
-------------------------------
A SPN usada pela autenticação deve ter **Storage Table Data Contributor** na Storage Account.

ENVs esperadas (SPN dinâmica + destino)
---------------------------------------
- AZURE_CLIENT_ID
- AZURE_TENANT_ID
- AZURE_CLIENT_SECRET
- MON_TABLE_ACCOUNT   -> nome da Storage Account (ex.: medalforgestorage)
- MON_TABLE_NAME      -> nome da tabela (default 'pipeline_runs') [opcional]
- ENV                 -> ambiente lógico (ex.: dev|hml|prd) [opcional]
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional
import json
import os
import uuid

# soft dependency: se as libs não existirem localmente (CI/ambiente sem Azure),
# o módulo funciona como no-op.
try:
    from azure.identity import DefaultAzureCredential  # type: ignore
    from azure.data.tables import TableServiceClient  # type: ignore
except Exception:  # pragma: no cover
    DefaultAzureCredential = None  # type: ignore
    TableServiceClient = None  # type: ignore


def _iso_now() -> str:
    """Timestamp atual em ISO-8601 UTC (com 'Z')."""
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _yyyymmdd(ts: Optional[datetime] = None) -> str:
    """Data base YYYYMMDD para particionamento."""
    ts = ts or datetime.now(tz=timezone.utc)
    return ts.strftime("%Y%m%d")


def _enabled() -> bool:
    """
    Verifica se o monitoramento deve rodar.

    Habilitamos somente quando:
      - libs Azure estão disponíveis, e
      - MON_TABLE_ACCOUNT está definido.
    """
    return bool(TableServiceClient and DefaultAzureCredential and os.getenv("MON_TABLE_ACCOUNT"))


def _table_client() -> Optional["TableServiceClient"]:
    """
    Constrói um TableClient para a tabela de runs.

    Retorna
    -------
    TableClient | None
        None quando desabilitado (no-op).
    """
    if not _enabled():  # no-op
        return None

    account = os.environ["MON_TABLE_ACCOUNT"].strip()
    table = os.getenv("MON_TABLE_NAME", "pipeline_runs").strip() or "pipeline_runs"
    endpoint = f"https://{account}.table.core.windows.net"
    cred = DefaultAzureCredential()  # usa AZURE_CLIENT_ID/SECRET/TENANT_ID

    svc = TableServiceClient(endpoint=endpoint, credential=cred)
    # create_table retorna/erra em concorrência; preferimos idempotência com try/except
    try:
        svc.create_table_if_not_exists(table_name=table)
    except Exception:
        pass
    return svc.get_table_client(table_name=table)


def _partition_key(env: str, pipeline: str, schema: str, ts: Optional[datetime] = None) -> str:
    """
    Monta a PartitionKey no formato: "{env}|{pipeline}|{schema}|{yyyymmdd}".
    """
    return f"{env}|{pipeline}|{schema}|{_yyyymmdd(ts)}"


@dataclass
class PipelineRunLogger:
    """
    Context manager para registrar início/fim de uma execução (run).

    Atributos
    ---------
    env : str
        Ambiente lógico (ex.: dev|hml|prd).
    pipeline : str
        Nome do pipeline (ex.: bronze|silver).
    schema : str
        Schema lógico do alvo (ex.: sales).
    table : str
        Tabela alvo (ex.: orders, t_clean).
    target_fqn : str
        FQN do alvo (ex.: silver.sales.t_clean).
    run_id : str
        Identificador único do run (se não informado, geramos UUID4).
    extra : dict
        Propriedades adicionais curtas (serão serializadas).
    _tc : TableClient | None
        Cliente da tabela (ou None para no-op).
    _start_iso : str
        Timestamp ISO-8601 do início.
    """

    env: str
    pipeline: str
    schema: str
    table: str
    target_fqn: str
    run_id: Optional[str] = None
    extra: Dict[str, str] = field(default_factory=dict)

    _tc: Optional[object] = field(init=False, default=None, repr=False)
    _start_iso: str = field(init=False, default="", repr=False)

    def __post_init__(self) -> None:
        self.run_id = self.run_id or uuid.uuid4().hex
        self._tc = _table_client()
        self._start_iso = _iso_now()

    # ---------- API pública ----------

    def __enter__(self) -> "PipelineRunLogger":
        """Registra entidade inicial (status='running')."""
        self._upsert(
            status="running",
            ts_start=self._start_iso,
            ts_end=None,
            duration_ms=None,
            error_json=None,
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Finalize o run com status 'ok' ou 'fail'."""
        status = "ok" if exc_type is None else "fail"
        self.finish(status=status, error_json=(str(exc) if exc else None))

    def finish(self, status: str = "ok", *, error_json: Optional[str] = None, **metrics) -> None:
        """
        Conclui o run atualizando ts_end, duração e métricas opcionais.

        Parâmetros
        ----------
        status : str
            'ok' | 'fail' | 'partial'
        error_json : str | None
            Mensagem/resumo de erro quando falho.
        metrics : dict
            Ex.: counts_valid=..., counts_quarantine=..., counts_final=...
        """
        ts_end = _iso_now()
        duration_ms = max(
            0,
            int(
                (datetime.fromisoformat(ts_end.replace("Z", "+00:00"))
                 - datetime.fromisoformat(self._start_iso.replace("Z", "+00:00"))
                 ).total_seconds() * 1000
            ),
        )
        self._upsert(
            status=status,
            ts_start=self._start_iso,
            ts_end=ts_end,
            duration_ms=duration_ms,
            error_json=error_json,
            **metrics,
        )

    # ---------- internos ----------

    def _upsert(
        self,
        *,
        status: str,
        ts_start: Optional[str],
        ts_end: Optional[str],
        duration_ms: Optional[int],
        error_json: Optional[str],
        **metrics,
    ) -> None:
        """Monta e faz upsert da entidade (no-op se sem cliente)."""
        if not self._tc:  # no-op
            return

        entity = {
            "PartitionKey": _partition_key(self.env, self.pipeline, self.schema),
            "RowKey": self.run_id,
            "pipeline": self.pipeline,
            "schema": self.schema,
            "table": self.table,
            "target_fqn": self.target_fqn,
            "status": status,
            "ts_start": ts_start,
            "ts_end": ts_end,
            "duration_ms": duration_ms,
        }

        # extras curtas, serializadas
        if self.extra:
            entity["extra_json"] = json.dumps(self.extra, ensure_ascii=False)[:32000]

        # métricas opcionais (valores simples / serão convertidos automaticamente)
        for k, v in (metrics or {}).items():
            entity[k] = v

        if error_json:
            entity["error_json"] = (error_json if len(error_json) < 32000 else error_json[:32000])

        self._tc.upsert_entity(mode="Merge", entity=entity)
