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
from typing import Dict, Optional, Any
import json
import os
import uuid

try:  
    from azure.identity import DefaultAzureCredential  
    from azure.data.tables import TableServiceClient, TableClient, UpdateMode  
except Exception:  
    DefaultAzureCredential = None  
    TableServiceClient = None  
    TableClient = None 
    UpdateMode = None  


def _iso_now() -> str:
    """
    Timestamp atual em ISO-8601 UTC (com 'Z').

    Retorno
    -------
    str
    """
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _yyyymmdd(ts: Optional[datetime] = None) -> str:
    """
    Data base YYYYMMDD para particionamento.

    Parâmetros
    ----------
    ts : datetime | None
        Momento de referência (default: agora em UTC).

    Retorno
    -------
    str
    """
    ts = ts or datetime.now(tz=timezone.utc)
    return ts.strftime("%Y%m%d")


def _enabled() -> bool:
    """
    Verifica se o monitoramento deve rodar.

    Habilita somente quando:
      - libs Azure estão disponíveis, e
      - MON_TABLE_ACCOUNT está definido.

    Retorno
    -------
    bool
    """
    return bool(TableServiceClient and DefaultAzureCredential and os.getenv("MON_TABLE_ACCOUNT"))


def _table_client() -> Optional["TableClient"]:
    """
    Constrói um TableClient para a tabela de runs.

    Retorno
    -------
    TableClient | None
        None quando desabilitado (no-op).
    """
    if not _enabled():  # no-op
        return None

    account = os.environ["MON_TABLE_ACCOUNT"].strip()
    table = (os.getenv("MON_TABLE_NAME", "pipeline_runs") or "pipeline_runs").strip()
    endpoint = f"https://{account}.table.core.windows.net"
    cred = DefaultAzureCredential()  # usa credenciais via ENV/Managed Identity

    svc = TableServiceClient(endpoint=endpoint, credential=cred)
    try:
        svc.create_table_if_not_exists(table_name=table)
    except Exception:
        # Condição de corrida / já existe: OK
        pass
    return svc.get_table_client(table_name=table)


def _partition_key(env: str, pipeline: str, schema: str, ts: Optional[datetime] = None) -> str:
    """
    Monta a PartitionKey no formato: "{env}|{pipeline}|{schema}|{yyyymmdd}".

    Parâmetros
    ----------
    env : str
        Ambiente lógico (ex.: dev|hml|prd).
    pipeline : str
        Pipeline lógico (ex.: bronze|silver|gold).
    schema : str
        Schema lógico (ex.: sales).
    ts : datetime | None
        Momento de referência (opcional).

    Retorno
    -------
    str
    """
    return f"{env}|{pipeline}|{schema}|{_yyyymmdd(ts)}"


@dataclass
class PipelineRunLogger:
    """
    Context manager para registrar início/fim de uma execução (run).

    Objetivo
    --------
    Persistir metadados mínimos do run (status, tempos, alvo) de forma idempotente.

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
        FQN do alvo (ex.: bronze.sales.orders).
    run_id : str | None
        Identificador único do run (se None, gera UUID4.hex).
    extra : dict[str, str]
        Propriedades adicionais curtas (serializadas em JSON).

    Atributos internos
    ------------------
    _tc : TableClient | None
        Cliente da Azure Table (ou None para no-op).
    _start_iso : str
        Timestamp ISO de início.
    _finished : bool
        Flag para prevenir finish duplo (quando __exit__ e finish() são chamados).
    """

    env: str
    pipeline: str
    schema: str
    table: str
    target_fqn: str
    run_id: Optional[str] = None
    extra: Dict[str, str] = field(default_factory=dict)

    _tc: Optional["TableClient"] = field(init=False, default=None, repr=False)
    _start_iso: str = field(init=False, default="", repr=False)
    _finished: bool = field(init=False, default=False, repr=False)

    def __post_init__(self) -> None:
        """Inicializa cliente da tabela, run_id e timestamp de início."""
        self.run_id = self.run_id or uuid.uuid4().hex
        self._tc = _table_client()
        self._start_iso = _iso_now()

    # ---------- API pública ----------

    def __enter__(self) -> "PipelineRunLogger":
        """
        Registra entidade inicial (status='running').

        Retorno
        -------
        PipelineRunLogger
        """
        self._upsert(
            status="running",
            ts_start=self._start_iso,
            ts_end=None,
            duration_ms=None,
            error_json=None,
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """
        Finaliza o run com status 'ok' ou 'fail'.

        Parâmetros
        ----------
        exc_type, exc, tb : Any
            Informações de exceção do contexto.

        Retorno
        -------
        None
        """
        if self._finished:
            return  # já finalizado manualmente via finish()

        status = "ok" if exc_type is None else "fail"
        self.finish(status=status, error_json=(str(exc) if exc else None))

    def finish(self, status: str = "ok", *, error_json: Optional[str] = None, **metrics: Any) -> None:
        """
        Conclui o run atualizando ts_end, duração e métricas opcionais.

        Parâmetros
        ----------
        status : str
            'ok' | 'fail' | 'partial'
        error_json : str | None
            Mensagem/resumo de erro quando falho.
        metrics : dict[str, Any]
            Métricas adicionais (ex.: counts_valid=..., counts_quarantine=...).

        Retorno
        -------
        None
        """
        if self._finished:
            return

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
        self._finished = True

    # ---------- internos ----------

    def _upsert(
        self,
        *,
        status: str,
        ts_start: Optional[str],
        ts_end: Optional[str],
        duration_ms: Optional[int],
        error_json: Optional[str],
        **metrics: Any,
    ) -> None:
        """
        Monta e faz upsert da entidade.

        Parâmetros
        ----------
        status : str
            Status do run ('running' | 'ok' | 'fail' | 'partial').
        ts_start : str | None
            Timestamp ISO de início.
        ts_end : str | None
            Timestamp ISO de término (quando finalizado).
        duration_ms : int | None
            Duração em milissegundos (quando finalizado).
        error_json : str | None
            Resumo/stack do erro (truncado).
        metrics : dict[str, Any]
            Métricas adicionais.

        Retorno
        -------
        None
        """
        if not self._tc: 
            return

        entity: Dict[str, Any] = {
            "PartitionKey": _partition_key(self.env, self.pipeline, self.schema),
            "RowKey": self.run_id,
            "pipeline": self.pipeline,
            "schema": self.schema,
            "table": self.table,
            "target_fqn": self.target_fqn,
            "status": status,
            "ts_start": ts_start,
        }

        # extras curtas, serializadas
        if self.extra:
            entity["extra_json"] = json.dumps(self.extra, ensure_ascii=False)[:32000]

        # Campos opcionais somente quando não-nulos
        if ts_end is not None:
            entity["ts_end"] = ts_end
        if duration_ms is not None:
            entity["duration_ms"] = duration_ms

        # métricas opcionais
        for key, value in (metrics or {}).items():
            if value is not None:
                entity[key] = value

        if error_json:
            entity["error_json"] = (error_json if len(error_json) < 32000 else error_json[:32000])

        # MERGE para preservar/atualizar parcial idempotente
        mode = UpdateMode.MERGE if UpdateMode is not None else "MERGE"
        self._tc.upsert_entity(mode=mode, entity=entity)
