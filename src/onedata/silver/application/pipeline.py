"""
Orquestrador do Pipeline Silver (contrato → estágios DQX/ETL/WRITE).
"""

from __future__ import annotations
from pyspark.sql import SparkSession, DataFrame
from onedata.silver.domain.silver import SilverYaml
from onedata.monitoring.azure_table_runs import PipelineRunLogger
from onedata.silver.application.settings import SETTINGS
from onedata.silver.stages import dqx_stage, etl_stage, write_stage

def run_pipeline(spark: SparkSession, cfg: SilverYaml) -> None:
    """
    Executa o pipeline Silver dado um contrato validado (SilverYaml).
    """
 
    logger = (print if SETTINGS.env == "dev" else None)

    with PipelineRunLogger(
        env=SETTINGS.env,
        pipeline="silver",
        schema=cfg.target.schema_name,
        table=cfg.target.table,
        target_fqn=cfg.target_fqn,
        extra={"contract_version": cfg.version},
    ) as runlog:

        # 1) Fonte Bronze
        bronze_tbl = cfg.source["bronze_table"]
        df_bronze: DataFrame = spark.table(bronze_tbl)

        # 2) DQX inicial
        valid_df, quarantine_df = dqx_stage.initial_split(df_bronze, cfg.dqx)

        # 3) Persistir quarentena bruta (com colunas técnicas)
        if cfg.quarantine.sink and "table" in cfg.quarantine.sink:
            write_stage.persist_df_append_external(
                quarantine_df,
                cfg.quarantine.sink["table"],
                external_base=SETTINGS.silver_external_base,
                partition_by=None,
            )

        # 4) Remediação na quarentena + re-check DQX
        qrem = etl_stage.strip_dqx_cols(quarantine_df)
        if cfg.quarantine.remediate:
            qrem = etl_stage.run_core_steps(qrem, cfg.quarantine.remediate)
        fixed_valid_df, still_bad_df = dqx_stage.recheck_after_remediation(qrem, cfg.dqx)

        # 5) ETL standard + customs (caso exista)
        valid_df = etl_stage.strip_dqx_cols(valid_df)
        fixed_valid_df = etl_stage.strip_dqx_cols(fixed_valid_df)

        std_steps = cfg.etl.get("standard", [])
        valid_df = etl_stage.run_core_steps(valid_df, std_steps)
        fixed_valid_df = etl_stage.run_core_steps(fixed_valid_df, std_steps)

        valid_df = etl_stage.run_customs_standard(
            valid_df, cfg.customs,
            allow_module_prefixes=SETTINGS.customs_prefixes,
            require_marked_decorator=SETTINGS.customs_strict,
            logger=logger,
        )
        fixed_valid_df = etl_stage.run_customs_standard(
            fixed_valid_df, cfg.customs,
            allow_module_prefixes=SETTINGS.customs_prefixes,
            require_marked_decorator=SETTINGS.customs_strict,
            logger=logger,
        )

        # 6) União final e escrita
        final_df = valid_df.unionByName(fixed_valid_df, allowMissingColumns=True)

        write_stage.ensure_uc_and_merge(
            spark,
            final_df,
            cfg.target_fqn,
            merge_keys=cfg.target.write.merge_keys,
            zorder_by=cfg.target.write.zorder_by,
            partition_by=cfg.target.write.partition_by,
            external_base=SETTINGS.silver_external_base,
        )

        # 7) Rejeitados pós-remediação
        if still_bad_df.count() > 0:
            still_bad_df = etl_stage.strip_dqx_cols(still_bad_df)
            if cfg.quarantine.sink and "table" in cfg.quarantine.sink:
                cat_sch_tbl = cfg.quarantine.sink["table"].split(".")
                cat, sch = cat_sch_tbl[0], cat_sch_tbl[1]
            else:
                cat = cfg.target.catalog
                sch = f"{cfg.target.schema_name}_quarantine"
            still_tbl = f"{cat}.{sch}.{cfg.target.table}_rejected"
            write_stage.persist_df_append_external(
                still_bad_df, still_tbl, external_base=SETTINGS.silver_external_base
            )

        # 8) Métricas
        try:
            runlog.finish(
                status="ok",
                counts_valid=int(valid_df.count()),
                counts_quarantine=int(quarantine_df.count()),
                counts_final=int(final_df.count()),
            )
        except Exception:
            pass
