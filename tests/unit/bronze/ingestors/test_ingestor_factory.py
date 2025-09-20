"""
IngestorFactory: registro e criação por formato.
"""

import pytest
from bronze.ingestors.factory import IngestorFactory


def test_register_formato_valido():
    """
    Garante que `register` aceita formato não-vazio e sobrescreve/insere no registry.
    """
    

    class Dummy:
        pass

    IngestorFactory.register(data_format="CSV", ingestor_cls=Dummy)  # case-insensitive
    # força criação (não executa init do ingestor real aqui)
    # usaremos monkeypatch em tests de streaming
    assert "csv" in IngestorFactory._REGISTRY
    assert IngestorFactory._REGISTRY["csv"] is Dummy


def test_register_formato_invalido_erro():
    """
    Formato vazio ou não-string deve levantar ValueError.
    """
    
    with pytest.raises(ValueError):
        IngestorFactory.register(data_format="  ", ingestor_cls=object)  # vazio


def test_create_formato_desconhecido_erro(spark):
    """
    `create` com formato não registrado deve levantar TypeError e listar suportados.
    """
   
    with pytest.raises(TypeError) as err:
        IngestorFactory.create(
            data_format="unknown",
            spark=spark,
            target_table_fqn="bronze.sales.orders",
            schema=None,
            partitions=[],
            reader_options={},
            source_directory="/raw",
            checkpoint_location="/chk",
        )
    assert "Suportados:" in str(err.value)
