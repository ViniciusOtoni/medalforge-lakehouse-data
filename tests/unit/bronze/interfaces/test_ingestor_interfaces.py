"""
Confere a interface/assinatura dos ingestors base e marcadores.
"""

import inspect
from bronze.interfaces.ingestor_interfaces import DataIngestor, StructuredDataIngestor, SemiStructuredDataIngestor, UnstructuredDataIngestor


def test_dataingestor_is_abstract_and_has_ingest_signature():
    """
    Verifica que DataIngestor é abstrata e que `ingest` existe com parâmetros esperados.
    """
    assert inspect.isclass(DataIngestor)
    assert inspect.isabstract(DataIngestor)

    assert hasattr(DataIngestor, "ingest")
    sig = inspect.signature(DataIngestor.ingest)
    params = list(sig.parameters.keys())
    # trigger, include_existing_files estão presentes (além de self)
    assert "trigger" in params and "include_existing_files" in params


def test_markers_are_classes():
    """
    Checa que marcadores existem e herdam de DataIngestor.
    """
    for cls in (StructuredDataIngestor, SemiStructuredDataIngestor, UnstructuredDataIngestor):
        assert inspect.isclass(cls)
        assert issubclass(cls, DataIngestor)
