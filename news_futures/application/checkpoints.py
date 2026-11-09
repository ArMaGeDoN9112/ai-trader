"""Deserialize saved LangGraph state with an explicit model allowlist."""

from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from news_futures.agents.company import CompanyContext, CompanyProposal
from news_futures.agents.contracts import Review


def checkpoint_serializer() -> JsonPlusSerializer:
    return JsonPlusSerializer(allowed_msgpack_modules=[
        *((model.__module__, model.__name__) for model in (CompanyContext, CompanyProposal, Review)),
        ('news_futures.decisions', 'CompanyContext'),
        ('news_futures.decisions', 'CompanyProposal'),
        ('news_futures.analysis', 'Review'),
    ])
