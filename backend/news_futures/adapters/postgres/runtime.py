"""PostgreSQL implementation of the runtime persistence ports."""

from contextlib import AbstractContextManager

from langgraph.checkpoint.postgres import PostgresSaver

from news_futures.adapters.postgres.checkpoints import checkpoints
from news_futures.adapters.postgres.events import EventArchive
from news_futures.adapters.postgres.operator_state import OperatorState
from news_futures.adapters.postgres.publications import Archive
from news_futures.application.config import RuntimeSettings


class PostgresRuntimeStorage:
    def __init__(self, settings: RuntimeSettings):
        self.settings = settings
        self.controls = OperatorState(settings.data_dir.resolve())

    def publications(self) -> Archive:
        return Archive(self.settings.publications)

    def events(self, *, testing_revision: str | None = None) -> EventArchive:
        return EventArchive(self.settings.events, testing_revision=testing_revision)

    def checkpoints(self) -> AbstractContextManager[PostgresSaver]:
        return checkpoints(self.settings.data_dir / 'checkpoints')
