"""SQLite implementation of the runtime persistence ports."""

from contextlib import AbstractContextManager

from langgraph.checkpoint.sqlite import SqliteSaver

from news_futures.adapters.sqlite.checkpoints import checkpoints
from news_futures.adapters.sqlite.events import EventArchive
from news_futures.adapters.sqlite.operator_state import OperatorState
from news_futures.adapters.sqlite.publications import Archive
from news_futures.application.config import RuntimeSettings


class SQLiteRuntimeStorage:
    def __init__(self, settings: RuntimeSettings):
        self.settings = settings
        self.controls = OperatorState(settings.data_dir.resolve())

    def publications(self) -> Archive:
        return Archive(self.settings.publications)

    def events(self, *, testing_revision: str | None = None) -> EventArchive:
        return EventArchive(self.settings.events, testing_revision=testing_revision)

    def checkpoints(self) -> AbstractContextManager[SqliteSaver]:
        return checkpoints(self.settings.data_dir / 'checkpoints.sqlite3')
