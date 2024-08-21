import dataclasses as dc


@dc.dataclass(slots=True, frozen=True)
class Event:
    pass


@dc.dataclass(slots=True, frozen=True)
class SyncedPageCountChanged(Event):
    synced_page_count: int


@dc.dataclass(slots=True, frozen=True)
class TotalPageCountChanged(Event):
    total_page_count: int
