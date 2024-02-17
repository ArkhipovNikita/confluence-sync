import dataclasses as dc
import typing as tp

from confluence_sync.confluence import StrDict


@dc.dataclass
class PageContext:
    src_id: str
    src_title: str
    dst_id: int | None = None


class PageHierarchyContext:
    def __init__(self, src_page: StrDict, descendant_pages: tp.Generator[StrDict, None, None] | list[StrDict]) -> None:
        self._page_id_map = {}
        self._page_title_map = {}

        self._add_page(src_page)

        for page in descendant_pages:
            self._add_page(page)

    def _add_page(self, page: StrDict) -> None:
        page_context = PageContext(src_id=page['id'], src_title=page['title'])
        self._page_id_map[page_context.src_id] = page_context
        self._page_title_map[page_context.src_title] = page_context

    @property
    def count(self) -> int:
        return len(self._page_id_map)

    def search_by_id(self, page_id: str) -> PageContext | None:
        return self._page_id_map.get(page_id)

    def search_by_title(self, title: str) -> PageContext | None:
        return self._page_title_map.get(title)
