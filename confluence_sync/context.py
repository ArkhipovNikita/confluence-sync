import dataclasses as dc


@dc.dataclass
class Page:
	src_id: str
	src_space: str
	src_title: str
	dst_id: str | None = None


class PageIndex:
	def __init__(self) -> None:
		self._page_id_map = {}
		self._page_title_map = {}

	@property
	def count(self) -> int:
		return len(self._page_id_map)

	def add_page(self, page_context: Page) -> None:
		self._page_id_map[page_context.src_id] = page_context
		self._page_title_map[(page_context.src_space, page_context.src_title)] = page_context

	def search_by_id(self, page_id: str) -> Page | None:
		return self._page_id_map.get(page_id)

	def search_by_title(self, space: str, title: str) -> Page | None:
		return self._page_title_map.get((space, title))
