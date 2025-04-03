import dataclasses as dc
import queue

from atlassian import confluence

from tests.integration import case


@dc.dataclass(frozen=True)
class Page:
	id: int | str
	name: str


@dc.dataclass(frozen=True)
class Space:
	name: str
	pages: dict[str, Page]

	@property
	def key(self) -> str:
		return self.name[:3]


@dc.dataclass(frozen=True)
class Confluence:
	spaces: dict[str, Space]


def create_confluence_context(client: confluence.Confluence, confluence_config: case.ConfluenceConfig) -> Confluence:
	spaces_by_name = {}

	for space_config in confluence_config.spaces:
		space = client.get_space(space_config.key)

		page_queue = queue.SimpleQueue()
		for page_config in space_config.pages:
			page_queue.put((page_config, None))

		pages_by_name = {}

		while not page_queue.empty():
			page_config, parent_id = page_queue.get()

			page = client.get_page_by_title(space['key'], page_config.name)

			pages_by_name[page['title']] = Page(id=page['id'], name=page['title'])

			for child_page_config in page_config.pages:
				page_queue.put((child_page_config, page['id']))

		space = Space(name=space['name'], pages=pages_by_name)
		spaces_by_name[space.name] = space

	return Confluence(spaces=spaces_by_name)
