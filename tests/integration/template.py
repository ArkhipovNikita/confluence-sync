import dataclasses as dc

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

		page_iterator = case.iterate_space_pages(space_config, space['homepage']['id'])

		try:
			page_config, parent_id = next(page_iterator)
		except StopIteration:
			continue

		pages_by_name = {}

		while True:
			page = client.get_page_by_title(space['key'], page_config.name)
			pages_by_name[page['title']] = Page(id=page['id'], name=page['title'])

			try:
				page_config, parent_id = page_iterator.send(page['id'])
			except StopIteration:
				break

		space = Space(name=space['name'], pages=pages_by_name)
		spaces_by_name[space.name] = space

	return Confluence(spaces=spaces_by_name)
