import dataclasses as dc
import itertools as it
import pathlib
import queue
import typing as tp

import yaml


@dc.dataclass(frozen=True)
class PageConfig:
	name: str
	content: str
	is_template: bool
	attachment_paths: list[str]
	pages: list['PageConfig']

	@classmethod
	def from_filesystem(cls, path: pathlib.Path) -> 'PageConfig':
		if not path.is_dir():
			raise ValueError('Page config path must be a directory')

		name = path.name

		content_paths = list(path.glob('content.*'))
		if len(content_paths) == 0:
			content = ''
			is_template = False
		elif len(content_paths) == 1:
			content_path = content_paths[0]
			is_template = content_path.suffix == '.j2'
			content = content_path.read_text()
		else:
			raise ValueError('Page content must have exactly one file')

		attachment_paths = []
		attachment_path = path / 'attachments'
		if attachment_path.exists():
			for attachment_path in attachment_path.iterdir():
				attachment_paths.append(str(attachment_path))

		pages = []
		for child_path in path.iterdir():
			if not child_path.is_dir() or child_path.stem == 'attachments':
				continue

			page = PageConfig.from_filesystem(child_path)
			pages.append(page)

		return PageConfig(
			name=name,
			content=content,
			is_template=is_template,
			attachment_paths=attachment_paths,
			pages=pages,
		)


@dc.dataclass(frozen=True)
class SpaceConfig:
	name: str
	pages: list[PageConfig]

	@property
	def key(self) -> str:
		return self.name[:3]

	@classmethod
	def from_filesystem(cls, path: pathlib.Path) -> 'SpaceConfig':
		if not path.is_dir():
			raise ValueError('Space config path must be a directory')

		name = path.name

		pages = []
		for page_path in path.iterdir():
			pages.append(PageConfig.from_filesystem(page_path))

		return SpaceConfig(name=name, pages=pages)


@dc.dataclass(frozen=True)
class ConfluenceConfig:
	spaces: list[SpaceConfig]

	@classmethod
	def from_filesystem(cls, path: pathlib.Path) -> 'ConfluenceConfig':
		if not path.is_dir():
			raise ValueError('Confluence config path must be a directory')

		spaces = []
		for space_path in path.iterdir():
			space = SpaceConfig.from_filesystem(space_path)
			spaces.append(space)

		return ConfluenceConfig(spaces=spaces)


@dc.dataclass(frozen=True)
class CLIConfig:
	args: list[str]


@dc.dataclass(frozen=True)
class TestConfig:
	cli: CLIConfig | None

	@classmethod
	def from_filesystem(cls, path: pathlib.Path) -> 'TestConfig':
		if not path.is_file():
			raise ValueError('Test config path must be a file')

		with path.open() as f:
			data = yaml.load(f, Loader=yaml.SafeLoader)

		if 'cli' in data:
			cli = CLIConfig(**data['cli'])
		else:
			cli = None

		return TestConfig(cli=cli)


@dc.dataclass(frozen=True)
class TestCase:
	id: int
	name: str
	description: str
	config: TestConfig | None
	src_confluence: ConfluenceConfig
	dst_orig_confluence: ConfluenceConfig
	dst_exp_confluence: ConfluenceConfig

	@classmethod
	def from_filesystem(cls, path: pathlib.Path) -> 'TestCase':
		if not path.is_dir():
			raise ValueError('Test config path must be a directory')

		name = path.name
		test_case_id = int(name.split('_', 1)[0])

		description_path = path / 'description.txt'
		if description_path.exists():
			description = description_path.read_text()
		else:
			description = ''

		config_path = path / 'config.yaml'
		if config_path.exists():
			config = TestConfig.from_filesystem(config_path)
		else:
			config = None

		src_confluence_path = path / 'src'
		if src_confluence_path.exists():
			src_confluence = ConfluenceConfig.from_filesystem(src_confluence_path)
		else:
			raise FileNotFoundError(f'Source confluence config is not found for test case "{name}"')

		dst_orig_confluence_path = path / 'dst_orig'
		if dst_orig_confluence_path.exists():
			dst_orig_confluence = ConfluenceConfig.from_filesystem(dst_orig_confluence_path)
		else:
			raise FileNotFoundError(f'Destination original confluence config is not found for test case "{name}"')

		dst_exp_confluence_path = path / 'dst_exp'
		if dst_exp_confluence_path.exists():
			dst_exp_confluence = ConfluenceConfig.from_filesystem(dst_exp_confluence_path)
		else:
			raise FileNotFoundError(f'Destination expected confluence config is not found for test case "{name}"')

		return TestCase(
			id=test_case_id,
			name=name,
			description=description,
			config=config,
			src_confluence=src_confluence,
			dst_orig_confluence=dst_orig_confluence,
			dst_exp_confluence=dst_exp_confluence,
		)

	@property
	def confluence_sync_args(self) -> list[str]:
		if self.config is not None and self.config.cli is not None:
			args = dict(it.zip_longest(self.config.cli.args[::2], self.config.cli.args[1::2]))
		else:
			args = {}

		if '--source-space' not in args and len(self.src_confluence.spaces) == 1:
			args['--source-space'] = self.src_confluence.spaces[0].name

		if '--source-page' not in args and '--source-space' in args:
			src_space_name = args['--source-space']

			for space in self.src_confluence.spaces:
				if space.name == src_space_name and len(space.pages) == 1:
					args['--source-title'] = space.pages[0].name
					break

		if '--dest-space' not in args and len(self.dst_orig_confluence.spaces) == 1:
			args['--dest-space'] = self.dst_orig_confluence.spaces[0].name

		if '--dest-page' not in args and '--dest-space' in args:
			dst_space_name = args['--dest-space']
			args['--dest-title'] = f'{dst_space_name} Home'

		return list(filter(None, it.chain.from_iterable(args.items())))


def get_test_cases() -> list[TestCase]:
	test_cases_path = pathlib.Path(__file__).parent / 'cases'

	test_cases = []
	for test_case_path in test_cases_path.iterdir():
		test_case = TestCase.from_filesystem(test_case_path)
		test_cases.append(test_case)

	return test_cases


def iterate_space_pages(space_config: SpaceConfig, homepage_id: str) -> tp.Generator[tuple[PageConfig, str], str, None]:
	page_queue = queue.SimpleQueue()

	for page_config in space_config.pages:
		page_queue.put((page_config, homepage_id))

	while not page_queue.empty():
		page_config, parent_page_id = page_queue.get()
		page_id = yield page_config, parent_page_id

		for child_page_config in page_config.pages:
			page_queue.put((child_page_config, page_id))
