import abc
import collections
import logging
import threading
import typing as tp

from lxml import etree

from confluence_sync import context
from confluence_sync.confluence import CustomConfluence
from confluence_sync.parser import StorageParser

_TEXT_FORMATTER = tp.Callable[[str, str], str]

_logger = logging.getLogger('confluence-sync')
_parser = StorageParser()


class TagFormatter(abc.ABC):
	_xpath: str

	@property
	def xpath(self) -> str:
		return self._xpath

	@abc.abstractmethod
	def format(self, page_context: context.Page, el: etree._Element) -> None:
		pass


class OutHierarchyPageTitleChecker(TagFormatter):
	_xpath = 'ri:page'

	def __init__(self, page_hierarchy_context: context.PageIndex, src_space: str) -> None:
		self._page_hierarchy_context = page_hierarchy_context
		self._src_space = src_space

	def format(self, page_context: context.Page, el: etree._Element) -> None:
		page_space = _parser.get_tag_attr(el, 'ri:space-key')
		page_title = _parser.get_tag_attr(el, 'ri:content-title')

		if not self._page_hierarchy_context.search_by_title(page_space or self._src_space, page_title):
			_logger.warning(
				'Out hierarchy page link "%s", page: "%s"',
				page_title,
				page_context.src_title,
			)


class OutHierarchyPageTitleKeeper(TagFormatter):
	_xpath = 'ri:page'

	def __init__(self, page_hierarchy_context: context.PageIndex, src_space: str) -> None:
		self._page_hierarchy_context = page_hierarchy_context
		self._src_space = src_space

		self.pages = set()
		self._pages_lock = threading.Lock()

	def format(self, page_context: context.Page, el: etree._Element) -> None:
		page_space = _parser.get_tag_attr(el, 'ri:space-key')
		page_title = _parser.get_tag_attr(el, 'ri:content-title')

		if not self._page_hierarchy_context.search_by_title(page_space or self._src_space, page_title):
			with self._pages_lock:
				self.pages.add((page_space or self._src_space, page_title))


class PageTittleFormatter(TagFormatter):
	_xpath = 'ri:page'

	def __init__(self, fn: _TEXT_FORMATTER, src_space: str, dst_space: str) -> None:
		self._fn = fn
		self._src_space = src_space
		self._dst_space = dst_space

	def format(self, page_context: context.Page, el: etree._Element) -> None:
		page_space = _parser.get_tag_attr(el, 'ri:space-key')

		if page_space:
			is_page_space_set = True
		else:
			is_page_space_set = False
			page_space = self._src_space

		page_title = _parser.get_tag_attr(el, 'ri:content-title')

		if is_page_space_set:
			_parser.set_tag_attr(el, 'ri:space-key', self._dst_space)

		_parser.set_tag_attr(el, 'ri:content-title', self._fn(page_space, page_title))


class HierarchyPageTittleFormatter(TagFormatter):
	_xpath = 'ri:page'

	def __init__(
		self,
		fn: _TEXT_FORMATTER,
		page_hierarchy_context: context.PageIndex,
		src_space: str,
		dst_space: str,
	) -> None:
		self._fn = fn
		self._page_hierarchy_context = page_hierarchy_context
		self._src_space = src_space
		self._dst_space = dst_space

	def format(self, page_context: context.Page, el: etree._Element) -> None:
		page_space = _parser.get_tag_attr(el, 'ri:space-key')

		if page_space:
			is_page_space_set = True
		else:
			is_page_space_set = False
			page_space = self._src_space

		page_title = _parser.get_tag_attr(el, 'ri:content-title')

		if self._page_hierarchy_context.search_by_title(page_space, page_title):
			if is_page_space_set:
				_parser.set_tag_attr(el, 'ri:space-key', self._dst_space)

			_parser.set_tag_attr(el, 'ri:content-title', self._fn(page_space, page_title))


class IncDrawIOFormatter(TagFormatter):
	"""Formatter to fix references in included draw.io diagrams.

	If a diagram references a page in the current copying hierarchy, the link will point to the new page ID.
	If the page hasn’t been copied yet (e.g., a sibling or descendant page), the fix is delayed and stored in _delayed_pages.

	If a draw.io diagram references a page outside the current hierarchy, it is copied along with its attachments.
	The page containing the diagram is marked as a source for other diagrams referencing the same target page to avoid duplicates
	and maintain relationships.

	The class is thread-safe. If this feature isn’t needed, the _delayed_pages logic can be removed.
	"""

	_xpath = "ac:structured-macro[@ac:name='inc-drawio']"

	_diagram_by_macro_id_xpath = 'ac:structured-macro[@ac:macro-id="{macro_id}"]'
	_diagram_by_name_xpath = 'ac:structured-macro[@ac:name="drawio"]/ac:parameter[@ac:name="diagramName" and text()="{diagram_name}"]'

	_macro_id_attr_xpath = 'ac:macro-id'
	_name_attr_xpath = 'ac:name'

	_page_id_param_xpath = 'ac:parameter[@ac:name="pageId"]'
	_diagram_name_param_xpath = 'ac:parameter[@ac:name="diagramName"]'
	_included_param_xpath = 'ac:parameter[@ac:name="includedDiagram"]'
	_revision_param_xpath = 'ac:parameter[@ac:name="revision"]'

	_delayed_comment = 'Fix references in included draw.io diagrams'

	def __init__(
		self,
		src_cli: CustomConfluence,
		dst_cli: CustomConfluence,
		page_hierarchy_context: context.PageIndex,
	) -> None:
		self._src_cli = src_cli
		self._dst_cli = dst_cli

		self._page_hierarchy_context = page_hierarchy_context

		#  page_id: [(macro_id, ref_page_id, ref_diagram_name), ...]
		self._delayed_pages = collections.defaultdict(list)
		self._delayed_pages_lock = threading.Lock()

		self._out_hierarchy_replacements = {}
		self._page_root_cache = {}

	@property
	def delayed_pages_count(self) -> int:
		return len(self._delayed_pages)

	def format(self, page_context: context.Page, el: etree._Element) -> None:
		if not self._is_included(el):
			return

		ref_page_id_param = self._extract_ref_page_param(el)

		# If a diagram points to a page in the current copying hierarchy and the new page ID is known, the reference can be replaced.
		if not self._try_substitute(ref_page_id_param):
			# If the new page ID isn’t known, wait to replace the reference until it is available.
			# Pages in the current copying hierarchy shouldn’t mark the current diagram as a new source right away,
			# since sibling pages might reference the same diagram, causing multiple new sources in multithreaded mode.
			macro_id = _parser.get_tag_attr(el, self._macro_id_attr_xpath)
			ref_diagram_name_param = self._extract_ref_diagram_name(el)

			diagram = (macro_id, ref_page_id_param.text, ref_diagram_name_param.text)

			with self._delayed_pages_lock:
				self._delayed_pages[page_context.src_id].append(diagram)

	def process_delayed_pages(self) -> tp.Generator[tuple[str, str, dict[str, list[str]], str], None, None]:
		"""Handle all delayed pages.

		:return: A generator that returns a tuple with:
			the source page ID
			the new page content
			a dict of attachments to copy from other pages
			a comment for the new page revision
		"""
		for page_id, diagrams in self._delayed_pages.items():
			page_context = self._page_hierarchy_context.search_by_id(page_id)
			# Get the destination page since it might have changed, because macros need to be fixed now
			root = self._get_page_root(self._dst_cli, page_context.dst_id)
			attachments = collections.defaultdict(list)

			for macro_id, ref_page_id, ref_diagram_name in diagrams:
				el = _parser.find(root, f'.//{self._diagram_by_macro_id_xpath.format(macro_id=macro_id)}')
				ref_page_id_param = self._extract_ref_page_param(el)

				if not self._try_substitute(ref_page_id_param):
					ref_root = self._get_page_root_cached(self._src_cli, ref_page_id)

					self._copy(el, ref_root, ref_diagram_name)
					self._out_hierarchy_replacements[ref_page_id] = page_context.dst_id
					attachments[ref_page_id].append(ref_diagram_name)
					attachments[ref_page_id].append(f'{ref_diagram_name}.png')
					attachments[ref_page_id].append(f'~{ref_diagram_name}.tmp')

			yield page_id, _parser.to_storage(root), attachments, self._delayed_comment

	def _try_substitute(self, ref_page_id_param: etree._Element) -> bool:
		"""Attempt to replace the source page ID.

		Substitution is only possible for pages that are already copied.
		"""
		page_id_replacement = self._get_ref_page_id_replacement(ref_page_id_param.text)

		if page_id_replacement:
			ref_page_id_param.text = page_id_replacement
			return True

		return False

	@classmethod
	def _copy(cls, el: etree._Element, ref_page: etree._Element, ref_diagram_name: str) -> None:
		"""Copy a diagram from a page outside the current hierarchy to a page within the current hierarchy."""
		src_diagram = _parser.xpath(
			ref_page,
			f'.//{cls._diagram_by_name_xpath.format(diagram_name=ref_diagram_name)}'
		)[0].getparent()

		_parser.set_tag_attr(el, cls._name_attr_xpath, 'drawio')

		for c in el:
			el.remove(c)

		# Delete the revision number because it’s kept when copying
		revision_param = _parser.find(src_diagram, cls._revision_param_xpath)
		src_diagram.remove(revision_param)

		src_params = src_diagram.getchildren()
		el.extend(src_params)

	def _get_ref_page_id_replacement(self, ref_page_id: str) -> str | None:
		"""Get a page ID to replace the provided page ID."""

		# Look for a replacement in pages outside the current hierarchy
		if ref_page_id in self._out_hierarchy_replacements:
			return self._out_hierarchy_replacements[ref_page_id]

		# Look for a replacement among pages within the current hierarchy. It is necessary to know the new page ID
		ref_page = self._page_hierarchy_context.search_by_id(ref_page_id)

		if ref_page and ref_page.dst_id:
			return ref_page.dst_id

		return None

	@classmethod
	def _get_page_root(cls, cli: CustomConfluence, page_id: str) -> etree._Element:
		page = cli.get_page_by_id(page_id, expand='body.storage')
		body = page['body']['storage']['value']
		return _parser.parse(body)

	def _get_page_root_cached(self, cli: CustomConfluence, page_id: str) -> etree._Element:
		if page_id not in self._page_root_cache:
			self._page_root_cache[page_id] = self._get_page_root(cli, page_id)

		return self._page_root_cache[page_id]

	@classmethod
	def _extract_ref_page_param(cls, el: etree._Element) -> etree._Element:
		return _parser.find(el, cls._page_id_param_xpath)

	@classmethod
	def _extract_ref_diagram_name(cls, el: etree._Element) -> etree._Element:
		return _parser.find(el, cls._diagram_name_param_xpath)

	@classmethod
	def _is_included(cls, el: etree._Element) -> bool:
		include_param = _parser.find(el, cls._included_param_xpath)
		return include_param.text == '1'


def format_page(page_context: context.Page, body: str, tag_formatters: tp.Iterable[TagFormatter]) -> str:
	xpath_tag_formatters_map = collections.defaultdict(list)

	for tf in tag_formatters:
		xpath_tag_formatters_map[tf.xpath].append(tf)

	if not xpath_tag_formatters_map:
		return body

	root = _parser.parse(body)

	for xpath, tag_formatters in xpath_tag_formatters_map.items():
		xpath = f'.//{xpath}'
		elements = _parser.iterfind(root, xpath)

		for el in elements:
			for tf in tag_formatters:
				tf.format(page_context, el)

	return _parser.to_storage(root)


def title_formatter(
	replace_text_substr: tuple[str, str] | None = None,
	start_text_with: str | None = None,
	src_space: str | None = None,
) -> _TEXT_FORMATTER:
	"""Create a function to change text."""

	def same(page_space: str, page_title: str) -> str:
		return page_title

	fn = same

	if src_space is not None:
		fn = _add_space_prefix(fn, src_space)

	if start_text_with:
		fn = _start_text_with(fn, start_text_with)

	if replace_text_substr:
		fn = _replace_text_substr(fn, *replace_text_substr)

	return fn


def _replace_text_substr(func: _TEXT_FORMATTER, old: str, new: str) -> _TEXT_FORMATTER:

	def wrapper(page_space: str, page_title: str) -> str:
		return func(page_space, page_title.replace(old, new))

	return wrapper


def _start_text_with(func: _TEXT_FORMATTER, start_with: str) -> _TEXT_FORMATTER:

	def wrapper(page_space: str, page_title: str) -> str:
		return func(page_space, start_with + page_title)

	return wrapper


def _add_space_prefix(func: _TEXT_FORMATTER, src_space: str) -> _TEXT_FORMATTER:

	def wrapper(page_space: str, page_title: str) -> str:
		if src_space != page_space:
			page_title = f'{page_space}: {page_title}'

		return func(page_space, page_title)

	return wrapper
