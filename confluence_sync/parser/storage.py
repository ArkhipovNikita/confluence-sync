import pathlib
import typing as tp
from urllib import parse

from lxml import etree

_ENTITIES = pathlib.Path(__file__).parent.joinpath('entities')


def _make_entity_uri(filename: str) -> str:
    return _ENTITIES.joinpath(filename).as_uri()


# TODO: remove all proxies methods, set ns globally to root
class StorageParser:
    _root_close = '</root>'
    _ns_prefixes = ('ac', 'ri', 'at')

    # needed to support html entities in XHTML confluence storage format
    # taken from https://www.w3.org/TR/xhtml1/ >
    # > http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd > Character mnemonic entities

    # more abot DTD here https://www.liquid-technologies.com/Reference/Glossary/XML_DocType.html
    _entities = (
        ('HTMLlat1', _make_entity_uri('xhtml-lat1.ent')),
        ('HTMLspecial', _make_entity_uri('xhtml-special.ent')),
        ('HTMLsymbol', _make_entity_uri('xhtml-symbol.ent')),
    )

    def __init__(self, remove_blank_text: bool = False):
        url = 'http://example.org'
        self._ns = {prefix: parse.urljoin(url, prefix) for prefix in self._ns_prefixes}
        self._root = self._build_root()
        self._doctype = self._build_doctype()
        self._parser = etree.XMLParser(
            resolve_entities=False,
            load_dtd=True,
            strip_cdata=False,
            remove_blank_text=remove_blank_text,
        )

    def parse(self, body: str) -> etree._Element:
        body = self._wrap(body)
        return etree.fromstring(body, self._parser)

    def get_tag_attr(self, element: etree._Element, attr: str) -> str | None:
        return element.get(self._ns_attr(attr))

    def set_tag_attr(self, element: etree._Element, attr, value) -> None:
        element.attrib[self._ns_attr(attr)] = value

    def find(self, element: etree._Element, path: str) -> etree._Element | None:
        return element.find(path, self._ns)

    def xpath(self, element: etree._Element, path: str) -> list[etree._Element]:
        return element.xpath(path, namespaces=self._ns)

    def iterfind(self, element: etree._Element, path: str) -> tp.Generator[etree._Element, None, None]:
        return element.iterfind(path, self._ns)

    def to_storage(self, root: etree._Element) -> str:
        res = self._to_storage(root)
        return self._strip_root(res)

    @classmethod
    def _to_storage(cls, element: etree._Element) -> str:
        return etree.tostring(element, encoding='unicode')

    # TODO: create with lxml xml.etree.ElementTree.TreeBuilder
    @classmethod
    def _build_doctype(cls) -> str:
        entity_pattern = '<!ENTITY % {name} SYSTEM "{uri}">'
        entity_registration_pattern = '%{name};'

        parts = ['<!DOCTYPE root [']

        for name, uri in cls._entities:
            parts.append(entity_pattern.format(name=name, uri=uri))
            parts.append(entity_registration_pattern.format(name=name))

        parts.append(']>')

        return '\n'.join(parts)

    def _build_root(self) -> str:
        root = etree.Element('root', nsmap=self._ns)
        root_str = self._to_storage(root)
        return root_str.replace('/>', '>')

    def _strip_root(self, body: str) -> str:
        return body[len(self._root):-len(self._root_close)]

    def _wrap(self, body: str) -> str:
        return ''.join((self._doctype, '\n', self._root, body, self._root_close))

    def _ns_attr(self, attr: str) -> str:
        if ':' in attr:
            ns_prefix, attr = attr.split(':', 1)
            ns_uri = self._ns[ns_prefix]
            return f'{{{ns_uri}}}{attr}'

        return attr
