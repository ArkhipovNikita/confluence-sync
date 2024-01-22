import logging
import typing as tp

import bs4

from confluence_sync.utils import bsoup4, text

_logger = logging.getLogger('confluence-sync')


def text_formatter(
    replace_text_substr: tuple[str, str] | None = None,
    start_text_with: str | None = None,
) -> tp.Callable[[str], str] | None:
    """Создание фукнции для форматирования текста.

    :param replace_text_substr: данные для замены подтстроки строки
    :param start_text_with: добавить префикс к строке
    :return: функция форматирования
    """
    funcs = []

    if replace_text_substr:
        funcs.append(lambda s: str.replace(s, *replace_text_substr))

    if start_text_with:
        funcs.append(lambda s: start_text_with + s)

    if not funcs:
        return None

    def _inner(s: str) -> str:
        for func in funcs:
            s = func(s)

        return s

    return _inner


def page_ri(
    title_: str,
    body: str,
    possible_ri_page_titles: set[str],
    formatter: tp.Callable[[str], str] | None = None,
) -> str:
    """Замена ссылок на страницы в тегах `page:ri`.

    :param title_: название страницы, для которой производиться замена ссылок
    :param body: тело страницы
    :param possible_ri_page_titles: множество названий страниц всех потомков
    :param formatter: функция-форматтер ссылок; если не передано, то замены не будет
    :return: отфармотированный текст
    """
    # используется html.parser, чтобы было указатель на начало тега
    # https://www.crummy.com/software/BeautifulSoup/bs4/doc/#line-numbers
    soup = bs4.BeautifulSoup(body, features='html.parser', parse_only=bs4.SoupStrainer('ri:page'))

    if formatter:
        page_ri_replacer = _page_ri(body, formatter)
        next(page_ri_replacer)
    else:
        page_ri_replacer = None

    for tag in soup:
        tag: bs4.Tag
        ri_page_title = tag['ri:content-title']

        if page_ri_replacer and ri_page_title in possible_ri_page_titles:
            if page_ri_replacer:
                page_ri_replacer.send(tag)
        else:
            _logger.warning('Out hierarchy page link "%s", page: "%s"', ri_page_title, title_)

    return page_ri_replacer.send(None) if page_ri_replacer else body


def _page_ri(text_: str, formatter: tp.Callable[[str], str]) -> tp.Generator[str, bs4.Tag | None, None]:
    """Замена ссылок в тексте."""
    with text.Replacer(text_) as replacer:
        while True:
            tag = yield

            if tag is None:
                yield replacer.getvalue()
                return

            bsoup4.make_tag_self_closed(tag)
            length = len(str(tag))

            tag['ri:content-title'] = formatter(tag['ri:content-title'])
            replacer.replace(tag.sourceline - 1, tag.sourcepos, length, str(tag))
