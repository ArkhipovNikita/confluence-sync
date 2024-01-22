import dataclasses as dc
import datetime as dt
import logging
import queue
import threading
from concurrent import futures

from confluence_sync import fmt, observer
from confluence_sync.confluence import CustomConfluence, StrDict


@dc.dataclass(frozen=True)
class ConfluenceConfig:
    url: str
    username: str | None = None
    password: str | None = None
    token: str | None = None


class ConfluenceSynchronizer(observer.Observable):
    """Синхронизатор страниц конфлюенса."""

    _datetime_parser = dt.datetime.fromisoformat
    _logger = logging.getLogger('confluence-sync')

    def __init__(self, source_conf: ConfluenceConfig, dest_conf: ConfluenceConfig) -> None:
        super().__init__()

        self._source_conf = source_conf
        self._dest_conf = dest_conf

        self._source_cli = None
        self._dest_cli = None

        self._total_page_count = None
        self._synced_paged_count = None

        self._opened = False

        self._executor = futures.ThreadPoolExecutor()
        self._lock = threading.Lock()
        self._futures = []

    def __enter__(self) -> 'ConfluenceSynchronizer':
        self._ensure_closed()

        self._source_cli = CustomConfluence(**dc.asdict(self._source_conf))
        self._dest_cli = CustomConfluence(**dc.asdict(self._dest_conf))
        self._executor = self._executor.__enter__()

        self._opened = True

        return self

    def __exit__(self, *args) -> None:
        self._ensure_opened()

        self._executor.__exit__(*args)
        self._source_cli.close()
        self._dest_cli.close()

    def _ensure_opened(self) -> None:
        if not self._opened:
            raise ValueError('ConfluenceSynchronizer must be entered')

    def _ensure_closed(self) -> None:
        if self._opened:
            raise ValueError('ConfluenceSynchronizer must be closed')

    @property
    def total_page_count(self) -> int:
        return self._total_page_count

    @property
    def synced_page_count(self) -> int:
        return self._synced_paged_count

    def _inc_synced_page_count(self) -> None:
        """Инкрементирование количества синхронизированных страниц."""
        with self._lock:
            self._synced_paged_count += 1

        self.notify()

    def _init_stats(self, total_page_count: int) -> None:
        """Установка начальных значений статистики."""
        self._total_page_count = total_page_count
        self._synced_paged_count = 0
        self.notify()

    def _clean_stats(self) -> None:
        """Очистка значений статистики."""
        self._total_page_count = 0
        self._synced_paged_count = 0

    def _run_task(self, fn, *args, **kwargs) -> None:
        ft = self._executor.submit(fn, *args, **kwargs)
        self._futures.append(ft)

    def _wait_tasks(self) -> None:
        done, _ = futures.wait(self._futures, return_when=futures.FIRST_EXCEPTION)

        # необходимо получить результат, чтобы ошибка в дочернем потоке
        # пробросилась в основной
        for ft in done:
            ft.result()

        self._futures.clear()

    def sync_page_hierarchy(
        self,
        source_space: str,
        source_title: str,
        dest_space: str,
        dest_title: str | None = None,
        *,
        replace_title_substr: tuple[str, str] | None = None,
        start_title_with: str | None = None,
    ) -> None:
        """Синхронизация всей иерархии страницы.

        :param source_space: спейс страницы источника
        :param source_title: название страницы источника
        :param dest_space: спейс страницы назначения
        :param dest_title: название страницы назначения
        :param replace_title_substr: данные для замены подтстроки заголовка
        :param start_title_with: добавить префикс к заголовку
        """
        self._ensure_opened()

        if dest_title:
            dest_page = self._dest_cli.get_page_by_title(dest_space, dest_title)
        else:
            dest_space_data = self._dest_cli.get_space(dest_space)
            dest_page = self._dest_cli.get_page_by_id(dest_space_data['homepage']['id'])

        source_page = self._source_cli.get_page_by_title(source_space, source_title, expand='body.storage')

        descendant_pages = self._source_cli.traverse_descendant_pages(source_space, source_page['id'])
        descendant_page_titles = {page['title'] for page in descendant_pages}

        self._init_stats(len(descendant_page_titles) + 1)

        pages_to_sync = queue.SimpleQueue()
        # тапл с итерируемым объектом исходных страниц
        # и идентификатором родительской страницы назначения
        pages_to_sync.put(((source_page,), dest_page['id']))

        def _task(source_page_: StrDict, dest_parent_page_id_: str) -> None:
            dest_page_id = self._sync_page(
                source_page_,
                dest_space,
                dest_parent_page_id_,
                descendant_page_titles,
                replace_title_substr,
                start_title_with,
            )

            source_child_pages = self._source_cli.get_page_child_by_type(source_page_['id'], expand='body.storage')
            pages_to_sync.put((source_child_pages, dest_page_id))

        while not pages_to_sync.empty():
            source_pages, dest_parent_page_id = pages_to_sync.get()

            for source_page in source_pages:
                self._run_task(_task, source_page, dest_parent_page_id)

            self._wait_tasks()

        self._clean_stats()

    def _sync_page(
        self,
        source_page: StrDict,
        dest_space: str,
        dest_parent_page_id: str,
        descendant_page_titles: set[str],
        replace_title_substr: str | None = None,
        start_title_with: str | None = None,
    ) -> str:
        """Синхронизация страницы.

        :param source_page: данные страницы источника
        :param dest_space: спейс страницы назначения
        :param dest_parent_page_id: идентификатор родительской страницы назначения
        :param descendant_page_titles: множество названий страниц всех потомков
        :param replace_title_substr: данные для замены подтстроки заголовка
        :param start_title_with: добавить префикс к заголовку
        :return: идентификатор страницы назначения
        """
        dest_page_id = self._sync_body(
            source_page,
            dest_parent_page_id,
            descendant_page_titles,
            replace_title_substr,
            start_title_with,
        )

        self._sync_attachments(source_page, dest_space, dest_page_id)

        self._logger.info('Page synced, "%s"', source_page['title'])
        self._inc_synced_page_count()

        return dest_page_id

    def _sync_body(
        self,
        source_page: StrDict,
        dest_page_parent_id: str,
        descendant_page_titles: set[str],
        replace_title_substr: tuple[str, str] | None = None,
        start_title_with: str | None = None,
    ) -> str:
        """Синхронизация текста страницы.

        :param source_page: данные страницы источника
        :param dest_page_parent_id: идентификатор родительской страницы назначения
        :param descendant_page_titles: множество названий страниц всех потомков
        :param replace_title_substr: данные для замены подтстроки заголовка
        :param start_title_with: добавить префикс к заголовку
        :return: идентификатор страницы назначения
        """
        formatter = fmt.text_formatter(replace_title_substr, start_title_with)

        old_title = source_page['title']
        new_title = formatter(old_title) if formatter else old_title

        old_body = source_page['body']['storage']['value']
        new_body = fmt.page_ri(old_title, old_body, descendant_page_titles, formatter)

        dest_page = self._dest_cli.update_or_create(
            parent_id=dest_page_parent_id,
            title=new_title,
            body=new_body,
        )

        self._logger.info('Page body synced, "%s"', old_title)

        return dest_page['id']

    def _sync_attachments(self, source_page: StrDict, dest_space: str, dest_page_id: str) -> None:
        """Синхронизация вложений страницы.

        :param source_page: данные страницы источника
        :param dest_space: спейс вложения назначения
        :param dest_page_id: идентифкатор страницы назначения
        """
        source_attachments = self._source_cli.traverse_page_attachments(source_page['id'], expand='history.lastUpdated')

        dest_attachments = self._dest_cli.traverse_page_attachments(dest_page_id, expand='history.lastUpdated')
        dest_attachments_map = {attachment['title']: attachment for attachment in dest_attachments}

        for source_attachment in source_attachments:
            dest_attachment = dest_attachments_map.get(source_attachment['title'])
            self._sync_attachment(
                source_page,
                source_attachment,
                dest_attachment,
                dest_space,
                dest_page_id
            )

    def _sync_attachment(
        self,
        source_page: StrDict,
        source_attachment: StrDict,
        dest_attachment: StrDict | None,
        dest_space: str,
        dest_page_id: str,
    ) -> None:
        """Синхронизация вложения страницы.

        Вложение синхронизируется, если его не существует или было обновлено после последней синхронизации.

        :param source_page: данные страницы источника
        :param source_attachment: данные вложения источника
        :param dest_attachment: данные вложения назначения
        :param dest_space: спейс вложения назначения
        :param dest_page_id: идентифкатор страницы назначения
        """
        if dest_attachment:
            source_attachment_last_updated = self._datetime_parser(source_attachment['history']['lastUpdated']['when'])
            dest_attachment_last_updated = self._datetime_parser(dest_attachment['history']['lastUpdated']['when'])

            if dest_attachment_last_updated >= source_attachment_last_updated:
                self._logger.warning(
                    'Attachment "%s" already synced, page: "%s"',
                    source_attachment['title'],
                    source_page['title'],
                )

                return

        def _task() -> None:
            download_url = source_attachment['_links']['download']
            content = self._source_cli.get(download_url, not_json_response=True)

            title = source_attachment['title']

            self._dest_cli.attach_content(
                content,
                space=dest_space,
                page_id=dest_page_id,
                title=title,
                name=title,
                comment=source_attachment['metadata'].get('comment'),
            )

            self._logger.info('Attachment "%s" synced, page: "%s"', title, source_page['title'])

        self._run_task(_task)
