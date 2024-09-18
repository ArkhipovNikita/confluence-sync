import typing as tp

import requests
from atlassian import Confluence, errors

StrDict = dict[str, tp.Any]


class CustomConfluence(Confluence):
    def traverse_descendant_pages(
        self,
        space: str,
        page_id: str,
        start: int | None = None,
        limit: int | None = 50,
        expand: str | None = None,
    ) -> tp.Generator[StrDict, None, None] | list[StrDict] | StrDict:
        """Получение всех дочерних страниц текущей рекурсивно."""
        cql = f'(space="{space}" and ancestor={page_id})'
        return self.cql_paged(cql, start, limit, expand)

    def cql_paged(
        self,
        cql: str,
        start: int | None = None,
        limit: int | None = None,
        expand: str | None = None,
    ) -> tp.Generator[StrDict, None, None] | list[StrDict] | StrDict:
        """Поиск по конфлюенсу с помощью CQL.

        Отличается от `cql` метода тем, что пролистывает все страницы.
        """
        params = {'cql': cql}

        if start:
            params['start'] = start
        if limit:
            params['limit'] = limit
        if expand:
            params['expand'] = expand

        url = 'rest/api/content/search'

        if not self.advanced_mode:
            return self._get_paged(url, params=params)
        else:
            response = self.get(url, params=params)
            if self.advanced_mode:
                return response
            return response.get('results')

    def traverse_page_attachments(
        self,
        page_id: str,
        start: int | None = None,
        limit: int | None = None,
        expand: str | None = None,
        filename: str | None = None,
        media_type: str | None = None,
    ) -> tp.Generator[StrDict, None, None] | list[StrDict] | StrDict:
        """Получение вложений страницы.

        Параметры и URL скопированы из метода `get_attachments_from_content`, добавлено получение всех вложений
        (авто пролистывание страниц).
        """
        params = {}

        if start:
            params['start'] = start
        if limit:
            params['limit'] = limit
        if expand:
            params['expand'] = expand
        if filename:
            params['filename'] = filename
        if media_type:
            params['mediaType'] = media_type

        url = 'rest/api/content/{id}/child/attachment'.format(id=page_id)

        try:
            if not self.advanced_mode:
                return self._get_paged(url, params=params)
            else:
                response = self.get(url, params=params)
                if self.advanced_mode:
                    return response
                return response.get('results')
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                # Raise ApiError as the documented reason is ambiguous
                raise errors.ApiError(
                    'There is no content with the given id, '
                    'or the calling user does not have permission to view the content',
                    reason=e,
                )

            raise

    # TODO: запрашивать не все вложения сразу, а только если в прошлых запросах не нашлось нужного
    def get_attachment_by_names(
        self,
        page_id: str,
        attachment_names: list[str],
        expand: str | None = None,
    ) -> list[StrDict]:
        attachments = list(self.traverse_page_attachments(page_id, expand=expand))
        return [attachment for attachment in attachments if attachment['title'] in attachment_names]

    def get_page_by_title_or_homepage(self, space: str, title: str | None = None, expand: tp.Any = None) -> StrDict:
        if title:
            return self.get_page_by_title(space, title, expand=expand)
        else:
            space_data = self.get_space(space)
            return self.get_page_by_id(space_data['homepage']['id'], expand)
