import queue
import typing as tp

import requests
from atlassian import Confluence, errors

StrDict = dict[str, tp.Any]


class CustomConfluence(Confluence):
	def traverse_descendant_pages(
		self,
		page_id: str,
		expand: str | None = None,
	) -> tp.Generator[StrDict, None, None]:
		"""Получение всех дочерних страниц текущей рекурсивно."""
		parent_page_id_queue = queue.SimpleQueue()
		parent_page_id_queue.put(page_id)

		while not parent_page_id_queue.empty():
			parent_page_id = parent_page_id_queue.get()

			child_pages = self.get_page_child_by_type(parent_page_id, type='page', expand=expand)
			for child_page in child_pages:
				parent_page_id_queue.put(child_page['id'])
				yield child_page

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
