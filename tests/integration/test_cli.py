import contextlib
import os.path
import pathlib
import queue
import re
import subprocess
import time
import typing as tp

import jinja2
import pytest
from atlassian import confluence

from tests.integration import case, config, template, confluencex
from tests.integration.utils import xhtml

PROJECT_HOME_DIR = pathlib.Path(__file__).parent.parent.parent


@pytest.fixture
def setup_and_clean_test_case(
	test_case: case.TestCase,
	src_confluence_config: config.ConfluenceConfig,
	dst_confluence_config: config.ConfluenceConfig,
	src_confluence_client: confluence.Confluence,
	dst_confluence_client: confluence.Confluence,
	jinja_env: jinja2.Environment,
) -> tp.Generator[None, None, None]:
	setup_confluence(src_confluence_client, test_case.src_confluence, jinja_env)
	setup_confluence(dst_confluence_client, test_case.dst_orig_confluence, jinja_env)

	with contextlib.suppress(Exception):
		yield

	clean_confluence(src_confluence_client, test_case.src_confluence)
	clean_confluence(dst_confluence_client, test_case.dst_orig_confluence)
	clean_confluence(dst_confluence_client, test_case.dst_exp_confluence)


def setup_confluence(
	client: confluence.Confluence,
	confluence_config: config.ConfluenceConfig,
	jinja_env: jinja2.Environment,
) -> None:
	client = confluencex.ConfluenceCacheClient.from_client(client)

	spaces = []

	template_pages = []

	for space_config in confluence_config.spaces:
		client.create_space(space_config.key, space_config.name)
		space = client.get_space(space_config.key)
		spaces.append(space)

		page_queue = queue.SimpleQueue()
		for page_config in space_config.pages:
			page_queue.put((page_config, None))

		while not page_queue.empty():
			page_config, parent_id = page_queue.get()

			page = client.create_page(
				space=space['key'],
				title=page_config.name,
				body=page_config.content,
				parent_id=parent_id
			)

			if page_config.is_template:
				template_pages.append((page['id'], page_config))

			for attachment in page_config.attachment_paths:
				client.attach_file(
					filename=attachment,
					name=os.path.basename(attachment),
					page_id=page['id']
				)

			for child_page_config in page_config.pages:
				page_queue.put((child_page_config, page['id']))

	template_context = template.create_confluence_context(client, confluence_config)

	for page_id, page_config in template_pages:
		content_template = jinja_env.from_string(page_config.content)
		actual_content = content_template.render(confluence=template_context)
		client.update_page(
			page_id=page_id,
			title=page_config.name,
			body=actual_content,
		)


def clean_confluence(client: confluence.Confluence, confluence_config: case.ConfluenceConfig) -> None:
	task_ids = []

	for space in confluence_config.spaces:
		try:
			response = client.delete_space(space.key)
		# not exists
		except confluence.ApiError:
			continue

		task_id = response['id']
		task_ids.append(task_id)

	while task_ids:
		time.sleep(1)

		task_idx = 0
		while task_idx < len(task_ids):
			task_id = task_ids[task_idx]

			response = client.check_long_task_result(task_id)
			successful = response['successful']

			if successful:
				task_ids.pop(task_idx)
			else:
				task_idx += 1


def assert_confluence(
	client: confluence.Confluence,
	confluence_config: case.ConfluenceConfig,
	jinja_env: jinja2.Environment,
) -> None:
	template_context = None

	for space_config in confluence_config.spaces:
		space = client.get_space(space_config.key, expand='homepage')
		homepage = space['homepage']

		page_queue = queue.SimpleQueue()
		for page_config in space_config.pages:
			page_queue.put((page_config, homepage['id']))

		while not page_queue.empty():
			page_config, parent_id = page_queue.get()

			page = client.get_page_by_title(space['key'], page_config.name, expand='body.storage,ancestors')

			if parent_id is None:
				assert len(page['ancestors']) == 0, 'Page must not have any ancestors'
			else:
				assert page['ancestors'][-1]['id'] == parent_id, 'Page has unexpected ancestor'

			if page_config.is_template:
				if template_context is None:
					template_context = template.create_confluence_context(client, confluence_config)

				content_template = jinja_env.from_string(page_config.content)
				actual_content = content_template.render(confluence=template_context)
			else:
				actual_content = page_config.content

			expected_content = xhtml.minify(actual_content)
			actual_content = xhtml.minify(page['body']['storage']['value'])

			actual_content = re.sub(r'(ac:macro-id=)"[^"]*"', r'\1""', actual_content)
			expected_content = re.sub(r'(ac:macro-id=)"[^"]*"', r'\1""', expected_content)

			assert actual_content == expected_content, 'Page content differs from expected'

			if page_config.attachment_paths:
				attachments = client.get_attachments_from_content(page['id'])

				actual_attachment_names = [attachment['title'] for attachment in attachments['results']]
				actual_attachment_names.sort()

				expected_attachment_names = [os.path.basename(a) for a in page_config.attachment_paths]
				expected_attachment_names.sort()

				assert actual_attachment_names == expected_attachment_names, 'Page has different attachments'

			for child_page_config in page_config.pages:
				page_queue.put((child_page_config, page['id']))


def create_confluence_sync_cmd(
	test_case: case.TestCase,
	src_confluence_config: config.ConfluenceConfig,
	dst_confluence_config: config.ConfluenceConfig,
) -> list[str]:
	cmd = [
		'python3',
		'-m',
		'confluence_sync',
		'--source-url',
		f'{src_confluence_config.url}',
		'--source-basic',
		f'{src_confluence_config.username}:{src_confluence_config.password}',
		'--dest-url',
		f'{dst_confluence_config.url}',
		'--dest-basic',
		f'{dst_confluence_config.username}:{dst_confluence_config.password}',
	]

	cmd.extend(test_case.confluence_sync_args)

	return cmd


def pytest_generate_tests(metafunc):
	test_case_fixture_name = 'test_case'

	if test_case_fixture_name in metafunc.fixturenames:
		test_cases = case.get_test_cases()

		test_case_ids = [test_case.id for test_case in test_cases]
		test_case_sort_indices = sorted(range(len(test_cases)), key=test_case_ids.__getitem__)

		sorted_test_cases = [test_cases[i] for i in test_case_sort_indices]
		sorted_test_case_names = [test_case.name for test_case in sorted_test_cases]

		metafunc.parametrize(test_case_fixture_name, sorted_test_cases, ids=sorted_test_case_names)


def test_cli(
	test_case: case.TestCase,
	setup_and_clean_test_case: None,
	src_confluence_config: config.ConfluenceConfig,
	dst_confluence_config: config.ConfluenceConfig,
	dst_confluence_client: confluence.Confluence,
	jinja_env: jinja2.Environment,
):
	confluence_sync_cmd = create_confluence_sync_cmd(test_case, src_confluence_config, dst_confluence_config)
	subprocess.run(confluence_sync_cmd, cwd=PROJECT_HOME_DIR, capture_output=True, check=True)
	assert_confluence(dst_confluence_client, test_case.dst_exp_confluence, jinja_env)
