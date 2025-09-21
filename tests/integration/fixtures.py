import jinja2
import pytest
from atlassian import confluence

from tests.integration import config


@pytest.fixture(scope='session')
def src_confluence_config() -> config.SRCConfluenceConfig:
	return config.SRCConfluenceConfig()


@pytest.fixture(scope='session')
def dst_confluence_config() -> config.DSTConfluenceConfig:
	return config.DSTConfluenceConfig()


@pytest.fixture(scope='session')
def src_confluence_client(src_confluence_config) -> confluence.Confluence:
	return confluence.Confluence(
		url=str(src_confluence_config.url),
		username=src_confluence_config.username,
		password=src_confluence_config.password,
	)


@pytest.fixture(scope='session')
def dst_confluence_client(dst_confluence_config) -> confluence.Confluence:
	return confluence.Confluence(
		url=str(dst_confluence_config.url),
		username=dst_confluence_config.username,
		password=dst_confluence_config.password,
	)

@pytest.fixture(scope='session')
def jinja_env() -> jinja2.Environment:
	return jinja2.Environment(loader=jinja2.BaseLoader(), autoescape=jinja2.select_autoescape())
