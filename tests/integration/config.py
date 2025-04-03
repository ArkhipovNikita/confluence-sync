import pydantic
import pydantic_settings


class ConfluenceConfig(pydantic_settings.BaseSettings):
	url: pydantic.AnyHttpUrl
	username: str
	password: str


class SRCConfluenceConfig(ConfluenceConfig):
	model_config = pydantic_settings.SettingsConfigDict(env_prefix='CONFLUENCE_SRC_')


class DSTConfluenceConfig(ConfluenceConfig):
	model_config = pydantic_settings.SettingsConfigDict(env_prefix='CONFLUENCE_DST_')
