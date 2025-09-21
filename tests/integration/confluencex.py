from atlassian import confluence


class ConfluenceCacheClient(confluence.Confluence):
	def __init__(self, *args, **kwargs) -> None:
		super().__init__(*args, **kwargs)

		self._space_cache = {}

	def get_space(self, space_key, expand='description.plain,homepage', params=None):
		cache_key = (space_key, expand)
		if cache_key in self._space_cache:
			return self._space_cache[cache_key]

		space = super().get_space(space_key, expand, params)
		self._space_cache[cache_key] = space

		return space

	@classmethod
	def from_client(cls, client: confluence.Confluence) -> 'ConfluenceCacheClient':
		return ConfluenceCacheClient(
			url=client.url,
			username=client.username,
			password=client.password,
			timeout=client.timeout,
			verify_ssl=client.verify_ssl,
			api_root=client.api_root,
			api_version=client.api_version,
			cookies=client.cookies,
			advanced_mode=client.advanced_mode,
			cloud=client.cloud,
			proxies=client.proxies,
			cert=client.cert,
			session=client.session
		)
