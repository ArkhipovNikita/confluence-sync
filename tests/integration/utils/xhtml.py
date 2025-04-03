from confluence_sync import parser

STORAGE_PARSER = parser.StorageParser(remove_blank_text=True)

def minify(xhtml: str) -> str:
	parsed_xhtml = STORAGE_PARSER.parse(xhtml)
	return STORAGE_PARSER.to_storage(parsed_xhtml)
