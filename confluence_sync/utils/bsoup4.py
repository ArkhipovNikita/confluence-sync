import bs4


def make_tag_self_closed(tag: bs4.Tag) -> bs4.Tag:
    """Строгое указание, что тэг должен быть самозакрывающимся."""
    tag.can_be_empty_element = True
    return tag
