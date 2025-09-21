# ConfluenceSync

A command-line utility for synchronizing page hierarchies across Confluence Data Center instances.

## Features

### Page hierarchy

Copy entire page hierarchies.

While copying, you can rename pages by adding a prefix or replacing a substring, similar to how Confluence's built-in Move Page utility
works.
If a page hasn't been modified since the last synchronization, it will not be copied again.

Source page versions are not copied. The destination page will start version numbering from the first copied version.

### Attachments

All attachments are copied along with the page content.

If an attachment hasn't been modified since the last synchronization, it will not be copied again.

### Draw.io diagrams

Supports copying draw.io diagrams, including embedded diagrams created by the corresponding plugin.

If an included draw.io diagram exists in the current page hierarchy being copied, the link will be updated to point to the new page.

If the included diagram is outside the current hierarchy, one of the diagrams linked to it will be used as the new source,
and the remaining links will point to this new source page within the hierarchy.

### Pages outside the hierarchy

Sometimes pages linked from the hierarchy are located outside the current page hierarchy.
By default, such links are logged.

If the flag `--sync-out-hierarchy` is used, these pages are copied to the root of the destination space.
Page name modifiers (prefixes or substring replacements) are applied to these pages as well.
If a linked page belongs to a different space than the source, the name will be prefixed with <LinkedPageSpaceName>: .

## Installation

```bash
pip install confluence-sync
```

## Usage

```bash
confluence-sync -h
```

| Название                 | Описание                                                                | Пример                     |
|:-------------------------|-------------------------------------------------------------------------|----------------------------|
| `--source-url`           | Source confluence URL                                                   | `"https://localhost:8090"` |
| `--source-token`         | Source confluence PAT-token                                             | `"12345"`                  |
| `--source-basic`         | Source confluence login and password                                    | `"username:password"`      |
| `--source-space`         | Source confluence space                                                 | `"MYSPACE"`                |
| `--source-title`         | Source confluence page title                                            | `"MYPAGE"`                 |
| `--source-id`            | Source confluence page id. Can be used instead if space and title.      | `"12345"`                  |
| `--dest-url`             | Destination confluence URL                                              | `"https://localhost:9090"` |
| `--dest-token`           | Destination confluence PAT-token                                        | `"12345"`                  |
| `--dest-basic`           | Destination confluence login and password                               | `"username:password"`      |
| `--dest-space`           | Destination confluence space                                            | `"MYSPACE"`                |
| `--dest-title`           | Destination confluence page title                                       | `"MYPAGE"`                 |
| `--dest-id`              | Destination confluence page id. Can be used instead if space and title. | `"12345"`                  |
| `--replace-title-substr` | Replace a substring in the page title                                   | `"[SRC]" "[DST]"`          |
| `--start-title-with`     | Add a prefix to the page title                                          | `"PY "`                    |
| `--sync-out-hierarchy`   | Copy pages outside the current page hierarchy                           | `--sync-out-hierarchy`     |
