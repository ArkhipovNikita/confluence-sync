import argparse
import logging

from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from confluence_sync import sync, observer, events

_logger = logging.getLogger('confluence-sync')


class ConfluenceSyncedPageProgressBar(observer.Observer):
    def __init__(self) -> None:
        self._progress_bar: tqdm | None = None
        self._logging_redirect_tqdm = None

    def __enter__(self) -> 'ConfluenceSyncedPageProgressBar':
        self._logging_redirect_tqdm = logging_redirect_tqdm([_logger])
        self._logging_redirect_tqdm.__enter__()

        return self

    def __exit__(self, *args) -> None:
        self._logging_redirect_tqdm.__exit__(*args)

    def _init_progress_bar(self, total: int) -> None:
        self._progress_bar = tqdm(
            desc='Synced page count',
            total=total,
            unit='',
        )

    def update(self, event: events.Event) -> None:
        if isinstance(event, events.TotalPageCountChanged):
            if not self._progress_bar:
                self._init_progress_bar(event.total_page_count)
            else:
                self._progress_bar.total = event.total_page_count
                self._progress_bar.refresh()
        elif isinstance(event, events.SyncedPageCountChanged):
            if not self._progress_bar:
                raise ValueError('Progress bar must be inited with total count initially')

            delta = event.synced_page_count - self._progress_bar.n
            self._progress_bar.update(delta)

            if delta < 0:
                self._progress_bar.refresh()
        else:
            raise NotImplementedError


def confluence_sync(args) -> None:
    # source
    source_kwargs = {'url': args.source_url}

    if args.source_basic:
        username, password = args.source_basic.split(':', 2)
        source_kwargs['username'] = username
        source_kwargs['password'] = password
    else:
        source_kwargs['token'] = args.source_token

    # destination
    dest_kwargs = {'url': args.dest_url}

    if args.dest_basic:
        username, password = args.dest_basic.split(':', 2)
        dest_kwargs['username'] = username
        dest_kwargs['password'] = password
    else:
        dest_kwargs['token'] = args.dest_token

    source = sync.ConfluenceConfig(**source_kwargs)
    dest = sync.ConfluenceConfig(**dest_kwargs)

    syncer = sync.ConfluenceSynchronizer(source, dest)

    with ConfluenceSyncedPageProgressBar() as progress_bar, syncer:
        session = syncer.sync_page_hierarchy(
            src_space=args.source_space,
            src_title=args.source_title,
            dst_space=args.dest_space,
            dst_title=args.dest_title,
            sync_out_hierarchy=args.sync_out_hierarchy,
            replace_title_substr=tuple(args.replace_title_substr) if args.replace_title_substr else None,
            start_title_with=args.start_title_with,
        )

        session.attach(progress_bar)
        session.run()


parser = argparse.ArgumentParser(prog='')
parser.set_defaults(func=confluence_sync)

# Source
parser.add_argument('--source-url', required=True)

source_auth_group = parser.add_mutually_exclusive_group(required=True)
source_auth_group.add_argument('--source-basic', help='Username and password separated by semicolon')
source_auth_group.add_argument('--source-token')

parser.add_argument('--source-space', required=True)
parser.add_argument('--source-title', required=False)

# Destination
parser.add_argument('--dest-url', required=True)

dest_auth_group = parser.add_mutually_exclusive_group(required=True)
dest_auth_group.add_argument('--dest-basic', help='Username and password separated by semicolon')
dest_auth_group.add_argument('--dest-token')

parser.add_argument('--dest-space', required=True)
parser.add_argument('--dest-title', required=False)

# Settings
parser.add_argument('--sync-out-hierarchy', action='store_true', help='Sync out hierarchy pages that are used by target one')
parser.add_argument('--replace-title-substr', nargs=2, help='Replace a page title substring with a new one')
parser.add_argument('--start-title-with', help='Add a prefix to a page title')
