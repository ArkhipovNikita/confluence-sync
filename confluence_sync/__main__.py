from confluence_sync import cli, logger

logger.setup()

args = cli.parser.parse_args()
args.func(args)
