from confluence_sync import cli, logger


def main() -> None:
	logger.setup()
	args = cli.parser.parse_args()
	args.func(args)


if __name__ == '__main__':
	main()
