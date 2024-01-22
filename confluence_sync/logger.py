import logging.config

_LOG_CONFIG = {
    'version': 1,
    'formatters': {
        'colored': {
            '()': 'colorlog.ColoredFormatter',
            'format': '%(log_color)s%(levelname)-8s %(white)s%(asctime)-8s%(reset)s %(blue)s%(message)s',
            'datefmt': '%d-%m-%Y %H:%M:%S'
        }
    },
    'handlers': {
        'console-colored': {
            'formatter': 'colored',
            'class': 'logging.StreamHandler',
            'level': 'DEBUG'
        }
    },
    'loggers': {
        'confluence-sync': {
            'handlers': ['console-colored'],
            'level': 'INFO',
            'propagate': False
        }
    }
}


def setup() -> None:
    logging.config.dictConfig(_LOG_CONFIG)
