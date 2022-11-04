import logging


def get_logger():
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname).1s %(message)s',
        datefmt='%Y.%m.%d %H:%M:%S',
    )

    return logging.getLogger()


SCHEDULER_MAX_RATE = 10
SCHEDULER_INTERVAL = 1
QUEUE_FILE = 'file.lock'
START_AT_JOB_FORMAT = '%d-%m-%Y %H:%M'
