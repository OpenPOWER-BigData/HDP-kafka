import logging
import time

logger = logging.getLogger("namedLogger")


def sleep(secs):
    logger.info("Sleeping for %d seconds.", secs)
    time.sleep(secs)
