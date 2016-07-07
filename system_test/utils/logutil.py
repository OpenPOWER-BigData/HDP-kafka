import logging
from test_constants import *

logger = logging.getLogger("namedLogger")


def log_status(pass_status, message):
    """
    Print a log line for the status.
    :rtype: bool
    :param pass_status: whether check has passed or failed
    :param message: message to print in log
    """
    if pass_status:
        logger.info("%s: %s", message, PASSED)
    else:
        logger.info("%s: %s", message, FAILED)
    return pass_status


def log_test_summary(status_dict, test_name):
    assert isinstance(status_dict, dict), "The supplied argument is not a test status."
    if SKIPPED in status_dict.values() and len(status_dict.values()) == 1:
        # test was skipped no need to print summary
        pass
    else:
        output = "Test result summary: "
        for key in sorted(status_dict.keys()):
            output += "\n\t" + key + " : " + status_dict[key]
        logger.info(output)

    def log_test_result(status):
        logger.info("Testing going to end for: %s ----- Status: %s", test_name, status)
    important_keys = [k for k in status_dict.keys() if "Validate for data matched on topic" in k or TEST_COMPLETED in k]
    important_values = [v for k, v in status_dict.items() if k in important_keys]
    if FAILED in important_values:
        log_test_result(FAILED)
    elif SKIPPED in important_values:
        log_test_result(SKIPPED)
    else:
        log_test_result(PASSED)
