import json
import time
import logging
import threading

import sys
import os.path
sys.path.append(os.path.dirname(os.path.abspath(__file__))+'/..')
from . import restApiControl


class RestLogHandler(logging.StreamHandler):
    """
    Handler which sends records over Rest API; there is no formatting happening on this side of the logger, it only sends LogRecord as JSON over to restApiControl.

    *extraData* is dictionary added to every logging message (it will overwrite standard fields of LogRecords, so be careful there).
    """
    def __init__(self, *, extraData=None):
        self.extraData = (extraData if extraData is not None else {})
        self.lock = threading.Lock()
        super().__init__()

    def emit(self, record):
        with self.lock:
            # fix for python 3.8
            try:
                restApiControl.logMessage(**(record.__dict__ | self.extraData))
            except:
                pass


if __name__ == '__main__':
    import restApiControl
    logging.getLogger().setLevel(logging.DEBUG)
    log = logging.getLogger('test')
    log.setLevel(logging.DEBUG)
    # constant extra data (e.g. when creating logger in in workflow_execution_script)
    log.addHandler(RestLogHandler(extraData={'weid': 'bda8f553-5696-4920-8b26-9cd1f2cb08a3'}))
    for i in range(100):
        log.debug(f'{i} debug message')
        time.sleep(.5)
        log.info(f'{i} info message')
        time.sleep(.5)
        # variable extra data for each logging call
        log.warning(f'{i} warning message', extra={'custom-warning-data': 'lorem ipsum dolor sit amet'})
        time.sleep(.5)
        try:
            0/0
        except ZeroDivisionError:
            log.exception('There was some trouble:')
        time.sleep(.5)
    
