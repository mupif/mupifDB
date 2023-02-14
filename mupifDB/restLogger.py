import json
import time
import logging
import threading

class RestLogHandler(logging.StreamHandler):
    """
    Handler which sends records over Rest API; there is no formatting happening on this side of the logger,
    it only sends LogRecord as JSON over to restApiControl.
    """
    def __init__(self, *, restApiControl):
        self.restApiControl=restApiControl
        self.lock = threading.Lock()
        super().__init__()

    def emit(self, record):
        # record.tag = self.tag
        with self.lock:
            self.restApiControl.logMessage(json.dumps(record.__dict__))

if __name__=='__main__':
    import sys, os.path
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    import restApiControl
    logging.getLogger().setLevel(logging.DEBUG)
    log=logging.getLogger('test')
    log.setLevel(logging.DEBUG)
    log.addHandler(RestLogHandler(restApiControl=restApiControl))
    for i in range(100):
        log.debug(f'{i} debug message')
        log.info(f'{i} info message')
        log.warning(f'{i} warning message')
        time.sleep(.5)
    
