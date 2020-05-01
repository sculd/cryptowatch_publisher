import datetime
from google.cloud import logging

LOG_NAME = 'cryptowatch_streaming_publisher'

_client = None

def _get_client():
    global _client
    if _client is None:
        _client = logging.Client()
    return _client

def get_logger(log_name=LOG_NAME):
    return _get_client().logger(log_name)

def _print_with_severity_datetime_prefix(severity, text):
    t_str = str(datetime.datetime.now())
    print('{datetime}, {severity}: {text}'.format(datetime=t_str, severity=severity, text=text))

def _print_with_severity_prefix(severity, text):
    _print_with_severity_datetime_prefix(severity, text)

def _log_print_with_severity(severity, text):
    _print_with_severity_prefix(severity, text)
    logger = get_logger()
    logger.log_text(text, severity=severity)

def debug(*messages):
    text = ', '.join(list(map(lambda m: str(m), messages)))
    _log_print_with_severity('DEBUG', text)

def info(*messages):
    text = ', '.join(list(map(lambda m: str(m), messages)))
    _log_print_with_severity('INFO', text)

def error(*messages):
    text = ', '.join(list(map(lambda m: str(m), messages)))
    _log_print_with_severity('ERROR', text)

def warning(*messages):
    text = ', '.join(list(map(lambda m: str(m), messages)))
    _log_print_with_severity('WARNING', text)

def warn(*messages):
    warning(*messages)
