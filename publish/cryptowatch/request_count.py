import os, time, datetime, threading
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

from google.cloud import monitoring_v3
import google.cloud.monitoring_v3.query as query
from google.cloud.monitoring import enums

_PROJECT_ID = 'alpaca-trading-239601'
_TOPIC_ID = 'cryptowatch_stream'
METRIC_TYPE = 'pubsub.googleapis.com/topic/send_request_count'
_API_ENDPOINT = 'monitoring.googleapis.com'

_client = None
def _get_client():
    global _client
    if _client is None:
        _client = monitoring_v3.MetricServiceClient(client_options={'api_endpoint': _API_ENDPOINT})
    return _client

_ = _get_client()

req_count = 0
def do_request(minutes):
    global req_count
    client = _get_client()

    q = query.Query(
        client,
        _PROJECT_ID,
        metric_type=METRIC_TYPE,
        end_time=None,
        days=0,
        hours=0,
        minutes=minutes). \
        align(enums.Aggregation.Aligner.ALIGN_SUM, minutes=5)

    df = q. \
        align(enums.Aggregation.Aligner.ALIGN_SUM, minutes=5). \
        as_dataframe()
    df_ = df.unstack().reset_index()
    df__ = df_[df_.topic_id == _TOPIC_ID][['level_5', 0]].set_index('level_5')

    if len(df__) == 0:
        req_count = 0
        return
    r = df__.iloc[-1]
    if len(r.values) == 0:
        req_count = 0
        return
    v = r.values[0]
    req_count = int(v)


def get_request_count(minutes=5):
    global req_count
    req_count = 0

    try:
        th = threading.Thread(target=do_request, args=(minutes,))
        th.start()
        th.join(timeout=5)
    except Exception as ex:
        print(ex)

    return req_count

