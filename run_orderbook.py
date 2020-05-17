import argparse
import os, time, threading, subprocess, signal, datetime, requests, json
import publish.influxdb
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

_API_KEY = os.environ["CRYPTOWATCH_API_KEY"]

_ORDERBOOK_REQUEST_URL_FORMAT = 'https://api.cryptowat.ch/markets/{exchange}/{pair}/orderbook?apikey={apikey}&span={change_percent}'
_PERCENT_MARKS = [1, 2, 3, 4, 5, 10, 15, 20]

def _empty_orderbook(orderbook):
    return len(orderbook['result']['asks']) == 0 or len(orderbook['result']['bids']) == 0

def _fill_cum_volume_quote_marks_for_side(orderbook, price_marks, side, cum_volume_quote_marks):
    if _empty_orderbook(orderbook): return

    spread_ask = orderbook['result']['asks'][0][0]
    spread_bid = orderbook['result']['bids'][0][0]

    cum_volume_quote = 0
    percent_marks_i = 0
    for p, vol_quote in orderbook['result'][side]:
        change_percent = abs(p - spread_ask) / spread_ask * 100.0
        # cross percent_mark
        while True:
            if percent_marks_i >= len(_PERCENT_MARKS):
                break
            percent_marks = _PERCENT_MARKS[percent_marks_i]
            if change_percent < percent_marks:
                break
            cum_volume_quote_marks[side][percent_marks] = (price_marks[side][percent_marks], cum_volume_quote)
            percent_marks_i += 1

        if percent_marks_i == len(_PERCENT_MARKS): 
            print('finished filling the marks')
            break
        cum_volume_quote += vol_quote

def fill_cum_volume_quote_marks(orderbook):
    cum_volume_quote_marks = {
        'asks': { },
        'bids': { }
    }
    if _empty_orderbook(orderbook): 
        print('the orderbook is empty')
        return cum_volume_quote_marks

    spread_ask = orderbook['result']['asks'][0][0]
    spread_bid = orderbook['result']['bids'][0][0]

    price_marks = {
        'asks': { },
        'bids': { }
    }
    for percent_mark in _PERCENT_MARKS:
        price_marks['asks'][percent_mark] = spread_ask * (1.0 + 0.01 * percent_mark)
        price_marks['bids'][percent_mark] = spread_bid * (1.0 - 0.01 * percent_mark)

    _fill_cum_volume_quote_marks_for_side(orderbook, price_marks, 'asks', cum_volume_quote_marks)
    _fill_cum_volume_quote_marks_for_side(orderbook, price_marks, 'bids', cum_volume_quote_marks)

    return cum_volume_quote_marks

def get_cum_volume_quote_marks(exchange, pair):
    url = _ORDERBOOK_REQUEST_URL_FORMAT.format(
        exchange = exchange,
        pair = pair,
        apikey = _API_KEY,
        change_percent = 20
        )
    orderbook = requests.get(url).json()
    cum_volume_quote_marks = fill_cum_volume_quote_marks(orderbook)
    return cum_volume_quote_marks

def publish_cum_volume_quote_marks(exchange, pair):
    cum_volume_quote_marks = get_cum_volume_quote_marks(exchange, pair)

    tag_dict = {}
    field_dict = {}

    for percent_mark in _PERCENT_MARKS:
        tags = {
            'exchange': exchange,
            'symbol': pair,
            'span_percent': percent_mark
        }
        if percent_mark not in cum_volume_quote_marks['asks'] or  percent_mark not in cum_volume_quote_marks['bids']:
            continue
        a_vol = cum_volume_quote_marks['asks'][percent_mark][1]
        b_vol = cum_volume_quote_marks['bids'][percent_mark][1]
        fields = {
            'asks': a_vol,
            'bids': b_vol,
            'bids_sub_asks': b_vol - a_vol
        }
        publish.influxdb.publish('orderbook', tags, fields)

    return cum_volume_quote_marks

class thread_publish_cum_volume_quote_marks(threading.Thread):
   def __init__(self, exchange, pair):
      threading.Thread.__init__(self)
      self.exchange = exchange
      self.pair = pair

   def run(self):
      print ("Starting ", self.exchange, self.pair)
      publish_cum_volume_quote_marks(self.exchange, self.pair)
      print ("Finishing ", self.exchange, self.pair)

class thread_publish_cum_volume_quote_marks_scan(threading.Thread):
    def __init__(self, exchange_markets, exchange):
        threading.Thread.__init__(self)
        self.exchange_markets = exchange_markets
        self.exchange = exchange

    def run(self):
        print ("Starting scan", self.exchange)
        for exchange, markets in self.exchange_markets.items():
            if exchange != self.exchange: continue
            for market in markets:
                th = thread_publish_cum_volume_quote_marks(self.exchange, market)
                th.start()
                time.sleep(0.1)
        print ("Finishing scan", self.exchange)


if __name__ == '__main__':
    import pprint
    exchange_markets = json.load(open('exchange_markets.json'))

    while True:
        th = thread_publish_cum_volume_quote_marks_scan(exchange_markets, 'kraken')
        th.start()
        time.sleep(10)

    for exchange, markets in exchange_markets.items():
        if exchange != 'kraken': continue
        for market in markets:
            if 'btc' not in market and 'eth' not in market and 'ltc' not in market: continue
            th = thread_publish_cum_volume_quote_marks(exchange, market)
            th.start()
            '''
            cum_volume_quote_marks = publish_cum_volume_quote_marks(exchange, market)
            print('exchange: {exchange}, market: {market}'.format(exchange=exchange, market=market))
            pprint.pprint(cum_volume_quote_marks)
            '''
            time.sleep(0.1)






