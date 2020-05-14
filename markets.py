import requests, json
from collections import defaultdict

_exchanges = requests.get('https://api.cryptowat.ch/exchanges').json()

def get_exchange_id_to_symbol():
    res = {}
    for exch in _exchanges['result']:
        res[exch['id']] = exch['symbol']
    return res

def get_market_id_to_symbol():
    res = {}
    exchange_id_to_symbol = get_exchange_id_to_symbol()
    for i, exchange in exchange_id_to_symbol.items():
        markets = requests.get('https://api.cryptowat.ch/markets/{exchange}'.format(exchange=exchange)).json()
        for market in markets['result']:
            res[market['id']] = market['pair']
    return res

def get_exchange_and_market_symbols():
    res = defaultdict(list)
    exchange_id_to_symbol = get_exchange_id_to_symbol()
    for _, exchange in exchange_id_to_symbol.items():
        markets = requests.get('https://api.cryptowat.ch/markets/{exchange}'.format(exchange=exchange)).json()
        for market in markets['result']:
            res[exchange].append(market['pair'])
    return res

if __name__ == '__main__':
    with open('exchanges.json', 'w') as fp:
        json.dump(get_exchange_id_to_symbol(), fp)  

    with open('markets.json', 'w') as fp:
        json.dump(get_market_id_to_symbol(), fp)    

    with open('exchange_markets.json', 'w') as fp:
        json.dump(get_exchange_and_market_symbols(), fp)    

