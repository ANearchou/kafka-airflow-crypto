from env.config import *
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
from json import dumps
from time import sleep
from kafka import KafkaProducer


def get_price(coin = 'BTC', currency = 'USD'):
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'
    
    parameters = {
        'convert':currency,
        'symbol': coin
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': CMC_API_KEY,
    }

    session = Session()
    session.headers.update(headers)

    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
        price = dict()
        price = {
            'timestamp': data['status']['timestamp'],
            'crypto': coin,
            'price': data['data'][coin]['quote'][currency]['price']
        }
        return(price)
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)
        return True



if __name__ == '__main__':
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    for j in range(9999):
        print("Iteration", j)
        data = get_price()
        producer.send('topic_test', value=data)
        sleep(30)