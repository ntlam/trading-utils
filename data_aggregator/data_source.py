import requests
import configparser
import pymongo
import time
import concurrent.futures
import pika
from threading import Thread, current_thread


def get_data_from_yahoo_finance(ticker: str, expiration: str = None) -> requests.Response:
    url = f'https://query2.finance.yahoo.com/v7/finance/options/{ticker}'
    queryparams = {
        'formatted': False,
        'lang': 'en-US'
    }

    if expiration is not None:
        queryparams['date'] = expiration
    
    response = requests.get(url, params = queryparams)
    response.raise_for_status()

    _validate_yahoo_finance_response(response, ticker)

    return response

def _validate_yahoo_finance_response(response: str, ticker: str) -> None:
    if (response is None or
        response.json() is None or
        not 'optionChain' in response.json() or
        not 'result' in response.json()['optionChain']):
        print(f'Unable to retrieve data for {ticker}. Skipping')
        raise

    if (len(response.json()['optionChain']['result']) > 1):
        print(f'{ticker} has more than one result. Should look into it')


def get_data_and_add_to_queue(ticker: str) -> None:
    response = get_data_and_add_to_queue_internal(ticker, None)
    result = response.json()['optionChain']['result']

    epochtimestamp = int(time.time())
    quote = result[0]['quote']
    quote['timestamp'] = epochtimestamp
    publish_stock_quote(quote)

    expiration_dates = result[0]['expirationDates']

    with concurrent.futures.ThreadPoolExecutor(max_workers = int(get_config('MaxThreadCount'))) as executor:
        for expiration in expiration_dates:
            executor.submit(get_data_and_add_to_queue_internal, ticker, expiration)

def get_data_and_add_to_queue_internal(ticker: str, expiration: str) -> requests.Response:
    response = get_data_from_yahoo_finance(ticker, expiration)
    # truncate milliseconds to align on the second boundary
    epochtimestamp = int(time.time())
    print(f'threadName:{current_thread().name}')
    print(f'threadId:{current_thread().ident}')
    print(f'timestamp:{epochtimestamp}')
    print(f'expiration:{expiration}')
    print('\n')

    # options is an array. must wrap it in a new object
    result = response.json()['optionChain']['result']
    options_chain = { 'options': result[0]['options'], 'timestamp': epochtimestamp }

    # consumer will do data cleansing
    publish_options_chain(options_chain)
    return response

def publish_stock_quote(json: str) -> None:
    # print(f'stock quote {json}')
    pass

    
    # channel = connection.channel() # start a channel
    # channel.queue_declare(queue='hello') # Declare a queue
    # channel.basic_publish(exchange='',
    #                 routing_key='hello',
    #                 body='Hello CloudAMQP!')

AMQP_CONNECTION_POOL = {}
AMQP_CONNECTION_PARAMS = pika.URLParameters(
    os.environ.get('CLOUDAMQP_URL')
)

def getAMQPConnection() -> pika.BlockingConnection:
    threadId = current_thread().ident
    if threadId in AMQP_CONNECTION_POOL:
        return AMQP_CONNECTION_POOL[threadId]

    connection = pika.BlockingConnection(AMQP_CONNECTION_PARAMS)
    AMQP_CONNECTION_POOL[threadId] = connection
    return connection

def publish_options_chain(json: str) -> None:
    # print(f'options chain {json}')
    pass

def get_config(configname: str, section: str = 'DEFAULT') -> str:
    config = configparser.ConfigParser()
    config.read('properties.ini')
    return config[section][configname]
    
def connect_to_database(host: str, db_name: str, username: str = None,
                        password: str = None, max_pool_size: int = 100) -> pymongo.database.Database:
    client = pymongo.MongoClient(
        host = host,
        username = username,
        password = password,
        retryWrites = False,
        maxPoolSize = max_pool_size
    )
    return client[db_name]



if (__name__ == 'main'):
    tickers = [ 'NFLX' ]

    # map(get_data_and_add_to_queue, tickers)

    get_data_and_add_to_queue(tickers[0])

    db = connect_to_database(
        host = get_config('DBHost'),
        db_name = get_config('TradingDBName'),
        username = get_config('DBUser'),
        password = get_config('DBPass'),
        max_pool_size = int(get_config('DBMaxPoolSize'))
    )

    print(get_config('MaxThreadCount'))

    # result = db[get_config('OptionsSchema')].insert_one({'test': 'test'})
    # print(result)