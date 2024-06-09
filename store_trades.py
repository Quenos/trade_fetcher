from datetime import datetime, timedelta, timezone
import os
import time
from configparser import ConfigParser
from datetime import date, datetime

import pymongo

from market_data import MarketData
from session import ApplicationSession
from tastytrade.instruments import (Future, NestedFutureOptionChain,
                                    get_option_chain)
from utils import log_startup

MAX_DTE = 365
PID_FILE = "/tmp/market_data_script.pid"
SCRIPT_NAME = os.path.basename(__file__)


def sleep_until(target_time_str):
    # Parse the input time string
    target_hour, target_minute = map(int, target_time_str.split(':'))

    while True:
        now = datetime.now(timezone.utc)
        target_time = now.replace(hour=target_hour, minute=target_minute,
                                  second=0, microsecond=0)

        if now >= target_time:
            # If the current time is past the target time, target the next day
            target_time += timedelta(days=1)

        sleep_duration = (target_time - now).total_seconds()
        time.sleep(sleep_duration)


def get_mongo_client():
    config = ConfigParser()
    config.read('config.ini')

    user = config['MONGODB']['User']
    password = config['MONGODB']['Password']
    uri = config['MONGODB']['URI']

    # Connect to MongoDB
    client = pymongo.MongoClient(f'mongodb://{user}:{password}@{uri}')
    return client


def fetch_symbols_from_db(client) -> list[str]:
    db = client['tastytrade']
    symbols_collection = db['symbols']

    # Fetch symbols
    symbols = [doc['symbol'] for doc in symbols_collection.find()]
    return symbols


def create_symbol_list_earlier_expirations(session,
                                           underlying_symbols: list[str]) \
        -> tuple[list[str | None], dict[str | None, str | None]]:
    equity_option_symbols = []
    future_option_symbols = []
    streamer_to_normal_symbols = {}

    for symbol in underlying_symbols:
        if not symbol.startswith('/'):
            # Get option chain for equity options
            option_chain = get_option_chain(session, symbol)
            for expiration, options in option_chain.items():
                dte = (expiration - date.today()).days
                if dte > MAX_DTE:
                    continue
                for option in options:
                    equity_option_symbols.append(symbol)
                    equity_option_symbols.append(option.streamer_symbol)
                    streamer_to_normal_symbols[option.streamer_symbol] = symbol
        elif symbol.startswith('/'):
            # Get option chain for future options
            option_chain = sorted(NestedFutureOptionChain
                                  .get_chain(session, symbol)
                                  .option_chains[0].expirations,
                                  key=lambda x: x.expiration_date)
            filtered_option_chain = [obj for obj in option_chain
                                     if (obj.expiration_date
                                         - date.today()).days <= MAX_DTE]
            for option in filtered_option_chain:
                underlying_symbol = Future.get_future(session,
                                                      option.underlying_symbol)
                future_option_symbols.append(underlying_symbol.streamer_symbol)
                for strike in option.strikes:
                    future_option_symbols.append(strike.call_streamer_symbol)
                    future_option_symbols.append(strike.put_streamer_symbol)
                    streamer_to_normal_symbols[strike.call_streamer_symbol] = \
                        underlying_symbol.streamer_symbol
                    streamer_to_normal_symbols[strike.put_streamer_symbol] = \
                        underlying_symbol.streamer_symbol
    return list(set(equity_option_symbols + future_option_symbols)), \
        streamer_to_normal_symbols


def store_symbol_map(client, symbol_map: dict[str, str]) -> None:
    db_target = client['tastytrade']
    symbol_map_data = db_target['symbol_map']
    documents = []
    for key, value in symbol_map.items():
        documents.append({'streamer_symbol': key, 'underlying_symbol': value})
    symbol_map_data.drop()
    try:
        symbol_map_data.insert_many(documents, ordered=False)
    except pymongo.errors.BulkWriteError as e:
        # Check for duplicate key error
        if any(error['code'] == 11000 for error in e.details['writeErrors']):
            print("Duplicate key error, deleting source document")
        else:
            print(f"error: {e}")


def write_pid():
    pid = str(os.getpid())
    with open(PID_FILE, 'w') as f:
        f.write(pid)
    print(f"Script is running with PID: {pid}")


def main():
    sleep_until('09:10')
    write_pid()
    client = get_mongo_client()
    log_startup(client, SCRIPT_NAME)
    session = ApplicationSession().session
    symbols = fetch_symbols_from_db(client)
    streamer_symbols, symbol_map = create_symbol_list_earlier_expirations(
        session, symbols)
    store_symbol_map(client, symbol_map)

    MAX_SIZE = 1000
    market_data = MarketData()
    market_data.start_streamer()
    for x in range(0, len(streamer_symbols), MAX_SIZE):
        if x + MAX_SIZE > len(streamer_symbols):
            x = len(streamer_symbols)
        market_data.subscribe_trades(streamer_symbols[x:x + MAX_SIZE])
        time.sleep(5)
    for x in range(0, len(streamer_symbols), MAX_SIZE):
        if x + MAX_SIZE > len(streamer_symbols):
            x = len(streamer_symbols)
        market_data.subscribe_greeks(streamer_symbols[x:x + MAX_SIZE])
        time.sleep(5)
    sleep_until('09:15')


if __name__ == '__main__':
    main()
