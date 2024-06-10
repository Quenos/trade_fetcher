import os
import time
from configparser import ConfigParser
from datetime import date, datetime, timedelta, timezone

import pymongo

from market_data import MarketData
from session import ApplicationSession
from tastytrade.instruments import (Future, NestedFutureOptionChain,
                                    get_option_chain, OptionType)
from utils import log

MAX_DTE = 365
PID_FILE = "/tmp/market_data_script.pid"
SCRIPT_NAME = os.path.basename(__file__)


def sleep_until(target_time_str):
    # Parse the input time string
    target_hour, target_minute = map(int, target_time_str.split(':'))

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


def create_symbol_list(session, underlying_symbols: list[str]):
    equity_option_symbols = []
    future_option_symbols = []
    streamer_to_normal_symbols = []

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
                    option_type = 'CALL' if (option.option_type ==
                                             OptionType.CALL) else \
                        'PUT'
                    symbol_map = {'streamer_symbol': option.streamer_symbol,
                                  'underlying_symbol': symbol,
                                  'base_symbol': symbol,
                                  'expiration_date': option.expires_at,
                                  'strike_price': option.strike_price,
                                  'option_type': option_type}
                    streamer_to_normal_symbols.append(symbol_map)
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
                    symbol_map = {'streamer_symbol':strike.call_streamer_symbol,
                                  'underlying_symbol': symbol,
                                  'base_symbol': symbol[:3],
                                  'expiration_date': option.expires_at,
                                  'strike_price': strike.strike_price,
                                  'option_type': 'CALL'}
                    streamer_to_normal_symbols.append(symbol_map)
                    symbol_map = {'streamer_symbol': strike.put_streamer_symbol,
                                  'underlying_symbol': symbol,
                                  'base_symbol': symbol[:3],
                                  'expiration_date': option.expires_at,
                                  'strike_price': strike.strike_price,
                                  'option_type': 'PUT'}
                    streamer_to_normal_symbols.append(symbol_map)

    return list(set(equity_option_symbols + future_option_symbols)), \
        streamer_to_normal_symbols


def store_symbol_map(client, symbol_map: list[dict]) -> None:
    db_target = client['tastytrade']
    symbol_map_data = db_target['symbol_map']
    try:
        symbol_map_data.insert_many(symbol_map, ordered=False)
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
    write_pid()
    client = get_mongo_client()
    log(client, SCRIPT_NAME, 'start up')
    session = ApplicationSession().session
    symbols = fetch_symbols_from_db(client)
    streamer_symbols, symbol_map = create_symbol_list(session, symbols)
    store_symbol_map(client, symbol_map)

    MAX_SIZE = 1000  # NOQA 
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
    sleep_until('02:00')
    log(client, SCRIPT_NAME, 'daily restart')


if __name__ == '__main__':
    main()
