import os
import signal
import time
from configparser import ConfigParser
from datetime import date, datetime

import pymongo
import pytz
import schedule

from market_data import MarketData
from session import ApplicationSession
from tastytrade.instruments import (Future, NestedFutureOptionChain,
                                    get_option_chain)

MAX_DTE = 365
PID_FILE = "/tmp/market_data_script.pid"


def create_symbol_list_earlier_expirations(underlying_symbols: list[str]) \
        -> tuple[list[str | None], dict[str | None, str | None]]:
    equity_option_symbols = []
    future_option_symbols = []
    streamer_to_normal_symbols = {}
    symbols = []
    session = ApplicationSession().session
    for symbol in underlying_symbols:
        if not symbol.startswith('/'):
            # Get option chain for equity options
            option_chain = get_option_chain(session, symbol)
            for expiration, options in option_chain.items():
                dte = (expiration - date.today()).days
                if dte > MAX_DTE:
                    continue
                for option in options:
                    symbols.append(symbol)
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


def store_symbol_map(symbol_map: dict[str, str]) -> None:
    config = ConfigParser()
    config.read('config.ini')

    user = config['MONGODB']['User']
    password = config['MONGODB']['Password']
    uri = config['MONGODB']['URI']

    # Connect to MongoDB
    client_target = pymongo.MongoClient(
        f'mongodb://{user}:{password}@{uri}')
    db_target = client_target['tastytrade']
    symbol_map_data = db_target['symbol_map']
    documents = []
    for key, value in symbol_map.items():
        documents.append({'streamer_symbol': key, 'underlying_symbol': value})
    symbol_map_data.insert_many(documents)


def main():
    streamer_symbols, symbol_map = create_symbol_list_earlier_expirations([
        '/ES', '/NQ',
        '/CL', '/GC',
        '/ZB', '/6C',
        '/6E', '/6A',
        'SPXW', 'SPY',
        'SPX', '/LE',
        '/ZS', '/ZW',
        '/ZC'])
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
    while True:
        time.sleep(1000)


def stop_existing_process():
    if os.path.isfile(PID_FILE):
        with open(PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        try:
            os.kill(pid, signal.SIGTERM)
            print(f"Stopped existing process with PID: {pid}")
        except ProcessLookupError:
            print("No process found with PID:", pid)
        os.remove(PID_FILE)


def write_pid():
    pid = str(os.getpid())
    with open(PID_FILE, 'w') as f:
        f.write(pid)
    print(f"Script is running with PID: {pid}")


if __name__ == '__main__':
    # Time zone setup for EST
    est = pytz.timezone('US/Eastern')


    # Function to run the main script
    def job():
        print(
            f"Script started at {datetime.now(est).strftime('%Y-%m-%d %H:%M:%S')} EST")
        stop_existing_process()
        write_pid()
        main()


    # Schedule the job at 5:30 AM EST every day
    schedule.every().day.at("17:30").do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)
