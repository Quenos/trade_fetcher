import time
from market_data import MarketData
from session import ApplicationSession
from datetime import date

from tastytrade.instruments import (get_option_chain, NestedFutureOptionChain,
                                    Future)
MAX_DTE: int = 365


def create_symbol_list_earlier_expirations(underlying_symbols: list[str]) -> list[str]:
    equity_option_symbols = []
    future_option_symbols = []
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
        elif symbol.startswith('/'):
            # Get option chain for future options
            option_chain = sorted(NestedFutureOptionChain
                                  .get_chain(session, symbol)
                                  .option_chains[0].expirations,
                                  key=lambda x: x.expiration_date)
            filtered_option_chain = [obj for obj in option_chain
                                     if (obj.expiration_date - date.today()).days <= MAX_DTE]
            for option in filtered_option_chain:
                underlying_symbol = Future.get_future(session,
                                                      option.underlying_symbol)
                future_option_symbols.append(underlying_symbol.streamer_symbol)
                for strike in option.strikes:
                    future_option_symbols.append(strike.call_streamer_symbol)
                    future_option_symbols.append(strike.put_streamer_symbol)
    return list(set(equity_option_symbols + future_option_symbols))


if __name__ == '__main__':
    streamer_symbols = create_symbol_list_earlier_expirations(['/ES', '/NQ',
                                                               '/CL', '/GC',
                                                               '/ZB', '/6C',
                                                               '/6E', '/6A'])
    MAX_SIZE = 1000
    market_data = MarketData()
    market_data.start_streamer()
    for x in range(0, len(streamer_symbols), MAX_SIZE):
        if x + MAX_SIZE > len(streamer_symbols):
            x = len(streamer_symbols)
        market_data.subscribe_trades(streamer_symbols[x:x+MAX_SIZE])
        # print(f'Trades: {x}')
    for x in range(0, len(streamer_symbols), MAX_SIZE):
        if x + MAX_SIZE > len(streamer_symbols):
            x = len(streamer_symbols)
        market_data.subscribe_greeks(streamer_symbols[x:x+MAX_SIZE])
        # print(f'Greeks: {x}')
    # print('All symbols subscribed')
    while True:
        time.sleep(1000)

