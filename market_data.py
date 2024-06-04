import asyncio
import time
from typing import List, Dict, ClassVar, Any
from threading import Thread
from dataclasses import dataclass, asdict, field


from tastytrade.streamer import DXLinkStreamer, EventType, Greeks, Trade, Quote
from tastytrade.instruments import Future

from session import ApplicationSession


@dataclass
class MarketData:
    # singleton class
    _instance: ClassVar['MarketData'] = None

    _subscribed_symbols: Dict[str, List[str]] = field(init=False,
                                                      default_factory=dict)
    _new_symbols: Dict[str, List[str]] = field(init=False, default_factory=dict)
    _cached_events: Dict[str, Dict[str, Any]] = field(init=False,
                                                      default_factory=dict)
    _stop_streaming: bool = field(init=False, default=False)
    _thread_runs: bool = field(init=False, default=False)

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(MarketData, cls).__new__(cls)
        return cls._instance

    def __post_init__(self):
        if not hasattr(self, '_initialized'):
            self._new_symbols[EventType.GREEKS] = []
            self._new_symbols[EventType.TRADE] = []
            self._new_symbols[EventType.QUOTE] = []
            self._subscribed_symbols[EventType.GREEKS] = []
            self._subscribed_symbols[EventType.TRADE] = []
            self._subscribed_symbols[EventType.QUOTE] = []
            self._cached_events[EventType.GREEKS] = {}
            self._cached_events[EventType.TRADE] = {}
            self._cached_events[EventType.QUOTE] = {}

    def subscribe_greeks(self, symbols: list[str]) -> List[Greeks] | None:
        return self._subscribe_symbol(EventType.GREEKS, symbols)

    def subscribe_trades(self, symbols: list[str]) -> List[Trade] | None:
        return self._subscribe_symbol(EventType.TRADE, symbols)

    def subscribe_quotes(self, symbols: list[str]) -> Quote | None:
        return self._subscribe_symbol(EventType.QUOTE, symbols)

    def get_greeks(self, symbols: list[str]) -> List[Greeks] | None:
        return self._get_events(EventType.GREEKS, symbols)

    def get_trades(self, symbols: list[str]) -> List[Trade] | None:
        return self._get_events(EventType.TRADE, symbols)

    def get_quotes(self, symbols: list[str]) -> Quote | None:
        return self._get_events(EventType.QUOTE, symbols)

    def start_streamer(self) -> None:
        if self._thread_runs:
            return
        self._thread_runs = True
        thread = Thread(target=self._streamer_thread, daemon=True)
        thread.start()

    def stop_streamer(self) -> None:
        self._stop_streaming = True
        time.sleep(5)
        self._thread_runs = False

    def _subscribe_symbol(self, event_type: EventType,
                          symbols: List[str]) -> None:
        new_symbols = list(
            set(symbols) - set(self._subscribed_symbols[event_type]))
        if new_symbols:
            self._new_symbols[event_type] = new_symbols
            self._subscribed_symbols[event_type] = list(
                set(self._subscribed_symbols[event_type]) | set(symbols))
            time.sleep(5)

    def _get_events(self, event_type: EventType, symbols: List[str]) \
            -> List[Greeks] | List[Trade] | List[Quote] | None:  # NOQA
        market_data = list(self._cached_events[event_type].values())
        market_data = [data for data in market_data if
                       data.eventSymbol in symbols]
        return market_data

    async def _fetch_greeks(self) -> None:
        session = ApplicationSession().session  # NOQA
        async with DXLinkStreamer(session, mongodb=True) as streamer:
            while not self._stop_streaming:
                if self._new_symbols[EventType.GREEKS]:
                    await streamer.subscribe(EventType.GREEKS,
                                             symbols=self._new_symbols[
                                                 EventType.GREEKS])
                    self._new_symbols[EventType.GREEKS] = []
                size = streamer._queues[EventType.QUOTE].qsize()
                if 0 < size % 100 < 25:
                    print(size)
                await asyncio.sleep(500)

    async def _fetch_trades(self) -> None:
        session = ApplicationSession().session  # NOQA
        async with DXLinkStreamer(session, mongodb=True) as streamer:
            while not self._stop_streaming:
                if self._new_symbols[EventType.TRADE]:
                    await streamer.subscribe(EventType.TRADE,
                                             symbols=self._new_symbols[
                                                 EventType.TRADE])
                    self._new_symbols[EventType.TRADE] = []
                size = streamer._queues[EventType.QUOTE].qsize()
                if 0 < size % 100 < 25:
                    print(size)
                await asyncio.sleep(500)

    async def _fetch_quotes(self) -> None:
        session = ApplicationSession().session  # NOQA
        async with DXLinkStreamer(session, mongodb=True) as streamer:
            while not self._stop_streaming:
                if self._new_symbols[EventType.QUOTE]:
                    await streamer.subscribe(EventType.QUOTE,
                                             symbols=self._new_symbols[
                                                 EventType.QUOTE])
                    self._new_symbols[EventType.QUOTE] = []
                size = streamer._queues[EventType.QUOTE].qsize()
                if 0 < size % 100 < 100:
                    print(size)
                await asyncio.sleep(500)

    async def _start_streamers(self) -> None:
        await asyncio.gather(self._fetch_quotes(), self._fetch_trades(),
                             self._fetch_greeks())

    def _streamer_thread(self) -> None:
        asyncio.run(self._start_streamers())


if __name__ == '__main__':
    session = ApplicationSession().session  # NOQA
    mes = Future.get_future(session, '/MESU4')
    nq = Future.get_future(session, '/NQU4')
    md = MarketData()
    md.start_streamer()
    md._subscribe_symbol(EventType.GREEKS, [".IWM240705P207"])  # NOQA
    md._subscribe_symbol(EventType.QUOTE,[mes.streamer_symbol, nq.streamer_symbol, "IWM"])  # NOQA
    md._subscribe_symbol(EventType.TRADE,[mes.streamer_symbol, nq.streamer_symbol])  # NOQA

    q = md.get_greeks([".IWM240628P207", ".IWM240705P207"])
    x = md.get_quotes([mes.streamer_symbol, nq.streamer_symbol, "IWM"])
    y = md.get_trades([mes.streamer_symbol, nq.streamer_symbol])
    md._subscribe_symbol(EventType.QUOTE, ['AAPL', 'NVDA', 'TSLA'])  # NOQA
    md.get_quotes(['AAPL', 'NVDA'])

    time.sleep(5)
    md._subscribe_symbol(EventType.QUOTE, ['AAPL', 'NVDA', 'TSLA'])  # NOQA
    # print(md.get_greeks(['AAPL']))
    print(md.get_trades(['AAPL', 'TSLA']))
    time.sleep(5)
    print(md.get_quotes(['IBM', 'AAPL']))
    md.stop_streamer()
