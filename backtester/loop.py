#PYTHON
from queue import Queue
import time

#PROJECT
from events import (
    MarketEvent,
    SignalEvent,
    OrderEvent,
    FillEvent
)
from data import HistoricCSVDataHandler
from strategy import BuyAndHoldStrategy
from portfolio import BacktestPortfolio
from broker import BacktestBroker

#MODULE
event_queue = Queue()
data = HistoricCSVDataHandler(event_queue, ["AAPL", "BRK-B", "CVX", "KO"])
strategy = BuyAndHoldStrategy(data, event_queue)
portfolio = BacktestPortfolio(event_queue, data, "2015-01-01", 10000)
broker = BacktestBroker(event_queue)

while True:
    if data.continue_backtest is True:
        data.update_latest_data()
    else:
        break

    while True:
        try:
            event = event_queue.get(block=False)
        except Queue.Empty:
            break

        if event is not None:
            if isinstance(event, MarketEvent):
                strategy.calculate_signals(event)
                portfolio.update_timeindex(event)
            elif isinstance(event, SignalEvent):
                portfolio.update_signal(event)
            elif isinstance(event, OrderEvent):
                broker.execute_order(event)
            elif isinstance(event, FillEvent):
                portfolio.update_fill(event)

    time.sleep(10*60)
