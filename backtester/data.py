#PYTHON
from abc import (
    ABC,
    abstractmethod
)
from datetime import datetime
from os.path import (
    dirname,
    join,
    realpath
)
from collections import defaultdict
#PACKAGES
import pandas

#PROJECT
from events import MarketEvent


class DataMetaclass(ABC):
    @abstractmethod
    def get_latest_data(
        self,
        symbol,
        quantity
    ):
        raise NotImplementedError

    @abstractmethod
    def update_data(self):
        raise NotImplementedError


class HistoricCSVDataHandler(DataMetaclass):
    def __init__(
        self,
        event_queue,
        symbol_list,
    ):
        self.event_queue = event_queue
        self.symbol_list = symbol_list

        self.symbol_data = {}
        self.latest_symbol_data = defaultdict(list)
        self.continue_backtest = True

        self.file_type = 'csv'
        self.folder_name = join(
            dirname(realpath(__file__)),
            self.file_type
        )

        self.initial_symbol_data()

    def initial_symbol_data(self):
        combined_index = None
        for symbol in self.symbol_list:
            #load data into pandas.DataFrame
            #for EACH symbol
            self.symbol_data[symbol] = pandas.io.parsers.read_csv(
                join(
                    self.folder_name,
                    '{file_name}.{file_extension}'.format(
                        file_name=symbol,
                        file_extension=self.file_type
                    )
                ),
                header=0,
                index_col=0,
                names=[
                    'datestamp',
                    'open',
                    'high',
                    'low',
                    'close',
                    'volume',
                    'adj_close'
                ]
            )

            #unionize index
            if combined_index is None:
                combined_index = self.symbol_data[symbol].index
            else:
                combined_index.union(self.symbol_data[symbol].index)

        for symbol in self.symbol_list:
            #iterrows() creates a generator
            self.symbol_data[symbol] = self.symbol_data[symbol].reindex(
                combined_index,
                method='pad'
            ).iterrows()

    def new_data_generator(
        self,
        symbol
    ):
        for row in self.symbol_data[symbol]:
            yield tuple([
                symbol,
                datetime.strptime(
                    row[0],
                    '%m/%d/%y'
                ).date(),
                row[1]['open'],
                row[1]['high'],
                row[1]['low'],
                row[1]['volume'],
                row[1]['adj_close']
            ])

    def get_latest_data(
        self,
        symbol,
        N=1
    ):
        try:
            return self.latest_symbol_data[symbol][-N:]
        except KeyError:
            print('{symbol} is not a valid symbol'.format(symbol=symbol))

    def update_latest_data(self):
        for symbol in self.symbol_list:
            try:
                data = next(self.new_data_generator(symbol))
            except Exception as e:
                self.continue_backtest = False
            else:
                if data is not None and len(data) > 0:
                    self.latest_symbol_data[symbol].append(data)

        self.event_queue.put(MarketEvent())

    def update_data(self):
        return
