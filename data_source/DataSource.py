from abc import abstractmethod, ABC
from pandas import DataFrame
from database.db_reader import read_table_header


class DataSource(ABC):

    @staticmethod
    def convert_header(table_name, dataframe):
        columns = read_table_header(table_name)
        dataframe.columns = columns
        return dataframe

    @abstractmethod
    def get_stock_list(self, *args) -> DataFrame:
        return DataFrame()

    @abstractmethod
    def get_indices_daily(self, *args) -> DataFrame:
        return DataFrame()

    @abstractmethod
    def get_indices_weekly(self, *args) -> DataFrame:
        return DataFrame()

    @abstractmethod
    def get_indices_monthly(self, *args) -> DataFrame:
        return DataFrame()

    @abstractmethod
    def get_quotations_daily(self, *args) -> DataFrame:
        return DataFrame()

    @abstractmethod
    def get_quotations_weekly(self, *args) -> DataFrame:
        return DataFrame()

    @abstractmethod
    def get_quotations_monthly(self, *args) -> DataFrame:
        return DataFrame()

    @abstractmethod
    def get_adj_factors(self, *args) -> DataFrame:
        return DataFrame()

