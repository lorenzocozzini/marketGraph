from ticker import Ticker
from tickersClass import Tickers
from multi import download

__author__ = "DonzelliPalomba"


def pdr_override():
    """
    make pandas datareader optional
    otherwise can be called via fix_yahoo_finance.download(...)
    """
    try:
        import pandas_datareader
        pandas_datareader.data.get_data_yahoo = download
        pandas_datareader.data.get_data_yahoo_actions = download
        pandas_datareader.data.DataReader = download
    except Exception:
        pass


__all__ = ['download', 'Ticker', 'Tickers', 'pdr_override']