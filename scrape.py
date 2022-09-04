#%%
from pandas.core.frame import DataFrame
from tqdm.auto import tqdm
import requests
import string
import pandas as pd
import urllib
import numpy as np
#%%
lookup_url = 'https://www.set.or.th/set/commonslookup.do?language=en&country=US&prefix={prefix}'
factsheet_url = 'https://www.set.or.th/set/factsheet.do?symbol={symbol}&ssoPageId=3&language=en&country=US'
factsheet_url_th = 'https://www.set.or.th/th/market/product/stock/quote/{symbol}/factsheet'
export_filepath = './reports/analysis.csv'
tqdm.pandas()


#%%
def get_stocks_from_prefix(prefix: str) -> DataFrame:
    url = lookup_url.format(prefix=prefix)
    response = requests.get(url)
    dfs = pd.read_html(response.text)
    return dfs[0]


def get_stock_dataframe() -> DataFrame:
    prefixes = ['NUMBER', *string.ascii_uppercase]
    return pd.concat([get_stocks_from_prefix(prefix=p) for p in prefixes])


def get_factsheet(symbol: str) -> list[DataFrame]:
    url = factsheet_url.format(symbol=urllib.parse.quote(symbol))
    response = requests.get(url)
    return pd.read_html(response.text)


def get_dividend_df(factsheet: list[DataFrame]) -> DataFrame:
    df = next(df for df in factsheet if 'Dividend' in str(df.iloc[0, 0]))
    df.columns = df.iloc[1]
    df = df.iloc[2:]
    if str(df.iloc[0, 0]) == 'No Information Found':
        return None
    try:
        df['payment_datetime'] = pd.to_datetime(df['Payment Date'])
        df[['op_start',
            'op_end']] = df['Operation Period'].str.split(' - ', expand=True)
        df[['op_start', 'op_end']] = df[['op_start',
                                         'op_end']].apply(pd.to_datetime)
        diff_months =  df.op_end.dt.month - df.op_start.dt.month
        diff_years = df.op_end.dt.year - df.op_start.dt.year

        df['op_period'] = diff_years * 12 + diff_months + 1

    except Exception:
        return None

    return df[(df['Unit'] == 'Baht')]


def get_price_df(factsheet: list[DataFrame]) -> DataFrame:
    df = next(df for df in factsheet if 'Price' in str(df.iloc[0, 0]))
    df.columns = df.iloc[0]
    return df.iloc[1:]


def get_price(factsheet: DataFrame) -> float:
    df = get_price_df(factsheet)
    return float(df.iloc[0, 0])


def get_latest_dividend(factsheet):
    df = get_dividend_df(factsheet)
    if df is None:
        return []
    return df['Dividend/Share'].astype(float).to_list()


def get_operation_start_date(factsheet):
    df = get_dividend_df(factsheet)
    if df is None:
        return []
    return df['op_start'].to_list()


def get_last_paid_date(factsheet):
    df = get_dividend_df(factsheet)
    if df is None:
        return None
    return df['op_end'].values[0]


def get_operation_period(factsheet):
    df = get_dividend_df(factsheet)
    if df is None or len(df) == 0:
        return 0
    period = df['op_period'].to_list()[0]
    return period


# %%
stock_df = get_stock_dataframe()
stock_df = stock_df.reset_index(drop=True)
stock_df

# %%
total = stock_df.count()['Symbol']
factsheets = {
    row['Symbol']: get_factsheet(row['Symbol'])
    for _, row in tqdm(stock_df.iterrows(), total=total)
}

# %%
stock_df['price'] = stock_df.progress_apply(
    lambda x: get_price(factsheets[x['Symbol']]), axis=1
)
stock_df['dividends'] = stock_df.progress_apply(
    lambda x: get_latest_dividend(factsheets[x['Symbol']]), axis=1
)
stock_df['op_start_date'] = stock_df.progress_apply(
    lambda x: get_operation_start_date(factsheets[x['Symbol']]), axis=1
)
stock_df['op_period'] = stock_df.progress_apply(
    lambda x: get_operation_period(factsheets[x['Symbol']]), axis=1
)

stock_df['avg_dividend'] = stock_df.dividends.apply(np.average)
stock_df['latest_dividend'] = stock_df.dividends.apply(lambda x: (x or [0])[0])
stock_df['std_dividend'] = stock_df.dividends.apply(np.std)
stock_df['avg_dividend_ratio'] = stock_df.avg_dividend * ( 12 / stock_df.op_period) / stock_df.price
stock_df['avg_over_std'] = stock_df.avg_dividend_ratio / stock_df.std_dividend
stock_df['latest_dividend_ratio'] = stock_df.latest_dividend * ( 12 / stock_df.op_period) / stock_df.price
stock_df['latest_dividend_over_std'] = stock_df.latest_dividend_ratio / stock_df.std_dividend
stock_df['payment_count'] = stock_df.op_start_date.apply(len)
stock_df['last_paid'] = stock_df.progress_apply(
    lambda x: get_last_paid_date(factsheets[x['Symbol']]), axis=1
)
stock_df['factsheet_url'] = stock_df.loc[:, 'Symbol'].apply( lambda x: factsheet_url.format(symbol=x))
stock_df['factsheet_url_th'] = stock_df.loc[:, 'Symbol'].apply( lambda x: factsheet_url_th.format(symbol=x))


stock_df.to_csv(export_filepath)
stock_df

# %%
