#%%
import requests
stock_list_url = "https://www.set.or.th/api/set/stock/list"

data = requests.get(stock_list_url).json()
stocks = data['securitySymbols']
stocks = [s for s in stocks if s['securityType'] == 'S']
stocks

# %%
symbols = [s['symbol'] for s in stocks]

# %%
import pandas as pd
data_df = pd.DataFrame(stocks)
# data_df = data_df[data_df.securityType == 'S']
data_df

# %%
stock_df = data_df[['symbol', 'nameTH', 'nameEN', 'market', 'industry', 'sector', 'isIFF']]
stock_df

# %%
from pqdm.threads import pqdm

dividend_url = "https://www.set.or.th/api/set/stock/{symbol}/corporate-action/historical?caType=XD&lang=th"
info_url = "https://www.set.or.th/api/set/stock/{symbol}/info?lang=th"
profile_url = "https://www.set.or.th/api/set/factsheet/{symbol}/profile?lang=th"

def get_info(symbol: str) -> dict:
    info = requests.get(info_url.format(symbol=symbol)).json()
    profile = requests.get(profile_url.format(symbol=symbol)).json()
    return {**info, **profile}

stock_infos = pqdm(symbols, get_info, n_jobs=32, argument_type='el')
stock_infos


#%%
def reformat_date(date_str: str) -> str:
    if not date_str:
        return ''
    data = date_str.split('/')
    data = data[::-1]
    while len(data) < 3:
        data.append('01')
    return '-'.join(data)

keys = ['establishedDate', 'last', 'high52Weeks', 'low52Weeks']
filtered_stock_infos = [{k: v for k, v in s.items() if k in keys} for s in stock_infos]
stock_info_df = pd.DataFrame(filtered_stock_infos)
high = stock_info_df.high52Weeks
low = stock_info_df.low52Weeks
stock_info_df['drawdown52Weeks'] = (low-high)/high * 100
stock_info_df['establishedDate'] = stock_info_df.establishedDate.apply(reformat_date)
stock_info_df


#%%
stock_df = pd.concat([stock_df, stock_info_df], axis=1)
stock_df['dividendYield'] = [info['dividendYield'] for info in stock_infos]
stock_df

#%%
def get_dividends(symbol: str) -> list:
    return requests.get(dividend_url.format(symbol=symbol)).json()

stock_dividend_lists = pqdm(symbols, get_dividends, n_jobs=32, argument_type='el')
stock_dividend_lists

#%%
def get_dividend_summary(dividen_list: list) -> dict:
    df = pd.DataFrame(dividen_list)
    if len(df) == 0:
        return {}

    return {
        "numberOfDividends": df.dividend.count(),
        "lastEndOfOperationDate": df.endOperation.iloc[0],
        "lastPaymentDate": df.paymentDate.iloc[0],
        "stdOfDividends": df.dividend.std(),
    }

dividend_df = pd.DataFrame([get_dividend_summary(l) for l in stock_dividend_lists])
dividend_df

# %%
result_df = pd.concat([stock_df, dividend_df], axis=1)
result_df['yield'] = result_df.dividendYield
result_df['yieldOverStd'] = result_df.dividendYield / result_df.stdOfDividends
result_df

# %%
columns_date = ["lastEndOfOperationDate", "lastPaymentDate"]
for c in columns_date:
    result_df[c] = pd.to_datetime(result_df[c]).dt.strftime('%Y-%m-%d')


# %%
factsheet_url = "https://www.set.or.th/th/market/product/stock/quote/{symbol}/factsheet"
factsheet_urls = [factsheet_url.format(symbol=stock['symbol']) for stock in stocks]
result_df['factsheet'] = factsheet_urls
result_df


# %%
csv_exported_uri = './reports/output.csv'
html_exported_uri = './reports/output.html'
result_df.to_csv(csv_exported_uri)
print(f'Exported to {csv_exported_uri}')

# %%
