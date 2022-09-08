#%%
import string
import urllib

import altair as alt
import numpy as np
import pandas as pd
import requests
from pandas.core.frame import DataFrame
from tqdm.auto import tqdm

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
        diff_months = df.op_end.dt.month - df.op_start.dt.month
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


def get_price_range_52w(factsheet: DataFrame) -> float:
    df = get_price_df(factsheet)
    string = df.iloc[0, 1].replace('\xa0', u' ')
    try:
        high, low = [float(x) for x in string.split(' / ')]
        return (high - low) / high
    except:
        return None


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
stock_df['price_range_52w'] = stock_df.progress_apply(
    lambda x: get_price_range_52w(factsheets[x['Symbol']]), axis=1
)
stock_df['std_dividend'] = stock_df.dividends.apply(np.std)
stock_df['avg_dividend_ratio'
        ] = stock_df.avg_dividend * (12 / stock_df.op_period) / stock_df.price
stock_df['avg_over_std'] = stock_df.avg_dividend_ratio / stock_df.std_dividend
stock_df['latest_dividend_ratio'] = stock_df.latest_dividend * (
    12 / stock_df.op_period
) / stock_df.price
stock_df['latest_dividend_over_std'
        ] = stock_df.latest_dividend_ratio / stock_df.std_dividend
stock_df['payment_count'] = stock_df.op_start_date.apply(len)
stock_df['last_paid'] = stock_df.progress_apply(
    lambda x: get_last_paid_date(factsheets[x['Symbol']]), axis=1
)
stock_df['factsheet_url'] = stock_df.loc[:, 'Symbol'].apply(
    lambda x: factsheet_url.format(symbol=x)
)
stock_df['factsheet_url_th'] = stock_df.loc[:, 'Symbol'].apply(
    lambda x: factsheet_url_th.format(symbol=x)
)

stock_df.to_csv(export_filepath)
stock_df

# %%
print(f"Successfully scraped. The output file is at {export_filepath}")

# %%
stock_df['type'] = 'company'
stock_df.loc[stock_df['Company/Security Name'].str.contains('TRUST'),
             'type'] = 'trust'
stock_df.loc[stock_df['Company/Security Name'].str.contains('FUND'),
             'type'] = 'fund'

filtered_columns = [
    'Symbol', 'Company/Security Name', 'type', 'price', 'avg_dividend',
    'latest_dividend', 'price_range_52w', 'std_dividend', 'avg_dividend_ratio',
    'avg_over_std', 'latest_dividend_ratio', 'latest_dividend_over_std',
    'payment_count', 'last_paid', 'factsheet_url', 'factsheet_url_th'
]
df = stock_df[filtered_columns]

# %% explore
selection = alt.selection_multi(fields=['type'])
color = alt.condition(
    selection, alt.Color('type:N', legend=None), alt.value('lightgray')
)
tooltip = [
    alt.Tooltip('Symbol'),
    alt.Tooltip('Company/Security Name', title='Name'),
    alt.Tooltip('avg_dividend_ratio', title='Average Dividend', format='.2%'),
    alt.Tooltip('std_dividend', title='STD', format='.4f'),
    alt.Tooltip('avg_over_std', title='Dividend/STD', format='.2f'),
    alt.Tooltip('last_paid', title='Last Paid'),
    alt.Tooltip('payment_count', title='Payment Count'),
]
scatter = alt.Chart(df).mark_point().encode(
    alt.X(alt.repeat('row'), type='quantitative'),
    alt.Y(alt.repeat('column'), type='quantitative'),
    color=color,
    tooltip=tooltip
).repeat(
    row=['payment_count', 'price_range_52w'],
    column=['avg_dividend_ratio', 'avg_over_std']
).add_selection(selection).transform_filter(
    {
        'or':
            [
                # alt.FieldLTPredicate(field='avg_dividend_ratio', lt=.40),
                alt.FieldLTPredicate(field='avg_over_std', lt=100),
            ]
    }
).interactive()

legend = alt.Chart(df).mark_point().encode(
    y=alt.Y('type:N', axis=alt.Axis(orient='right')), color=color
).add_selection(selection)

scatter | legend

# %%
alt.Chart(df).mark_circle().encode(
    x=alt.X('price_range_52w', type='quantitative', axis=alt.Axis(format='%')),
    y='avg_dividend_ratio',
    color=alt.Color(
        field='last_paid',
        scale=alt.Scale(scheme='greenblue'),
        type='quantitative',
    ),
    # size='last_paid',
    tooltip=tooltip,
).transform_filter(
    alt.FieldOneOfPredicate(field='type', oneOf=['fund', 'trust'])
).interactive()

# %% violin plot
y_field = 'avg_over_std'
alt.Chart(df).transform_density(
    y_field,
    as_=[y_field, 'density'],
    groupby=['type'],
).mark_area(orient='horizontal').encode(
    x=alt.X(
        'density:Q',
        stack='center',
        impute=None,
        title=None,
        axis=alt.Axis(labels=False, values=[0], grid=False, ticks=True),
    ),
    y=y_field,
    column='type',
    color='type'
).configure_view(stroke=None).configure_facet(spacing=0).properties(width=100)
