# -*- coding: utf-8 -*-
"""
Created on Mon Aug 27 18:18:50 2019

@author: Ignacio Colino
"""
import time
import pandas as pd
import requests
import os

"""This module has the function to get the daily price data for different
markets."""


# Get the adjusted Data
def get_adjusted_data(market='ASX', num_stocks=None, tickers=None):
    """This function outputs a csv file with the historical daily price data
    for the corresponding market. The stock codes are picked up from the csv
    files in the folder"""
    try:
        # Parameters for API request
        stock_list = {'ASX': '20180801-asx200.csv',
                      'SNP': '20180922_SP500_list.csv',
                      'CRY': 'digital_currency_list.CSV'}
        symbol_list = pd.read_csv(stock_list[market], header=1)
        names = ['open', 'high', 'low', 'close', 'adjusted_close', 'volume',
                 'dividend_amount', 'split_coefficient', 'symbol', 'date']
        cry_names_drop = ['1b. open (USD)', '2b. high (USD)',
                          '3b. low (USD)', '4b. close (USD)']
        cry_names = ['open', 'high', 'low', 'close', 'volume', 'market_cap',
                     'symbol', 'date']
        data = pd.DataFrame(columns=['Symbol'])
        url = "https://www.alphavantage.co/query?"
        if tickers is not None:
            symbol_list = pd.DataFrame(tickers)
        if num_stocks is not None:
            symbol_list = symbol_list.sample(num_stocks)
        # Loop through stock list and concatenate
        for code in symbol_list.iloc[:, 0]:
            if code not in data['Symbol'].unique():
                # query structure
                para = {"function": "TIME_SERIES_DAILY_ADJUSTED",
                        "apikey": os.getenv('ALPHA_VANTAGE')}
                if market == 'CRY':
                    para['market'] = 'USD'
                    para['function'] = 'DIGITAL_CURRENCY_DAILY'
                    para["symbol"] = code,
                    ts = 'Time Series (Digital Currency Daily)'
                    sym = '2. Digital Currency Code'
                    names = cry_names
                else:
                    ts, sym = 'Time Series (Daily)', '2. Symbol'
                    para["outputsize"] = "full"
                    para["symbol"] = code,
                page = requests.get(url, params=para)
                time.sleep(13)  # 5 requests per minute allowed
                if ts in page.json():
                    data2 = pd.DataFrame.from_dict(page.json()[ts],
                                                   orient='index', dtype=float)
                    data2['Symbol'] = page.json()['Meta Data'][sym]
                    data2.index = pd.to_datetime(data2.index)
                    data2.reset_index(level=0, inplace=True)
                    data2['index'] = data2['index'].apply(lambda x: x.date())
                    data = pd.concat([data, data2], axis=0, ignore_index=True,
                                     sort=True)

        # Print Summary and export to csv
        print(data.Symbol.unique())
        if market == 'CRY':
            data.drop(columns=cry_names_drop, inplace=True)
        data.rename(columns={i: j for i, j in zip(data.columns, names)},
                    inplace=True)
        data['symbol'] = data['symbol'].astype('str')
        data['date'] = pd.to_datetime(data['date'])
        # data.to_csv(market+'_adjusted_data.csv', index=False)
    except Exception as error:
        print(error)
    finally:
        # if the api returns an error still return the fetched tickers
        return data
    return data


def main():
    return get_adjusted_data(market='SNP', tickers=['AAPL'])


if __name__ == "__main__":
    # pass
    main()
