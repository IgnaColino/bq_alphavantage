# -*- coding: utf-8 -*-
"""
Created on Sun Mar 15 17:21:33 2020

@author: Ignacio
"""

from google.cloud.bigquery import Client
from data_to_bq import load_data_to_bq
from data_fetch import get_adjusted_data
from create_bq_table import create_bq_dataset, create_bq_table
import pandas as pd
import time


kraken_tickers = ['BTC', 'ETC']


class data_updater:

    def __init__(self, table='SNP', num_stocks=None,
                 tickers=None, dataset='price_data', project=None):
        self.num_stocks = num_stocks
        self.tickers = tickers
        self.dataset = dataset
        self.project = project
        self.table = table
        self.max_date = None

    def validate_dataset(self):
        client = Client(project='investing-management')
        datasets = [i.dataset_id for i in list(client.list_datasets())]
        if self.dataset not in datasets:
            create_bq_dataset(dataset_name=self.dataset)

    def create_tables(self):
        client = Client(project='investing-management')
        tables = [i.table_id for i in
                  client.list_tables(".".join([client.project, self.dataset]))]
        if self.table not in tables:
            create_bq_table(table_name=self.table, dataset_name=self.dataset)

    def eval_tickers(self):
        ticker_list = {'ASX': '20180801-asx200.csv',
                       'SNP': '20180922_SP500_list.csv',
                       'CRY': kraken_tickers}
        max_dates = \
            pd.read_gbq(f'''select symbol, max(date) as max_date
                        from {self.dataset+"."+self.table}
                        group by symbol''', dialect="legacy")
        if self.table != 'CRY':
            ttl_tickers = pd.read_csv(ticker_list[self.table]).symbol
        else:
            ttl_tickers = pd.DataFrame(ticker_list[self.table],
                                       columns=['symbol'])
        ttl_tickers = pd.merge(ttl_tickers, max_dates, on='symbol',
                               suffixes=('', '_y'), how='left')
        self.max_date = ttl_tickers.max_date.max()
        if not ttl_tickers.loc[ttl_tickers.max_date < self.max_date,
                               :].empty:
            condition = (ttl_tickers.max_date < self.max_date) |\
                (ttl_tickers.max_date.isnull())
            ttl_tickers = ttl_tickers.loc[condition, :]
            ttl_tickers.sort_values('max_date', ascending=False,
                                    inplace=True)
        else:
            ttl_tickers.sort_values('max_date', ascending=False,
                                    na_position='first', inplace=True)
        self.tickers = ttl_tickers.symbol.to_list()[:self.num_stocks]

    def update_market_table(self):
        data = get_adjusted_data(market=self.table, num_stocks=self.num_stocks,
                                 tickers=self.tickers)
        load_data_to_bq(df=data, table_name=self.table, project=self.project)

    def update_bq_db(self):
        self.validate_dataset()
        self.create_tables()
        self.eval_tickers()
        self.update_market_table()


def main():
    start = time.time()
    updater = data_updater(table='SNP', num_stocks=1, tickers=None,
                           project='investing-management')
    updater.update_bq_db()
    print(f'Function runtime: {time.time()-start}s')


if __name__ == '__main__':
    main()
