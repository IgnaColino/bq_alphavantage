# -*- coding: utf-8 -*-
"""
Created on Sun Mar 15 15:09:27 2020

@author: Ignacio
"""

from google.cloud.bigquery import Client
import pandas as pd


def load_data_to_bq(df=None, table_name='CRY', dataset='price_data'):
    client = Client()
    table = client.get_table(".".join([client.project, dataset, table_name]))
    if table.num_rows == 0 and df is not None:
        df.to_gbq(".".join([dataset, table_name]), if_exists='append')
        print(f"{len(df)} rows inserted")
    else:
        delete_qry = f'''DELETE FROM `{dataset+"."+table_name}` AS t2
                         WHERE concat(symbol, cast(date as string)) IN
                         (SELECT concat(symbol, cast(MAX(date) as string))
                         FROM `{dataset+"."+table_name}`
                         GROUP BY symbol) AND symbol IN
                         {'("'+'","'.join(df.symbol.unique())+'")'}'''
        delete_DML = client.query(delete_qry)
        delete_DML.result()
        existing = pd.read_gbq(f'''select symbol, max(date) as max_date
                               from {dataset+"."+table_name}
                               group by symbol''', dialect="legacy")
        df = df.merge(existing, on='symbol', how='left')
        df = df.loc[df.date.dt.tz_localize('UTC') > df.max_date, :]
        df.drop('max_date', axis=1, inplace=True)
        df.to_gbq(".".join([dataset, table_name]), if_exists='append')
        print(f"{len(df)} rows inserted")


if __name__ == "__main__":
    data = load_data_to_bq()
