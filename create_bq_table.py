# -*- coding: utf-8 -*-
"""
Created on Sun Mar 15 15:09:41 2020

@author: Ignacio
"""
from google.cloud.bigquery import Client, Dataset, SchemaField, Table


def create_bq_dataset(dataset_name='price_data'):
    '''Create dataset if not exists'''
    client = Client()
    datasets = [client.project + "." + i.dataset_id
                for i in list(client.list_datasets())]
    if client.project + "." + dataset_name not in datasets:
        dataset = Dataset(dataset_name)
        dataset.location = "US"
        client.create_dataset(dataset)
    else:
        print("Dataset already exists")


def create_bq_table(table_name='CRY', dataset_name='price_data'):
    '''Create table if not exists'''
    client = Client()
    tables = [i.table_id for i in
              client.list_tables(client.project+"."+dataset_name)]
    if table_name not in tables:
        if table_name == 'CRY':
            schema = [
                SchemaField("open", "FLOAT64", mode="NULLABLE"),
                SchemaField("high", "FLOAT64", mode="NULLABLE"),
                SchemaField("low", "FLOAT64", mode="NULLABLE"),
                SchemaField("close", "FLOAT64", mode="NULLABLE"),
                SchemaField("volume", "FLOAT64", mode="NULLABLE"),
                SchemaField("market_cap", "FLOAT64", mode="NULLABLE"),
                SchemaField("symbol", "STRING", mode="NULLABLE"),
                SchemaField("date", "TIMESTAMP", mode="NULLABLE"),
            ]
        else:
            schema = [
                SchemaField("open", "FLOAT64", mode="NULLABLE"),
                SchemaField("high", "FLOAT64", mode="NULLABLE"),
                SchemaField("low", "FLOAT64", mode="NULLABLE"),
                SchemaField("close", "FLOAT64", mode="NULLABLE"),
                SchemaField("adjusted_close", "FLOAT64", mode="NULLABLE"),
                SchemaField("volume", "FLOAT64", mode="NULLABLE"),
                SchemaField("dividend_amount", "FLOAT64", mode="NULLABLE"),
                SchemaField("split_coefficient", "FLOAT64", mode="NULLABLE"),
                SchemaField("symbol", "STRING", mode="NULLABLE"),
                SchemaField("date", "TIMESTAMP", mode="NULLABLE"),
            ]
        table = Table(client.project+"."+dataset_name+"."+table_name,
                      schema=schema)
        table = client.create_table(table)
    else:
        print("Table already exists")
