import os
import numpy as np
import pandas as pd
from datetime import datetime ,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_csv_to_postgres():

    csv_path = "/opt/airflow/data/dirty_cafe_sales.csv"
    
    df = pd.read_csv(csv_path)
    
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    
    engine = hook.get_sqlalchemy_engine()
    
    df.to_sql("my_csv_table", engine, if_exists="replace", index=False)
    

def transformations():
    
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    
    engine = hook.get_sqlalchemy_engine()
    
    df = pd.read_sql("SELECT * FROM my_csv_table", engine)

    # cleaning & transformations
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
    df['Price Per Unit'] = pd.to_numeric(df['Price Per Unit'], errors='coerce')
    df['Total Spent'] = pd.to_numeric(df['Total Spent'], errors='coerce')
    df['Transaction Date'] = pd.to_datetime(df['Transaction Date'], errors='coerce')
            
    # standardization
    df['Transaction ID'] = df['Transaction ID'].str.upper().str.strip()
    df['Item'] = df['Item'].str.title().str.strip()
    df['Payment Method'] = df['Payment Method'].str.title().str.strip()
    df['Location'] = df['Location'].str.title().str.strip()

    # replace ['unknown','error'] values by nan
    df['Item'] = df['Item'].replace(['Unknown','Error'], np.nan)
    df['Payment Method'] = df['Payment Method'].replace(['Unknown','Error'], np.nan)
    df['Location'] = df['Location'].replace(['Unknown','Error'], np.nan)

   # fill nan of item depend on price per unit
    price_to_items = {
        3.0: ["Cake", "Juice"],
        2.0: ["Coffee"],
        1.0: ["Cookie"],
        5.0: ["Salad"],
        4.0: ["Sandwich", "Smoothie"],
        1.5: ["Tea"]
    }
    counters = {k: 0 for k in price_to_items}

    def assign_item(row):
        if pd.isna(row["Item"]):
            price = row["Price Per Unit"]
            if price in price_to_items:
                items = price_to_items[price]
                idx = counters[price] % len(items)
                counters[price] += 1
                return items[idx]
        return row["Item"]

    df["Item"] = df.apply(assign_item, axis=1)

    # fill nan of item
    np.random.seed(42)
    prob_item = df['Item'].value_counts(normalize=True)
    nan_count = df['Item'].isna().sum()
    fill_items = np.random.choice(prob_item.index, size=nan_count, p=prob_item.values)
    df.loc[df['Item'].isna(), 'Item'] = fill_items
    
    # filling nan values
    df['Quantity'] = df['Quantity'].fillna(df.groupby('Item')['Quantity'].transform('mean')).round().astype(int)
    df['Price Per Unit'] = df['Price Per Unit'].fillna(df.groupby('Item')['Price Per Unit'].transform(lambda x: x.mode()[0]))
    df['Total Spent'] = df['Quantity'] * df['Price Per Unit']

    # fill nan of payment method
    np.random.seed(42)
    prob_per_item = df.groupby('Item')['Payment Method'].value_counts(normalize=True)
    prob_dict = {item: prob_per_item[item] for item in prob_per_item.index.levels[0]}
    nan_counts = df[df['Payment Method'].isna()]['Item'].value_counts()
    fill_values = []
    for item, count in nan_counts.items():
        dist = prob_dict[item]
        fill_values.extend(np.random.choice(dist.index, size=count, p=dist.values))
    df.loc[df['Payment Method'].isna(), 'Payment Method'] = fill_values

    # fill nan of location
    np.random.seed(42)
    prob_per_item_loc = df.groupby('Item')['Location'].value_counts(normalize=True)
    prob_dict_loc = {item: prob_per_item_loc[item] for item in prob_per_item_loc.index.levels[0]}
    nan_counts_loc = df[df['Location'].isna()]['Item'].value_counts()
    fill_values_loc = []
    for item, count in nan_counts_loc.items():
        dist = prob_dict_loc[item]
        fill_values_loc.extend(np.random.choice(dist.index, size=count, p=dist.values))
    df.loc[df['Location'].isna(), 'Location'] = fill_values_loc

    # cleaning
    df = df.dropna(subset=['Transaction Date'])

    df.to_sql("my_csv_table_clean", engine, if_exists="replace", index=False)
 

def analysis():
    
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    
    engine = hook.get_sqlalchemy_engine()
 
    df = pd.read_sql("SELECT * FROM my_csv_table_clean", engine)

    
    # check dataset
    print(df.head(10))
    print('----------------------')
    print(df.tail(10))
    print('----------------------')
    print(df.info())
    print('----------------------')
    print(df.isna().sum())
    print('----------------------')
    print(df.duplicated())
    print('----------------------')
    
    # analysis
    print("Total revenue for each item")
    print(df.groupby('Item')['Total Spent'].sum().sort_values(ascending=False))
    print('---------------------------------------------------------------------')

    print("Most popular products")
    print(df.groupby('Item')['Quantity'].sum().sort_values(ascending=False))
    print('---------------------------------------------------------------------')

    print("Highest item price")
    print(df.groupby('Item')['Price Per Unit'].unique().sort_values(ascending=False).head(1))
    print('---------------------------------------------------------------------')

    print("Lowest item price")
    print(df.groupby('Item')['Price Per Unit'].unique().sort_values().head(1))
    print('---------------------------------------------------------------------')

    print("Total revenue")
    print(df['Total Spent'].sum())
    print('---------------------------------------------------------------------')
    
    print("Most used payment method")
    print(df['Payment Method'].value_counts().head(1))
    print('---------------------------------------------------------------------')
    
    print("Most used location")
    print(df['Location'].value_counts().head(1))
    print('---------------------------------------------------------------------')

    print("Day with highest revenue")
    print(df.groupby('Transaction Date')['Total Spent'].sum().sort_values(ascending=False).head(1))
    print('---------------------------------------------------------------------')

    print("Number of locations per item")
    print(df.groupby('Item')['Location'].value_counts())
    print('---------------------------------------------------------------------')

with DAG(
dag_id='first_dag',
description='this is first time to use this dag',
start_date=datetime(2025,1,1),
schedule_interval=timedelta(minutes=15),
catchup=False,
dagrun_timeout=timedelta(minutes=30),
tags=['sales']
) as dag:
   
    extraction_task=PythonOperator(
    task_id='extraction_task',
    python_callable=load_csv_to_postgres
) 
  
    transformation_task=PythonOperator(
    task_id='transformation_task',
    python_callable=transformations
       )

    analysis_task = PythonOperator(
    task_id="analysis_task",
    python_callable=analysis
)
 

extraction_task >> transformation_task >> analysis_task