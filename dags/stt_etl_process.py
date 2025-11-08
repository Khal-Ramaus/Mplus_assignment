from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
import os

def etl_process(**context):
    input_dir='/data'
    output_dir='/output'
    files=sorted([f for f in os.listdir(input_dir)])

    dfs=[]

    for file in files:
        path=os.path.join(input_dir,file)
        df=pd.read_csv(path)
        dfs.append(df)
        
        print(f"Extracted from {file}, number of rows: {len(df)}")

    df_combined=pd.concat(dfs, ignore_index=True)
    print(df_combined.head())
    print(f"NUMBER OF ROWS -------> {len(df_combined)}")
    print("Check client_code == 'MS10001' after combine")
    print(df_combined[df_combined['client_code'] == 'MS10001'])

    df_clean=df_combined.drop_duplicates(subset=['number'], keep='last')
    print(df_clean.head())
    print(f"NUMBER OF ROWS -------> {len(df_clean)}")
    print("Check client_code == 'MS10001' agter drop duplicates")
    print(df_clean[df_clean['client_code'] == 'MS10001'])

    df_clean=df_clean.dropna()
    print(df_clean.head())
    print(f"NUMBER OF ROWS -------> {len(df_clean)}")
    print("Check client_code == 'MS10001' after dropna")
    print(df_clean[df_clean['client_code'] == 'MS10001'])

    df_group=df_clean.groupby(['date','client_code','client_type']).agg(
        stt_count=('number','count'),
        total_amount=('amount','sum')
    ).reset_index()
    print(df_group.head())
    print(f"NUMBER OF ROWS -------> {len(df_group)}")
    print("Check client_code == 'MS10001' After groupby")
    print(df_group[df_group['client_code'] == 'MS10001'])

    df_group['debit']=df_group.apply(
        lambda x: x['total_amount'] if x['client_type']=='C' else None,
        axis=1
    )
    df_group['credit']=df_group.apply(
        lambda x: x['total_amount'] if x['client_type']=='V' else None,
        axis=1
    )
    print('Columns name -----> ',df_group.columns)
    df_final=df_group.drop(['client_type','total_amount'], axis=1)
    print(df_final.head())
    print(f"NUMBER OF ROWS -------> {len(df_group)}")
    print("Check client_code == 'MS10001' after drop columns")
    print(df_group[df_group['client_code'] == 'MS10001'])

    parquet_path=os.path.join(output_dir, 'df_final.parquet')
    csv_path=os.path.join(output_dir, 'df_final.csv')
    excel_path = os.path.join(output_dir, 'df_final.xlsx')

    df_final.to_parquet(parquet_path, index=False)
    df_final.to_csv(csv_path, index=False)
    df_final.to_excel(excel_path, index=False)
    print(f'Data Saved to:\n -Parquet -> {parquet_path}\n -CSV -> {csv_path}\n -XLSX -> {excel_path}')


with DAG(
    dag_id="stt__etl_process",
    description="STT Aggregation Process: extract files → transform (combined->count and sum) → load to storage",
    start_date=datetime(2025, 11, 8),
    schedule='0 7 * * *',
    catchup=False,
    tags=['STT','ETL']
) as dag:

    etl_task=PythonOperator(
        task_id="stt__etl_process",
        python_callable=etl_process,
    )
    etl_process