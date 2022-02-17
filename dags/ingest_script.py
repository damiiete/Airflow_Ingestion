from datetime import time
import pandas as pd
from sqlalchemy import create_engine
import os


def ingest_callable(user, password, host, port, db, table_name, csv_name, execution_date):
    print(table_name, csv_name, execution_date)


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    engine = engine.connect()

    print('connection established, inserting data...')
    t_start =time()
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    
    df.to_sql(name=table_name, con=engine, if_exists='append')

    t_end = time()
    print(f'inserted fist chunk of data, took {t_end - t_start} seconds')

    while True:
        t_start =time()
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print(f'inserted another chunk of data, took {t_end - t_start} seconds')




