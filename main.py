import os
from pathlib import Path
import time
import pandas as pd
from prefect import flow
import psycopg2
from sqlalchemy import create_engine
from prefect_sqlalchemy import SqlAlchemyConnector



@flow(log_prints=True)
def process_data() -> pd.DataFrame:
    # Get the file path
    excel_file = os.path.join('SpaceNK_2.0.xlsx')
    # Read the excel file
    df = pd.read_excel(excel_file, sheet_name='Last Week Report by Store', header= None, na_values=['FOREO'])
    # Drop all empty rows and columns
    df = df.dropna(how='all')
    df = df.dropna(axis=1, how='all')
    # Reset index to reflext current dataframe
    df = df.reset_index(drop=True)
    # Save the first row, remove it from the data frame and use it as a new header
    header = df.iloc[0]
    df = df[1:]
    df.columns = header
    df.drop(df.tail(1).index, inplace=True)
    # Return the processed data frame
    return df

@flow(log_prints=True)
def upload_data(data: pd.DataFrame) -> None:
    # Load the block of data with the database URL information
    database_block = SqlAlchemyConnector.load("foreo-database")
    # Create the engine from the database block
    engine = database_block.get_client(client_type="engine")
    # Transcribe data to postgres database
    df.to_sql('store_sales', engine, if_exists='replace', index=False, method='multi', chunksize=1000)


if __name__ == "__main__":
    df: pd.DataFrame = process_data()
    upload_data(df)