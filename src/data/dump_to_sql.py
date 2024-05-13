import gc
import os
import sys

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database


def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    """
    Dumps a Dataframe to the DBMS engine

    Parameters:
        - dataframe (pd.DataFrame) : The dataframe to dump into the DBMS engine

    Returns:
        - bool : True if the connection to the DBMS and the dump to the DBMS is successful, False if either
        execution is failed
    """
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_warehouse",
        "dbms_table": "nyc_raw",
        "dbms_database2": "nyc_datamart",
        "sql_script_path": "data\datamart\creation.sql"
    }

    # First database URL
    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    
    # Second database URL
    db_config["database_url2"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database2']}"
    )

    # Create first database if it doesn't exist
    if not database_exists(db_config["database_url"]):
        create_database(db_config["database_url"])

    # Create second database if it doesn't exist
    if not database_exists(db_config["database_url2"]):
        create_database(db_config["database_url2"])

    try:
        # Connect to the first database
        engine = create_engine(db_config["database_url"])
        with engine.connect():
            success: bool = True
            print("Connection to the first database successful! Processing DataFrame")
            # Write DataFrame to the first database
            dataframe.to_sql(db_config["dbms_table"], engine, index=False, if_exists='append')

        # Connect to the second database
        engine2 = create_engine(db_config["database_url2"])
        with engine2.connect() as conn2:
            # Execute SQL script in the second database
            with open(db_config["sql_script_path"], "r") as sql_file:
                sql_script = sql_file.read()
                conn2.execute(sql_script)

    except Exception as e:
        success: bool = False
        print(f"Error connection to the database: {e}")
        return success

    return success


def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Take a Dataframe and rewrite it columns into a lowercase format.
    Parameters:
        - dataframe (pd.DataFrame) : The dataframe columns to change

    Returns:
        - pd.Dataframe : The changed Dataframe into lowercase format
    """
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe


def main() -> None:
    # folder_path: str = r'..\..\data\raw'
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the relative path to the folder
    
    print(f'script dir: {script_dir}')
    
    folder_path = os.path.join(script_dir, '..', '..', 'data', 'raw')

    print(f'folder path: {folder_path}')
    
    parquet_files = [f for f in os.listdir(folder_path) if
                     f.lower().endswith('.parquet') and os.path.isfile(os.path.join(folder_path, f))]

    for parquet_file in parquet_files:
        print(f'parquet file: {parquet_file}')
        parquet_df: pd.DataFrame = pd.read_parquet(os.path.join(folder_path, parquet_file), engine='pyarrow')

        clean_column_name(parquet_df)
        if not write_data_postgres(parquet_df):
            del parquet_df
            gc.collect()
            return

        del parquet_df
        gc.collect()


if __name__ == '__main__':
    sys.exit(main())
