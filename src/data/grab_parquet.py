from minio import Minio
import urllib.request
import pandas as pd
import sys
import os

def main():
    grab_data()
    write_data_minio()
    

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """

    # Define the URL of the data source (December 2023)
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-12.parquet"

    # Define the path to save the file
    save_path = "data/raw/yellow_tripdata_2023-12.parquet"  # Descriptive filename

    # Use urllib.request.urlretrieve to download the data file
    urllib.request.urlretrieve(url, save_path)

    print(f"Data downloaded from {url} and saved to {save_path}")

    # Use urllib.request.urlretrieve to download the data file
    urllib.request.urlretrieve(url, save_path)

    print(f"Data downloaded and saved to {save_path}")


def write_data_minio():
    """
    This method put all Parquet files into Minio
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "bucket-yellow-taxi"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print(f"Bucket {bucket} already exists")

    # Define the path where the files are saved
    save_dir = "data/raw"

    # Get a list of all files in the directory
    files = os.listdir(save_dir)

    # Filter the list to only include .parquet files
    parquet_files = [f for f in files if f.endswith('.parquet')]

    # Upload each file to the Minio bucket
    for file in parquet_files:
        file_path = os.path.join(save_dir, file)
        client.fput_object(bucket, file, file_path)
        print(f"File {file} uploaded to {bucket}")

if __name__ == '__main__':
    sys.exit(main())
