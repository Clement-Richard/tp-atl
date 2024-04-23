from minio import Minio
import requests
import pandas as pd
import sys
import os

def main():
    grab_data()
    
def grab_data() -> None:
    urls = ["https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet", "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-12.parquet"]

    for i, url in enumerate(urls):
        save_path = f"../../data/raw/data{i+1}.parquet"

        # Create the directory if it does not exist
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        # Use requests to download the data file
        response = requests.get(url)

        print(response)
        
        # Write the content to a file
        with open(save_path, 'wb') as file:
            file.write(response.content)

        print(f"Data downloaded from {url} and saved to {save_path}")

def write_data_minio():
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "BucketYellowTaxi"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")

if __name__ == '__main__':
    sys.exit(main())
