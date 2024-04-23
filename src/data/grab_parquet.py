from minio import Minio
import urllib.request
import pandas as pd
import sys

def main():
    grab_data()
    

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """

    # Define the URL of the data source
    url = "http://example.com/data.parquet"

    # Define the path to save the file
    save_path = "../../data/raw/data.parquet"

    # Use urllib.request.urlretrieve to download the data file
    urllib.request.urlretrieve(url, save_path)

    print(f"Data downloaded and saved to {save_path}")


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
    bucket: str = "NOM_DU_BUCKET_ICI"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")

if __name__ == '__main__':
    sys.exit(main())
