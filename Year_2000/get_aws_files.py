import boto3
import os
from pathlib import Path
from dotenv import load_dotenv

def download_s3_bucket(bucket_name: str, download_path:str) -> None:
    """
    Download all files from an S3 bucket to a local directory.

    :param bucket_name: Name of the S3 bucket
    :param download_path: Local directory where files will be saved
    """
    load_dotenv()
    
    s3 = boto3.resource('s3', \
                        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'), \
                        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
    bucket = s3.Bucket(bucket_name)

    # Ensure the download directory exists
    download_path = Path(download_path)
    download_path.mkdir(parents=True, exist_ok=True)

    for obj in bucket.objects.all():
        if obj.key == "/":
            continue
        target = download_path / obj.key
        target.parent.mkdir(parents=True, exist_ok=True)  # Ensure target directory exists

        print(f"Downloading {obj.key} to {target}")
        bucket.download_file(obj.key, str(target))
        # print(obj.key)

if __name__ == "__main__":
    bucket_name = 'redistricting-data-2024'  # Replace with your bucket name
    # bucket_name = 'pokemon-emr-ex-msd'  # Replace with your bucket name
    download_path = './downloaded-files'  # Replace with your desired download directory

    download_s3_bucket(bucket_name, download_path)
