import json
import boto3
import csv
from io import StringIO
from datetime import datetime

def process_s3_objects():
    # Initialize the S3 client using the specified profile
    session = boto3.Session(profile_name='PowerUserAccess-484850288072')
    s3 = session.client('s3')
    
    # Specify the bucket name and prefix
    bucket_name = 'gulp-data-vault-decode'
    prefix = '4134/'
    
    # Initialize the dictionary to hold filename and timestamps with counts
    filename_timestamp_count = {}
    
    # Use paginator to handle large list of objects
    paginator = s3.get_paginator('list_objects_v2')
    
    # Paginate through the objects in the bucket with the specified prefix
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                filename = key.split('/')[-1]
                
                # Split the filename by hyphen and process
                parts = filename.split('-')
                if len(parts) >= 6:
                    actual_filename = parts[0]
                    year = parts[1]
                    month = parts[2]
                    day = parts[3].split('_H')[0]
                    
                    # Create a proper timestamp
                    timestamp_str = f"{year}-{month}-{day}"
                    try:
                        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d")
                    except ValueError as e:
                        print(f"Error parsing date: {e}")
                        print(parts)
                        continue
                    
                    # Increment count for the filename
                    filename_key = f"{actual_filename}_{year}_{month}_{day}"
                    if filename_key in filename_timestamp_count:
                        filename_timestamp_count[filename_key] += 1
                    else:
                        filename_timestamp_count[filename_key] = 1
                    
                    # Construct S3 file path
                    s3_path = f"4134-processed-final/{actual_filename}/year={year}/month={month}/day={day}/file={actual_filename}_FILE_{filename_timestamp_count[filename_key]}/{actual_filename}_FILE_{filename_timestamp_count[filename_key]}"
                    print(s3_path)
                    # Upload the file to S3
                    s3.copy_object(
                        Bucket=bucket_name,
                        CopySource={'Bucket': bucket_name, 'Key': key},
                        Key=s3_path
                    )
    
    print("Objects processed and copied to new paths in S3.")

if __name__ == "__main__":
    process_s3_objects()
