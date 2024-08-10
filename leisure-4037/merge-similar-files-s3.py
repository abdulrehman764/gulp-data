# import boto3
# import jsonlines
# import json
# import os
# from io import BytesIO, StringIO

# # AWS profile name
# aws_profile = 'PowerUserAccess-484850288072'

# # Local temporary directory
# local_tmp_directory = 'tmp'

# # Create local temporary directory if it doesn't exist
# if not os.path.exists(local_tmp_directory):
#     os.makedirs(local_tmp_directory)

# # S3 client using specific profile
# session = boto3.Session(profile_name=aws_profile)
# s3_client = session.client('s3')

# # S3 bucket and prefixes
# source_bucket = 'gulp-data-vault-decode'
# source_prefix = 'filtered-data-4037-updated/'
# destination_bucket = 'gulp-data-vault-decode'
# destination_prefix = 'filtered-data-4037-updated-json-merged/'







# # Create local temporary directory if it doesn't exist
# if not os.path.exists(local_tmp_directory):
#     os.makedirs(local_tmp_directory)

# # Function to download a file from S3 to local directory
# def download_file_from_s3(bucket, key, local_path):
#     s3_client.download_file(bucket, key, local_path)

# # Function to upload a file from local directory to S3
# def upload_file_to_s3(local_path, bucket, key):
#     s3_client.upload_file(local_path, bucket, key)



# import re

# # Function to merge JSON objects from files with similar names
# def merge_json_files(source_bucket, source_prefix, destination_bucket, destination_prefix, local_tmp_directory):
#     # Dictionary to store JSON objects for each group
#     merged_data = {}
#     # List objects in the source S3 bucket with the given prefix
#     print("Listing source files from S3...")
#     source_objects = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
#     # Iterate over files in the source directory
#     print("Downloading and merging JSON files...")
#     for obj in source_objects.get('Contents', []):
#         key = obj['Key']
#         if key.endswith(".json") and "_2024" in key:
#             # Extract the prefix before "_2024"
#             file_name = re.sub(r'[^\w\-_\. ]', '_', os.path.basename(key))
#             local_file_path = os.path.join('tmp', file_name)
#             # logging.info(f"Downloading {source_key} to {local_file_path}")
#             # Download the object to the local file
#             # source_s3_client.download_file(source_bucket_name, source_key, local_file_path)
#             prefix = key.split("_2024")[0].split('/')[-1]  # Getting the last part of the key as filename prefix
#             # Local path for downloaded file
#             print("local file path1: ", local_file_path)
#             # local_file_path = os.path.join(local_tmp_directory, os.path.basename(key))
#             # print("local file path2: ", local_file_path)
#             # Download the file from S3 to local directory
#             download_file_from_s3(source_bucket, key, local_file_path)
#             # Read the JSON lines file and extract JSON objects
#             with jsonlines.open(local_file_path) as reader:
#                 for json_obj in reader:
#                     merged_data.setdefault(prefix, []).append(json_obj)
#             # Delete the local file after processing
#             # os.remove(local_file_path)

#     # Write merged data to new files in the local temporary directory and then upload to destination S3 bucket
#     print("Writing merged data to new files and uploading to S3...")
#     for prefix, data in merged_data.items():
#         local_output_path = os.path.join(local_tmp_directory, f"{prefix}.json")
#         with jsonlines.open(local_output_path, mode='w') as writer:
#             writer.write_all(data)
#         # Upload the merged file to S3
#         output_key = f"{destination_prefix}{prefix}.json"
#         upload_file_to_s3(local_output_path, destination_bucket, output_key)
#         # Delete the local output file after uploading
#         os.remove(local_output_path)

#     print("Merging process completed.")

# # Call the function to merge JSON files
# merge_json_files(source_bucket, source_prefix, destination_bucket, destination_prefix, local_tmp_directory)




import boto3
import jsonlines
import json
import os
import re
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


# AWS profile name
aws_profile = 'PowerUserAccess-484850288072'

# Local temporary directory
local_tmp_directory = 'tmp'

# Create local temporary directory if it doesn't exist
if not os.path.exists(local_tmp_directory):
    os.makedirs(local_tmp_directory)

# S3 client using specific profile
session = boto3.Session(profile_name=aws_profile)
# s3_client = session.client('s3')
# AWS S3 bucket and prefixes
source_bucket = 'gulp-data-vault-decode'
source_prefix = 'filtered-data-4037-updated/'
destination_bucket = 'gulp-data-vault-decode'
destination_prefix = 'filtered-data-4037-updated-json-merged/'

# Initialize the S3 client
s3 = session.client('s3')

# Function to merge JSON objects from files with similar names
def merge_json_files(source_bucket, source_prefix, destination_bucket, destination_prefix):
    # Temporary directory to store downloaded files
    local_temp_dir = "tmp"
    if not os.path.exists(local_temp_dir):
        os.makedirs(local_temp_dir)

    # Dictionary to store JSON objects for each group
    merged_data = {}
    
    try:
        # List objects in the source bucket with the specified prefix
        print("Merging JSON files...")
        response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
        for obj in response.get('Contents', []):
            source_key = obj['Key']
            if source_key.endswith(".json") and "_2024" in source_key:
                # Safe filename handling
                file_name = re.sub(r'[^\w\-_\. ]', '_', os.path.basename(source_key))
                local_file_path = os.path.join(local_temp_dir,  file_name)

                # Download the file from S3
                s3.download_file(source_bucket, source_key, local_file_path)
                
                # Extract the prefix before "_2024"
                prefix = file_name.split("_2024")[0]

                # Read the JSON lines file and extract JSON objects
                with jsonlines.open(local_file_path) as reader:
                    for json_obj in reader:
                        merged_data.setdefault(prefix, []).append(json_obj)
        
        # Write merged data to new files in the destination bucket
        print("Writing merged data to new files...")
        for prefix, data in merged_data.items():
            s3_key = f"{destination_prefix}{prefix}.json"
            merged_content = ""
            for json_obj in data:
                merged_content += json.dumps(json_obj) + "\n"
            s3.put_object(Bucket=destination_bucket, Key=s3_key, Body=merged_content)
            print("s3 upload done: ", s3_key)
        
        print("Merging process completed.")
    except NoCredentialsError:
        print("Error: AWS credentials not found.")
    except PartialCredentialsError:
        print("Error: Incomplete AWS credentials provided.")
    except Exception as e:
        print(f"Error: {e}")

# Call the function to merge JSON files
merge_json_files(source_bucket, source_prefix, destination_bucket, destination_prefix)
