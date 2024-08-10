# import boto3
# import os
# import re

# # Define the source role ARN and the S3 bucket details
# source_role_arn = 'arn:aws:iam::454139100209:role/junaid-s3-data'
# source_bucket_name = 'gulp-data-vault-decoded'
# source_prefix = '4037/hercules1datablob/'
# destination_bucket_name = 'gulp-data-vault-decode'
# destination_prefix = 'filtered-data-4037-updated/'

# # Destination account credentials
# destination_access_key_id = 'ASIAXBY2UIXEKGLTKH2H'
# destination_secret_access_key = 'hPzxNjR5JaiV9XDypYIqWjwMIOZArJ41kIj38Y4S'
# destination_session_token = 'IQoJb3JpZ2luX2VjEBEaCXVzLWVhc3QtMSJGMEQCIDXHz70MAl9W2LcVkpM0f5+8jSMe2N10kyXPN7agiISOAiAnuzO+xJ/5xNjlOnsiXoNdtC/MxR6otgD8NgXlY3JOHyqEAwiq//////////8BEAAaDDQ4NDg1MDI4ODA3MiIMX49wtluECmI7cTAFKtgC1fzIV5mVlis1KF3iCQ7HFUOlb7PvP7gtLsHF5ugR0iVXNd2uUJx1KhBQd8PMfVd/qholILv/5qWT7au9wFBVElCWDc7wB/hlTH/wnhNxDks26irDXc723umy9/CjtD35ckgbUKR0Q5OQzG2xPuHuGHsUh80ArE+uZvhieD3M7eWbPKXr66+yqR2AzgnZEdyOHhSKoEOzcVuBt8JPrJC9QCcCxMyDYTqZmNWUQB/asVbfodbgF+00NLH4j8Y7IOoxAX2sC24wS22SXFc4/u9N+ApE39nEGTh21WXagJXF0x80veJmeIcy8/aMxd0/bsbfDPa+UG4RFhhKqZ7DXz3LXixJC76UW57dGSWYi7jTSycWdvn+fxqqI8yVm3qvkalTd6WMaKzworag7TQofsMPhuP2nMIDNnQptBtBbta8CPQf5TsbUNMCuLNHYOH0dmEmGxDDvQhYzgkw2eOxswY6qAFIg41dZFWx3pbnRuYM9ojKA/t5BRXJ8uHyt1lSAooIy22v0w5anraCRJv429Y56QcK2/8atWfYbmpGLQpxqln0WzDzk4/9fZLONmx2KTpBMpQUetoLZ+YX03EA99oTY9/jYePjOLr0CewruqJGg8Gz3j/stcaU6bqdr4dJhf5qOM11vcmxJNj3y60ZsClizCBbpRJKNHAuSZnmiWorK+i3BMi9un8S85Y='

# # destination_access_key_id = 'ASIAXBY2UIXEJ7NX7OAR'
# # destination_secret_access_key = 'dZ/4uZjcGyCuAfqTC/T7aOnaSbnGCCh+7QvDfF2W'
# # destination_session_token = 'IQoJb3JpZ2luX2VjEJH//////////wEaCXVzLWVhc3QtMSJIMEYCIQCzT1dlcBQV81EU53kP81HrfAElMqWd98hV+uZe27+GowIhAL51x/k6eY9vjNcYk46W1J9DUQ960Y5IDklxc2Il8BtlKvsCCBoQABoMNDg0ODUwMjg4MDcyIgzV08aF5XY5kcndc8Mq2AL9jr1Wb8/JWjnQhHVLyyAXSeOzLpOSiIRfd/2giNIzP8XWvZxwWMkCmvHucsTYmwF9dzcyeCwRICwTUjyrCSa+KWhBXEBEmEjMwd5ClblMn4I0j8CONthKyPMDQ0SidsDJTr4QhaH/9/rU/OOKsJy+oijrPvIeZ20k1chvLW5sFEz6PqTcsfdhWyT5IYoVtTInwS8VrHZfnnvsIIknjcMCHCrH8Eygy44Gq91BbwhNo8KxetHHeZpgIIzAvh7k1B4Ozi+VPgjDgnpcZk5xTnjH8M/k8WwqtRx0IPB5ivIW+0HnkqeqOT5TENEebOEJN7ndD9MGJotyBkhL9kxOcCWb4HcUtXnpoTPLR7+RHTPJSAhphq1/a76yqrEws0s3IHw8QB922KixvwBfJBKZZz0uKxKT4VuK4kMZLP6rgkbyp+mrCdtFO+zL6NN4c2vuwZlcCTA/BOOBajDiu92yBjqmAW8cqThCAYH4NkBn2RRESVxZmPmPlsoheTVUauoi7wUfPThxEPUweagbCVLt6Dn4mZo5PzLfS6KnhtaaMBkvEMr5w2Pm3H29XevJzbGXIKObBIiJw8ag5xiaUv38XPVMPlbYvYMRbzHzmOnl3YSBAG7idGYeyx/0MhcNWB7xmobxiCAFds401WSZ5kY6KE01seP2Towu+9qkRZMfOCexp2gIvCNO1cg='

# # Function to assume an IAM role and return temporary credentials
# def assume_role(role_arn):
#     sts_client = boto3.client('sts')
#     assumed_role = sts_client.assume_role(
#         RoleArn=role_arn,
#         RoleSessionName='AssumeRoleSession'
#     )
#     return assumed_role['Credentials']

# # Assume the role for the source bucket
# source_credentials = assume_role(source_role_arn)

# # Create S3 clients using the temporary credentials for the source and fixed credentials for the destination
# source_s3_client = boto3.client(
#     's3',
#     aws_access_key_id=source_credentials['AccessKeyId'],
#     aws_secret_access_key=source_credentials['SecretAccessKey'],
#     aws_session_token=source_credentials['SessionToken']
# )

# destination_s3_client = boto3.client(
#     's3',
#     aws_access_key_id=destination_access_key_id,
#     aws_secret_access_key=destination_secret_access_key,
#     aws_session_token=destination_session_token
# )

# # Function to copy non-empty objects from the source bucket to the destination bucket
# def download_and_upload_objects(source_s3_client, destination_s3_client, source_bucket_name, source_prefix, destination_bucket_name, destination_prefix):
#     paginator = source_s3_client.get_paginator('list_objects_v2')
#     page_iterator = paginator.paginate(Bucket=source_bucket_name, Prefix=source_prefix)

#     for page in page_iterator:
#         if 'Contents' in page:
#             for obj in page['Contents']:
#                 if obj['Size'] > 3:
#                     source_key = obj['Key']
#                     print("SourceKey: ", source_key)
#                     destination_key = destination_prefix + source_key[len(source_prefix):]

#                     # Create a safe local file path
#                     file_name = re.sub(r'[^\w\-_\. ]', '_', os.path.basename(source_key))
#                     local_file_path = os.path.join(r"D:\timestamped-files", file_name)
                    
#                     print("localFilePath: ", local_file_path)

#                     # Download the object to the local file
#                     source_s3_client.download_file(source_bucket_name, source_key, local_file_path)

#                     # Upload the local file to the destination bucket
#                     destination_s3_client.upload_file(local_file_path, destination_bucket_name, destination_key)
#                     print("File uploaded success: ", destination_key )
#                     # Remove the local file
#                     os.remove(local_file_path)

# # Ensure the temporary directory exists
# os.makedirs(r"D:\timestamped-files", exist_ok=True)

# # Download and upload the non-empty objects
# download_and_upload_objects(
#     source_s3_client, 
#     destination_s3_client, 
#     source_bucket_name, 
#     source_prefix, 
#     destination_bucket_name, 
#     destination_prefix
# )



import boto3
import os
import re
import logging
import shutil

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("s3_sync.log"),
        logging.StreamHandler()
    ]
)

# Define the source role ARN and the S3 bucket details
source_role_arn = 'arn:aws:iam::454139100209:role/junaid-s3-data'
source_bucket_name = 'gulp-data-vault-decoded'
source_prefix = '6554/2024-05-31/'
destination_bucket_name = 'gulp-data-vault-decode'
destination_prefix = '6554/'

# Destination account credentials
destination_access_key_id = 'ASIAXBY2UIXEMACPONES'
destination_secret_access_key = '1Xm/zmY6FMA+5cCl8vwcrHU4AZEnyS7zgAem+r0u'
destination_session_token = 'IQoJb3JpZ2luX2VjEFIaCXVzLWVhc3QtMSJHMEUCIQCxYh410ac/JB7qiFXyYjwd8dgyZ1YMSATthkWn/HwIGwIgeF/lsjsgPrJqd1uV9fAmimLTkxK7iWsJmVaQi0klJvMq/AIIGxAAGgw0ODQ4NTAyODgwNzIiDHK/QF8GOAIcTXudVSrZAuC/QAN/DFdVhGRAIZeZnAYT23hqfYfHFx1x9BKGvdkuuxXVmhahmsYNBQxVVA3ReBOyJJXwoCbSIgZP7SHlSvAOVyARflCPdh4POCSYSG/ELpeBh2Fbzri0bh/OTwx/uvPZ6ExsjDDiKHSGdyL2zwsa/QTSpGOn3v2PBxz75W8C2Ajx3gxgr5y4Tn3eWIRFZ0FzDzDnrmKDiB5J2uThsUKk7UfSSYUy0BsHl6tlTWelpj3h1+PRZ/mumgZ5Bo5xBRZOcTaqaiWjISrRWV4XCUkN3L03l9vtYExBotHVzNNZBiNzGBraKg5zSy2dALETMTpWI8Je2+7T8tk+oKxmHcgCGzmCFiCpfY04BjufquiVtHcKpDnxFchT9B6tUSMXlEjkdrglhmdC7XhxMg4ZXdtinrplu1Dye9AEFnItuiH8yKUcnaJtb6IzJcFdo+yrZO0xq3p7Hm7RdjDqw7C0BjqnAbUxiTw7GHTNFc1V/4LK6RaqNgc9OY9g+/LID9T4si/qD9LREGlmjDILdvfXSCDCAUICEMkYNRk99WYLIR5yQSng41TSMtnR4rdJFaQhmlAlGBtfBnAOnjclkNTorxzrcDenUa/COTu0E7Rzu97wE2Xs1D+tHoS/+9dgnc9JHby5GyTZ1JALdowk7v9HQnRu/SQMUuannP1tWHQCZeJjMI7MIN/hi0/6'

# Function to assume an IAM role and return temporary credentials
def assume_role(role_arn):
    sts_client = boto3.client('sts')
    assumed_role = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName='AssumeRoleSession'
    )
    logging.info(f"Assumed role {role_arn}")
    return assumed_role['Credentials']

# Assume the role for the source bucket
source_credentials = assume_role(source_role_arn)

# Create S3 clients using the temporary credentials for the source and fixed credentials for the destination
source_s3_client = boto3.client(
    's3',
    aws_access_key_id=source_credentials['AccessKeyId'],
    aws_secret_access_key=source_credentials['SecretAccessKey'],
    aws_session_token=source_credentials['SessionToken']
)

destination_s3_client = boto3.client(
    's3',
    aws_access_key_id=destination_access_key_id,
    aws_secret_access_key=destination_secret_access_key,
    aws_session_token=destination_session_token
)

# # Function to check if an object exists in the destination bucket
# def object_exists(s3_client, bucket, key):
#     try:
#         s3_client.head_object(Bucket=bucket, Key=key)
#         logging.info(f"File {key} already exists in bucket {bucket}")
#         return True
#     except:
#         logging.info(f"File {key} does not exist in bucket {bucket}")
#         return False

# # Function to check available disk space
# def check_disk_space(required_space):
#     total, used, free = shutil.disk_usage("/")
#     logging.info(f"Disk space - Total: {total}, Used: {used}, Free: {free}")
#     return free >= required_space

# # Function to copy non-empty objects from the source bucket to the destination bucket
# def download_and_upload_objects(source_s3_client, destination_s3_client, source_bucket_name, source_prefix, destination_bucket_name, destination_prefix):
#     paginator = source_s3_client.get_paginator('list_objects_v2')
#     page_iterator = paginator.paginate(Bucket=source_bucket_name, Prefix=source_prefix)
#     iterator  = 0
#     for page in page_iterator:
#         if 'Contents' in page:
#             for obj in page['Contents']:
#                 if obj['Size'] > 3:
#                     source_key = obj['Key']
#                     destination_key = destination_prefix + source_key[len(source_prefix):]

#                     # if not object_exists(destination_s3_client, destination_bucket_name, destination_key):
#                         # Check if there is enough disk space for the download
#                     if check_disk_space(obj['Size']):
#                         # Create a safe local file path
#                         print("soruce key: ", source_key)
#                         file_name = re.sub(r'[^\w\-_\. ]', '_', os.path.basename(source_key))
#                         local_file_path = os.path.join(r"D:\timestamped-files", file_name)
                        
#                         logging.info(f"Downloading {source_key} to {local_file_path}")

#                         # Download the object to the local file
#                         source_s3_client.download_file(source_bucket_name, source_key, local_file_path)

#                         # Upload the local file to the destination bucket
#                         # destination_s3_client.upload_file(local_file_path, destination_bucket_name, destination_key)
#                         logging.info(f"Uploaded {local_file_path} to {destination_key}")

#                         # Remove the local file
#                         # os.remove(local_file_path)
#                         print(f"Files done {iterator}")
#                         iterator+=1
#                     else:
#                             logging.error(f"Not enough disk space to download {source_key}")
#                     # else:
#                     #     logging.info(f"Skipping upload for {destination_key} as it already exists")
#                     #     file_name = re.sub(r'[^\w\-_\. ]', '_', os.path.basename(source_key))
#                     #     local_file_path = os.path.join(r"D:\timestamped-files", file_name)
#                     #     source_s3_client.download_file(source_bucket_name, source_key, local_file_path)


# # Ensure the temporary directory exists
# local_temp_dir = r"D:\timestamped-files"
# os.makedirs(r"D:\timestamped-files", exist_ok=True)
# logging.info("Temporary directory created")

# # Download and upload the non-empty objects
# download_and_upload_objects(
#     source_s3_client, 
#     destination_s3_client, 
#     source_bucket_name, 
#     source_prefix, 
#     destination_bucket_name, 
#     destination_prefix
# )


# print("Done Uploading")


# Function to check if an object exists in the destination bucket
def object_exists(s3_client, bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        logging.info(f"File {key} already exists in bucket {bucket}")
        return True
    except:
        logging.info(f"File {key} does not exist in bucket {bucket}")
        return False

# Function to check available disk space
def check_disk_space(required_space):
    total, used, free = shutil.disk_usage("/")
    logging.info(f"Disk space - Total: {total}, Used: {used}, Free: {free}")
    return free >= required_space

# Function to check if a filename has a timestamp at the end
def has_timestamp(filename):
    pattern = r'.*\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\.json$'
    return re.match(pattern, filename) is not None

# Function to copy non-empty objects from the source bucket to the destination bucket
def download_and_upload_objects(source_s3_client, destination_s3_client, source_bucket_name, source_prefix, destination_bucket_name, destination_prefix):
    paginator = source_s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=source_bucket_name, Prefix=source_prefix)
    iterator  = 0
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Size'] > 3:
                    source_key = obj['Key']
                    destination_key = destination_prefix + source_key[len(source_prefix):]

                    # Skip files with a timestamp at the end
                    # if not has_timestamp(os.path.basename(source_key)):
                    #     logging.info(f"Skipping file with timestamp: {source_key}")
                    #     continue

                    # if not object_exists(destination_s3_client, destination_bucket_name, destination_key):
                        # Check if there is enough disk space for the download
                    if check_disk_space(obj['Size']):
                        # Create a safe local file path
                        print("soruce key: ", source_key)
                        file_name = re.sub(r'[^\w\-_\. ]', '_', os.path.basename(source_key))
                        local_file_path = os.path.join(r"D:\timestamped-files", file_name)
                        
                        logging.info(f"Downloading {source_key} to {local_file_path}")

                        # Download the object to the local file
                        source_s3_client.download_file(source_bucket_name, source_key, local_file_path)

                        # Upload the local file to the destination bucket
                        destination_s3_client.upload_file(local_file_path, destination_bucket_name, destination_key)
                        logging.info(f"Uploaded {local_file_path} to {destination_key}")

                        # Remove the local file
                        os.remove(local_file_path)
                        print(f"Files done {iterator}")
                        iterator+=1
                    else:
                        logging.error(f"Not enough disk space to download {source_key}")
                    # else:
                    #     logging.info(f"Skipping upload for {destination_key} as it already exists")
                    #     file_name = re.sub(r'[^\w\-_\. ]', '_', os.path.basename(source_key))
                    #     local_file_path = os.path.join(r"D:\timestamped-files", file_name)
                    #     source_s3_client.download_file(source_bucket_name, source_key, local_file_path)


# Ensure the temporary directory exists
local_temp_dir = r"D:\timestamped-files"
os.makedirs(local_temp_dir, exist_ok=True)
logging.info("Temporary directory created")

# Download and upload the non-empty objects
download_and_upload_objects(
    source_s3_client, 
    destination_s3_client, 
    source_bucket_name, 
    source_prefix, 
    destination_bucket_name, 
    destination_prefix
)

print("Done Uploading")