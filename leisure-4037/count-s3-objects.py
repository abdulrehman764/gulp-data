import boto3

# Define the role ARN and the S3 bucket
role_arn = 'arn:aws:iam::454139100209:role/junaid-s3-data'
bucket_name = 'gulp-data-vault-decoded'
prefix = '4037/hercules1datablob/'
source_prefix = '4037/hercules1datablob/'
destination_prefix = 'filtered-data-4037-updated/'
# Create an STS client
sts_client = boto3.client('sts')

# Assume the role
assumed_role = sts_client.assume_role(
    RoleArn=role_arn,
    RoleSessionName='AssumeRoleSession'
)

# Extract the temporary credentials
credentials = assumed_role['Credentials']

# Create an S3 client using the temporary credentials
s3_client = boto3.client(
    's3',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken']
)

# Function to count the number of objects in the specified bucket and prefix
def count_objects_in_bucket(s3_client, bucket_name, prefix):
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    object_count = 0
    for page in page_iterator:
        if 'Contents' in page:
            object_count += len(page['Contents'])

    return object_count

# Count the objects
object_count = count_objects_in_bucket(s3_client, bucket_name, prefix)
print(f'Total number of objects in s3://{bucket_name}/{prefix}: {object_count}')


# Function to count the number of empty objects in the specified bucket and prefix
def count_empty_objects_in_bucket(s3_client, bucket_name, prefix):
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    empty_object_count = 0
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                # print("ObjSize: ",obj['Size'] )
                if obj['Size'] == 3:
                    empty_object_count += 1

    return empty_object_count

# Count the empty objects
empty_object_count = count_empty_objects_in_bucket(s3_client, bucket_name, prefix)
print(f'Total number of empty objects in s3://{bucket_name}/{prefix}: {empty_object_count}')




# Function to copy non-empty objects to a new folder
def copy_non_empty_objects(s3_client, bucket_name, source_prefix, destination_prefix):
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=source_prefix)

    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Size'] > 3:
                    source_key = obj['Key']
                    destination_key = destination_prefix + source_key[len(source_prefix):]

                    copy_source = {
                        'Bucket': bucket_name,
                        'Key': source_key
                    }

                    s3_client.copy_object(
                        CopySource=copy_source,
                        Bucket=bucket_name,
                        Key=destination_key
                    )

# Copy the non-empty objects
copy_non_empty_objects(s3_client, bucket_name, source_prefix, destination_prefix)
print(f'Non-empty objects have been copied to s3://{bucket_name}/{destination_prefix}')