import json
import boto3
import os
# Create an STS client
sts_client = boto3.client('sts')
role_arn = 'arn:aws:iam::454139100209:role/junaid-s3-data'
# Assume the role
# assumed_role = sts_client.assume_role(
#     RoleArn=role_arn,
#     RoleSessionName='AssumeRoleSession'
# )
# Destination account credentials
destination_access_key_id = 'ASIAXBY2UIXEG2J7IXMK'
destination_secret_access_key = 'JMYc5dLBCBnh+Hly9wLzb6K3Q7XSXrmmSkrwPxIE'
destination_session_token = 'IQoJb3JpZ2luX2VjEAsaCXVzLWVhc3QtMSJIMEYCIQDBEJrRxgnoyCqmF5nozMSqws5s63+PAgwbBU2ojbFndAIhALeLtRNvFJgyRxTJwkaK/69JF7vKXYTKvuBVvZzCJFjFKoQDCKT//////////wEQABoMNDg0ODUwMjg4MDcyIgwUjZtTd62mcMfpi3Eq2AIda0dUGMk/0uqPoH8asTF9iGv/OPN2GPw8KU0oEUfr2J/CSgcFBraVNxSY+z018wgau8Z62MBbucvTxXcmh1i8ZzUhXdIsnJRykvtOsy+WDeup04HuJZD7E7/Mo21urxJqS3ZH5gSnhupQQdAseCTWVACPY/oOSvWl0TlVDqoUj7u4AT5IqQpk79A24+tGygs3PqwoeumuVI9/7FB8QGVOgx1J3DbiINW7z4bpcjs2tZe7d0hB/pByupprjkWb9kE3BaCqoGKUr+Y9d9As0fX1jIC/OtS8NXqC22uUgKlEcdNTXwy5j57AtiGkpF2W6AamXu5Aoj7RXdjupfVbcgezTtwb9C08TaHyiRy2bMSQE/rseKD+DiyLqzegAwZbKbqNw0u83B2OXtzibfQrrUYIvp8PBDkUfyXqv8Ygud8/PHM5ic+4TKOftg3DLtfYbVcJti4j1HleizCnwLCzBjqmAYvoqkk8aaDCYlO9zTHMAZcc+kzDcffnfao4DmkyL3pgDCUJgNvHO+G843gyEDnvTrquruebquXYeE6UlwZZZ/L8dTUztwgmz+F6Lf+JkZYTPWG3jPXiyfCm1FfOpUjqrTLEQnbajrCSuVAdmW1X4KOfh7/1+rRzoKwgC34qAB9p2cZjn1odKRfeihC3z0IWLVGQV5ec7R+72SgOCawuP+ex44GQ+g8='

# # Extract the temporary credentials
# credentials = assumed_role['Credentials']

# # Create an S3 client using the temporary credentials
# s3 = boto3.client(
#     's3',
#     aws_access_key_id=credentials['AccessKeyId'],
#     aws_secret_access_key=credentials['SecretAccessKey'],
#     aws_session_token=credentials['SessionToken']
# )
# # s3 = boto3.client('s3')

# s3 = boto3.client(
#     's3',
#     aws_access_key_id=destination_access_key_id,
#     aws_secret_access_key=destination_secret_access_key,
#     aws_session_token=destination_session_token
# )

session = boto3.Session(profile_name='PowerUserAccess-484850288072')

# Use the session to create a client for S3
s3 = session.client('s3')

def lambda_handler(event, context):
    source_bucket = 'gulp-data-vault-decode'
    source_prefix = '6554/'
    destination_bucket = 'gulp-data-vault-decode'
    destination_prefix = '6554-processed/'
    
    # List objects in the source directory
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
    
    if 'Contents' not in response:
        return {
            'statusCode': 404,
            'body': json.dumps('No files found in source directory')
        }
    
    counter = 1
    for obj in response['Contents']:
        source_key = obj['Key']
        
        # Skip directories
        if source_key.endswith('/'):
            continue
        
        filename = os.path.basename(source_key)
        
        # Check if filename starts with an underscore and remove it
        if filename.startswith('_'):
            filename = filename[1:]
        
        destination_key = f'{destination_prefix}{filename}/{filename}'
        
        # Copy object
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        s3.copy(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
        
        counter += 1
    
    return {
        'statusCode': 200,
        'body': json.dumps('Files copied successfully')
    }


lambda_handler("", "")