import boto3

def list_s3_objects_with_profile(bucket_name, prefix='', profile_name='default'):
    # Create a session using the specified profile
    session = boto3.Session(profile_name=profile_name)
    
    # Create an S3 client using the session
    s3_client = session.client('s3')
    
    # Paginator to handle large number of objects
    paginator = s3_client.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket_name}
    if prefix:
        operation_parameters['Prefix'] = prefix

    page_iterator = paginator.paginate(**operation_parameters)
    paths = []
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                paths.append(obj['Key'])
                # print(obj['Key'])
    with open("my_list.txt", "w") as file:
        for item in paths:
            file.write("%s\n" % item)
# Usage example
bucket_name = 'gulp-data-vault-decode'
prefix = '4116/'
profile_name = 'PowerUserAccess-484850288072'

list_s3_objects_with_profile(bucket_name, prefix, profile_name)
