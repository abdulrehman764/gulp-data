import boto3

def list_s3_objects(bucket_name, prefix, profile_name):
    session = boto3.Session(profile_name=profile_name)
    s3_client = session.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    object_keys = []
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                object_keys.append(obj['Key'])

    return object_keys

# Example usage
bucket_name = 'gulp-data-6486-processed'
prefix = ''
profile_name = 'PowerUserAccess-484850288072'

all_keys = list_s3_objects(bucket_name, prefix, profile_name)
for key in all_keys:
    print(key)

output_file = 's3_keys.txt'
with open(output_file, 'w') as file:
    for key in all_keys:
        file.write('@my_s3_stage_4/' + key + '\n')

print(f"Object keys have been written to {output_file}")
