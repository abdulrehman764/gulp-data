# # import boto3
# # from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# # def copy_s3_objects(src_bucket, dest_bucket):
# #     profile_name = 'PowerUserAccess-484850288072'
# #     session = boto3.Session(profile_name=profile_name)
    
# #     # Create an S3 client using the session
# #     s3 = session.client('s3')
# #     # s3 = boto3.client('s3')
    
# #     try:
# #         # List all objects in the source bucket
# #         response = s3.list_objects_v2(Bucket=src_bucket)
# #         if 'Contents' not in response:
# #             print("No objects found in the source bucket.")
# #             return

# #         for obj in response['Contents']:
# #             src_key = obj['Key']
# #             # Example key: 4116/2023-10-01/account_config_attributes.csv
# #             parts = src_key.split('/')
# #             if len(parts) != 3:
# #                 print(f"Unexpected key format: {src_key}")
# #                 continue
            
# #             account_id = parts[0]
# #             date_str = parts[1]  # Example: 2023-10-01
# #             filename = parts[2]  # Example: account_config_attributes.csv
            
# #             date_parts = date_str.split('-')
# #             if len(date_parts) != 3:
# #                 print(f"Unexpected date format in key: {src_key}")
# #                 continue
            
# #             year = date_parts[0]
# #             month = date_parts[1]
# #             day = date_parts[2]
            
# #             # Construct the new key
# #             dest_key = f"{account_id}/{filename.split('.')[0]}/{year}/{month}/{day}/{filename}"
# #             print(f"Copying {src_key} to {dest_key}")
            
# #             # Copy the object to the new destination
# #             copy_source = {'Bucket': src_bucket, 'Key': src_key}
# #             s3.copy(copy_source, dest_bucket, dest_key)
        
# #         print("Copying completed successfully.")
    
# #     except NoCredentialsError:
# #         print("Credentials not available.")
# #     except PartialCredentialsError:
# #         print("Incomplete credentials provided.")
# #     except Exception as e:
# #         print(f"An error occurred: {e}")

# # # Example usage:
# # src_bucket_name = 'your-source-bucket'
# # dest_bucket_name = 'your-destination-bucket'
# # copy_s3_objects(src_bucket_name, dest_bucket_name)
# # import boto3
# # from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# # def copy_s3_objects(src_bucket, dest_bucket):
# #     s3 = boto3.client('s3')
    
# #     try:
# #         # List all objects in the source bucket
# #         response = s3.list_objects_v2(Bucket=src_bucket)
# #         if 'Contents' not in response:
# #             print("No objects found in the source bucket.")
# #             return

# #         for obj in response['Contents']:
# #             src_key = obj['Key']
# #             # Example key: 4116/2023-10-01/account_config_attributes.csv
# #             parts = src_key.split('/')
# #             if len(parts) != 3:
# #                 print(f"Unexpected key format: {src_key}")
# #                 continue
            
# #             account_id = parts[0]
# #             date_str = parts[1]  # Example: 2023-10-01
# #             filename = parts[2]  # Example: account_config_attributes.csv
            
# #             date_parts = date_str.split('-')
# #             if len(date_parts) != 3:
# #                 print(f"Unexpected date format in key: {src_key}")
# #                 continue
            
# #             year = date_parts[0]
# #             month = date_parts[1]
# #             day = date_parts[2]
            
# #             # Construct the new key
# #             dest_key = f"{account_id}/{filename.split('.')[0]}/{year}/{month}/{day}/{filename}"
# #             print(f"Copying {src_key} to {dest_key}")
            
# #             # Copy the object to the new destination
# #             copy_source = {'Bucket': src_bucket, 'Key': src_key}
# #             s3.copy(copy_source, dest_bucket, dest_key)
        
# #         print("Copying completed successfully.")
    
# #     except NoCredentialsError:
# #         print("Credentials not available.")
# #     except PartialCredentialsError:
# #         print("Incomplete credentials provided.")
# #     except Exception as e:
# #         print(f"An error occurred: {e}")

# # # Example usage:
# # src_bucket_name = 'your-source-bucket'
# # dest_bucket_name = 'your-destination-bucket'
# # copy_s3_objects(src_bucket_name, dest_bucket_name)




# import boto3
# from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# def copy_s3_objects(src_bucket, src_prefix, dest_bucket, dest_prefix):
#     profile_name = 'PowerUserAccess-484850288072'
#     session = boto3.Session(profile_name=profile_name)
    
#     # Create an S3 client using the session
#     s3 = session.client('s3')
#     # s3 = boto3.client('s3')
    
#     try:
#         # List all objects in the source bucket with the specified prefix
#         response = s3.list_objects_v2(Bucket=src_bucket, Prefix=src_prefix)
#         if 'Contents' not in response:
#             print("No objects found in the source bucket.")
#             return

#         for obj in response['Contents']:
#             src_key = obj['Key']
#             # Example key: 4116/2023-10-01/account_config_attributes.csv
#             parts = src_key.split('/')
#             print("Parts: ", parts)
#             if len(parts) != 3:  # Adjusted to match the new structure
#                 print(f"Unexpected key format: {src_key}")
#                 continue
            
#             date_str = parts[1]  # Example: 2023-10-01
#             filename = parts[2]  # Example: account_config_attributes.csv
            
#             date_parts = date_str.split('-')
#             print("Date: ", date_parts, " Filename: ", filename)
#             if len(date_parts) != 3:
#                 print(f"Unexpected date format in key: {src_key}")
#                 continue
            
#             year = date_parts[0]
#             month = date_parts[1]
#             day = date_parts[2]
            
#             # Construct the new key
#             dest_key = f"{dest_prefix}/{filename.split('.')[0]}/{year}/{month}/{day}/{filename}"
#             print(f"Copying {src_key} to {dest_key}")
            
#             # Copy the object to the new destination
#             copy_source = {'Bucket': src_bucket, 'Key': src_key}
#             s3.copy(copy_source, dest_bucket, dest_key)
        
#         print("Copying completed successfully.")
    
#     except NoCredentialsError:
#         print("Credentials not available.")
#     except PartialCredentialsError:
#         print("Incomplete credentials provided.")
#     except Exception as e:
#         print(f"An error occurred: {e}")

# # Example usage:
# src_bucket_name = 'gulp-data-vault-decode'
# src_prefix = '4116/'
# dest_bucket_name = 'gulp-data-vault-decode'
# dest_prefix = 'processed-4116'
# copy_s3_objects(src_bucket_name, src_prefix, dest_bucket_name, dest_prefix)


import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def copy_s3_objects(src_bucket, src_prefix, dest_bucket, dest_prefix):
    profile_name = 'PowerUserAccess-484850288072'
    session = boto3.Session(profile_name=profile_name)
    
    # Create an S3 client using the session
    s3 = session.client('s3')
    
    try:
        # Initialize the continuation token
        continuation_token = None
        
        while True:
            # List all objects in the source bucket with the specified prefix
            if continuation_token:
                response = s3.list_objects_v2(Bucket=src_bucket, Prefix=src_prefix, ContinuationToken=continuation_token)
            else:
                response = s3.list_objects_v2(Bucket=src_bucket, Prefix=src_prefix)

            if 'Contents' not in response:
                print("No objects found in the source bucket.")
                return

            for obj in response['Contents']:
                src_key = obj['Key']
                # Example key: 4116/2023-10-01/account_config_attributes.csv
                parts = src_key.split('/')
                print("Parts: ", parts)
                if len(parts) != 3:  # Adjusted to match the new structure
                    print(f"Unexpected key format: {src_key}")
                    continue
                
                date_str = parts[1]  # Example: 2023-10-01
                filename = parts[2]  # Example: account_config_attributes.csv
                
                date_parts = date_str.split('-')
                print("Date: ", date_parts, " Filename: ", filename)
                if len(date_parts) != 3:
                    print(f"Unexpected date format in key: {src_key}")
                    continue
                
                year = date_parts[0]
                month = date_parts[1]
                day = date_parts[2]
                
                # Construct the new key
                dest_key = f"{dest_prefix}/{filename.split('.')[0]}/{year}/{month}/{day}/{filename}"
                print(f"Copying {src_key} to {dest_key}")
                
                # Copy the object to the new destination
                copy_source = {'Bucket': src_bucket, 'Key': src_key}
                s3.copy(copy_source, dest_bucket, dest_key)
            
            # Check if there are more pages to retrieve
            if 'NextContinuationToken' in response:
                continuation_token = response['NextContinuationToken']
            else:
                break
        
        print("Copying completed successfully.")
    
    except NoCredentialsError:
        print("Credentials not available.")
    except PartialCredentialsError:
        print("Incomplete credentials provided.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage:
src_bucket_name = 'gulp-data-vault-decode'
src_prefix = '4116-remaining/'
dest_bucket_name = 'gulp-data-vault-decode'
dest_prefix = 'processed-4116'
copy_s3_objects(src_bucket_name, src_prefix, dest_bucket_name, dest_prefix)
