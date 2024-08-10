import json
import boto3
import pandas as pd
from io import BytesIO
import openpyxl

def lambda_handler(event, context):
    # Initialize boto3 client for Glue
    session = boto3.Session(profile_name='PowerUserAccess-484850288072')
    glue_client = session.client('glue', region_name='us-east-1')
    
    # Parameters
    database_name = "mmlist-6554"
    
    # Fetch tables in the specified database with pagination
    all_tables = []
    paginator = glue_client.get_paginator('get_tables')
    for page in paginator.paginate(DatabaseName=database_name):
        all_tables.extend(page['TableList'])
    
    print("LEN TABLES: ", len(all_tables))
    
    # Initialize a dictionary to store table columns
    table_columns = {}
    
    # Loop through each table to extract columns
    for table in all_tables:
        table_name = table['Name']
        columns = table['StorageDescriptor']['Columns']
        
        # Extract column names
        column_names = [col['Name'] for col in columns]
        table_columns[table_name] = column_names
    
    # Initialize a dictionary to store relationships
    relationships = {}

    # Find relationships based on matching column names
    for table, columns in table_columns.items():
        for other_table, other_columns in table_columns.items():
            if table != other_table:
                common_columns = set(columns).intersection(set(other_columns))
                if common_columns:
                    if table in relationships:
                        relationships[table].append({'related_table': other_table, 'columns': list(common_columns)})
                    else:
                        relationships[table] = [{'related_table': other_table, 'columns': list(common_columns)}]
    
    # Create a DataFrame to store the tree-like structure
    tree_data_list = []
    for parent, children in relationships.items():
        for child in children:
            tree_data_list.append({
                'Table': parent,
                'Related Table': child['related_table'],
                'Common Columns': ', '.join(child['columns'])
            })

    tree_data = pd.DataFrame(tree_data_list)
    print("Tree Data: ", tree_data)
    
    # Create an Excel writer object
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        tree_data.to_excel(writer, index=False, sheet_name='Relationships')
        
        # Access the workbook
        workbook = writer.book
        worksheet = writer.sheets['Relationships']
        
        # Format the columns
        for row in worksheet.iter_rows(min_row=2, max_row=len(tree_data) + 1, min_col=1, max_col=3):
            for cell in row:
                cell.alignment = openpyxl.styles.Alignment(wrap_text=True)

    # Save the Excel file locally
    excel_buffer.seek(0)
    local_filename = f'{database_name}-relationships.xlsx'
    with open(local_filename, 'wb') as f:
        f.write(excel_buffer.getvalue())
    
    # Save the Excel file to S3
    session = boto3.Session(profile_name='PowerUserAccess-484850288072')
    s3_client = session.client('s3')
    
    s3_bucket = 'gulp-data-vault-decode'  # Replace with your S3 bucket name
    s3_key = f'{database_name}_relationships.xlsx'  # Save the file with a unique key
    s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=excel_buffer.getvalue())
    print("Files saved to S3")
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Relationships file saved to S3 bucket: {s3_bucket}, with key: {s3_key}')
    }

lambda_handler('', '')
