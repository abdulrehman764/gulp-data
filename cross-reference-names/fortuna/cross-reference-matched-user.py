# import pandas as pd
# from tqdm import tqdm

# # Set the file paths
# fortuna_emails_path = r'D:\cross-reference-names\fortuna\fortuna-emails.csv'
# tx_registered_data_brokers_path = r'D:\cross-reference-names\fortuna\TX registered-data-brokers.csv'
# tx_registered_data_brokers_path = r'D:\cross-reference-names\fortuna\CA data-brokers (1).csv'
# # Set the chunk size
# chunk_size = 100000

# # Initialize empty lists to store the results
# cross_reference_list = []
# matched_users_list = []

# # Load the Excel file and extract the 'EXECUTED_BY' column into a dataframe
# df_excel = pd.read_csv(tx_registered_data_brokers_path)
# print("Columns in df_excel: \n", df_excel.columns.tolist())

# df_grantee = df_excel[['Data Broker Name']].drop_duplicates()
# print("Grantee: \n", df_grantee)

# # Read and process the CSV file in chunks
# with tqdm(total=408248478, desc="Processing fortuna-emails.csv") as pbar:
#     for chunk in pd.read_csv(fortuna_emails_path, dtype=str, chunksize=chunk_size):
#         # Clean up NaNs in 'first_name' and 'last_name' columns (if any)
#         chunk['first_name'].fillna('', inplace=True)
#         chunk['last_name'].fillna('', inplace=True)
#         # Add a new column 'name' which concatenates 'first_name' and 'last_name'
#         chunk['name'] = chunk['first_name'] + ' ' + chunk['last_name']

#         # Cross-reference the 'Grantee' names with the 'name' column in the chunk
#         cross_reference_chunk = df_grantee[df_grantee['Data Broker Name'].isin(chunk['name'])]
#         matched_users_chunk = chunk[chunk['name'].isin(df_grantee['Data Broker Name'])]

#         # Append the cross-referenced chunk and matched users to the respective lists
#         cross_reference_list.append(cross_reference_chunk)
#         matched_users_list.append(matched_users_chunk)

#         # Update the progress bar
#         pbar.update(len(chunk))

# # Concatenate all the cross-referenced chunks into a single dataframe
# cross_reference_df = pd.concat(cross_reference_list)
# matched_users_df = pd.concat(matched_users_list)

# # Display the cross-referenced dataframe and matched users dataframe
# print("Cross Referenced DF: \n", cross_reference_df)
# print("Matched Users DF: \n", matched_users_df)

# # Save the cross-referenced dataframe and matched users dataframe to Excel files
# cross_reference_df.to_excel('CA-data-brokers-cross-referenced-fortuna.xlsx', index=False)
# matched_users_df.to_excel('ca-matched-users-fortuna.xlsx', index=False)



import pandas as pd
from tqdm import tqdm
import logging

# Set up logging
logging.basicConfig(filename='processing_log.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Set the file paths
fortuna_emails_path = r'D:\cross-reference-names\fortuna\fortuna-emails.csv'
tx_registered_data_brokers_path = r'D:\cross-reference-names\fortuna\TX registered-data-brokers.csv'
ca_registered_data_brokers_path = r'D:\cross-reference-names\fortuna\CA data-brokers (1).csv'

# Set the chunk size
chunk_size = 100000

# Initialize empty lists to store the results
cross_reference_list = []
matched_users_list = []

# Load the CSV file and extract the 'Data Broker Name' column into a dataframe
df_excel = pd.read_csv(ca_registered_data_brokers_path)
logging.info(f"Columns in df_excel: {df_excel.columns.tolist()}")

df_grantee = df_excel[['Data Broker Name']].drop_duplicates()
logging.info(f"Grantee DataFrame: {df_grantee.head()}")

# Read and process the CSV file in chunks
try:
    with tqdm(total=408248478, desc="Processing fortuna-emails.csv") as pbar:
        for chunk in pd.read_csv(fortuna_emails_path, dtype=str, chunksize=chunk_size):
            logging.info(f"Processing chunk with {len(chunk)} records")
            
            # Clean up NaNs in 'first_name' and 'last_name' columns (if any)
            chunk['first_name'] = chunk['first_name'].fillna('')
            chunk['last_name'] = chunk['last_name'].fillna('')

            # Add a new column 'name' which concatenates 'first_name' and 'last_name'
            chunk['name'] = chunk['first_name'] + ' ' + chunk['last_name']

            # Cross-reference the 'Grantee' names with the 'name' column in the chunk
            cross_reference_chunk = df_grantee[df_grantee['Data Broker Name'].isin(chunk['name'])]
            matched_users_chunk = chunk[chunk['name'].isin(df_grantee['Data Broker Name'])]

            # Append the cross-referenced chunk and matched users to the respective lists
            cross_reference_list.append(cross_reference_chunk)
            matched_users_list.append(matched_users_chunk)

            # Update the progress bar
            pbar.update(len(chunk))

except Exception as e:
    logging.error(f"Error occurred: {e}")

# Concatenate all the cross-referenced chunks into a single dataframe
cross_reference_df = pd.concat(cross_reference_list, ignore_index=True)
matched_users_df = pd.concat(matched_users_list, ignore_index=True)

# Display the cross-referenced dataframe and matched users dataframe
logging.info(f"Cross Referenced DF: {cross_reference_df.head()}")
logging.info(f"Matched Users DF: {matched_users_df.head()}")

# Save the cross-referenced dataframe and matched users dataframe to Excel files
cross_reference_df.to_excel('CA-data-brokers-cross-referenced-fortuna.xlsx', index=False)
matched_users_df.to_excel('ca-matched-users-fortuna.xlsx', index=False)

print("Processing complete.")
