# # with legal full name

# import pandas as pd
# from tqdm import tqdm
# # Load CSV files into dataframes
# # df_userscontacts = pd.read_csv(r'userscontacts.csv')
# # Set the file path and chunk size

# df_users = pd.read_csv(r'D:\cross-reference-names\fortuna\fortuna-emails.csv', dtype=str)


# print("read success")
# # Merge the dataframes on the 'name' column
# merged_df = df_users
# # Merge the dataframes on the 'name' column
# merged_df = df_users.copy()  # Make a copy to avoid modifying df_users directly
# # # Clean up NaNs in 'first_name' and 'last_name' columns (if any)
# merged_df['first_name'].fillna('', inplace=True)
# merged_df['last_name'].fillna('', inplace=True)
# # Add a new column 'name' which concatenates 'first_name' and 'last_name'
# # merged_df['name'] = merged_df.apply(lambda row: row['first_name'] + ' ' + row['last_name'], axis=1)
# merged_df['name'] = merged_df['first_name'] + ' ' + merged_df['last_name']

# print("merged users: \n", merged_df)
# # Save the merged dataframe as 'uva-users.csv'
# # merged_df.to_csv('fortuna-users.csv', index=False)

# # print("Merged users saved to fortuna-users.csv")

# # Load the Excel file and extract the 'Grantee' column into a dataframe
# df_excel = pd.read_csv(r'D:\cross-reference-names\fortuna\TX registered-data-brokers.csv')
# print("Grantee: \n", df_excel)
# print("Columns in df_excel: \n", df_excel.columns.tolist())
    
# df_grantee = df_excel[['EXECUTED_BY']].drop_duplicates()
# print("Grantee: \n", df_excel)
# # Cross-reference the 'Grantee' names with the 'name' column in the merged dataframe
# cross_reference_df = df_grantee[df_grantee['EXECUTED_BY'].isin(tqdm(merged_df['name'], desc="Cross-referencing"))]

# # cross_reference_df_original = df_excel[df_excel['EXECUTED_BY'].isin(merged_df['name'])]
# # Display the cross-referenced dataframe
# print("Cross Referenced DF: \n", cross_reference_df)


# # cross_reference_df.to_csv('cross-referenced-users.csv', index=False)

# cross_reference_df.to_excel('TX registered-data-brokers-cross-referenced-fortuna.xlsx', index=False)









# # #with EXECUTED_BY

# # import pandas as pd

# # # Load CSV files into dataframes
# # # df_userscontacts = pd.read_csv(r'userscontacts.csv')
# # df_users = pd.read_csv(r'D:\cross-reference-names\fortuna\fortuna-emails.csv', nrows=100)


# # print("read success")
# # # Merge the dataframes on the 'name' column
# # # merged_df = df_users
# # # Merge the dataframes on the 'name' column
# # merged_df = df_users.copy()  # Make a copy to avoid modifying df_users directly
# # # Clean up NaNs in 'first_name' and 'last_name' columns (if any)
# # merged_df['first_name'].fillna('', inplace=True)
# # merged_df['last_name'].fillna('', inplace=True)
# # # Add a new column 'name' which concatenates 'first_name' and 'last_name'
# # # merged_df['name'] = merged_df.apply(lambda row: row['first_name'] + ' ' + row['last_name'], axis=1)
# # merged_df['name'] = merged_df['first_name'] + ' ' + merged_df['last_name']

# # print("merged users: \n", merged_df)
# # # Save the merged dataframe as 'uva-users.csv'
# # merged_df.to_csv('fortuna-users.csv', index=False)

# # print("Merged users saved to fortuna-users.csv")

# # # Load the Excel file and extract the 'Grantee' column into a dataframe
# # df_excel = pd.read_csv(r'D:\cross-reference-names\fortuna\TX registered-data-brokers.csv')
# # print("Grantee: \n", df_excel)
# # print("Columns in df_excel: \n", df_excel.columns.tolist())
    
# # df_grantee = df_excel[['EXECUTED_BY']].drop_duplicates()
# # print("Grantee: \n", df_excel)
# # # Cross-reference the 'Grantee' names with the 'name' column in the merged dataframe
# # cross_reference_df = df_grantee[df_grantee['EXECUTED_BY'].isin(merged_df['name'])]

# # cross_reference_df_original = df_excel[df_excel['EXECUTED_BY'].isin(merged_df['name'])]
# # # Display the cross-referenced dataframe
# # print("Cross Referenced DF: \n", cross_reference_df)


# # cross_reference_df.to_csv('cross-referenced-users.csv', index=False)

# # cross_reference_df_original.to_excel('cross-referenced.xlsx', index=False)







import pandas as pd
from tqdm import tqdm

# Set the file paths
fortuna_emails_path = r'D:\cross-reference-names\fortuna\fortuna-emails.csv'
tx_registered_data_brokers_path = r'D:\cross-reference-names\fortuna\TX registered-data-brokers.csv'

# Set the chunk size
chunk_size = 100000

# Initialize an empty list to store the results
cross_reference_list = []

# Load the Excel file and extract the 'EXECUTED_BY' column into a dataframe
df_excel = pd.read_csv(tx_registered_data_brokers_path)
print("Columns in df_excel: \n", df_excel.columns.tolist())

df_grantee = df_excel[['EXECUTED_BY']].drop_duplicates()
print("Grantee: \n", df_grantee)

# Read and process the CSV file in chunks
with tqdm(total=408248478, desc="Processing fortuna-emails.csv") as pbar:
    for chunk in pd.read_csv(fortuna_emails_path, dtype=str, chunksize=chunk_size):
        # Clean up NaNs in 'first_name' and 'last_name' columns (if any)
        chunk['first_name'].fillna('', inplace=True)
        chunk['last_name'].fillna('', inplace=True)
        # Add a new column 'name' which concatenates 'first_name' and 'last_name'
        chunk['name'] = chunk['first_name'] + ' ' + chunk['last_name']

        # Cross-reference the 'Grantee' names with the 'name' column in the chunk
        cross_reference_chunk = df_grantee[df_grantee['EXECUTED_BY'].isin(chunk['name'])]
        cross_reference_list.append(cross_reference_chunk)

        # Update the progress bar
        pbar.update(len(chunk))

# Concatenate all the cross-referenced chunks into a single dataframe
cross_reference_df = pd.concat(cross_reference_list)

# Display the cross-referenced dataframe
print("Cross Referenced DF: \n", cross_reference_df)

# Save the cross-referenced dataframe to an Excel file
cross_reference_df.to_excel('TX registered-data-brokers-cross-referenced-fortuna.xlsx', index=False)
