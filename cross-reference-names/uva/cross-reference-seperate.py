# import pandas as pd

# # Load the CSV file
# merged_df = pd.read_csv('userscontacts.csv.csv')

# # Load the Excel file
# df_excel = pd.read_excel('pr_act_60___22.xlsx')
# print("Grantee: \n", df_excel)
# print("Columns in df_excel: \n", df_excel.columns.tolist())

# # Drop duplicate 'Grantee' names
# df_grantee = df_excel[['Grantee ']].drop_duplicates()
# print("Grantee: \n", df_grantee)

# # Cross-reference the 'Grantee' names with the 'full_name' column in the merged dataframe
# cross_reference_df = df_grantee[df_grantee['Grantee '].isin(merged_df['name'])]
# cross_reference_df_original = df_excel[df_excel['Grantee '].isin(merged_df['name'])]

# # Filter the merged dataframe for matched users
# filtered_merged_df = merged_df[merged_df['name'].isin(df_grantee['Grantee '])]

# # Display the cross-referenced dataframes
# print("Cross Referenced DF: \n", cross_reference_df)
# print("Filtered Merged DF: \n", filtered_merged_df)

# # Save the cross-referenced dataframes to CSV and Excel files
# # cross_reference_df.to_csv('cross-referenced-users.csv', index=False)
# # cross_reference_df_original.to_excel('cross-referenced.xlsx', index=False)
# filtered_merged_df.to_excel('filtered-userscontacts.xlsx', index=False)

# print("Cross-referenced data has been saved to 'cross-referenced-users.csv' and 'cross-referenced.xlsx'")
# print("Filtered merged data has been saved to 'filtered-users.xlsx'")





# import pandas as pd

# # Load the CSV file
# merged_df = pd.read_csv('leads_user.csv.csv')

# # Load the Excel file
# df_excel = pd.read_excel('pr_act_60___22.xlsx')
# print("Grantee: \n", df_excel)
# print("Columns in df_excel: \n", df_excel.columns.tolist())

# # Drop duplicate 'Grantee' names
# df_grantee = df_excel[['Grantee ']].drop_duplicates()
# print("Grantee: \n", df_grantee)

# # Cross-reference the 'Grantee' names with the 'full_name' column in the merged dataframe
# cross_reference_df = df_grantee[df_grantee['Grantee '].isin(merged_df['name'])]
# cross_reference_df_original = df_excel[df_excel['Grantee '].isin(merged_df['name'])]

# # Filter the merged dataframe for matched users
# filtered_merged_df = merged_df[merged_df['name'].isin(df_grantee['Grantee '])]

# # Display the cross-referenced dataframes
# print("Cross Referenced DF: \n", cross_reference_df)
# print("Filtered Merged DF: \n", filtered_merged_df)

# # Save the cross-referenced dataframes to CSV and Excel files
# # cross_reference_df.to_csv('cross-referenced-users.csv', index=False)
# # cross_reference_df_original.to_excel('cross-referenced.xlsx', index=False)
# filtered_merged_df.to_excel('filtered-leadsusers.xlsx', index=False)

# print("Cross-referenced data has been saved to 'cross-referenced-users.csv' and 'cross-referenced.xlsx'")
# print("Filtered merged data has been saved to 'filtered-users.xlsx'")








# import pandas as pd

# # Load the CSV file
# merged_df = pd.read_csv('hubspot_users.csv.csv')

# # Load the Excel file
# df_excel = pd.read_excel('pr_act_60___22.xlsx')
# print("Grantee: \n", df_excel)
# print("Columns in df_excel: \n", df_excel.columns.tolist())

# # Drop duplicate 'Grantee' names
# df_grantee = df_excel[['Grantee ']].drop_duplicates()
# print("Grantee: \n", df_grantee)

# # Cross-reference the 'Grantee' names with the 'full_name' column in the merged dataframe
# cross_reference_df = df_grantee[df_grantee['Grantee '].isin(merged_df['name'])]
# cross_reference_df_original = df_excel[df_excel['Grantee '].isin(merged_df['name'])]

# # Filter the merged dataframe for matched users
# filtered_merged_df = merged_df[merged_df['name'].isin(df_grantee['Grantee '])]

# # Display the cross-referenced dataframes
# print("Cross Referenced DF: \n", cross_reference_df)
# print("Filtered Merged DF: \n", filtered_merged_df)

# # Save the cross-referenced dataframes to CSV and Excel files
# # cross_reference_df.to_csv('cross-referenced-users.csv', index=False)
# # cross_reference_df_original.to_excel('cross-referenced.xlsx', index=False)
# filtered_merged_df.to_excel('filtered-hubspotusers.xlsx', index=False)

# print("Cross-referenced data has been saved to 'cross-referenced-users.csv' and 'cross-referenced.xlsx'")
# print("Filtered merged data has been saved to 'filtered-users.xlsx'")





# import pandas as pd

# # Load the CSV file
# merged_df = pd.read_csv('userlocation.csv.csv')

# # Load the Excel file
# df_excel = pd.read_excel('pr_act_60___22.xlsx')
# print("Grantee: \n", df_excel)
# print("Columns in df_excel: \n", df_excel.columns.tolist())

# # Drop duplicate 'Grantee' names
# df_grantee = df_excel[['Grantee ']].drop_duplicates()
# print("Grantee: \n", df_grantee)

# # Cross-reference the 'Grantee' names with the 'full_name' column in the merged dataframe
# cross_reference_df = df_grantee[df_grantee['Grantee '].isin(merged_df['name'])]
# cross_reference_df_original = df_excel[df_excel['Grantee '].isin(merged_df['name'])]

# # Filter the merged dataframe for matched users
# filtered_merged_df = merged_df[merged_df['name'].isin(df_grantee['Grantee '])]

# # Display the cross-referenced dataframes
# print("Cross Referenced DF: \n", cross_reference_df)
# print("Filtered Merged DF: \n", filtered_merged_df)

# # Save the cross-referenced dataframes to CSV and Excel files
# # cross_reference_df.to_csv('cross-referenced-users.csv', index=False)
# # cross_reference_df_original.to_excel('cross-referenced.xlsx', index=False)
# filtered_merged_df.to_excel('filtered-userlocation.xlsx', index=False)

# print("Cross-referenced data has been saved to 'cross-referenced-users.csv' and 'cross-referenced.xlsx'")
# print("Filtered merged data has been saved to 'filtered-users.xlsx'")





import pandas as pd

# Load the CSV file
merged_df = pd.read_csv('scrapper.csv')

# Load the Excel file
df_excel = pd.read_excel('pr_act_60___22.xlsx')
print("Grantee: \n", df_excel)
print("Columns in df_excel: \n", df_excel.columns.tolist())

# Drop duplicate 'Grantee' names
df_grantee = df_excel[['Grantee ']].drop_duplicates()
print("Grantee: \n", df_grantee)

# Cross-reference the 'Grantee' names with the 'full_name' column in the merged dataframe
cross_reference_df = df_grantee[df_grantee['Grantee '].isin(merged_df['name'])]
cross_reference_df_original = df_excel[df_excel['Grantee '].isin(merged_df['name'])]

# Filter the merged dataframe for matched users
filtered_merged_df = merged_df[merged_df['name'].isin(df_grantee['Grantee '])]

# Display the cross-referenced dataframes
print("Cross Referenced DF: \n", cross_reference_df)
print("Filtered Merged DF: \n", filtered_merged_df)

# Save the cross-referenced dataframes to CSV and Excel files
# cross_reference_df.to_csv('cross-referenced-users.csv', index=False)
# cross_reference_df_original.to_excel('cross-referenced.xlsx', index=False)
filtered_merged_df.to_excel('filtered-userlocation.xlsx', index=False)

print("Cross-referenced data has been saved to 'cross-referenced-users.csv' and 'cross-referenced.xlsx'")
print("Filtered merged data has been saved to 'filtered-users.xlsx'")