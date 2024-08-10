import pandas as pd

# Load CSV files into dataframes
df_userscontacts = pd.read_csv('userscontacts.csv')
df_users = pd.read_csv('users.csv')
df_hubspot_users = pd.read_csv('hubspot_users.csv')
df_leads_user = pd.read_csv('leads_user.csv')
df_location_user = pd.read_csv('userlocation.csv')
df_audience = pd.read_csv('audience.csv')
df_invitation = pd.read_csv('invitation.csv')
df_municipalities = pd.read_csv('municipalities.csv')
df_drivers = pd.read_csv('drivers.csv')
df_scrapper = pd.read_csv('scrapper.csv')
# Merge the dataframes on the 'name' column
merged_df = pd.concat([df_userscontacts, df_users, df_hubspot_users, df_leads_user, df_location_user, df_audience, df_invitation, df_municipalities, df_drivers, df_scrapper], ignore_index=True).drop_duplicates(subset='name')

print("merged users: \n", merged_df)
# Save the merged dataframe as 'uva-users.csv'
merged_df.to_csv('uva-users.csv', index=False)

print("Merged users saved to uva-users.csv")
# Load the Excel file and extract the 'Grantee' column into a dataframe
df_excel = pd.read_excel('pr_act_60___22.xlsx')
print("Grantee: \n", df_excel)
print("Columns in df_excel: \n", df_excel.columns.tolist())

df_grantee = df_excel[['Grantee ']].drop_duplicates()
print("Grantee: \n", df_excel)
# Cross-reference the 'Grantee' names with the 'name' column in the merged dataframe
cross_reference_df = df_grantee[df_grantee['Grantee '].isin(merged_df['name'])]

cross_reference_df_original = df_excel[df_excel['Grantee '].isin(merged_df['name'])]
# Display the cross-referenced dataframe
print("Cross Referenced DF: \n", cross_reference_df)


cross_reference_df.to_csv('cross-referenced-users.csv', index=False)

cross_reference_df_original.to_excel('cross-referenced.xlsx', index=False)
