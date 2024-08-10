import pandas as pd
import numpy as np

own = pd.read_csv('rent.csv')

print(own)

# Remove double quotes from all values
own = own.map(lambda x: x.replace('"', '') if isinstance(x, str) else x)

# Replace empty strings with NaN
own.replace('', pd.NA, inplace=True)

print(own)

# Save the cleaned DataFrame back to a CSV file (optional)
own.to_csv('cleaned_rent.csv', index=False)
