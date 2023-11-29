import pandas as pd

# Read the CSV file into a DataFrame
df = pd.read_csv('your_file.csv')

# Dictionary to store data for each category
dev_data = []
qa_data = []
prod_data = []

# Process each row in the DataFrame
for index, row in df.iterrows():
    col3_value = row['col3']

    if 'dev' in col3_value:
        dev_data.append(col4_value)
    elif 'qa' in col3_value:
        qa_data.append(col4_value)
    elif 'prod' in col3_value:
        prod_data.append(col4_value)

# Print the final results
print(f"dev = ({', '.join(map(str, dev_data))})")
print(f"qa = ({', '.join(map(str, qa_data))})")
print(f"prod = ({', '.join(map(str, prod_data))})")


