import pandas as pd
import json

df = pd.read_csv('/Users/ranjithrreddyabbidi/ranjith/ranjith-doc/test.csv')

df.columns = df.columns.str.strip()
df['source'] = df['source'].str.strip()

df_oracle = df[df['source'].str.lower() == 'oracle']
df_snowflake = df[df['source'].str.lower() == 'snoflake']

# result_df = pd.merge(df_oracle, df_snowflake, on='name', suffixes=('_OnPrem', '_SF'))
result_df = pd.merge(df_oracle, df_snowflake, on='name', suffixes=('_OnPrem', '_SF'), how='outer')

technical_rec_ids = ['ran', 'ra', 'an', 'rn', 'r', 'r1']

for technical_rec_id in technical_rec_ids:
    output_list = []
    exclude_columns = ['name', 'source']
    
    # Filter rows for the current technical_rec_id
    filtered_rows = result_df[result_df['name'] == technical_rec_id]

    for index, row in filtered_rows.iterrows():
        for column_name in df.columns:
            if column_name not in exclude_columns:
                on_prem_val = str(row[column_name + '_OnPrem'])
                sf_val = str(row[column_name + '_SF']) if pd.notna(row[column_name + '_SF']) else None
                test_result = "Pass" if on_prem_val == sf_val else "Fail"

                output_list.append({
                    "Column_name": column_name,
                    "On-prem Val": on_prem_val,
                    "SF_Val": sf_val,
                    "Test_Result": test_result,
                    "Technical_rec_id": str(row['name'])
                })

    # If no data is found, set Test_Result to "Fail" and include an empty list
    if not output_list:
        output_list.append({
            "Test_Result": "Fail",
            "Technical_rec_id": technical_rec_id
        })

    json_output = json.dumps(output_list, indent=2, default=str)  # Use default=str to handle None

    # Save the JSON object to a separate file for each technical_rec_id
    filename = f"/Users/ranjithrreddyabbidi/ranjith/ranjith-doc/output_{technical_rec_id}.json"
    with open(filename, 'w') as file:
        file.write(json_output)
