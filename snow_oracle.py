

import pandas as pd
import json

df = pd.read_csv('/Users/ranjithrreddyabbidi/ranjith/ranjith-doc/test.csv')

df.columns = df.columns.str.strip()
df['source'] = df['source'].str.strip()

df_oracle = df[df['source'].str.lower() == 'oracle']
df_snowflake = df[df['source'].str.lower() == 'snoflake']

#result_df = pd.merge(df_oracle, df_snowflake, on='name', suffixes=('_OnPrem', '_SF'))
result_df = pd.merge(df_oracle, df_snowflake, on='name', suffixes=('_OnPrem', '_SF'), how='outer')

technical_rec_ids = ['ran', 'ra', 'an', 'rn', 'r', 'r1']

# for technical_rec_id in technical_rec_ids:
#     print(technical_rec_id)
#     output_list = []
#     exclude_columns = ['name', 'source']
    
#     # Filter rows for the current technical_rec_id
#     print(result_df['name'], technical_rec_id, "206"*10)
#     filtered_rows = result_df[result_df['name'] == technical_rec_id]
#     print(filtered_rows, "207"*10)

#     for index, row in filtered_rows.iterrows():
#         for column_name in df.columns:
#             if column_name not in exclude_columns:
#                 on_prem_val = str(row[column_name + '_OnPrem'])
#                 sf_val = str(row[column_name + '_SF']) if pd.notna(row[column_name + '_SF']) else None
#                 test_result = "Pass" if on_prem_val == sf_val else "Fail"

#                 output_list.append({
#                     "Column_name": column_name,
#                     "On-prem Val": on_prem_val,
#                     "SF_Val": sf_val,
#                     "Test_Result": test_result,
#                     "Technical_rec_id": str(row['name'])
#                 })

#     json_output = json.dumps(output_list, indent=2, default=str)  # Use default=str to handle None

#     # Save the JSON object to a separate file for each technical_rec_id
#     filename = f"/Users/ranjithrreddyabbidi/ranjith/ranjith-doc/output_{technical_rec_id}.json"
#     with open(filename, 'w') as file:
#         file.write(json_output)




technical_rec_ids = ['ran', 'ra', 'an', 'rn', 'r', 'r1']

for technical_rec_id in technical_rec_ids:
    print(technical_rec_id)
    output_list = []
    exclude_columns = ['name', 'source', 'uid']

    # Filter rows for the current technical_rec_id
    filtered_rows = result_df[result_df['name'] == technical_rec_id]

    # Get 'uid' from the original dataframe if present
    uid_column_name = 'uid'
    if uid_column_name in df.columns and not df[df['name'] == technical_rec_id][uid_column_name].empty:
        uid_value = str(df[df['name'] == technical_rec_id][uid_column_name].values[0]).strip()
    else:
        uid_value = 'unknown'

    for index, row in filtered_rows.iterrows():
        for column_name in df.columns:
            if column_name not in exclude_columns:
                on_prem_val = str(row[column_name + '_OnPrem']).strip()
                sf_val = str(row[column_name + '_SF']).strip() if pd.notna(row[column_name + '_SF']) else None
                test_result = "Pass" if on_prem_val == sf_val else "Fail"

                output_list.append({
                    "Column_name": column_name,
                    "On-prem Val": on_prem_val,
                    "SF_Val": sf_val,
                    "Test_Result": test_result,
                    "Technical_rec_id": str(row['name'])
                })

    json_output = json.dumps(output_list, indent=2, default=str)

    # Write JSON output to a file using uid column with no spaces
    output_file_path = f'/Users/ranjithrreddyabbidi/ranjith/ranjith-doc/{uid_value.replace(" ", "_")}_comparison_result.json'

    with open(output_file_path, 'w') as output_file:
        output_file.write(json_output)

    print(f"Comparison result for {technical_rec_id} written to {output_file_path}")


for technical_rec_id in technical_rec_ids:
    print(technical_rec_id)
    output_list = []
    exclude_columns = ['TECHNICAL_REC_ID', 'source']

    # Filter rows for the current technical_rec_id
    filtered_rows = result_df[result_df['TECHNICAL_REC_ID'] == technical_rec_id]

    # Get 'uid' from the original dataframe if present
    uid_column_name = 'uid'
    if uid_column_name in df.columns and not df[df['TECHNICAL_REC_ID'] == technical_rec_id][uid_column_name].empty:
        uid_value = str(df[df['TECHNICAL_REC_ID'] == technical_rec_id][uid_column_name].values[0]).strip()
    else:
        uid_value = 'unknown'

    for _, row in filtered_rows.iterrows():
        for column_name in df.columns:
            if column_name not in exclude_columns:
                on_prem_val = str(row[column_name + '_OnPrem']).strip()
                sf_val = str(row[column_name + '_SF']).strip() if pd.notna(row[column_name + '_SF']) else None
                test_result = "Pass" if on_prem_val == sf_val else "Fail"

                output_list.append({
                    "Column_name": column_name,
                    "On-prem Val": on_prem_val,
                    "SF_Val": sf_val,
                    "Test_Result": test_result,
                    "Technical_rec_id": str(row['TECHNICAL_REC_ID'])
                })

    json_output = json.dumps(output_list, indent=2, default=str)

    # Write JSON output to a file using uid column with no spaces
    output_file_path = f'/Users/ranjithrreddyabbidi/ranjith/ranjith-doc/{uid_value.replace(" ", "_")}_comparison_result.json'

    with open(output_file_path, 'w') as output_file:
        output_file.write(json_output)


    print(f"Comparison result for {technical_rec_id} written to {output_file_path}")
    



