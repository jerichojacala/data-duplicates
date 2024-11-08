import pandas as pd

def merge_duplicates(csv_file):
    # Load the CSV file into a DataFrame
    df = pd.read_csv(csv_file)

    # Dictionary to store merged records
    merged_records = {}

    # Function to merge two records
    def merge_two_records(record1, record2):
        merged = {}
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                merged[col] = record1[col] if pd.isna(record1[col]) else record1[col] + record2[col]
            elif col == 'new_ids' or col == 'confirmed_duplicates':  # Merge lists
                list1 = record1[col].split(',') if pd.notna(record1[col]) else []
                list2 = record2[col].split(',') if pd.notna(record2[col]) else []
                merged[col] = ', '.join(sorted(set(list1 + list2)))
            else:  # Concatenate strings, or choose non-empty value
                merged[col] = record1[col] if pd.notna(record1[col]) else record2[col]
        return merged

    # Process each row
    for index, row in df.iterrows():
        current_id = row['id']
        confirmed_duplicates = row['confirmed_duplicates']

        if pd.notna(confirmed_duplicates):
            duplicate_ids = confirmed_duplicates.split(', ')
            duplicate_ids.append(current_id)
            duplicate_ids = sorted(set(duplicate_ids))  # Ensure unique IDs

            # Find or create merged entry
            first_id = duplicate_ids[0]
            if first_id not in merged_records:
                merged_records[first_id] = row.to_dict()  # Convert row to a dictionary
            else:
                merged_records[first_id] = merge_two_records(merged_records[first_id], row.to_dict())
        else:
            merged_records[current_id] = row.to_dict()

    # Convert merged records (dict of dicts) to a DataFrame
    merged_df = pd.DataFrame(list(merged_records.values()))
    
    return merged_df

# Example usage
merged_df = merge_duplicates('merged_input.csv')
print(merged_df)
merged_df.to_csv('merged_output.csv', index=False)