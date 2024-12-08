import pandas as pd

input_file = 'Institution Duplication Check - Sheet1.csv'
output_file = 'institutionoutput.csv'

#load the csv file into a dataframe
df = pd.read_csv(input_file)

#separate rows where both checked and duplicates are true
checked_duplicates_df = df[(df['Checked'] == True) & (df['Duplicates'] == True)]

#drop exact duplicates, keeping only the first occurrence
unique_checked_duplicates_df = checked_duplicates_df.drop_duplicates()

#select rows that do not have both checked and duplicates as true
remaining_df = df[~((df['Checked'] == True) & (df['Duplicates'] == True))]

#concatenate the unique checked duplicates with the remaining records
output_df = pd.concat([remaining_df, unique_checked_duplicates_df])

#save the final dataframe to a new file
output_df.to_csv(output_file, index=False)
print(f"Processed and saved the data to {output_file}")
