import csv

# File paths
file1 = "Tianfeng Cleaning (Shengyi Final) - INST.csv"  # Contains the column 'WESTERN_NAME'
file2 = "Institution Duplication Check - Sheet1.csv"  # Contains the column 'Name1'
output_file = "outputForTianfengCleaning.csv"

# Read data from the second file into a set for efficient lookup
name1_set = set()
with open(file2, newline='', encoding='utf-8') as f2:
    reader = csv.DictReader(f2)
    for row in reader:
        name1_set.add(row['Name1'])

# Compare and write matching records to the output file
with open(file1, newline='', encoding='utf-8') as f1, open(output_file, 'w', newline='', encoding='utf-8') as out:
    reader = csv.DictReader(f1)
    writer = csv.writer(out)
    writer.writerow(['WESTERN_NAME'])  # Writing the header

    for row in reader:
        if row['WESTERN_NAME'].strip() in name1_set:
            writer.writerow([row['WESTERN_NAME']])
