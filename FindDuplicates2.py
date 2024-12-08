from py2neo import Graph
import sys
from py2neo.matching import *
import pandas as pd
import editdistance
#from py_stringmatching.similarity_measure import *
import math
import dimsim
import re
import csv
import affine
import time

#reads in a csv to a dataframe
#def read_and_split_csv(file_path, delimiter='@'):
#    try:
#        with open(file_path, 'r', encoding='utf-8') as file:
#            lines = file.readlines()
#        return [line.strip().split(',') for line in lines]
#    except UnicodeDecodeError:
#        # Try using a different encoding if utf-8 fails
#        with open(file_path, 'r', encoding='ISO-8859-1') as file:
#            lines = file.readlines()
#        return [line.strip().split(',') for line in lines]

def read_and_split_csv(file_path):
    try:
        df = pd.read_csv(file_path, encoding='utf-8', delimiter='@')
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, encoding='ISO-8859-1', delimiter='@')
    return df
    
def fill_missing_gender(people_list):
    # Extract header row to find the index of the "gender" column
    header = people_list[0]
    
    if "gender" in header:
        gender_index = header.index("gender")
        
        # Iterate over rows (starting from index 1 to skip the header)
        for row in people_list[1:]:
            # If gender is None or empty, set it to "Unknown"
            if not row[gender_index]:
                row[gender_index] = "Unknown"
    else:
        print("No 'gender' column found.")

class FindDuplicates():

    def __init__(self) -> None:
        # Defining constants that act as thresholds for the scores
        self.EDITDISTANCE_THRESHOLD = 0.20       # want to be less than this
        self.AFFINE_THRESHOLD = 0.70             # want to be greater than this
        self.CHINESE_PHONETIC_THRESHOLD = 10     # want to be less than this

    # Compares two names in Chinese characters and returns true/false
    def compare_two_names_chinese(self, name1, name2):
        # Using following package: https://github.com/IBM/MAX-Chinese-Phonetic-Similarity-Estimator 
        if len(name1) == len(name2) and len(name2) > 0:
            # print(f"{name1} ** {name2}")
            hz_sim_score = dimsim.get_distance(name1, name2)   
            return hz_sim_score < self.CHINESE_PHONETIC_THRESHOLD
        return False
      
    # Compares two names in a Western language
    def compare_two_names_western(self, name1, name2):
        #aff = affine.Affine()

        if len(name1) == 0 or len(name2) == 0:
            return False

        ed_score = 2.0 * editdistance.eval(name1, name2) / (len(name1) + len(name2))
        #aff_score = aff.get_raw_score(name1, name2) / min(len(name1), len(name2))
        
        # print(f"{name1} ** {name2} = {ed_score} {aff_score}")

        # Update output
        #return (ed_score < self.EDITDISTANCE_THRESHOLD) or (aff_score > self.AFFINE_THRESHOLD)
        return (ed_score < self.EDITDISTANCE_THRESHOLD)
        
    def compare_two_ages(self, birth1, death1, birth2, death2):
        output = True
        if birth1 > 0 and birth2 > 0 and abs(birth1-birth2) > 10:
            output = False
        if death1 > 0 and death2 > 0 and abs(death1-death2) > 10:
            output = False
        return output 

    def compare_two_people_entries(self, entry1, entry2):
        # compare the birth and death dates
        result1 = self.compare_two_ages(int(entry1["birth_year"]), int(entry1["death_year"]), int(entry2["birth_year"]), int(entry2["death_year"]))
        if result1 == False:
            return False
        
        # first_name_1 last_name_1 WITH first_name_2 last_name_2
        x = " ".join([entry1.loc["given_name_western"], entry1.loc["family_name_western"]])
        y = " ".join([entry2.loc["given_name_western"], entry2.loc["family_name_western"]])
        result2 = self.compare_two_names_western(x, y)
        
        return result2

        # Currently doing nothing with Chinese names
        # chinese_family_name_1 chinese_given_name_1 WITH chinese_family_name_2 chinese_given_name_2
        x = "".join([entry1.loc["chinese_family_name_hanzi"], entry1.loc["chinese_given_name_hanzi"]])
        x = "".join(re.findall(r'[\u4e00-\u9fff]+', x))
        y = "".join([entry2.loc["chinese_family_name_hanzi"], entry2.loc["chinese_given_name_hanzi"]])
        result1 = compare_two_names_chinese(x, y)
        
        # Currently doing nothing with alternative names 

    # Loads in all known people already in the database into a data frame
    def load_known_people(self, graph):
        # Create dataframe with all people names 
        q1 = '''MATCH (i:Person) RETURN i.id as id, 
        i.family_name_western as family_name_western, i.given_name_western as given_name_western, 
        i.alternative_name_western as alternative_name_western, 
        i.chinese_family_name_hanzi as chinese_family_name_hanzi, i.chinese_given_name_hanzi as chinese_given_name_hanzi, 
        i.alternative_chinese_name_hanzi as alternative_chinese_name_hanzi,
        i.birth_year as birth_year, i.death_year as death_year,
        i.gender as gender
        '''
        
        df = graph.run(q1)
        df = df.to_data_frame().set_index(["id"])
        df["birth_year"]            = df["birth_year"].fillna(0)
        df["death_year"]            = df["death_year"].fillna(0)
        df["family_name_western"]   = df["family_name_western"].fillna("")
        df["given_name_western"]    = df["given_name_western"].fillna("")
        df["gender"]                = df["gender"].replace("Male", "M")
        df["gender"]                = df["gender"].replace("Female", "F")
        return df

    # Compares a new name to all names in the data frame
    def compare_name_to_known_names(self, df, new_entry, output):
        for id1, entry in df.iterrows():
            similar = self.compare_two_people_entries(entry, new_entry)
            if similar:
                x = " ".join([entry.loc["given_name_western"], entry.loc["family_name_western"]])
                x = f'{x}@({id1})@'
                output.append(x)
        return output
    
    def compare_name_to_known_names2(self, df, new_entry, output):
        for id1, entry in df.iterrows():
            similar = self.compare_two_people_entries(entry, new_entry)
            if similar:
                x = " ".join([entry.loc["given_name_western"], entry.loc["family_name_western"],entry.loc["chcd_id"]])
                x = f'{x}@{id1}@'
                output.append(x)
        return output

    def load_in_people_to_test(self, csv):
        new_people = pd.read_csv(csv, sep='@') #added @ delimiter
        new_people = new_people.fillna("")
        date_reg = r'([0-9][0-9][0-9][0-9])'
        #new_people["birth_year"] = new_people.birth_year.str.extract(date_reg, expand=False)
        #new_people["birth_year"] = new_people["birth_year"].fillna(0)
        #new_people["death_year"] = new_people.death_year.str.extract(date_reg, expand=False)
        #new_people["death_year"] = new_people["death_year"].fillna(0)

        new_people["birth_year"] = new_people["birth_year"].astype(str).str.extract(r'([0-9]{4})', expand=False)
        new_people["birth_year"] = new_people["birth_year"].fillna(0).astype(int)
    
        new_people["death_year"] = new_people["death_year"].astype(str).str.extract(r'([0-9]{4})', expand=False)
        new_people["death_year"] = new_people["death_year"].fillna(0).astype(int)

        return new_people[["family_name_western", "given_name_western", "gender", "birth_year", "death_year"]]
        #return new_people[["temp_id", "family_name_western", "given_name_western", "gender", "birth_year", "death_year"]]
        # return new_people[["PERS_TEMP_ID", "family_name_western", "given_name_western", "alternative_chinese_name_hanzi", "chinese_given_name_hanzi", "chinese_family_name_hanzi", "alternative_name_western", "birth_year", "death_year", "gender"]]

    def find_duplicates(self, new_sheet, output_file):
        # set up output
        csv_file = open(output_file, "w")
        writer = csv.writer(csv_file, delimiter="@")
        
        # Connect to the database
        #graph = Graph("neo4j+s://portal.chcdatabase.com:7687", [REDACTED])
        graph = Graph("neo4j+s://portal.chcdatabase.com:7687", [REDACTED])
        
        # Load in known people
        known_people = self.load_known_people(graph)

        #fill in NaN values for gender
        known_people["gender"] = known_people["gender"].fillna("Unknown")

        # Split known_people by gender
        male_known_people   = known_people[known_people["gender"].str.match("M")].copy()
        female_known_people = known_people[known_people["gender"].str.match("F")].copy()
        u_known_people      = known_people[known_people["gender"].str.match("Unknown")].copy()

        # Load in people to test
        new_people = self.load_in_people_to_test(new_sheet)
        
        writer.writerow(["Person", "Potential Duplicates"])
        
        # Iterate over all new people and test for duplicates
        for id, entry in new_people.iterrows():
            name = " ".join([entry.loc["given_name_western"], entry.loc["family_name_western"]])
            
            # Filter based on gender
            similar_people = []
            if entry.loc["gender"] == "M":
                self.compare_name_to_known_names(male_known_people, entry, similar_people)
                self.compare_name_to_known_names(u_known_people, entry, similar_people)
            elif entry.loc["gender"] == "F":
                self.compare_name_to_known_names(female_known_people, entry, similar_people)
                self.compare_name_to_known_names(u_known_people, entry, similar_people)
            else:
                self.compare_name_to_known_names(male_known_people, entry, similar_people)
                self.compare_name_to_known_names(female_known_people, entry, similar_people)
                self.compare_name_to_known_names(u_known_people, entry, similar_people)
            
            row = []
            if len(similar_people) == 0:
                row = [name, "NONE"]
            else:
                people = ", ".join(similar_people) 
                row = [name, people]
            writer.writerow(row)
            print("{: >30} {: >100}".format(*row))
        csv_file.close()
    
    def find_duplicates2(self, existing_nodes, new_sheet, output_file):
        # set up output
        csv_file = open(output_file, "w")
        writer = csv.writer(csv_file, delimiter="@")
        
        # Connect to the database
        #graph = Graph("neo4j+s://portal.chcdatabase.com:7687", [REDACTED])
        
        # Load in known people
        known_people = read_and_split_csv(existing_nodes)

        #fill in NaN values for gender
        known_people["gender"] = known_people["gender"].fillna("Unknown")
        known_people["death_year"] = known_people["death_year"].fillna("3000")
        known_people["birth_year"] = known_people["birth_year"].fillna("3000")

        # Split known_people by gender
        male_known_people   = known_people[known_people["gender"].str.match("M")].copy()
        female_known_people = known_people[known_people["gender"].str.match("F")].copy()
        u_known_people      = known_people[known_people["gender"].str.match("Unknown")].copy()

        # Load in people to test
        new_people = self.load_in_people_to_test(new_sheet)
        
        writer.writerow(["Person", "Potential Duplicates"])
        
        # Iterate over all new people and test for duplicates
        for id, entry in new_people.iterrows():
            name = " ".join([entry.loc["given_name_western"], entry.loc["family_name_western"]])
            
            # Filter based on gender
            similar_people = []
            if entry.loc["gender"] == "M":
                self.compare_name_to_known_names2(male_known_people, entry, similar_people)
                self.compare_name_to_known_names2(u_known_people, entry, similar_people)
            elif entry.loc["gender"] == "F":
                self.compare_name_to_known_names2(female_known_people, entry, similar_people)
                self.compare_name_to_known_names2(u_known_people, entry, similar_people)
            else:
                self.compare_name_to_known_names2(male_known_people, entry, similar_people)
                self.compare_name_to_known_names2(female_known_people, entry, similar_people)
                self.compare_name_to_known_names2(u_known_people, entry, similar_people)
            
            row = []
            if len(similar_people) == 0:
                row = [name, "NONE"]
            else:
                people = ", ".join(similar_people) 
                row = [name, people]
            writer.writerow(row)
            print("{: >30} {: >100}".format(*row))
        csv_file.close()
    

start_time = time.time()
obj1 = FindDuplicates()
obj1.find_duplicates(new_sheet='inputsmall.csv',output_file='output.csv')
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Elapsed time for original: {elapsed_time} seconds")

start_time = time.time()
obj2 = FindDuplicates()
obj2.find_duplicates2(existing_nodes='input.csv',new_sheet='inputsmall.csv',output_file='output2.csv')
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Elapsed time for new algorithm: {elapsed_time} seconds")