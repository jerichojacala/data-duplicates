from py2neo import Graph
import pandas as pd
import numpy as np 
import csv

# Connect to the database
graph = Graph("neo4j+s://portal.chcdatabase.com:7687", [redacted])

# Basic test query to see if the connection worked

# TEST 1: checking a known duplicate to see the data
df = graph.run('''
    MATCH (p:Person) 
    WHERE p.id IN ['P_000739', 'P_000408']
    RETURN p
''').to_data_frame()

# TEST 2: checking a known non-duplicate to see the data
df_1 = graph.run('''
    MATCH (p:Person) 
    WHERE p.id IN ['P_000361', 'P_033053']
    RETURN p
''').to_data_frame()

# print("Column names:", df.columns) # see what the cols are 

nationality = {} 
not_dups = {}


for idx, row in df_1.iterrows():
    person = row.iloc[0]  # get the person
    person_id = person['id']  # get the ID of the person
    person_nationality = person.get('nationality', 'Unknown')  # get the nationality, default to 'Unknown' if not present
    
    nationality[person_id] = person_nationality # update nat dict
    
    print(f"ID: {person_id} - Nationality: {person_nationality}")  # for debugging

# to check the nationality dict is being updated
print("\nNationality Dictionary:", nationality)


def check_nationality_match(nationality_dict):
    """
        helper function that checks if nationalities of two ids match
        idea: if two nationalities do not match and neither are unknown, then they cannot be duplicates
    """

    nationalities = list(nationality_dict.values()) # get all nationalities from the dictionary values
    
    # check if unknown for either nationality if so we cannot assume they are not duplicates
    if 'Unknown' in nationalities:
        print("Cannot consider entries as duplicates because one or both have an unknown nationality")
    else:
        if len(set(nationalities)) == 1: # check if all values in the list are the same
            print("The nationalities match")
        else:
            print("The nationalities do not match and none unknowns --> not duplicates")
            for person_id, person_nationality in nationality_dict.items():
                print(f"ID: {person_id} - Nationality: {person_nationality}") # for debugging


check_nationality_match(nationality)