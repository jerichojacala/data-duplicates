# data-duplicates

Overview
In this repository exists a series of scripts to clean and manage duplicate detection in the database. Many of the scripts take csv files as input and output csv files as well. The reason for this is that accessing the Cypher database is time consuming and possibly insecure if a proper keyring is not used. Specific details for each file can be found below in "Important files."

Important files
connect_db.py - demonstration of how to compare strings - more of a proof of concept for FindDuplicates.py, not really worth exploring
merge.py - working Python file on merging duplicate records in Cypher - since we are moving away from Cypher, again not really worth exploring
FindDuplicates2.py - seeks to list the number of duplicates for each new record specified in a csv file - running FindDuplicates.py will find duplicates for hardcoded files
duplicate1.py - redundant helper methods already specified in FindDuplicates.py. Before running, replace [REDACTED] with the correct credentials to access the Cypher database where required.
institutiondetection.py - script to compare Tianfeng institution dataset to current data, printing out any duplicate records. It currently compares the "name1" column from the existing data to the "western name" of the new data, and prints out any identical matches. Note that more work could be done to refine this algorithm to catch more duplicates!
mergeinstitution.py - cleans the existing data, removing identical records that have been flagged as both checked and as duplicates, leaving only unique records

input.csv - database nodes version 2.3
inputsmall.csv - a smaller subset of input.csv used for testing
outputForTianfengCleaning.csv - file of proposed additions to the institution dataset
Institution Duplication Check - Sheet1 - file of current institutions in the database

Pipfile/Pipfile.lock - files which handle dependencies using Pipenv - they list the dependencies required, but are not needed if you are not using Pipenv
