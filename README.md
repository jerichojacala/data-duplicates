# data-duplicates

Changes
Created new methods that access existing database through csv files rather than through Cypher. After preliminary tests, the new method appears to significantly reduce runtime. This involved creating new helper methods that were compatible with pandas.

Files
connect_db.py - demonstration of how to compare strings - more of a proof of concept for FindDuplicates.py, not really worth exploring
merge.py - working Python file on merging duplicate records in Cypher - since we are moving away from Cypher, again not really worth exploring
FindDuplicates.py - seeks to list the number of duplicates for each new record specified in a csv file - running FindDuplicates.py will find duplicates for hardcoded files
duplicate1.py - redundant helper methods already specified in FindDuplicates.py

input.csv - database nodes version 2.3
inputsmall.csv - a smaller subset of input.csv used for testing
output.csv - file to which FindDuplicates.py stores output