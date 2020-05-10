# -*- coding: utf-8 -*-
"""
Created on Tue Aug  6 09:26:31 2019

@author: dale.hanson
"""


import pandas as pd, os, time


path_of_program_file = 'C:/Users/dale.hanson/Fuzzy Match Algorithm'

if os.path.exists(path_of_program_file) == True:
    os.chdir(path_of_program_file)
    import Mod_Clean_strings_functs as cln_func
    import Mod_easy_import_export_database as dbtools
    import Matching_Main_Ingestion_funct as ingest
else:
    print('File path does not exist!')


#-----------------------------------------------------------
#Importing data
#-----------------------------------------------------------
 
left_tbl = pd.read_csv('C:/Users/dale.hanson/Desktop/people.csv', encoding = "ISO-8859-1")
right_tbl = pd.read_csv('C:/Users/dale.hanson/Desktop/companies.csv',encoding = "ISO-8859-1")



#-----------------------------------------------------------
#Prepping data
#-----------------------------------------------------------
t0 = time.time()

left_tbl.columns[1:20]
right_tbl.columns[1:20]

#Prepping data
left_tbl['Email'].str.split('@', expand = True).loc[:,1]
right_tbl['Name'].str.replace('[^A-Za-z0-9 ]+','')

#Cleaning Name Data
print('Cleaning Names')
cln_func.clean_company_names(left_tbl,'company')
cln_func.clean_company_names(right_tbl,'Company')


#Getting email domain
print('Cleaning Address Data')
left_tbl['domain'] = cln_func.convert_email_address_to_domain(left_tbl,'email')
right_tbl['domain'] = cln_func.clean_addresses(right_tbl, 'email')


#Setting fields to match
field_pairs = [('Company','Name'),('Email','Email Domain')] #Use pairs in list format (ex. ) [('field1','field2'),('field1','field3')]



#-----------------------------------------------------------
#Matching tables
#-----------------------------------------------------------
print('Calculating Main Scores')
st = time.time()
results = ingest.main_match_processing(left_tbl, right_tbl, field_pairs, score_method = 'Cosine', result_collection_threshold_score = .5, true_match_score_threshhold = .95)
print(round((time.time() - st),2), 'seconds process time for round')


#-----------------------------------------------------------
#Exporting results
#-----------------------------------------------------------


if results.shape[0] >0:
    dbtools.database_import(results,'Matching Process Tables','Match_Results_Company')
else:
    print('No possible matches found')
print(round((time.time() - t0)/60,2), 'minutes total program process time')

