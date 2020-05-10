
import pandas as pd, pyodbc, os, time
from pandas import DataFrame
from collections import Counter
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine

if os.path.exists("P:/Marketing Project Folders/Dale/Matching Process") == True:
    os.chdir("P:/Marketing Project Folders/Dale/Matching Process")
if os.path.exists("N:\Marketing_new\Marketing Project Folders\Dale\Matching Process") == True:
    os.chdir("N:\Marketing_new\Marketing Project Folders\Dale\Matching Process")
 
from Mod_Clean_strings_functs import *
from Mod_matching_functs import *
from Mod_easy_import_export_vasu import *
from nnet_model import *

#SQL data tables to load
sqlQueryCusts = "SELECT * FROM [Matching Process Tables].[dbo].[Unmatched_Bank_and_CU_customers] where Master_ID is not null" 
sqlQueryExternal1 = "SELECT * FROM [Matching Process Tables].[dbo].[FI_HQ_Data_for_matching]"
sqlQueryExternal2 = "SELECT * FROM [Matching Process Tables].[dbo].[FI_Branch_Data_for_Matching]"



#Setting score thresholds
#NAZ_true_match_score = .85
#NZ_true_match_score = .95
result_collection_threshold_score_NAZ = 0.6
result_collection_threshold_score_NZ = 0.8

m = nnet_model(input_shape, dense_layers, dense_units)
m.load_weights('saved_weights.h5f')



t0 = time.time()

#Prepping data
print('Prepping data')
custs = get_vasu_data(sqlQueryCusts)
Ext1 = get_vasu_data(sqlQueryExternal1)
Ext2 = get_vasu_data(sqlQueryExternal2)

#Cleaning Name Data
print('Cleaning Names')
clean_company_names(custs,'CustName')
clean_company_names(Ext1,'Institution Name')
clean_company_names(Ext2,'Institution Name')


#Cleaning Address Data
print('Cleaning Address Data')
clean_addresses(custs, 'ShipAddr1')
custs['ShipSt'] = custs['ShipSt'].str.strip()
custs['ShipSt'] = custs['ShipSt'].str.lower()
clean_addresses(Ext1, 'site_address')
Ext1['State'] = Ext1['State'].str.strip()
Ext1['State'] = Ext1['State'].str.lower()
clean_addresses(Ext2, 'site_address')
Ext2['State'] = Ext2['State'].str.strip()
Ext2['State'] = Ext2['State'].str.lower()


#Creating NAZ field for matching
print('Creating NAZ field for matching')
custs['NAZ_matching'] = custs.loc[:,'CustName'] + ' ' + custs.loc[:,'ShipAddr1'] + ' ' + custs.loc[:,'zip5']
Ext1['NAZ_matching'] = Ext1.loc[:,'Institution Name'] + ' ' + Ext1.loc[:,'site_address'] + ' ' + Ext1.loc[:,'zip']
Ext2['NAZ_matching'] = Ext2.loc[:,'Institution Name'] + ' ' + Ext2.loc[:,'site_address'] + ' ' + Ext2.loc[:,'ZIP']

#Creating NZ field for matching
print('Creating NZ field for matching')
custs['NZ_matching'] = custs.loc[:,'CustName'] + ' ' + custs.loc[:,'zip5']
Ext1['NZ_matching'] = Ext1.loc[:,'Institution Name'] + ' ' + Ext1.loc[:,'zip']
Ext2['NZ_matching'] = Ext2.loc[:,'Institution Name'] + ' ' + Ext2.loc[:,'ZIP']



results_df = DataFrame(columns=('CustNbr','ShiptoCode','Database_ID','Cust_vector','Ext_data_vector','score','match_variables','date_matched','method','true_match_1'))
results_df =results_df.astype({'CustNbr': str, 'ShiptoCode': str, 'Database_ID': str, 'Cust_vector': str, 'Ext_data_vector': str, 'score': float, 'match_variables': str,'date_matched': object,'method': str,'true_match_1': int})

#-----------------------------------------------------------
#Matching data by Name, address, and zip code
#Matching Mains
print('Calculating Main Scores')
st = time.time()
NAZmatch = cross_join(custs,Ext1)
results = match_score(NAZmatch, 'NAZ_matching_x', 'NAZ_matching_y', result_collection_threshold_score_NAZ, true_match_score_threshhold = 1, score_method = 'both')
results = results.rename(index=str, columns={"NAZ_matching_x": "Cust_vector", "NAZ_matching_y": "Ext_data_vector", "NAZcos_score":"cos_score", "NAZjaq_score":"jaccardian_score"})
r2 = match_score(NAZmatch, 'CustName', 'Institution Name', result_collection_threshold_score_NAZ, true_match_score_threshhold = 1, score_method = 'both')
results['Name_cos_score'] = r2['cos_score']
results['Name_jac_score'] = r2['jaccardian_score']
r2 = match_score(NAZmatch, 'ShipSt', 'site_address', result_collection_threshold_score_NAZ, true_match_score_threshhold = 1, score_method = 'both')
results['street_cos_score'] = r2['cos_score']
results['street_jac_score'] = r2['jaccardian_score']
r2 = match_score(NAZmatch, 'zip5', 'zip', result_collection_threshold_score_NAZ, true_match_score_threshhold = 1, score_method = 'both')
results['zip_cos_score'] = r2['cos_score']
results['zip_jac_score'] = r2['jaccardian_score']
print(round((time.time() - st),2), 'seconds process time for round')

X = np.array(results[['NAZcos_score','NAZjaq_score','Name_cos_score','Name_jac_score','street_cos_score','street_jac_score','zip_cos_score','zip_jac_score']])
results['true_match_1'] = m.predict(X)
results_df = results



#----------------------------

#Matching Branches
print('Calculating Branch Scores')
true_matches_df = results_df[results_df['true_match_1'] == 1]
true_match_list = set(true_matches_df['CustNbr'].tolist())
custs2 = custs[~custs['CustNbr'].isin(true_match_list)]
print('branch match cust list length: '+str(custs2.shape[0]))

if custs2.shape[0] > 0:
    t1 = time.time()
    NAZmatch = cross_join(custs2,Ext2)
    results = match_score(NAZmatch, 'NAZ_matching_x', 'NAZ_matching_y', result_collection_threshold_score_NAZ, true_match_score_threshhold = 1, score_method = 'both')
    results = results.rename(index=str, columns={"NAZ_matching_x": "Cust_vector", "NAZ_matching_y": "Ext_data_vector", "NAZcos_score":"cos_score", "NAZjaq_score":"jaccardian_score"})
    r2 = match_score(NAZmatch, 'CustName', 'Institution Name', result_collection_threshold_score_NAZ, true_match_score_threshhold = 1, score_method = 'both')
    results['Name_cos_score'] = r2['cos_score']
    results['Name_jac_score'] = r2['jaccardian_score']
    r2 = match_score(NAZmatch, 'ShipSt', 'site_address', result_collection_threshold_score_NAZ, true_match_score_threshhold = 1, score_method = 'both')
    results['street_cos_score'] = r2['cos_score']
    results['street_jac_score'] = r2['jaccardian_score']
    r2 = match_score(NAZmatch, 'zip5', 'zip', result_collection_threshold_score_NAZ, true_match_score_threshhold = 1, score_method = 'both')
    results['zip_cos_score'] = r2['cos_score']
    results['zip_jac_score'] = r2['jaccardian_score']
    X = np.array(results[['NAZcos_score','NAZjaq_score','Name_cos_score','Name_jac_score','street_cos_score','street_jac_score','zip_cos_score','zip_jac_score']])
    results['true_match_1'] = m.predict(X)
    results_df = results
    results_df =results_df.append(results)
else:
    print('All customers already matched!!')


#------------------------------
#Collecting Final results
print('Collecting Final results')
results_df['Source_to_Match'] = 'FDIC-NCUA List'
results_df['match_type'] = 'Customer'
results_df['max_flag'] = ''

results_df = results_df[['CustNbr','ShiptoCode','Database_ID','Cust_vector','Ext_data_vector','date_matched','Source_to_Match','true_match_1','match_type','max_flag','NAZcos_score','NAZjaq_score','Name_cos_score','Name_jac_score','street_cos_score','street_jac_score','zip_cos_score','zip_jac_score']]

#Exporting results to VASU
if results_df.shape[0] >0:
    vasu_import(results_df,'Matching Process Tables','Match_Results_Customer')
else:
    print('No possible matches found')
print(round((time.time() - t0)/60,2), 'minutes total program process time')



#results_df.to_csv('N:/Marketing_new/Marketing Project Folders/Dale/Matching Process/Cust_results_df.csv', sep=',')
#custs.to_csv('N:/Marketing_new/Marketing Project Folders/Dale/Matching Process/Custs_df.csv', sep=',')
#Ext1.to_csv('N:/Marketing_new/Marketing Project Folders/Dale/Matching Process/Ext1_df.csv', sep=',')
#Ext2.to_csv('N:/Marketing_new/Marketing Project Folders/Dale/Matching Process/Ext2_df.csv', sep=',')
