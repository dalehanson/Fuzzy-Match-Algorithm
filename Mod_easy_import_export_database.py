# -*- coding: utf-8 -*-
"""
Created on Fri Feb  1 15:33:19 2019

@author: hansond
"""
import pyodbc
from sqlalchemy import create_engine
import pandas as pd

def get_database_data(query):
    print("pull from SQL Server" )
    driver = '{SQL Server}' 
    server = 'VASU\MARKETING_DB'
    port = '1433'
    database = 'master'
    trusted_conn = 'yes'
    conn = pyodbc.connect('driver=%s;server=%s;port=%s;trusted_conn=%s;'%(driver,server,port,trusted_conn))
    #cursor = conn.cursor()
    df = pd.read_sql(query, conn)
    conn.close()
    return df



#Creating sql connection to add results back into SQL server
def database_import(df, database,table_name):
    engine = create_engine('mssql+pyodbc://PythonConnect:Python2016@VASU\MARKETING_DB/'+database+'?driver=SQL+Server')
    df.to_sql(name=table_name,con=engine ,if_exists = 'append',index = False)

