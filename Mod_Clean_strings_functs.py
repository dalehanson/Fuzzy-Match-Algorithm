# -*- coding: utf-8 -*-
"""
Created on Thu Jan 31 15:08:39 2019

@author: hansond
"""

import re, pandas as pd, pyodbc, os, datetime, time, sys, math
from pandas import DataFrame
from collections import Counter
import numpy as np

def clean_company_names(df, colname):
    df[colname] = df[colname].str.lower()
    df[colname]  = df[colname].str.replace('federal credit union' , '')
    df[colname] = df[colname].str.replace('credit union' , '')
    df[colname] = df[colname].str.replace('c.u.' , '')
    df[colname] = df[colname].str.replace('cred un' , '')
    df[colname] = df[colname].str.replace('fcu' , '')
    df[colname] = df[colname].str.replace(' cu ' , '')
    df[colname] = df[colname].str.replace(' national association' , '')
    df[colname] = df[colname].str.replace('bb&t' , 'branch banking and trust company')
    df[colname] = df[colname].str.replace('bbva compass' , 'compass bank')
    df[colname] = df[colname].str.replace(', n.a.' , '')
    df[colname] = df[colname].str.replace(' & ' , ' and ')
    df[colname] = df[colname].str.replace('.' , '')
    df[colname] = df[colname].str.replace(',' , '')
    df[colname] = df[colname].str.replace('the ' , '')
    df[colname] = df[colname].str.replace('\.' , ' ')
    df[colname] = df[colname].str.replace('/' , ' ')
    df[colname] = df[colname].str.rstrip()


def clean_addresses(df, colname):
    df[colname] = df[colname].str.lower()
    df[colname] = df[colname].replace({'\r': ' '}, regex=True)
    df[colname] = df[colname].replace('\n',' ', regex=True)
    df[colname] = df[colname].str.replace('\.' , '')
    df[colname] = df[colname].str.split(' ste ', expand = True).loc[:,0]
    df[colname] = df[colname].str.split(' suite ', expand = True).loc[:,0]
    df[colname] = df[colname].str.split(' fl ', expand = True).loc[:,0]
    df[colname] = df[colname].str.split(' floor ', expand = True).loc[:,0]
    df[colname] = df[colname].str.split(' rm ', expand = True).loc[:,0]
    df[colname] = df[colname].str.split(' room ', expand = True).loc[:,0]
    df[colname] = df[colname].str.split(' bldg ', expand = True).loc[:,0]
    df[colname] = df[colname].str.split(' building ', expand = True).loc[:,0]
    df[colname] = df[colname].str.split(' dept ', expand = True).loc[:,0]
    df[colname] = df[colname].str.split('#', expand = True).loc[:,0]
    df[colname] = df[colname].str.split(',', expand = True).loc[:,0]
    df[colname] = df[colname].str.replace(' n ' , ' ')
    df[colname] = df[colname].str.replace(' s ' , ' ')
    df[colname] = df[colname].str.replace(' e ' , ' ')
    df[colname] = df[colname].str.replace(' w ' , ' ')
    df[colname] = df[colname].str.replace(' north ' , ' ')
    df[colname] = df[colname].str.replace(' south ' , ' ')
    df[colname] = df[colname].str.replace(' east ' , ' ')
    df[colname] = df[colname].str.replace(' west ' , ' ')
    df[colname] = df[colname].str.replace(' al ' , '')
    df[colname] = df[colname].str.replace(' no ' , '')
    df[colname] = df[colname].str.replace(' co ' , '')
    df[colname] = df[colname].str.replace(' sq ' , '')
    df[colname] = df[colname].str.replace(' street?' , ' st ')
    df[colname] = df[colname].str.replace(' avenue?' , ' ave ')
    df[colname] = df[colname].str.replace(' drive?' , ' dr ')
    df[colname] = df[colname].str.replace(' boulevard?' , ' blvd ')
    df[colname] = df[colname].str.replace(' road ' , ' rd ')
    df[colname] = df[colname].str.replace(' lane ' , ' ln ')
    df[colname] = df[colname].str.replace(' hwy ' , ' highway ')
    df[colname] = df[colname].str.rstrip()


