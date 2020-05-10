#This modeule contains string matching algorithms that produce a score between 0 and 1



import re, pandas as pd, math, time
from collections import Counter
import numpy as np


#creates cross join (cartisian product)
def cross_join(df1, df2):
    df1['key'] = 0
    df2['key'] = 0
    comb_df = pd.merge(df1, df2,on='key')
    return comb_df

def text_to_vector(text):
    WORD = re.compile(r'\w+') #function that grabs all words in a string (tokenizes) based on the spaces. uses \w+ as a setting for this
    words = WORD.findall(text)
    return Counter(words)


print('test')
#creates function that takes a string and puts all the words into a vector and counts the number of times each word comes up in the string and calculates a score of similiarity
#cosine function calculates the similarity between 2 vectors based on the angle between them: cos(theta) = (A dot B)/(lenth(A) * lenth(B)) where (A dot B) is the intercetion of the of the two vectors
def match_score(df, field1, field2, collection_score_treshhold = 0, true_match_score_threshhold = 1, score_method = 'Cosine'):
    #tokenize field 1 and 2
    dfnull = df[(pd.isnull(df[field1]) == True) | (pd.isnull(df[field2]) == True)]
    nullidx = dfnull.index.values
    if len(nullidx)>0:
        df.drop(df.index[nullidx], inplace=True)
        df.reset_index(inplace = True)
    if score_method == 'Cosine':
        l1 = []
        for j in range(0,df.shape[0]):
            l1.append(text_to_vector(df.loc[j,field1]))
        l2 = []
        for j in range(0,df.shape[0]):
            l2.append(text_to_vector(df.loc[j,field2]))
        #Calculating cosine numererator
        numerator = []
        for j in range(0,df.shape[0]):
            intersection = set(l1[j].keys()) & set(l2[j].keys())
            numerator.append(sum([l1[j][x] * l2[j][x] for x in intersection]))
        #Calculating cosine denominator
        denominator=[]
        for j in range(0,df.shape[0]):
            denominator.append(math.sqrt(sum(np.square(list(l1[j].values())))) * math.sqrt(sum(np.square(list(l2[j].values())))))
        #creating results
        df['cos_score'] = np.divide(numerator,denominator)
        df['true_match_1'] = np.where(df['cos_score'] >= true_match_score_threshhold, 1, 0)
        df['date_matched'] = time.strftime("%Y-%m-%d")
        df['method'] = 'Cosine'
        df = df[df['cos_score' ]>= collection_score_treshhold]
        return df
    if score_method == 'Jaccardian':
        intersection = []
        for j in range(0,df.shape[0]):
            intersection.append(len(set.intersection(*[set(df.loc[j,field1]), set(df.loc[j,field2])]))) #intersection_cardinality
        union = []
        for j in range(0,df.shape[0]):
            union.append(len(set.union(*[set(df.loc[j,field1]), set(df.loc[j,field2])]))) #union_cardinality
        df['jaccardian_score'] = np.divide(intersection,union)
        df['true_match_1'] = np.where(df['jaccardian_score'] >= true_match_score_threshhold, 1, 0)
        df['date_matched'] = time.strftime("%Y-%m-%d")
        df['method'] = 'Jaccardian'
        df = df[df['jaccardian_score']>= collection_score_treshhold]
        return df
    if score_method == 'both':
        l1 = []
        for j in range(0,df.shape[0]):
            l1.append(text_to_vector(df.loc[j,field1]))
        l2 = []
        for j in range(0,df.shape[0]):
            l2.append(text_to_vector(df.loc[j,field2]))
        #Calculating cosine numererator
        numerator = []
        for j in range(0,df.shape[0]):
            intersection = set(l1[j].keys()) & set(l2[j].keys())
            numerator.append(sum([l1[j][x] * l2[j][x] for x in intersection]))
        #Calculating cosine denominator
        denominator=[]
        for j in range(0,df.shape[0]):
            denominator.append(math.sqrt(sum(np.square(list(l1[j].values())))) * math.sqrt(sum(np.square(list(l2[j].values())))))
        #creating results
        df['cos_score'] = np.divide(numerator,denominator)
        intersection = []
        for j in range(0,df.shape[0]):
            intersection.append(len(set.intersection(*[set(df.loc[j,field1]), set(df.loc[j,field2])]))) #intersection_cardinality
        union = []
        for j in range(0,df.shape[0]):
            union.append(len(set.union(*[set(df.loc[j,field1]), set(df.loc[j,field2])]))) #union_cardinality
        df['jaccardian_score'] = np.divide(intersection,union)
        df['true_match_1_Cosine'] = np.where(df['cos_score'] >= true_match_score_threshhold, 1, 0)
        df['true_match_1_Jaccardian'] = np.where(df['jaccardian_score'] >= true_match_score_threshhold, 1, 0)
        df['date_matched'] = time.strftime("%Y-%m-%d")
        df['method'] = 'Cosine and Jaccardian'
        df = df[(df['cos_score']>= collection_score_treshhold) | (df['jaccardian_score']>= collection_score_treshhold)]
        return df
    else:
        print('Error, select valid method: Cosine, Jaccardian, or both')

