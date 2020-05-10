import numpy as np
import Mod_matching_functs as mfunc
import nnet_model as nn

#-------------------------------------------------------
#This function takes the matching algorithm and productionizes for 2 data frames
#-------------------------------------------------------
def main_match_processing(left_tbl, right_tbl, field_pairs, score_method = 'Cosine', use_ml_match = False, result_collection_threshold_score = None, true_match_score_threshhold = 1):
    if result_collection_threshold_score == None:
        result_collection_threshold_score = list(np.repeat(0,len(field_pairs)))
    
    cart_prod = mfunc.cross_join(left_tbl,right_tbl)
    
    results = mfunc.match_score(cart_prod, field_pairs[0][0], field_pairs[0][1], collection_score_treshhold = result_collection_threshold_score[0], true_match_score_threshhold = true_match_score_threshhold, score_method = score_method)
    if 'cos_score' in results.columns:
        results.rename(columns ={'cos_score':'cos_score' + field_pairs[0][0] + '_' +  field_pairs[0][1]})
    if 'jaccardian_score' in results.columns:
        results.rename(columns ={'jaccardian_score':'jaccardian_score' + field_pairs[0][0] + '_' +  field_pairs[0][1]})

    if len(field_pairs) > 1:
        for i in range(1,len(field_pairs)):
           r2 = mfunc.match_score(cart_prod, field_pairs[i][0], field_pairs[i][1], collection_score_treshhold = result_collection_threshold_score[i], true_match_score_threshhold = true_match_score_threshhold, score_method = score_method)
           if 'cos_score' in r2.columns:
               results['cos_score' + field_pairs[i][0] + '_' +  field_pairs[i][1]] = r2['cos_score']
           if 'jaccardian_score' in r2.columns:
               results['jaccardian_score' + field_pairs[i][0] + '_' +  field_pairs[i][1]] = r2['jaccardian_score']
    if use_ml_match == True:
        m = nn.nnet_model(nn.input_shape, nn.dense_layers, nn.dense_units)
        m.load_weights('saved_weights.h5f')
        X = results.to_numpy()
        results['true_match_1'] = m.predict(X)
    return results
