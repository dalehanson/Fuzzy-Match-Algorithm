# -*- coding: utf-8 -*-
"""
Created on Tue Feb 26 16:44:16 2019

@author: hansond
"""

import pandas as pd
import numpy as np
import random as r
from keras.models import Sequential
from keras.layers import Dense, InputLayer
from keras.optimizers import Adam

df = pd.read_csv('matching_train.csv')

#Defining functions
def nnet_model(input_shape, dense_layers, dense_units, add_batch_norm = False, add_drop_out = False, dropout = .1):
    model = Sequential()
    model.add(InputLayer(input_shape=input_shape))
    for i in range(dense_layers):
        model.add(Dense(units=dense_units, activation='relu'))
        if add_drop_out ==True:
            model.add(Dropout(dropout))
        if add_batch_norm == True:
            model.add(BatchNormalization())
    model.add(Dense(1, activation='sigmod'))
    return model


input_shape = (8,)
dense_layers = 9
dense_units = 8
batch_size = 30
N_EPOCHS = 2000
learning_rate = .001

train_xsz = int(3/4 * df.shape[0])
train = r.sample(range(df.shape[0]), train_xsz)
x_train = np.array(df.loc[train ,df.columns[2:8]])
y_train = np.array(df.loc[train, 'true_match_1'])
x_test = np.array(df.loc[-train,df.columns[2:8]])
y_test = np.array(df.loc[-train,df.columns[2:8]])


m = nnet_model(input_shape, dense_layers, dense_units, add_batch_norm = True, add_drop_out = False, dropout = .15)
m.compile(optimizer=Adam(learning_rate), loss='binary_crossentropy')
m.fit(x_train, y_train, batch_size=batch_size, epochs=N_EPOCHS, verbose=2, shuffle=True, validation_data=(x_test, y_test))

m.save_weights('saved_weights.hdf5')