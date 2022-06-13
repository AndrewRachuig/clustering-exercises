import os
import env
import pandas as pd
import numpy as np

from sklearn.preprocessing import MinMaxScaler

def get_mall():
    query = '''
    SELECT * 
    FROM customers;
    '''
    url = env.get_db_url('mall_customers')
    df = pd.read_sql(query, url)
    return df

def wrangle(df):
    # making dummies for categorical column
    dummy_df = pd.get_dummies(df, columns = ['gender'], drop_first=True, dummy_na=False)

    # Scaling columns
    columns_to_scale = dummy_df.select_dtypes('number').columns.tolist()
    scaler = MinMaxScaler()
    scaled_df = dummy_df.copy()
    scaled_df[columns_to_scale] = pd.DataFrame(scaler.fit_transform(dummy_df[columns_to_scale]),
                                          columns=columns_to_scale).set_index([dummy_df.index])
    
    return scaled_df