import env
import pandas as pd
import os
import numpy as np

def get_zillow():
    '''
    This function acquires the requisite zillow data from the Codeup SQL database and caches it locally it for future use in a csv 
    document; once the data is accessed the function then returns it as a dataframe.
    '''

    filename = "zillow.csv"

    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        query = '''
        SELECT 
            bedroomcnt, 
            bathroomcnt, 
            calculatedfinishedsquarefeet, 
            taxvaluedollarcnt, 
            yearbuilt, 
            taxamount, 
            fips
        FROM 
            properties_2017 
        JOIN
            propertylandusetype USING (propertylandusetypeid)
        WHERE
            propertylandusedesc = 'Single Family Residential';        
        '''
        url = env.get_db_url('zillow')
        df = pd.read_sql(query, url)
        df.to_csv(filename, index = False)

        return df  

def not_outlier(df_column, thresh=3.5):
    """
    Returns a boolean array with True if points are not outliers and False 
    otherwise.

    Parameters:
    -----------
        df_column : An numobservations by numdimensions array of observations
        thresh : The modified z-score to use as a threshold. Observations with
            a modified z-score (based on the median absolute deviation) greater
            than this value will be classified as outliers.

    Returns:
    --------
        mask : A numobservations-length boolean array.

    References:
    ----------
        Boris Iglewicz and David Hoaglin (1993), "Volume 16: How to Detect and
        Handle Outliers", The ASQC Basic References in Quality Control:
        Statistical Techniques, Edward F. Mykytka, Ph.D., Editor. 
    """
    if len(df_column.shape) == 1:
        df_column = np.array(df_column).reshape(-1,1)
    median = np.median(df_column, axis=0)
    diff = np.sum((df_column - median)**2, axis=-1)
    diff = np.sqrt(diff)
    med_abs_deviation = np.median(diff)

    modified_z_score = 0.6745 * diff / med_abs_deviation

    return modified_z_score < thresh

def minimum_sqr_ft(df):
    '''
    Function that takes in a dataframe and finds the minimum sq footage necessary given an input number of bathrooms and bedrooms.
    
    Returns a total minimum amount
    '''
    # min square footage for type of room
    bathroom_min = 10
    bedroom_min = 70
    
    # total MIN sqr feet
    total = (df.bathroomcnt * bathroom_min) + (df.bedroomcnt * bedroom_min)
    # return MIN sqr feet
    return total

def clean_sqr_feet(df):
    '''
    Takes in a dataframe finds the theoretical minimum sq footage given bathroom and bedroom inputs and compares that to the actual
    given sq footage.  
    Returns a dataframe where containing results only having an actual sq footage larger than the calculate minimum.
    '''
    # get MIN sqr ft
    min_sqr_ft = minimum_sqr_ft(df)
    # return df with sqr_ft >= min_sqr_ft
    # change 'sqr_ft' to whichever name you have for sqr_ft in df
    return df[df.calculatedfinishedsquarefeet >= min_sqr_ft]

def map_counties(df):
    # identified counties for fips codes 
    counties = {6037: 'los_angeles',
                6059: 'orange',
                6111: 'ventura'}
    # map counties to fips codes
    df.fips = df.fips.map(counties)
    df.rename(columns=({ 'fips': 'county'}), inplace=True)
    return df

def wrangle_zillow(df):
    '''
    This function takes in an uncleaned zillow dataframe and peforms various cleaning functions. It returns a cleaned zillow dataframe.
    '''

    # Dropping ALL nulls from the dataset
    df = df.dropna()

    # Getting rid of invalid, wrong, or incorrectly entered data
    df = df[df.bedroomcnt != 0]
    df = df[df.bathroomcnt != 0]
    df = df[df.calculatedfinishedsquarefeet >= 70]
    
    # Changing datatypes for selected columns to improve efficiency
    df.bedroomcnt = df.bedroomcnt.astype('uint8')
    df.bathroomcnt = df.bathroomcnt.astype('float16')
    df.yearbuilt = df.yearbuilt.astype('uint16')
    df.fips = df.fips.astype('uint16')

    # Running the not_outlier function to get rid of outliers with a z score of greater than 5.5. 
    # Keeps the vast majority of data and makes it more applicable.
    df = df[not_outlier(df.taxvaluedollarcnt, thresh=5.5)]
    df = df[not_outlier(df.calculatedfinishedsquarefeet, thresh=5.5)]

    # Getting rid of nonsense entries where the house has a sq footage value smaller than a theoretical minimum
    df = clean_sqr_feet(df)

    # Changing the fips column in the dataframe to show actual counties represented by fips number
    df = map_counties(df)

    return df
    
