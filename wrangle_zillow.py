import pandas as pd
import numpy as np

import env
import os

def get_zillow():
    '''
    This function acquires the requisite zillow data from the Codeup SQL database and caches it locally it for future use in a csv 
    document; once the data is accessed the function then returns it as a dataframe.
    '''

    filename = "zillow.csv"

    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        query = query = '''
        SELECT 
        `parcelid`,
        `airconditioningtypeid`,
        `architecturalstyletypeid`,
        `basementsqft`,
        `bathroomcnt`,
        `bedroomcnt`,
        `buildingclasstypeid`,
        `buildingqualitytypeid`,
        `calculatedbathnbr`,
        `decktypeid`,
        `finishedfloor1squarefeet`,
        `calculatedfinishedsquarefeet`,
        `finishedsquarefeet12`,
        `finishedsquarefeet13`,
        `finishedsquarefeet15`,
        `finishedsquarefeet50`,
        `finishedsquarefeet6`,
        `fips`,
        `fireplacecnt`,
        `fullbathcnt`,
        `garagecarcnt`,
        `garagetotalsqft`,
        `hashottuborspa`,
        `heatingorsystemtypeid`,
        `latitude`,
        `longitude`,
        `lotsizesquarefeet`,
        `poolcnt`,
        `poolsizesum`,
        `pooltypeid10`,
        `pooltypeid2`,
        `pooltypeid7`,
        `propertycountylandusecode`,
        `propertylandusetypeid`,
        `propertyzoningdesc`,
        `rawcensustractandblock`,
        `regionidcity`,
        `regionidcounty`,
        `regionidneighborhood`,
        `regionidzip`,
        `roomcnt`,
        `storytypeid`,
        `threequarterbathnbr`,
        `typeconstructiontypeid`,
        `unitcnt`,
        `yardbuildingsqft17`,
        `yardbuildingsqft26`,
        `yearbuilt`,
        `numberofstories`,
        `fireplaceflag`,
        `structuretaxvaluedollarcnt`,
        `taxvaluedollarcnt`,
        `assessmentyear`,
        `landtaxvaluedollarcnt`,
        `taxamount`,
        `taxdelinquencyflag`,
        `taxdelinquencyyear`,
        `censustractandblock`,
        `propertylandusedesc`,
        `logerror`,
        `transactiondate`
        
        FROM `properties_2017`
        JOIN
            propertylandusetype USING (propertylandusetypeid)
        JOIN
            predictions_2017 USING (parcelid)
        Where
            propertylandusedesc = 'Single Family Residential' AND 
            transactiondate LIKE '2017-%%';   
        '''
        url = env.get_db_url('zillow')
        df = pd.read_sql(query, url)
        df.to_csv(filename, index = False)

        return df 







def obs_attr(df):
    num_rows_missing = []
    pct_rows_missing = []
    column_name = []
    for column in df.columns.tolist():
        num_rows_missing.append(df[column].isna().sum())
        pct_rows_missing.append(df[column].isna().sum() / len(df))
        column_name.append(column)
    new_info = {'column_name':column_name, 'num_rows_missing': num_rows_missing, 'pct_rows_missing': pct_rows_missing}
    return pd.DataFrame(new_info, index=None)

def drop_undesired(df, prop_required_column = .9, prop_required_row = .9):
    ''' This function takes in a dataframe and drops columns based on whether it meets the threshold for having values
    in rows and not null values. It then drops any rows based on whether it meets the threshold for having enough
    values in the row.
    
    Arguments: df - a dataframe
                prop_required_column - the proportion of a given column that must be filled by values and not nulls
                prop_required_row - the proportion of a given row that must be filled by values and not nulls
    Returns: a dataframe which no longer has the rows and columns dropped that didn't meet the threshhold.
    '''
    for column in df.columns.tolist():
        if 1-(df[column].isna().sum() / len(df)) < prop_required_column:
            df = df.drop(column, axis = 1)
            
    for row in range(len(df)):
        if 1-(df.loc[row].isna().sum() / len(df.loc[row])) < prop_required_row:
            df = df.drop(row, axis=0)
    return df