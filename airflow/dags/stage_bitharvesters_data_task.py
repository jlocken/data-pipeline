#!/usr/bin/env python3
from steamaco_etl import *
from steama_utils import *
from sqlalchemy import create_engine


if __name__=='__main__':
    
    try:
    
        util =Utils()   
        params_dict=util.params_dict
        
        engine=create_engine(params_dict['database']['connection_string'])
        #create etl object
        etl=steamaco_etl(params_dict)
        token=etl.get_auth_token()
        
        #Columns to be extracted from the api response
        fetch_cols=params_dict['api_columns']['bitharvesters']
        
        bitharvesters_data_from_api= etl.get_data_from_api('bit_harvesters',token, fetch_cols)
        
        #Dump customers data  into staging table customers
        etl.insert_data_into_database(engine, bitharvesters_data_from_api, 'bit_harvesters', 'steamaco_staging', 1000)
        print("Task executed successfully")
    except Exception as e:
        
        print("Task Failed to execute")
        print(e)