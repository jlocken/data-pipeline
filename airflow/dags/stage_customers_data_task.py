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
        fetch_cols=params_dict['api_columns']['customers']
        
        customers_data_from_api= etl.get_data_from_api('customers',token, fetch_cols)
        
        #Dump customers data  into staging table customers
        etl.insert_data_into_database(engine, customers_data_from_api, 'customers', 'steamaco_staging', 1000)
        print("Task executed succesfully")  
    
    except Exception as e:
        print("Task failed to execute")
        print(e)