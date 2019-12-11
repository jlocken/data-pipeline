#!/usr/bin/env python3
from steamaco_etl import *
from steama_utils import Utils
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
        fetch_cols=params_dict['api_columns']['agents']
        
        agents_data_from_api= etl.get_data_from_api('agents',token, fetch_cols)
        
        #Dump customers data  into staging table customers
        etl.insert_data_into_database(engine, agents_data_from_api, 'agents', 'steamaco_staging', 1000)
        print("Task completed Succesfully")
        
    except Exception as e:
        print("Task failed to execute")
        print(e)
    
    

  
 