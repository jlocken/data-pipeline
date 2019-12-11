#!/usr/bin/env python3
from steamaco_etl import *
from steama_utils import *
from sqlalchemy import create_engine
import pandas as pd



if __name__=='__main__':
    
    try:
            
        util =Utils()   
        params_dict=util.params_dict
        
        engine=create_engine(params_dict['database']['connection_string'])
        #create etl object
        etl=steamaco_etl(params_dict)
        token=etl.get_auth_token()
        
        fetch_time_sql=params_dict['database']['queries']['select_fetch_date']
        meters_sql=params_dict['database']['queries']['select_meters']
        #Fetch last fetch_date from the database    
        last_end_time=etl.fetch_data_from_database(fetch_time_sql,engine,None,pd)
        
        #Fetch meter list to get usage for from the database
        meters=etl.fetch_data_from_database(meters_sql,engine,None,pd)
        
        #Get usage of the meters fetched
        usage_data= etl.get_usage_data_from_api(meters,token,last_end_time,engine,pd)
        
        #Dump usage data  to staging table
        etl.insert_data_into_database(engine, usage_data, 'power_usage_measures', 'steamaco_staging', 1000)
        
        print("Task executed succesfully")
        
    except Exception as e:
        
        print("Task Failed to execute")
        print(e)