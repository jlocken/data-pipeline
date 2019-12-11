#!/usr/bin/env python3
import json, requests, psycopg2
import pandas as pd
import dateutil.parser
from datetime import datetime, timedelta


class steamaco_etl:
    
    def __init__(self,params_dict):
        
        self.params_dict=params_dict
        
        
    
    def get_auth_token(self):
        
        auth_params=self.params_dict['auth_params']
        token_uri=self.params_dict['endpoints']['token']

        
        response=requests.post(token_uri,data=auth_params)
        
        if response.status_code==200:
            print(response)
            return response.json()['token']
        else:
            return "Error querying the api endpoint"
        

        
    def get_data_from_api(self,endpoint_name,token, cols):
    
        
        next_page=self.params_dict['endpoints'][f'{endpoint_name}']
        
        items_list=[] 
        
        while next_page:
            
            
            response = requests.get(next_page,headers={"Authorization" : "Token "+token})
            
            if(response.status_code==200):
                
                json_data=response.json()['results']
                
                for list_item in json_data:
                    items_list.append(list_item)
                  
                next_page=response.json()['next']
                
            else:
                return "Error querying the api endpoint"

        data=pd.DataFrame(items_list,columns=cols)
        
        return  data
    
    
    
    def insert_data_into_database(self, engine, data, table_name, schema, insert_chunk_size):
        
        try:
            data.to_sql(f'{table_name }',engine,schema=f'{ schema }', if_exists='replace', chunksize= insert_chunk_size,method='multi',index=False)
            
            return 'Data inserted succesfully'
        
        except Exception as e:
            return e
        
  
        
        
    def fetch_data_from_database(self,query, engine,chunksize,pd):
        
        dataframe =pd.read_sql_query(sql=query,con=engine,chunksize=chunksize)
        
        return dataframe
    
    
  
    def get_usage_data_from_api(self,meters_dataframe, auth_token,last_fetch_endtime,db_engine,pd):
        
        start_time=str(dateutil.parser.parse(str(last_fetch_endtime.values[0][0]))+timedelta(seconds=1)).replace(' ','T')
        end_time=str(dateutil.parser.parse(start_time)+timedelta(hours=24,seconds=-1)).replace(' ','T')
        

        
        usage_endpoint=self.params_dict['endpoints']['usage']
        
        usage_data_to_persist=[]
        
        for index,meter in meters_dataframe.iterrows():

            steamaco_customer_fk=float(meter['steamaco_customer_id'])
            steamaco_meter_fk=float(meter['steamaco_meter_id'])
            steamaco_site_fk=float(meter['steamaco_site_id'])
            bit_harvester_fk=float(meter['bit_harvester'])
            name=meter['first_name']

            URL= usage_endpoint.format(int(steamaco_customer_fk),int(steamaco_meter_fk),start_time, end_time)
            print(URL)

            response=requests.get(URL, headers={"Authorization" : "Token "+auth_token} )

            if(response.status_code==200):

                usage_data=response.json()

                for meter_usage in usage_data:

                    usage_data_to_persist.append((int( meter_usage['timestamp'][0:10].replace('-','')), int( meter_usage['timestamp'][11:13]),int(steamaco_customer_fk),int(steamaco_meter_fk),int(steamaco_site_fk),int(bit_harvester_fk),meter_usage['usage'],name))
                    
        dataframe=pd.DataFrame(data=usage_data_to_persist,columns=['measure_date_fk','measure_date_time_fk', 'stemaco_customer_fk','steamaco_meter_fk','steamaco_site_fk','steamaco_bit_harvester_fk','power_usage_kWh','first_name'])
        
        fetch_dates=[(start_time,end_time)]
        fetch_dates_df=pd.DataFrame(data=fetch_dates,columns=['start_date','end_date'])
        fetch_dates_df.to_sql('steamaco_fetch_dates',db_engine,schema='steamaco_staging',if_exists='append',index=False)
          
        return dataframe
    
 
            
            


