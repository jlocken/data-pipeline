import sqlalchemy as db
import pandas as pd 
from steama_utils import *
from pandas.io import sql


if __name__=='__main__':
    
    
    try:
        util =Utils()   
        params_dict=util.params_dict
        engine=db.create_engine(params_dict['database']['connection_string'])
        connection=engine.connect()
        metadata=db.MetaData()
        
        power_usage = db.Table('power_usage_measures', metadata, autoload=True, autoload_with=engine,schema='steamaco_staging')
        query = db.select([power_usage])
        ResultProxy = connection.execute(query)
        
        flag=True
        while flag:
            partial_results=ResultProxy.fetchmany(1000)
            

            if(partial_results)!=[]:
                
                query=params_dict['database']['queries']['insert_usage']
                sql.execute(query, engine, params=partial_results)
            
            else:
                flag=False
        ResultProxy.close()
        
        print("Power Usage data pushed from staging to fact table")

    except Exception as e:
        print("Failed to push power usage data data from staging to fact")
        print(e)
    
    