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
        
        customers = db.Table('customers', metadata, autoload=True, autoload_with=engine,schema='steamaco_staging')
        query = db.select([customers])
        ResultProxy = connection.execute(query)
        
        flag=True
        while flag:
            
            partial_results=ResultProxy.fetchmany(1000)
            
            if(partial_results)!=[]:
                query=params_dict['database']['queries']['insert_customers']
                sql.execute(query, engine, params=partial_results)
            
            else:
                flag=False
        ResultProxy.close()
        
        print("Cutomers data pushed from staging to dimension table")
    except Exception as e:
        print("Failed to push customers data from staging to dimensions")
        print(e)
   
 