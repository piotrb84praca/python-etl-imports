import pandas as pd
from sqlalchemy import text
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyProjectLastPils:
    def __init__(self):
            self.mssql_conn = ConnectFort()
            self.oracle_engine = ConnectOracleDevEngine()           
            self.start_pos = 0
            self.batch_size = 15000
            self.logger = ImportLogger()
            self.oracle_connection = self.oracle_engine.connect()                 

    def deleteFromOracle(self):        
                
                # Truncate the target table in Oracle
                with self.oracle_connection.begin():
                    cursor = self.oracle_engine.get_cursor()
                    cursor.execute("BEGIN KLIK.CLEAN_TABLE('IMPORT_EASY_LAST_PROJECT_PIL'); END;")   
                
    def importToOracle(self,df): 
               
                 # Prepare insert statement
                insert_stmt = text("""
                    INSERT INTO KLIK.IMPORT_EASY_LAST_PROJECT_PIL ( 
                        ID_SOW,
                        PROJECT_NUMBER,
                        PROJECT_CREATE_DATE,
                        ID_PIL_MOBILE,
                        PIL_CREATE_DATE)
                    VALUES (
                        :ID_SOW,
                        :PROJECT_NUMBER,
                        :PROJECT_CREATE_DATE,
                        :ID_PIL_MOBILE,
                        :PIL_CREATE_DATE
                    )
                """)
    
                # Insert new data into Oracle using executemany for bulk insert
                self.oracle_connection.execute(insert_stmt, df.to_dict(orient='records'))
                # Commit the transaction
                self.oracle_connection.commit()

                # Log the number of rows affected
                #self.logger.add_to_import(f"""Rows imported {self.start_pos} """) 
                
        


    def parseData(self, df):


                    return df
        
    def run(self):
            try:
                
                self.logger.start_import(self.__class__.__name__)
    
                # Connect to MSSQL
                mssql_connection = self.mssql_conn.connect()
                self.deleteFromOracle()
                
                # Extract data from MSSQL Query 1
                query = """               
                select
                a.elob_sowid as [ID_SOW]
                ,o.elob_OpportunityID as [PROJECT_NUMBER]
                ,format(o.CreatedOn,'yyyy-MM-dd') as [PROJECT_CREATE_DATE]
                ,isnull(pl.elob_PL_Id,'') as [ID_PIL_MOBILE]
                ,format(ap.CreatedOn,'yyyy-MM-dd') as [PIL_CREATE_DATE]
            from
                accountbase a 
                join StringMapBase mapa on a.elob_BusinessSegment=mapa.AttributeValue and mapa.ObjectTypeCode=1 and mapa.[LangId]=1033 and mapa.AttributeName='elob_BusinessSegment'
                join OpportunityBase o on a.accountid=o.customerid
                left join elob_plBase pl on o.OpportunityId=pl.elob_Project and pl.elob_PLType=2
                left join ActivityPointerBase ap on pl.activityid=ap.ActivityId
            where
                a.elob_BusinessSegment in ('1','2') and a.StateCode=0
                and
                ((ap.createdon >=(getdate()-90) or ap.createdon is null) )
                """

                cursor = mssql_connection.cursor() 
                cursor.execute(query)    
                data = cursor.fetchall()
              
                # Convert fetched data to DataFrame
                columns = [column[0] for column in cursor.description]  # Get column names
                df = pd.DataFrame(data, columns=columns)
                df = self.parseData(df)                
                
                while self.start_pos < len(data):
                   
                    dt = df[self.start_pos:self.start_pos +self.batch_size]
                    self.start_pos += self.batch_size
                    self.importToOracle(dt)
        
                #self.importAgregateToOracle()    
                self.logger.end_import(len(data))
                
                
            except Exception as e:
                self.logger.log_error(str(e))
                if self.mssql_conn.connection:
                    self.mssql_conn.close()
                if self.oracle_engine.connection:
                    self.oracle_engine.close()

