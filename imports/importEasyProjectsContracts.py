import pandas as pd
from sqlalchemy import text
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyProjectsContracts:
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
                    cursor.execute("BEGIN KLIK.CLEAN_TABLE('IMPORT_EASY_PROJECTS_CONTRACTS'); END;")
                    #self.oracle_connection.execute(text("TRUNCATE TABLE IMPORT_EASY_PROJECTS_CONTRACTS"))
                
    def importToOracle(self,df): 
               
                 # Prepare insert statement
                insert_stmt = text("""INSERT INTO IMPORT_EASY_PROJECTS_CONTRACTS (                  
                    PROJECT_NUMBER,
                    CONTRACT_NUMBER
                    )
            VALUES 
                    (
                    :PROJECT_NUMBER,
                    :CONTRACT_NUMBER
                    )""")
    
                # Insert new data into Oracle using executemany for bulk insert
                self.oracle_connection.execute(insert_stmt, df.to_dict(orient='records'))
                # Commit the transaction
                self.oracle_connection.commit()
    
    def parseData(self, df):
                   
                    return df
        
    def run(self):
            try:
                
                self.logger.start_import(self.__class__.__name__)
    
                # Connect to MSSQL
                mssql_connection = self.mssql_conn.connect()

                
                # Extract data from MSSQL Query 1
                query = """ SELECT  o.elob_OpportunityID as PROJECT_NUMBER , c.ContractNumber as CONTRACT_NUMBER
                    from B2B_MSCRM.dbo.elob_contract_opportunityBase
                    join Opportunity o on o.OpportunityId = elob_contract_opportunityBase.opportunityid
                    join contractbase c on c.ContractId = elob_contract_opportunityBase.contractid
                    where  o.CreatedOn >= '2020-01-01' """

                cursor = mssql_connection.cursor() 
                cursor.execute(query)    
                data = cursor.fetchall()
                
                self.deleteFromOracle()
                # Convert fetched data to DataFrame
                columns = [column[0] for column in cursor.description]  # Get column names
                df = pd.DataFrame(data, columns=columns)
                df = self.parseData(df)                
                
                while self.start_pos < len(data):
                   
                    dt = df[self.start_pos:self.start_pos +self.batch_size]
                    self.start_pos += self.batch_size
                    self.importToOracle(dt)        
               
                self.logger.end_import(len(data))
                
                
            except Exception as e:
                self.logger.log_error(str(e))
                if self.mssql_conn.connection:
                    self.mssql_conn.close()
                if self.oracle_engine.connection:
                    self.oracle_engine.close()

