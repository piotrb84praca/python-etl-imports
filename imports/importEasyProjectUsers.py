import pandas as pd
from sqlalchemy import text
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyProjectUsers:
    def __init__(self):
            self.mssql_conn = ConnectFort()
            self.oracle_engine = ConnectOracleDevEngine()           
            self.start_pos = 0
            self.batch_size = 15000
            self.logger = ImportLogger()
            self.oracle_connection = self.oracle_engine.connect()
    
    def importToOracle(self,df): 
               
                 # Prepare insert statement
                insert_stmt = text("""UPDATE IMPORT_EASY_PROJECTS SET
                                    USER_ICT_1 = :USER_ICT_1, 
                                    USER_ICT_2 = :USER_ICT_2, 
                                    USER_ICT_3 = :USER_ICT_3, 
                                    USER_ICT_4 = :USER_ICT_4
                                    WHERE PROJECT_NUMBER = :PROJECT_NUMBER
                                   """)
    
                # Insert new data into Oracle using executemany for bulk insert
                self.oracle_connection.execute(insert_stmt, df.to_dict(orient='records'))
                # Commit the transaction
                self.oracle_connection.commit()
    
    def parseData(self, df):

                    df['USER_ICT_1'] = df['USER_ICT_1'].fillna('').replace("/", "")
                    df['USER_ICT_2'] = df['USER_ICT_2'].fillna('').replace("/", "")
                    df['USER_ICT_3'] = df['USER_ICT_3'].fillna('').replace("/", "")
                    df['USER_ICT_4'] = df['USER_ICT_4'].fillna('').replace("/", "")
        
                    return df
        
    def run(self):
            try:
                
                self.logger.start_import(self.__class__.__name__)
    
                # Connect to MSSQL
                mssql_connection = self.mssql_conn.connect()

                
                # Extract data from MSSQL Query 1
                query = """WITH RankedUsers AS (
                                SELECT 
                                    o.elob_OpportunityID AS PROJECT_NUMBER,
                                    LOWER(REPLACE(ow.DomainName, 'TP\', '')) AS PROJECT_USER_LOGIN,
                                    ROW_NUMBER() OVER (PARTITION BY o.elob_OpportunityID ORDER BY ow.DomainName) AS rn
                                FROM elob_opportunity_systemuser t
                                INNER JOIN Opportunity o ON t.opportunityid = o.opportunityid
                                INNER JOIN SystemUser ow ON ow.SystemUserId = t.systemuserid
                                WHERE o.createdon BETWEEN '2018-01-01' AND '2025-12-31'
                                AND BusinessUnitIdName = 'ICT Sales'
                            )
                            
                            SELECT 
                                PROJECT_NUMBER,
                                MAX(CASE WHEN rn = 1 THEN PROJECT_USER_LOGIN END) AS USER_ICT_1,
                                MAX(CASE WHEN rn = 2 THEN PROJECT_USER_LOGIN END) AS USER_ICT_2,
                                MAX(CASE WHEN rn = 3 THEN PROJECT_USER_LOGIN END) AS USER_ICT_3,
                                MAX(CASE WHEN rn = 4 THEN PROJECT_USER_LOGIN END) AS USER_ICT_4
                            FROM RankedUsers
                            GROUP BY PROJECT_NUMBER"""

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
               
                self.logger.end_import(len(data))
                
                
            except Exception as e:
                self.logger.log_error(str(e))
                if self.mssql_conn.connection:
                    self.mssql_conn.close()
                if self.oracle_engine.connection:
                    self.oracle_engine.close()

