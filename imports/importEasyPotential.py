import pandas as pd
from sqlalchemy import text
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyPotential:
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
                    cursor.execute("BEGIN KLIK.CLEAN_TABLE('import_easy_potential'); END;")   
                
    def importToOracle(self,df): 
               
                 # Prepare insert statement
                insert_stmt = text("""
                    INSERT INTO KLIK.IMPORT_EASY_POTENTIAL ( 
                         CUSTOMER,
                        EOP,
                        ID_SOW,
                        INCOME,
                        OPERATOR,
                        PORTFOLIO,
                        PRODUCT,
                        PRODUCT_BRANCH,
                        QUANTITY,
                        POTENTIAL_COMMENT,
                        OS_MODYFIKUJACA)
                    VALUES (
                        :CUSTOMER,
                        :EOP,
                        :ID_SOW,
                        :INCOME,
                        :OPERATOR,
                        :PORTFOLIO,
                        :PRODUCT,
                        :PRODUCT_BRANCH,
                        :QUANTITY,
                        :POTENTIAL_COMMENT,
                        :OS_MODYFIKUJACA
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
SELECT DISTINCT 
                por.elob_name AS PORTFOLIO,
                CASE
                    WHEN com.elob_name like '%-%' THEN SUBSTRING(com.elob_name, 1, CHARINDEX('-', com.elob_name)-2)
                    ELSE ''
                END AS PRODUCT_BRANCH,
                com.elob_name AS PRODUCT,
                com.elob_Comments AS COMMENT,
                COALESCE(CONVERT(DECIMAL(10,2),com.elob_income),0) AS INCOME,
                COALESCE(CONVERT(DECIMAL(10,0),com.elob_Quantity),0) AS QUANTITY,
                com.elob_operator AS OPERATOR,
                acc.Name AS CUSTOMER,
                CASE 
                    WHEN acc.elob_sowid like 'R%R___' THEN acc.elob_sowid
                    WHEN acc.elob_sowid like 'R%R%' THEN substring(acc.elob_sowid , 2, CHARINDEX ('R',acc.elob_sowid,2)-2)
                    ELSE acc.elob_sowid
                END AS ID_SOW,
                CAST(com.elob_EOP AS DATE) AS EOP,
                com.elob_comments AS POTENTIAL_COMMENT,
                su.FullName as OS_MODYFIKUJACA
            FROM 
                [dbo].[elob_competitorproductBase] com
                JOIN [dbo].[AccountBase] AS acc ON (acc.accountId = com.elob_account)
                LEFT JOIN [dbo].[elob_portfolioBase] por ON (por.elob_portfolioId = acc.elob_Portfolio)
                LEFT JOIN [dbo].[OwnerBase] dh ON (dh.OwnerId = por.OwnerId)
                LEFT JOIN [dbo].BusinessUnit sup ON (sup.BusinessUnitId = por.OwningBusinessUnit)
                LEFT JOIN systemuserbase su on ( com.ModifiedBy=su.SystemUserId ) 
            WHERE
                (com.elob_Quantity IS NOT NULL OR
                (com.elob_income <> 0 AND com.elob_income is not null))
                AND (com.elob_name like '%-%'
                    OR com.elob_name ='Orange Energia') 
                    AND com.statecode =0  
                    AND acc.statecode =0 /*PB2020.01.27 */ 
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

