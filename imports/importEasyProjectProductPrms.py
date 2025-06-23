import pandas as pd
from sqlalchemy import text
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyProjectProductPrms:
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
                    cursor.execute("BEGIN KLIK.CLEAN_TABLE('IMPORT_EASY_PROJECT_PROD_PRMS'); END;") 
                    #self.oracle_connection.execute(text("TRUNCATE TABLE IMPORT_EASY_PROJECT_PROD_PRMS"))
                
    def importToOracle(self,df): 
               
                 # Prepare insert statement
                insert_stmt = text("""INSERT INTO IMPORT_EASY_PROJECT_PROD_PRMS (  
                    PROJECT_NUMBER,
                    PRODUCT_ID,
                    PARAMETER,
                    VALUE)
            VALUES 
                    (
                    :PROJECT_NUMBER,
                    :PRODUCT_ID,
                    :PARAMETER,
                    :VALUE )""")
    
                # Insert new data into Oracle using executemany for bulk insert
                self.oracle_connection.execute(insert_stmt, df.to_dict(orient='records'))
                # Commit the transaction
                self.oracle_connection.commit()
    
    def parseData(self, df):

                              
                    #if(df['SALE_TYPE']=='Nowa Sprzedaż'):
                    ##    df['SALE_TYPE'] = 'Nowa sprzedaż'
                        
                    #df['SALE_TYPE'] = df['SALE_TYPE'].strip()          
                    return df
        
    def run(self):
            try:
                
                self.logger.start_import(self.__class__.__name__)
    
                # Connect to MSSQL
                mssql_connection = self.mssql_conn.connect()

                
                # Extract data from MSSQL Query 1
                query = """ SELECT
                         REPLACE((CONVERT(CHAR(40),o.elob_opportunityId)),' ','' ) as PROJECT_NUMBER
                        , REPLACE((CONVERT(CHAR(40),op.elob_SoldProduct)),' ','' ) as PRODUCT_ID
                        ,spp.elob_name as PARAMETER
                        ,isnull(spp.elob_ParameterValue,'') as VALUE
                    FROM
                        opportunitybase o
                        join OpportunityProductBase op on (o.OpportunityId=op.OpportunityId)
                        join elob_soldproductbase sp on (op.elob_SoldProduct=sp.elob_soldproductId and sp.elob_name ='zarządzanie siecią lan')
                        join elob_soldproductBase sp2 on (sp.elob_soldproductId=sp2.elob_ParentSoldProduct)
                        join elob_soldproductparameterBase spp on (sp2.elob_soldproductId=spp.elob_SoldProduct 
                                                                        and spp.elob_ParameterValue is not null
                                                                        and spp.elob_name ='liczba urządzeń UTM')
                    WHERE
                        o.CreatedOn >= '2018-01-01'"""

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

