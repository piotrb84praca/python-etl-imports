import pandas as pd
from sqlalchemy import text
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyProjectProducts:
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
                    cursor.execute("BEGIN KLIK.CLEAN_TABLE('IMPORT_EASY_PROJECT_PRODUCTS'); END;")
                    #self.oracle_connection.execute(text("TRUNCATE TABLE IMPORT_EASY_PROJECT_PRODUCTS"))
                
    def importToOracle(self,df): 
               
                 # Prepare insert statement
                insert_stmt = text("""INSERT INTO IMPORT_EASY_PROJECT_PRODUCTS (  
                    PROJECT_NUMBER,
                    SALE_TYPE,
                    SERVICE,
                    PRODUCT_ID,
                    PRODUCT_NUMBER,
                    CONTRACT_VALUE,
                    MONTHLY_FEE,
                    UP_FRONT,
                    CONTRACT_DURATION,
                    QUANTITY,
                    CATEGORY,
                    SERVICE_GROUP
                    )
            VALUES 
                    (
                    :PROJECT_NUMBER,
                    :SALE_TYPE,
                    :SERVICE,
                    :PRODUCT_ID,
                    :PRODUCT_NUMBER,
                    :CONTRACT_VALUE ,             
                    :MONTHLY_FEE,
                    :UP_FRONT,
                    :CONTRACT_DURATION,
                    :QUANTITY,
                    :CATEGORY,
                    :SERVICE_GROUP
                    )""")
    
                # Insert new data into Oracle using executemany for bulk insert
                self.oracle_connection.execute(insert_stmt, df.to_dict(orient='records'))
                # Commit the transaction
                self.oracle_connection.commit()
    
    def parseData(self, df):

                    df['SALE_TYPE'] = df['SALE_TYPE'].replace('Nowa Sprzedaż', 'Nowa sprzedaż')                        
                    df['SALE_TYPE'] = df['SALE_TYPE'].replace(' ','')
        
                    return df
        
    def run(self):
            try:
                
                self.logger.start_import(self.__class__.__name__)
    
                # Connect to MSSQL
                mssql_connection = self.mssql_conn.connect()

                
                # Extract data from MSSQL Query 1
                query = """ SELECT
                 REPLACE((CONVERT(CHAR(40),o.elob_OpportunityID )),' ','' ) PROJECT_NUMBER, 
                saleType.Value SALE_TYPE,
                prodDic.Name SERVICE,
                prodDic.ProductNumber PRODUCT_NUMBER,
                ISNULL(cast(prod.baseAmount as DECIMAL(20,0)),0) CONTRACT_VALUE,
                ISNULL(cast( prod.elob_TotalMonthlyFee as DECIMAL(20,0)),0) MONTHLY_FEE,
                ISNULL(cast( prod.elob_FinalUpFrontPayment as DECIMAL(20,0)),0)  UP_FRONT,
                CASE 
                    WHEN prod.elob_contractduration = 100 THEN  ISNULL(cast( prod.elob_SpecifyDurationinMonths as DECIMAL),0) 
                    ELSE  ISNULL(cast( prod.elob_contractduration as DECIMAL),0) 
                END CONTRACT_DURATION,
                ISNULL(cast( prod.elob_Quantity as DECIMAL),0)  QUANTITY,
                [elob_productcategoryPLTable].value CATEGORY,
		        REPLACE((CONVERT(CHAR(40),prod.elob_SoldProduct )),' ','' ) PRODUCT_ID,
            servDic.name as SERVICE_GROUP
            FROM 
                Opportunity o
                LEFT JOIN OpportunityProductBase prod ON (prod.OpportunityId = o.OpportunityId)
                LEFT JOIN ProductBase prodDic ON (prodDic.ProductId = prod.ProductId)
                LEFT JOIN ProductBase servDic ON (servDic.ProductId = prodDic.elob_Service )
                JOIN Account a ON (o.accountid = a.accountid )
                JOIN SystemUser sp_o on (o.ownerid = sp_o.SystemUserId )
                LEFT JOIN StringMap [saleType] on 
                                ([saleType].AttributeName = 'elob_saletype'
                                and [saleType].ObjectTypeCode = 10036
                                and [saleType].AttributeValue = prod.elob_saletype
                                and [saleType].LangId = 1045)
                LEFT JOIN StringMap [elob_productcategoryPLTable] on 
                                ([elob_productcategoryPLTable].AttributeName = 'elob_productcategory'
                                and [elob_productcategoryPLTable].ObjectTypeCode = 3
                                and [elob_productcategoryPLTable].AttributeValue = o.elob_productcategory
                                and [elob_productcategoryPLTable].LangId = 1045)
            WHERE
                o.CreatedOn >= '2018-01-01'
              
                AND a.elob_businesssegment IN (1,2,4) """

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

