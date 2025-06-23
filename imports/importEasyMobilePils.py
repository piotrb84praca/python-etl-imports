import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyMobilePils:
    
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
                    cursor.execute("BEGIN KLIK.CLEAN_TABLE('import_easy_mobile_pils'); END;") 
                    #self.oracle_connection.execute(text("TRUNCATE TABLE import_easy_mobile_pils"))
                
    def importToOracle(self,df): 
               
                 # Prepare insert statement
                insert_stmt = text("""
INSERT INTO import_easy_mobile_pils (sfa_pil_id, field_code, field_value, field_value_czyste, field_key,w1,w2,w3,w4)
                    VALUES (:sfa_pil_id, :field_code, :field_value, :field_value_czyste, :field_key,:w1,:w2,:w3,:w4)
                """)
    
                # Insert new data into Oracle using executemany for bulk insert
                self.oracle_connection.execute(insert_stmt, df.to_dict(orient='records'))
                # Commit the transaction
                self.oracle_connection.commit()
               

    def parseData(self, df):
                     # Ensure sfa_pil_id is of integer type
                    df['sfa_pil_id'] = pd.to_numeric(df['sfa_pil_id'], errors='coerce').fillna(0).astype(int)
                    df['field_value_czyste'] = df['field_value_czyste'].str.replace("'", "", regex=False)  # Remove single quotes
                    df['field_value_czyste'] = df['field_value_czyste'].str.replace(' zł', '', regex=False)  # Remove ' zł'
                    df['field_value_czyste'] = df['field_value_czyste'].str.replace('zł', '', regex=False)  # Remove 'zł'
                    # Apply conditions to set field_value_czyste to 0
                    df['field_value_czyste'] = np.where(df['field_value_czyste'].str.isalpha(), 0, df['field_value_czyste'])
                    df['field_value_czyste'] = np.where(df['field_value_czyste'].str.contains(r'[a-z]', case=False), 0, df['field_value_czyste'])
                    df['field_value_czyste'] = pd.to_numeric(df['field_value_czyste'], errors='coerce').fillna(0)
                   
                    # Split field_code into separate columns
                    split_codes = df['field_code'].str.split('|', expand=True)
                    df['w1'] = split_codes[0]
                    df['w2'] = split_codes[1]
                    df['w3'] = split_codes[2]
                    df['w4'] = split_codes[3]
        
                    return df
        
    def run(self):
            try:
                
                self.logger.start_import(self.__class__.__name__)
    
                # Connect to MSSQL
                mssql_connection = self.mssql_conn.connect()
                self.deleteFromOracle()
                
                query = """
                SELECT
                    sfa_pil_id,
                    field_code,
                    field_value AS field_value,
                    REPLACE(REPLACE(RTRIM(LTRIM(field_value)), ' ', ''), ',', '.') AS field_value_czyste,
                    field_key 
                FROM 
                    [FORT_Produkcja_V2].[dbo].[AP_Rozliczenia_wyniki_przetworzone]
                WHERE 
                    field_value <> '#N/A' AND field_value IS NOT NULL

                    UNION ALL
                    
                SELECT 
                    sfa_pil_id,
                    field_code,
                    field_value ,
                    replace(replace( rtrim(ltrim(field_value)),' ',''),',','.')   as field_value_czyste,
                    field_key
                    FROM
                    FORT_Produkcja_V2.dbo.piltool_miscellaneous_field 
                    WHERE
                    (
                        field_code  =   'MOBILE|SIMfree|rozliczenie|deklaracja_wykorzystanie'   or
                        field_code like 'MOBILE|SIMfree|rozliczenie|BSCS_terminal_%'            or
                        field_code  =   'MOBILE|WSKAZNIKI|WARTOSC|KWOTA_PROWIZJI_KLUCZ_LUB_KORPO' or 
                    	( field_code like 'MOBILE|SIMfree|rozliczenie|deklaracja_wykorzystanie%' and FIELD_VALUE='odsprzedaż zł' )
                    )
                """
               
                cursor = mssql_connection.cursor() 
                cursor.execute(query)    
                data = cursor.fetchall()
                
                columns = [column[0] for column in cursor.description]  # Get column names
                df = pd.DataFrame(data, columns=columns)
                df = self.parseData(df)   
                
                while self.start_pos < len(data):
                   
                    dt = df[self.start_pos:self.start_pos +self.batch_size]
                    self.start_pos += self.batch_size
                    self.importToOracle(dt)       
                          
                    
                self.mssql_conn.close()
                self.logger.end_import(len(data)) 
                
            except Exception as e:
                self.logger.log_error(str(e))
                if self.mssql_conn.connection:
                    self.mssql_conn.close()
                if self.oracle_conn.connection:
                    self.oracle_conn.close()