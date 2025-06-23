import pandas as pd
from sqlalchemy import text
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyFixOrdersValue:
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
                    cursor.execute("BEGIN KLIK.CLEAN_TABLE('IMPORT_EASY_FIX_ORDERS_VALUE'); END;")   
                
    def importToOracle(self,df): 
               
                 # Prepare insert statement
                insert_stmt = text("""
                    INSERT INTO KLIK.IMPORT_EASY_FIX_ORDERS_VALUE ( 
                        PKB,
                        ABO,
                        INS,
                        LOJALKA,
                        DATA)
                    VALUES (
                         :PKB,
                        :ABO,
                        :INS,
                        :LOJALKA,
                        :DATA
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
        isnull(so.elob_ordernumber,'') as PKB
        ,isnull(p2.elob_value,'') as ABO
        ,isnull(p3.elob_value,'') as INS
        ,isnull(p1.czas,'') as LOJALKA
        ,convert(date,so.CreatedOn) as DATA
        from
        elob_sokxorder2Base so
        left join
        (select so.elob_sokxorder2Id, pdv.elob_value as [czas] 
            from elob_sokxorder2Base so
        join elob_parameterBase p on so.elob_sokxorder2Id=p.elob_SOKXOrder and p.elob_name like 'czas trwania umowy'
        join elob_parameterdefinitionBase pd on p.elob_ParameterDefinition=pd.elob_parameterdefinitionId
        join elob_parametervaluesdefinitionBase pdv on pd.elob_parameterdefinitionId=pdv.elob_ParameterDefinition and p.elob_value=pdv.elob_index
        ) p1 on so.elob_sokxorder2Id=p1.elob_sokxorder2Id
        left join elob_parameterBase p2 on so.elob_sokxorder2Id=p2.elob_SOKXOrder and p2.elob_name like 'Opłata abonamentowa netto%kwota PLN%'
        left join elob_parameterBase p3 on so.elob_sokxorder2Id=p3.elob_SOKXOrder and p3.elob_name like'Opłata instalacyjna netto%kwota PLN%'
        WHERE
        so.elob_ordernumber is not null 
        and convert(date,so.CreatedOn)>='2020-09-01'
        and (p2.elob_value is not null or  p3.elob_value is not null )
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

