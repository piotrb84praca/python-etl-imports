import pandas as pd
from sqlalchemy import text
from datetime import datetime, timedelta
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyStatusHistory:
    
    def __init__(self):
            self.mssql_conn = ConnectFort()
            self.oracle_engine = ConnectOracleDevEngine()           
            self.start_pos = 0
            self.batch_size = 15000
            self.logger = ImportLogger()
            self.oracle_connection = self.oracle_engine.connect()            
        
        
    def deleteFromOracle(self):        
 
                with self.oracle_connection.begin():
                    self.oracle_connection.execute(text("TRUNCATE TABLE "))
                
    def importToOracle(self,df):                

                insert_stmt = text("""
                    MERGE INTO IMPORT_EASY_STATUS_HISTORY trgt
        USING            
            ( SELECT :id as ID from dual ) src ON (src.ID = trgt.ID )
        WHEN MATCHED THEN   
            UPDATE   
                SET                
                trgt.project_number =  :project_number,
                trgt.status = :status,
                trgt.status_reason =:status_reason,
                trgt.created_on = TO_DATE(:created_on,'YYYY-MM-DD HH24:MI:SS'),
                trgt.modified_on = TO_DATE(:modified_on,'YYYY-MM-DD HH24:MI:SS'),
                trgt.status_start_date = TO_DATE(:status_start_date,'YYYY-MM-DD HH24:MI:SS'),
                trgt.status_end_date = TO_DATE(:status_end_date,'YYYY-MM-DD HH24:MI:SS'),
                trgt.owner = :owner,
                trgt.modified_by = :modified_by,
                trgt.behalf_of =  :behalf_of
        WHEN NOT MATCHED THEN
            INSERT (
                id,
                project_number,
                status,
                status_reason,
                created_on,
                modified_on,
                status_start_date,
                status_end_date,
                owner,
                modified_by,
                behalf_of
                    )
            VALUES 
                    (
                    :id,
                    :project_number,
                    :status,
                    :status_reason,
                     TO_DATE(:created_on,'YYYY-MM-DD HH24:MI:SS'),
                     TO_DATE(:modified_on,'YYYY-MM-DD HH24:MI:SS'),
                     TO_DATE(:status_start_date,'YYYY-MM-DD HH24:MI:SS'),
                     TO_DATE(:status_end_date,'YYYY-MM-DD HH24:MI:SS'),
                    :owner,
                    :modified_by,
                    :behalf_of
                    )""")
    
                # Insert new data into Oracle using executemany for bulk insert
                self.oracle_connection.execute(insert_stmt, df.to_dict(orient='records'))
                # Commit the transaction
                self.oracle_connection.commit()
    
    def parseData(self, df):

                   # df['marza_kontrakt_proc'] = df['marza_kontrakt_proc'].fillna('')
                  
        
                    return df
        
    def run(self):
            try:
                
                self.logger.start_import(self.__class__.__name__)             
                mssql_connection = self.mssql_conn.connect()              

                query = """ 
                SELECT  
     REPLACE((CONVERT(CHAR(40),st.elob_statushistoryId )),' ','' ) as id,
     REPLACE((CONVERT(CHAR(40),o.elob_OpportunityID )),' ','' ) as project_number,
    case  
    when st.elob_bpfstage = 0 then 'Suspecting'
    when st.elob_bpfstage = 1 then 'Prospecting'
    when st.elob_bpfstage = 2 then 'Analyzing'
    when st.elob_bpfstage = 3 then 'Negotiating'
    when st.elob_bpfstage = 4 then 'Closing'
    end as status,
    st.elob_statusreasonname as status_reason,
    (DATEADD(hour, 2,st.CreatedOn))  as created_on,
    (DATEADD(hour, 2,st.ModifiedOn))  as modified_on,
    (DATEADD(hour, 2,st.elob_startdate))  as status_start_date,
    (DATEADD(hour, 2,st.elob_enddate))  as status_end_date,
    lower(substring(sp_o.DomainName, 4, 20)) as owner,
    lower(substring(modified.DomainName, 4, 20))  as modified_by,
    lower(substring(behalf.DomainName, 4, 20)) as behalf_of
    from Opportunity o
    join B2B_MSCRM.dbo.elob_statushistoryBase st ON ( st.elob_projectid = o.OpportunityId)
    JOIN SystemUser sp_o ON (st.elob_owninguserid = sp_o.SystemUserId)
    JOIN SystemUser modified ON (st.ModifiedBy = modified.SystemUserId)
    LEFT JOIN SystemUser behalf ON (st.CreatedOnBehalfBy = behalf.SystemUserId)
    WHERE st.ModifiedOn  >= getdate() - 30
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
               
                self.logger.end_import(len(data))
                
                
            except Exception as e:
                self.logger.log_error(str(e))
                if self.mssql_conn.connection:
                    self.mssql_conn.close()
                if self.oracle_engine.connection:
                    self.oracle_engine.close()