import pandas as pd
from sqlalchemy import text
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyAppointments:
    def __init__(self):
            self.mssql_conn = ConnectFort()
            self.oracle_engine = ConnectOracleDevEngine()           
            self.start_pos = 0
            self.batch_size = 15000
            self.logger = ImportLogger()
            self.oracle_connection = self.oracle_engine.connect()

    def importToOracle(self,df): 
               
                 # Prepare insert statement
                insert_stmt = text("""MERGE INTO IMPORT_EASY_APPOINTMENTS trgt
                                        USING 
                                            ( SELECT :ACTIVITY_ID as appointment_id FROM dual ) src ON (src.appointment_id = trgt.appointment_id )
                                        WHEN MATCHED THEN   
                                            UPDATE   
                                                SET trgt.TOPIC = :SUBJECT,
                                                    trgt.TOPIC_TYPE = :SUBJECT_TYPE,
                                                    trgt.ID_SOW = :ID_SOW,
                                                    trgt.CUSTOMER = :CUSTOMER_NAME,
                                                    trgt.MODIFICATION_DATE = :MODIFIED_ON,
                                                    trgt.STATE = :STATUS,
                                                    trgt.EXTENDED_STATUS = :EXTENDED_REASON,
                                                    trgt.OWNER = :OWNER_LOGIN,
                                                    trgt.SCHEDULED_START = :SCHEDULED_START,
                                                    trgt.SCHEDULED_END = :SCHEDULED_END,
                                                    trgt.MODIFIED_BY = :MODIFIED_BY,
                                                    trgt.PROJECT_NUMBER = :PROJECT_NUMBER,
                                                    trgt.ACTIVE = 1
                                        WHEN NOT MATCHED THEN
                                            INSERT (
                                                    APPOINTMENT_ID,
                                                    TOPIC,
                                                    TOPIC_TYPE,
                                                    ID_SOW,
                                                    CUSTOMER,
                                                    MODIFICATION_DATE,
                                                    STATE,
                                                    EXTENDED_STATUS,
                                                    OWNER,
                                                    SCHEDULED_START,
                                                    SCHEDULED_END,
                                                    CREATE_DATE,
                                                    CREATED_BY,
                                                    MODIFIED_BY,
                                                    PROJECT_NUMBER,
                                                    ACTIVE
                                                    )
                                            VALUES 
                                                    (
                                                    :ACTIVITY_ID,
                                                    :SUBJECT,
                                                    :SUBJECT_TYPE,
                                                    :ID_SOW,
                                                    :CUSTOMER_NAME,
                                                    :MODIFIED_ON,
                                                    :STATUS,
                                                    :EXTENDED_REASON,
                                                    :OWNER_LOGIN,
                                                    :SCHEDULED_START,
                                                    :SCHEDULED_END,
                                                    :CREATED_ON,
                                                    :CREATED_BY,
                                                    :MODIFIED_BY,
                                                    :PROJECT_NUMBER,
                                                    1
                                                    )""")
    
                # Insert new data into Oracle using executemany for bulk insert
                self.oracle_connection.execute(insert_stmt, df.to_dict(orient='records'))
                # Commit the transaction
                self.oracle_connection.commit()

                # Log the number of rows affected
                #self.logger.add_to_import(f"""Rows imported {self.start_pos} """) 
                
        
    def parseData(self, df):

                    df['MODIFIED_ON'] = df['MODIFIED_ON'].dt.date
                    df['SCHEDULED_START'] = df['SCHEDULED_START'].dt.date
                    df['SCHEDULED_END'] = df['SCHEDULED_END'].dt.date
        
                    return df
        
    def run(self):
            try:
                
                self.logger.start_import(self.__class__.__name__)
    
                # Connect to MSSQL
                mssql_connection = self.mssql_conn.connect()

                
                # Extract data from MSSQL Query 1
                query = """
                           SELECT 
                                REPLACE((CONVERT(CHAR(40),apb.ActivityId )),' ','' ) ACTIVITY_ID,
                                CASE 
                                WHEN abc.elob_name IS NOT NULL THEN abc.elob_name
                                WHEN acc2.AccountId IS NULL THEN acc.Name ELSE acc2.Name 
                                END CUSTOMER_NAME,            
                                CASE 
                                    WHEN abc.elob_BillingNumber2 IS NOT NULL THEN abc.elob_BillingNumber2 
                                    WHEN
                                        CASE WHEN acc2.AccountId IS NULL THEN acc.elob_sowid 
                                             ELSE acc2.elob_sowid 
                                        END like 'R%R___' THEN CASE WHEN acc2.AccountId IS NULL THEN acc.elob_sowid ELSE acc2.elob_sowid END
                                    WHEN
                                        CASE WHEN acc2.AccountId IS NULL THEN acc.elob_sowid 
                                             ELSE acc2.elob_sowid 
                                        END like 'R%R%' THEN substring(CASE WHEN acc2.AccountId IS NULL THEN acc.elob_sowid 
                                                                       ELSE acc2.elob_sowid END , 2, CHARINDEX ('R',CASE WHEN acc2.AccountId IS NULL THEN acc.elob_sowid ELSE acc2.elob_sowid END,2)-2)
                                    ELSE CASE WHEN acc2.AccountId IS NULL THEN acc.elob_sowid 
                                    ELSE acc2.elob_sowid END
                                END AS ID_SOW,
                                
                                apb.SUBJECT,
                                elob_subjecttype.value as SUBJECT_TYPE,
                    
                                apb.SCHEDULEDSTART as SCHEDULED_START,
                                apb.SCHEDULEDEND as SCHEDULED_END,
                                apb.createdon as CREATED_ON,
                                apb.MODIFIEDON as MODIFIED_ON,
                                
                                statuscode.value STATUS,
                                elob_extendedstausreason.value as EXTENDED_REASON,
                                substring(su.DomainName,4,10) OWNER_LOGIN,
                                
                                [lk_appointment_createdby].firstname +' '+[lk_appointment_createdby].lastname as CREATED_BY,
                                [lk_appointment_createdonbehalfby].firstname+' '+[lk_appointment_createdonbehalfby].lastname createdonbehalfby,
                                [lk_appointment_modifiedby].firstname+' '+[lk_appointment_modifiedby].lastname MODIFIED_BY,
                                [lk_appointment_modifiedonbehalfby].firstname+' '+ [lk_appointment_modifiedonbehalfby].lastname modifiedonbehalfby,
                                prj.elob_opportunityId as PROJECT_NUMBER
                    	FROM
                                [ActivityPointerBase]  apb
                                JOIN [AppointmentBase] fapp on (apb.[ActivityId] = fapp.[ActivityId] and apb.[ActivityTypeCode] = 4201)
                                
                    
                                LEFT JOIN [B2B_MSCRM].[dbo].[Accountbase] as acc ON (acc.accountid = apb.regardingobjectid)
                                LEFT JOIN [B2B_MSCRM].[dbo].OpportunityBase as prj ON (prj.OpportunityId = apb.regardingobjectid)
                                LEFT JOIN [AccountBase] acc2 on (fapp.elob_relatedaccount = acc2.[AccountId])
                                LEFT JOIN [B2B_MSCRM].[dbo].elob_billingaccountBase  as abc ON (abc.elob_billingaccountId = apb.regardingobjectid)
                    
                    
                                LEFT join [B2B_MSCRM].[dbo].SystemUser su ON (su.SystemUserId = CASE WHEN apb.OwnerId = '5EF4F0A7-69FD-E411-80D3-00505601285D' THEN apb.CreatedBy ELSE apb.OwnerId END)
                                LEFT join [B2B_MSCRM].[dbo].SystemUser su1 ON (su.ParentSystemUserId = su1.SystemUserId)
                                LEFT join [B2B_MSCRM].[dbo].SystemUser su2 ON (su1.ParentSystemUserId = su2.SystemUserId)
                    
                                LEFT JOIN StringMap elob_subjecttype ON (elob_subjecttype.AttributeName = 'elob_subjecttype'
                                                                                AND elob_subjecttype.LangId = 1033 
                                                                                AND elob_subjecttype.ObjectTypeCode = 4201
                                                                                AND elob_subjecttype.AttributeValue = fapp.elob_subjecttype )
                    
                                LEFT JOIN StringMap statuscode ON (statuscode.AttributeName = 'statuscode'
                                                                                AND statuscode.LangId = 1033 
                                                                                AND statuscode.ObjectTypeCode = 4201
                                                                                AND statuscode.AttributeValue = apb.StatusCode )
                    
                                LEFT JOIN StringMap elob_extendedstausreason ON (elob_extendedstausreason.AttributeName = 'elob_extendedstausreason'
                                                                                AND elob_extendedstausreason.LangId = 1033 
                                                                                AND elob_extendedstausreason.ObjectTypeCode = 4201
                                                                                AND elob_extendedstausreason.AttributeValue = fapp.elob_extendedstausreason )
                                LEFT join [SystemUserBase] [lk_appointment_createdby] with(nolock) on (apb.[CreatedBy] = [lk_appointment_createdby].[SystemUserId])
                                LEFT join [SystemUserBase] [lk_appointment_createdonbehalfby] with(nolock) on (apb.[CreatedOnBehalfBy] = [lk_appointment_createdonbehalfby].[SystemUserId])
                                LEFT join [SystemUserBase] [lk_appointment_modifiedby] with(nolock) on (apb.[ModifiedBy] = [lk_appointment_modifiedby].[SystemUserId])
                                LEFT join [SystemUserBase] [lk_appointment_modifiedonbehalfby] with(nolock) on (apb.[ModifiedOnBehalfBy] = [lk_appointment_modifiedonbehalfby].[SystemUserId])
                    	WHERE 
                                apb.scheduledstart >= '2019-01-01'
                                AND apb.[ActivityTypeCode] = 4201    
                                AND (apb.SUBJECT <> 'e-zamawiaja' or apb.SUBJECT is null)
                                --AND  apb.ActivityId = '3E1AD09A-468B-ED11-80FC-0050560100C4'
                                AND (su1.ParentSystemUserIdName in (
                                    'Runowska Katarzyna','Komoń Rafał',
                                    'Szupiluk Paweł','Syty Robert','Mucha Grzegorz', 'Imgront Marcin', 'Organiak Sławomir','Otka Małgorzata','Szczerek Marcin',
                                    'Turczyński Marcin','Wasiakowski Norbert','Grudzień Gabriel','Bronk Mariusz','Chojnacki Mariusz','Dreslerski Tomasz','Madejska Agnieszka',
                                    'Brucki Marcin','Chajduk Paweł','Czerski Tomasz','Dębek Maciej','Helbing Wojciech','Jaskóła Jacek','Kiełbicka Agnieszka','Kornacki Marcin','Majchrzak Bogusław','Mateńka-Łazarz Magdalena',
                                    'Mendyk Piotr','Miller Sławomir','Niewiadomski Przemysław','Organiak Sławomir','Otka Małgorzata','Sateja Piotr','Sawko Tomasz',
                                    'Stachowski Artur','Szczerek Marcin','Szuper Bartłomiej','Wawryk Bartosz','Wieleba Jarosław','Włodarska-Krawczyk Marta',
                                    'Wojtowicz Jerzy','Wudkiewicz Allan','Zapart Dorota','Zawistowska Agnieszka','Żaroń Roman')
                                   or su.ParentSystemUserIdName in (
                                    'Runowska Katarzyna', 'Komoń Rafał',
                                    'Szupiluk Paweł','Syty Robert','Mucha Grzegorz', 'Imgront Marcin', 'Organiak Sławomir','Otka Małgorzata','Szczerek Marcin',
                                    'Turczyński Marcin','Wasiakowski Norbert','Grudzień Gabriel','Bronk Mariusz','Chojnacki Mariusz','Dreslerski Tomasz','Madejska Agnieszka',
                                    'Brucki Marcin','Chajduk Paweł','Czerski Tomasz','Dębek Maciej','Helbing Wojciech','Jaskóła Jacek','Kiełbicka Agnieszka','Kornacki Marcin','Majchrzak Bogusław','Mateńka-Łazarz Magdalena',
                                    'Mendyk Piotr','Miller Sławomir','Niewiadomski Przemysław','Organiak Sławomir','Otka Małgorzata','Sateja Piotr','Sawko Tomasz',
                                    'Stachowski Artur','Szczerek Marcin','Szuper Bartłomiej','Wawryk Bartosz','Wieleba Jarosław','Włodarska-Krawczyk Marta',
                                    'Wojtowicz Jerzy','Wudkiewicz Allan','Zapart Dorota','Zawistowska Agnieszka','Żaroń Roman')
                                    )   
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
               
                self.logger.end_import(len(data))
                
                
            except Exception as e:
                self.logger.log_error(str(e))
                if self.mssql_conn.connection:
                    self.mssql_conn.close()
                if self.oracle_engine.connection:
                    self.oracle_engine.close()

