import pandas as pd
from sqlalchemy import text
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyContacts:
    def __init__(self):
            self.mssql_conn = ConnectFort()
            self.oracle_engine = ConnectOracleDevEngine()           
            self.start_pos = 0
            self.batch_size = 15000
            self.logger = ImportLogger()
            self.oracle_connection = self.oracle_engine.connect()

    def importToOracle(self,df): 
               
                 # Prepare insert statement
                insert_stmt = text("""MERGE INTO import_easy_contacts trgt
                                USING 
                                    ( SELECT :ID as ID FROM dual ) src ON (src.ID = trgt.ID )
                                WHEN MATCHED THEN   
                                    UPDATE   
                                        SET trgt.nip = :NIP,
                                            trgt.regon = :REGON,
                                            trgt.sow_id = :SOW_ID,
                                            trgt.segment_klienta = :SEGMENT_KLIENTA,
                                            trgt.portfel = :PORTFEL,
                                            trgt.wlasciciel_klienta =:WLASCICIEL_KLIENTA,
                                            trgt.msa = :MSA,
                                            trgt.status_kontaktu_glownego =:STATUS_KONTAKTU_GLOWNEGO,
                                            trgt.nazwisko_i_imie_kontakt_glowny = :NAZWISKO_I_IMIE_KONTAKT_GLOWNY,
                                            trgt.imie_kontakt_glowny = :IMIE_KONTAKT_GLOWNY,
                                            trgt.nazwisko_kontakt_glowny = :NAZWISKO_KONTAKT_GLOWNY,
                                            trgt.telefon_kontakt_glowny = :TELEFON_KONTAKT_GLOWNY,
                                            trgt.email_kontakt_glowny = :EMAIL_KONTAKT_GLOWNY                    
                                WHEN NOT MATCHED THEN
                                    INSERT (
                                            id,
                                            nip,
                                            regon,
                                            sow_id,
                                            segment_klienta,
                                            portfel,
                                            wlasciciel_klienta,
                                            msa,
                                            status_kontaktu_glownego,
                                            nazwisko_i_imie_kontakt_glowny,
                                            imie_kontakt_glowny,
                                            nazwisko_kontakt_glowny,
                                            telefon_kontakt_glowny,
                                            email_kontakt_glowny
                                            )
                                    VALUES 
                                            (:ID, :NIP, :REGON, :SOW_ID, :SEGMENT_KLIENTA,:PORTFEL,:WLASCICIEL_KLIENTA, 
                                             :MSA, :STATUS_KONTAKTU_GLOWNEGO, :NAZWISKO_I_IMIE_KONTAKT_GLOWNY, :IMIE_KONTAKT_GLOWNY,:NAZWISKO_KONTAKT_GLOWNY,:TELEFON_KONTAKT_GLOWNY,:EMAIL_KONTAKT_GLOWNY
                                            )""")
    
                # Insert new data into Oracle using executemany for bulk insert
                self.oracle_connection.execute(insert_stmt, df.to_dict(orient='records'))
                # Commit the transaction
                self.oracle_connection.commit()

                # Log the number of rows affected
                #self.logger.add_to_import(f"""Rows imported {self.start_pos} """) 
                
        
    def parseData(self, df):

                    #df['CONTRACT_ID'] = str(df['CONTRACT_ID'])  # uuid to string
                    return df
        
    def run(self):
            try:
                
                self.logger.start_import(self.__class__.__name__)
    
                # Connect to MSSQL
                mssql_connection = self.mssql_conn.connect()

                
                # Extract data from MSSQL Query 1
                query = """
                            SELECT 
                            REPLACE((CONVERT(CHAR(40),c.ContactId )),' ','' ) as ID
                            ,isnull(a.elob_nip,'') as NIP
                            ,isnull(a.elob_regon,'') as REGON
                            ,isnull(a.elob_sowid,'') as SOW_ID
                            ,mapa.value as SEGMENT_KLIENTA
                            ,p.elob_name as PORTFEL
                            ,su.fullname as WLASCICIEL_KLIENTA
                            ,isnull(su1.fullname,'') as [MSA]
                            ,case when c.statecode=0 then 'Aktywny' when c.statecode=1 then 'Nieaktywny' else '' end as STATUS_KONTAKTU_GLOWNEGO
                            ,isnull(replace(replace(c.FullName,char(10),''),char(13),''),'')  as NAZWISKO_I_IMIE_KONTAKT_GLOWNY
                            ,isnull(replace(replace(c.FirstName,char(10),''),char(13),''),'')  as IMIE_KONTAKT_GLOWNY
                            ,isnull(replace(replace(c.LastName,char(10),''),char(13),''),'')  as NAZWISKO_KONTAKT_GLOWNY
                            ,case when c.MobilePhone is null then ' ' else replace(replace(c.MobilePhone,char(10),''),char(13),'')  end as TELEFON_KONTAKT_GLOWNY
                            ,case when c.EMailAddress1 is null then ' ' else replace(replace(c.EMailAddress1,char(10),''),char(13),'')  end as EMAIL_KONTAKT_GLOWNY
                
                            from
                            accountbase a 
                            join contactbase c on a.primarycontactid=c.contactid
                            join stringmapbase mapa on a.elob_BusinessSegment=mapa.AttributeValue and mapa.ObjectTypeCode=1 and mapa.[LangId]=1033 and mapa.AttributeName='elob_BusinessSegment'
                            join elob_portfoliobase p on a.elob_Portfolio=p.elob_portfolioId
                            join systemuserbase su on a.ownerid=su.SystemUserId
                            left join SystemUserBase su1 on su.ParentSystemUserId=su1.SystemUserId
                
                            where
                            a.statecode=0 and a.elob_BusinessSegment not in ('1','2') --nie k2
                            and p.elob_name like 'KKB%'
                            and c.ContactId is not null
                            and a.elob_sowid not in 
                            ('1111111111R002','R5260016437R3917243','1111111111R001','1150006300','1181344530','1310185810','4435271286',
                            '5045876177','4796154768','5950002447','4054054037','1881881916','9059058982','1874726586','2475019362','4179755066','2354243830',
                            'R5452597594R2214940','3960520680','2225984122','6341624259','6861176577','4172162185','7742728255','9784731062','4676855126',
                            '4295543163','4459698814','K8812336','7457613270','1188665941','K1281365','1747077061','2710444807','9999990001','4845164070',
                            'K5645273','K10417615','3142171411','K6247219','1323738903','3493865659','4519403893','8157104056','5058094689','3466973534',
                            '2591030458','1152683560','9819448230','K11977016','6172139589','2814770928','9453262082','4543379141','1796179235','K10640910',
                            '1655844127','2773365029','1071043224','3265360032','2835892163','K7300890','K8812345','9542755828','1031331454','1230944254',
                            'K11622354','6328998356','9372344309','2780769638','7829430851','7730588587','1561637818','1477498797','7355954375','5524157670',
                            '5663427009','2533327785','K11200615','K6608')
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

