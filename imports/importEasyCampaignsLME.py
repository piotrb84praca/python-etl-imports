import pandas as pd
from sqlalchemy import text
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyCampaignsLME:
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
                    cursor.execute("BEGIN KLIK.CLEAN_TABLE('IMPORT_EASY_CAMPAIGNS_LME'); END;")   
                
    def importToOracle(self,df): 
               
                 # Prepare insert statement
                insert_stmt = text("""
                    INSERT INTO KLIK.IMPORT_EASY_CAMPAIGNS_LME ( 
                     nazwa_kampanii,
                    id_kampanii,
                    data_utw_kampanii,
                    dzialanie,
                    data_utworzenia,
                    temat_rozmowy,
                    data_utworzenia_rozmowy,
                    nazwa_kolejki,
                    data_wejscia_na_kolejke,
                    data_pobrania_z_kolejki,
                    podjete_z_kolejki_przez,
                    ostatnia_data_modyfikacji,
                    zmodyfikowano_przez,
                    obecny_wl_rozmowy_tel,
                    przewidywana_data_start,
                    przewidywana_data_konca,
                    stan_rozmowy,
                    status_rozmowy_tel,
                    data_odroczenia_sprawy,
                    notatka_z_rozmowy,
                    lead_tytul,
                    numer_listy_marketingowej,
                    id_sow_klienta,
                    nip_klienta,
                    klient,
                    odpowiedz_do_rozmowy,
                    tworca_rozmowy)
                    VALUES (
                            :NAZWA_KAMPANII,
                            :ID_KAMPANII,
                            :DATA_UTW_KAMPANII,
                            :DZIALANIE,
                            :DATA_UTWORZENIA,
                            :TEMAT_ROZMOWY,
                            :DATA_UTWORZENIA_ROZMOWY,
                            :NAZWA_KOLEJKI,
                            :DATA_WEJSCIA_NA_KOLEJKE,
                            :DATA_POBRANIA_Z_KOLEJKI,
                            :PODJETE_Z_KOLEJKI_PRZEZ,
                            :OSTATNIA_DATA_MODYFIKACJI,
                            :ZMODYFIKOWANO_PRZEZ,
                            :OBECNY_WL_ROZMOWY_TEL,
                            :PRZEWIDYWANA_DATA_START,
                            :PRZEWIDYWANA_DATA_KONCA,
                            :STAN_ROZMOWY,
                            :STATUS_ROZMOWY_TEL,
                            :DATA_ODROCZENIA_SPRAWY,
                            :NOTATKA_Z_ROZMOWY,
                            :LEAD_TYTUL,
                            :NUMER_LISTY_MARKETINGOWEJ,
                            :ID_SOW_KLIENTA,
                            :NIP_KLIENTA,
                            :KLIENT,
                            :ODPOWIEDZ_DO_ROZMOWY,
                            :TWORCA_ROZMOWY
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
camp.Name as NAZWA_KAMPANII
,camp.elob_campaignid as ID_KAMPANII
,format(camp.CreatedOn,'yyy-MM-dd') as DATA_UTW_KAMPANII
,replace(replace(replace(replace(replace(ap.[Subject],char(10),''),char(12),''),char(13),''),';',''),'^','') as  DZIALANIE
,format(ap.createdon,'yyyy-MM-dd') as DATA_UTWORZENIA
,replace(replace(replace(replace(replace(appc.[Subject],char(10),''),char(12),''),char(13),''),';',''),'^','') as  TEMAT_ROZMOWY

,case when appc.CreatedOn between '2020-03-29' and '2020-10-25' then format(dateadd(HH,2,appc.CreatedOn),'yyyy-MM-dd HH:mm')
when appc.CreatedOn between '2020-10-25' and '2021-03-28' then format(dateadd(HH,1,appc.CreatedOn),'yyyy-MM-dd HH:mm')
when appc.CreatedOn between '2021-03-28' and '2021-10-31' then format(dateadd(HH,2,appc.CreatedOn),'yyyy-MM-dd HH:mm')
else isnull(format(dateadd(HH,2,appc.CreatedOn ),'yyyy-MM-dd HH:mm'),'') end as DATA_UTWORZENIA_ROZMOWY
,supcc.fullname as TWORCA_ROZMOWY
,isnull(q.name,'') as NAZWA_KOLEJKI


,case when qi.EnteredOn  between '2020-03-29' and '2020-10-25' then format(dateadd(HH,2,qi.EnteredOn ),'yyyy-MM-dd HH:mm')
when qi.EnteredOn  between '2020-10-25' and '2021-03-28' then format(dateadd(HH,1,qi.EnteredOn ),'yyyy-MM-dd HH:mm')
when qi.EnteredOn  between '2021-03-28' and '2021-10-31' then format(dateadd(HH,2,qi.EnteredOn ),'yyyy-MM-dd HH:mm')
else isnull(format(dateadd(HH,2,qi.EnteredOn  ),'yyyy-MM-dd HH:mm'),'') end  as DATA_WEJSCIA_NA_KOLEJKE

,case when qi.WorkerIdModifiedOn  between '2020-03-29' and '2020-10-25' then format(dateadd(HH,2,qi.WorkerIdModifiedOn ),'yyyy-MM-dd HH:mm')
when qi.WorkerIdModifiedOn  between '2020-10-25' and '2021-03-28' then format(dateadd(HH,1,qi.WorkerIdModifiedOn ),'yyyy-MM-dd HH:mm')
when qi.WorkerIdModifiedOn  between '2021-03-28' and '2021-10-31' then format(dateadd(HH,2,qi.WorkerIdModifiedOn ),'yyyy-MM-dd HH:mm')
else isnull(format(dateadd(HH,2,qi.WorkerIdModifiedOn  ),'yyyy-MM-dd HH:mm'),'') end  as DATA_POBRANIA_Z_KOLEJKI
,isnull(suw.fullname,'') as PODJETE_Z_KOLEJKI_PRZEZ

,case when appc.ModifiedOn between '2020-03-29' and '2020-10-25' then format(dateadd(HH,2,appc.ModifiedOn),'yyyy-MM-dd HH:mm')
when appc.ModifiedOn between '2020-10-25' and '2021-03-28' then format(dateadd(HH,1,appc.ModifiedOn),'yyyy-MM-dd HH:mm')
when appc.ModifiedOn between '2021-03-28' and '2021-10-31' then format(dateadd(HH,2,appc.ModifiedOn),'yyyy-MM-dd HH:mm')
else isnull(format(dateadd(HH,2,appc.ModifiedOn ),'yyyy-MM-dd HH:mm'),'') end as OSTATNIA_DATA_MODYFIKACJI
,supcm.fullname as ZMODYFIKOWANO_PRZEZ
,supc.fullname as OBECNY_WL_ROZMOWY_TEL

,case when appc.scheduledstart  between '2020-03-29' and '2020-10-25' then format(dateadd(HH,2,appc.scheduledstart ),'yyyy-MM-dd HH:mm')
when appc.scheduledstart  between '2020-10-25' and '2021-03-28' then format(dateadd(HH,1,appc.scheduledstart ),'yyyy-MM-dd HH:mm')
when appc.scheduledstart  between '2021-03-28' and '2021-10-31' then format(dateadd(HH,2,appc.scheduledstart ),'yyyy-MM-dd HH:mm')
else isnull(format(dateadd(HH,2,appc.scheduledstart  ),'yyyy-MM-dd HH:mm'),'') end as PRZEWIDYWANA_DATA_START


,case when appc.scheduledend between '2020-03-29' and '2020-10-25' then format(dateadd(HH,2,appc.scheduledend),'yyyy-MM-dd HH:mm')
when appc.scheduledend between '2020-10-25' and '2021-03-28' then format(dateadd(HH,1,appc.scheduledend),'yyyy-MM-dd HH:mm')
when appc.scheduledend between '2021-03-28' and '2021-10-31' then format(dateadd(HH,2,appc.scheduledend),'yyyy-MM-dd HH:mm')
else isnull(format(dateadd(HH,2,appc.scheduledend ),'yyyy-MM-dd HH:mm'),'') end as PRZEWIDYWANA_DATA_KONCA

,mappc1.value as STAN_ROZMOWY
,mappc.Value as STATUS_ROZMOWY_TEL

,case when pc.elob_onholddate  between '2020-03-29' and '2020-10-25' then format(dateadd(HH,2,pc.elob_onholddate ),'yyyy-MM-dd HH:mm')
when pc.elob_onholddate  between '2020-10-25' and '2021-03-28' then format(dateadd(HH,1,pc.elob_onholddate ),'yyyy-MM-dd HH:mm')
when pc.elob_onholddate  between '2021-03-28' and '2021-10-31' then format(dateadd(HH,2,pc.elob_onholddate ),'yyyy-MM-dd HH:mm')
else isnull(format(dateadd(HH,2,pc.elob_onholddate  ),'yyyy-MM-dd HH:mm'),'') end  as DATA_ODROCZENIA_SPRAWY
,isnull(replace(replace(replace(replace(replace(pc.new_1,char(10),''),char(12),''),char(13),''),';',''),'^',''),'') as NOTATKA_Z_ROZMOWY

,isnull(l.[Subject],'') as LEAD_TYTUL
,isnull(elob_marketinglistid,'') as NUMER_LISTY_MARKETINGOWEJ
,isnull(a.elob_sowid,'') as ID_SOW_KLIENTA
,isnull(a.elob_nip,'') as NIP_KLIENTA
,replace(replace(replace(replace(replace(a.name,char(10),''),char(12),''),char(13),''),'^',''),';','') as KLIENT
,isnull(mca.elob_answer,'') as ODPOWIEDZ_DO_ROZMOWY

from
---kampania
CampaignBase camp
join systemuserbase suc on camp.CreatedBy=suc.SystemUserId
--działanie w ramach kampanii CA
join ActivityPointerBase ap on camp.CampaignId=ap.RegardingObjectId
join CampaignActivityBase ca on ap.ActivityId=ca.ActivityId
---Phone Call
join activitypointerbase appc on ca.ActivityId=appc.RegardingObjectId
join PhoneCallBase pc on appc.ActivityId=pc.ActivityId
join systemuserbase supc on appc.OwnerId=supc.SystemUserId
join systemuserbase supcc on appc.createdby=supcc.SystemUserId
join systemuserbase supcm on appc.ModifiedBy=supcm.SystemUserId

join stringmapbase mappc on appc.StatusCode=mappc.AttributeValue and mappc.ObjectTypeCode=4210 and mappc.[LangId]=1045 and mappc.AttributeName='StatusCode'
join stringmapbase mappc1 on appc.statecode=mappc1.AttributeValue and mappc1.ObjectTypeCode=4210 and mappc1.[LangId]=1045 and mappc1.AttributeName='statecode'
left join elob_marketingcampainanswersBase mca on pc.elob_answer=mca.elob_marketingcampainanswersId
--Kolejka
left join QueueItemBase qi on pc.ActivityId=qi.ObjectId
left join queuebase q on qi.queueid=q.QueueId
left join SystemUserBase suw on qi.WorkerId=suw.systemuserid

---Lead
left join LeadBase l on pc.elob_RelatedLead=l.LeadId
-- left join accountbase a on l.parentaccountid=a.accountid
left join accountbase a on pc.elob_relatedaccount=a.accountid

where
suc.systemuserid in ('979B6E5C-08BE-E611-80D7-00505601285D','8CFE75D1-CD3D-EA11-80EE-0050560100C4') --Kranc Paweł, Kozak Arek

and (qi.queueid in ('1FE78150-9C9A-EA11-80EE-0050560100C4')--kolejka do kampanii: K2_LME_Kampania 
or qi.queueid is null
)
and supcc.systemuserid not in ('A8B2EB58-FA35-E511-80CC-00505601285C')
and appc.StatusCode not in ('3') /*PB nie anulowane*/
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

