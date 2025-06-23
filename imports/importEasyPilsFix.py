import pandas as pd
from sqlalchemy import text
from datetime import datetime, timedelta
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyPilsFix:
    
    def __init__(self):
            self.mssql_conn = ConnectFort()
            self.oracle_engine = ConnectOracleDevEngine()           
            self.start_pos = 0
            self.batch_size = 15000
            self.logger = ImportLogger()
            self.oracle_connection = self.oracle_engine.connect()            
        
        
    def deleteFromOracle(self):        
 
                with self.oracle_connection.begin():
                    cursor = self.oracle_engine.get_cursor()
                    cursor.execute("BEGIN KLIK.CLEAN_TABLE('IMPORT_EASY_FIX_PILS'); END;") 
                    #self.oracle_connection.execute(text("TRUNCATE TABLE IMPORT_EASY_FIX_PILS"))
                
    def importToOracle(self,df):                

                insert_stmt = text("""
                    INSERT into IMPORT_EASY_FIX_PILS (
                    id_projektu,
                    data_utw_pil,
                    id_pil,
                    typ_pila,
                    status_pil,
                    pil_finalny,
                    czy_moc,      
                    poziom_akceptacji,
                    kod_akceptacji,
                    sciezka_akceptacji_pil,                    
                    marza_kontrakt_proc,
                    wartosc_reve_nowa,
                    marza_uwzgl_prowizje_proc,
                    nazwa_uslugi_produktu,
                    utrzymanie_akwizycja,
                    dl_lojalki,
                    marza_jedn_na_produkcie,
                    ilosc,
                    abo_za_szt,                
                    ulga_oplata_ins,
                    dlugosc_kontraktu,
                    oplata_ins,
                    rodzaj_sprzedazy,
                    opcja_produktu_uslugi,           
                    ilosc_portow,
                    oplata_mies_serwis,             
                    ilosc_pakietow,
                    projekt_flaga_koronawirus,
                    opcja_sla,                            
                    oplata_za_sla,
                    odsprzedaz_sprzetu_wartosc,
                    kanal_sprzedazy,
                    marza_sprzedaz_gotowka_raty
                    )
                VALUES 
                        (
                            :id_projektu,
                            :data_utw_pil,
                            :id_pil,
                            :typ_pila,
                            :status_pil,
                            :pil_finalny,
                            :czy_moc,      
                            :poziom_akceptacji,
                            :kod_akceptacji,
                            :sciezka_akceptacji_pil,                            
                            :marza_kontrakt_proc,
                            :wartosc_reve_nowa,
                            :marza_uwzgl_prowizje_proc,
                            :nazwa_uslugi_produktu,
                            :utrzymanie_akwizycja,
                            :dl_lojalki,
                            :marza_jedn_na_produkcie,
                            :ilosc,
                            :abo_za_szt,                
                            :ulga_oplata_ins,
                            :dlugosc_kontraktu,
                            :oplata_ins,
                            :rodzaj_sprzedazy,
                            :opcja_produktu_uslugi,           
                            :ilosc_portow,                            
                            :oplata_mies_serwis,             
                            :ilosc_pakietow,
                            :projekt_flaga_koronawirus,
                            :opcja_sla,                            
                            :oplata_za_sla,
                            :odsprzedaz_sprzetu_wartosc,                            
                            :kanal_sprzedazy,
                            :marza_sprzedaz_gotowka_raty                                                                         
                         )""")
    
                # Insert new data into Oracle using executemany for bulk insert
                self.oracle_connection.execute(insert_stmt, df.to_dict(orient='records'))
                # Commit the transaction
                self.oracle_connection.commit()
    
    def parseData(self, df):

                   
                   # df['marza_kontrakt_proc'] = df['marza_kontrakt_proc'].fillna('').replace(".", ",")
        
                    return df
        
    def run(self):
            try:
                
                self.logger.start_import(self.__class__.__name__)             
                mssql_connection = self.mssql_conn.connect()              

                query = """ 
                select
                o.elob_OpportunityID as [id_projektu]
,format(ap.createdon,'yyyy-MM-dd') as [data_utw_pil]
,cast(pl.elob_pl_id as int) as [id_pil]
,case when pl.elob_PLType=2 then 'Mobile' when pl.elob_PLType=1 then 'Fix' else '' end as [typ_pila]
,case when ap.statuscode is null then '' else mappl.value end as [status_pil]
,case when pl.elob_FinalPL=1 then 'Tak' when pl.elob_FinalPL=0 then 'Nie' else '' end as [pil_finalny]
,case when plp_moc.elob_name is not null then 'Tak' else 'Nie' end as [czy_moc]
,cast(pl.elob_AcceptanceLevel as int) as [poziom_akceptacji]
,pls.elob_acceptance_code as [kod_akceptacji]
,isnull(pls.elob_acceptancepath,'') as [sciezka_akceptacji_pil]
,isnull(cast(pls.elob_fix_margin_on_contract as float),'') as [marza_kontrakt_proc]
,isnull(cast(pls.elob_fix_total_revenue_new as float),'') as [wartosc_reve_nowa]
,isnull(cast(plp10.elob_ParameterValue as float),0) as [marza_uwzgl_prowizje_proc]
,isnull(plp01.elob_ParameterValue,'') as [nazwa_uslugi_produktu]
,case when plp01.elob_ParameterValue like '%utrzymanie' then 'Utrzymanie'
when plp01.elob_ParameterValue like '%akwizycja' then 'Akwizycja' else '' 
end as [utrzymanie_akwizycja]
,case when plp01.elob_ParameterValue like '%umowa na 12%' then '12'
when plp01.elob_ParameterValue like '%umowa na 24%' then '24'
when plp01.elob_ParameterValue like '%kontrakt na 24%' then '24' 
when plp01.elob_ParameterValue like '%kontrakt na 12%' then '12' 
when plp01.elob_ParameterValue like '%kontrakt na 12/24%' then '12/24(?)'  else ''
end as [dl_lojalki]
,isnull(cast(plp02.elob_ParameterValue as float),'') as [marza_jedn_na_produkcie]
,isnull(cast(plp04.elob_ParameterValue as float),'') as [ilosc],
isnull(cast(plp05.elob_ParameterValue as float),'') as [abo_za_szt]
,isnull(cast(plp06.elob_ParameterValue as float),'') as [ulga_oplata_ins]
,isnull(cast(plp07.elob_ParameterValue as float),'') as [dlugosc_kontraktu]
,isnull(cast(plp08.elob_ParameterValue as float),'') as [oplata_ins]
,isnull(plp09.elob_ParameterValue,'') as [rodzaj_sprzedazy]
,isnull(plp11.elob_ParameterValue,'') as [opcja_produktu_uslugi]
,isnull(cast(plp16.elob_ParameterValue as float),'') as [ilosc_portow]
,isnull(cast(plp17.elob_ParameterValue as float),'') as [oplata_mies_serwis]
,isnull(cast(plp19.elob_ParameterValue as int),'') as [ilosc_pakietow] 
,isnull(plp22.elob_ParameterValue,'0') as [opcja_sla]
,isnull(cast(plp23.elob_ParameterValue as float) ,'0') as [oplata_za_sla]
,isnull(plp24.elob_ParameterValue,'0') as [odsprzedaz_sprzetu_wartosc]
,case when o.elob_coronavirus=1 then 'Tak' when o.elob_coronavirus=0 then 'Nie' end as [projekt_flaga_koronawirus]
,isnull(plp_sc.elob_ParameterValue,'') as [kanal_sprzedazy]
,isnull(plp_msg.elob_ParameterValue,'') as [marza_sprzedaz_gotowka_raty]
from
elob_plbase pl
join activitypointerbase ap on pl.ActivityId=ap.ActivityId
--left join elob_validationbase v on pl.ActivityId=v.elob_PL
join elob_plresultBase pls on pl.elob_PLstatistic=pls.elob_plresultId--stat 
join elob_plsalesresultBase plr on pls.elob_pandl_id=plr.elob_pandl_id
join OpportunityBase o on pl.elob_Project=o.OpportunityId
join accountbase a on o.CustomerId= a.accountid
join stringmapbase mappl on ap.statuscode=mappl.AttributeValue and mappl.ObjectTypeCode=10046 and mappl.[LangId]=1045 and mappl.AttributeName='statuscode'
--nazwa usługi
left join 
( 
select plr.elob_pandl_id, plp01.elob_PL_ResultId,
case when plp01.elob_name ='fix_service_ons_1' then 'fix_service_ons_355'
when plp01.elob_name ='fix_service_ons_2' then 'fix_service_ons_356' else plp01.elob_name end as [elob_name]
,plp01.elob_ParameterValue
from
elob_plsalesresultBase plr
join elob_plparameterbase plp01 on plr.elob_pandl_id=plp01.elob_PL_ResultId 
and (plp01.elob_name like'fix_service_(%'--,'fix_option_miejski ethernet_%'
or plp01.elob_name like'fix_option_miejski%' or plp01.elob_name like 'fix_service_ons%' 
or plp01.elob_name like 'fix_service_fixvoice%' or plp01.elob_name like 'fix_option_other%'
or plp01.elob_name like 'fix_service_ons%' or plp01.elob_name like 'fix_service_ssl%' or plp01.elob_name like 'fix_option_itdf%'
or plp01.elob_name like 'fix_option_managed%' or plp01.elob_name like 'fix_option_pabx%' or plp01.elob_name like 'fix_option_printer%'
or plp01.elob_name like 'fix_option_map_of_competences%')
) plp01 on plr.elob_pandl_id=plp01.elob_pandl_id
left join elob_plparameterbase plp02 on plp01.elob_PL_ResultId=plp02.elob_PL_ResultId and plp02.elob_name like'fix_unitary_margin%'
and substring(plp01.elob_name, len(plp01.elob_name)-charindex('_',reverse(plp01.elob_name))+2,charindex('_',reverse(plp01.elob_name)))=
substring(plp02.elob_name, len(plp02.elob_name)-charindex('_',reverse(plp02.elob_name))+2,charindex('_',reverse(plp02.elob_name)))
left join elob_plparameterbase plp05 on plp01.elob_PL_ResultId=plp05.elob_PL_ResultId and plp05.elob_name like 'fix_subscription_%'
and substring(plp01.elob_name, len(plp01.elob_name)-charindex('_',reverse(plp01.elob_name))+2,charindex('_',reverse(plp01.elob_name)))=
substring(plp05.elob_name, len(plp05.elob_name)-charindex('_',reverse(plp05.elob_name))+2,charindex('_',reverse(plp05.elob_name)))
left join elob_plparameterbase plp04 on plp01.elob_PL_ResultId=plp04.elob_PL_ResultId and plp04.elob_name like'fix_quantity%'
and substring(plp01.elob_name, len(plp01.elob_name)-charindex('_',reverse(plp01.elob_name))+2,charindex('_',reverse(plp01.elob_name)))=
substring(plp04.elob_name, len(plp04.elob_name)-charindex('_',reverse(plp04.elob_name))+2,charindex('_',reverse(plp04.elob_name)))
left join elob_plparameterbase plp06 on plp01.elob_PL_ResultId=plp06.elob_PL_ResultId and plp06.elob_name like'fix_installation_discount_%'
and substring(plp01.elob_name, len(plp01.elob_name)-charindex('_',reverse(plp01.elob_name))+2,charindex('_',reverse(plp01.elob_name)))=
substring(plp06.elob_name, len(plp06.elob_name)-charindex('_',reverse(plp06.elob_name))+2,charindex('_',reverse(plp06.elob_name)))
left join elob_plparameterbase plp07 on plp01.elob_PL_ResultId=plp07.elob_PL_ResultId and plp07.elob_name like'fix_contract_length%'
and substring(plp01.elob_name, len(plp01.elob_name)-charindex('_',reverse(plp01.elob_name))+2,charindex('_',reverse(plp01.elob_name)))=
substring(plp07.elob_name, len(plp07.elob_name)-charindex('_',reverse(plp07.elob_name))+2,charindex('_',reverse(plp07.elob_name)))
left join elob_plparameterbase plp08 on plp01.elob_PL_ResultId=plp08.elob_PL_ResultId and (plp08.elob_name like 'fix_installation_2'
or plp08.elob_name like 'fix_installation_1' or plp08.elob_name like 'fix_installation_3' or plp08.elob_name like 'fix_installation_4'
or plp08.elob_name like 'fix_installation_5' or plp08.elob_name like 'fix_installation_6' or plp08.elob_name like 'fix_installation_7'
or plp08.elob_name like 'fix_installation_8' or plp08.elob_name like 'fix_installation_9' or plp08.elob_name like 'fix_installation_1%'
or plp08.elob_name like 'fix_installation_2%' or plp08.elob_name like 'fix_installation_3%' or plp08.elob_name like 'fix_installation_4%'
or plp08.elob_name like 'fix_installation_5%' or plp08.elob_name like 'fix_installation_6%' or plp08.elob_name like 'fix_installation_7%'
or plp08.elob_name like 'fix_installation_9%' or plp08.elob_name like 'fix_installation_9%'
)

and substring(plp01.elob_name, len(plp01.elob_name)-charindex('_',reverse(plp01.elob_name))+2,charindex('_',reverse(plp01.elob_name)))=
substring(plp08.elob_name, len(plp08.elob_name)-charindex('_',reverse(plp08.elob_name))+2,charindex('_',reverse(plp08.elob_name)))
-----jaki rodzaj sprzedaży
left join elob_plparameterbase plp09 on plp01.elob_PL_ResultId=plp09.elob_PL_ResultId and plp09.elob_name like'fix_new_sales_retention__%'
and substring(plp01.elob_name, len(plp01.elob_name)-charindex('_',reverse(plp01.elob_name))+2,charindex('_',reverse(plp01.elob_name)))=
substring(plp09.elob_name, len(plp09.elob_name)-charindex('_',reverse(plp09.elob_name))+2,charindex('_',reverse(plp09.elob_name)))
---marża uwzględniająca prowizję
left join elob_plparameterbase plp10 on plr.elob_pandl_id=plp10.elob_PL_ResultId and plp10.elob_name like 'fix_margin_80'
left join elob_plparameterbase plp11 on plp01.elob_PL_ResultId=plp11.elob_PL_ResultId 

and (plp11.elob_name like'fix_type_miejski eth%' or plp11.elob_name like'fix_service_level%' or plp11.elob_name like 'fix_support_1%'
or plp11.elob_name like 'fix_support_2%' or plp11.elob_name like 'fix_support_3%' or plp11.elob_name like 'fix_support_4%'
or plp11.elob_name like 'fix_support_5%' or plp11.elob_name like 'fix_support_6%' or plp11.elob_name like 'fix_support_7%'
or plp11.elob_name like 'fix_support_8%' or plp11.elob_name like 'fix_support_9%'
)
and substring(plp01.elob_name, len(plp01.elob_name)-charindex('_',reverse(plp01.elob_name))+2,charindex('_',reverse(plp01.elob_name)))=
substring(plp11.elob_name, len(plp11.elob_name)-charindex('_',reverse(plp11.elob_name))+2,charindex('_',reverse(plp11.elob_name)))

left join elob_plparameterbase plp16 on plp01.elob_PL_ResultId=plp16.elob_PL_ResultId and (plp16.elob_name like 'fix_number_ports%' or plp16.elob_name like 'fix_number_of_ports%')
and substring(plp01.elob_name, len(plp01.elob_name)-charindex('_',reverse(plp01.elob_name))+2,charindex('_',reverse(plp01.elob_name)))=
substring(plp16.elob_name, len(plp16.elob_name)-charindex('_',reverse(plp16.elob_name))+2,charindex('_',reverse(plp16.elob_name)))
-- opłata miesieczna za serwis fix_support_subscription_90
left join elob_plparameterbase plp17 on plp01.elob_PL_ResultId=plp17.elob_PL_ResultId and plp17.elob_name like 'fix_support_subscription%'
and substring(plp01.elob_name, len(plp01.elob_name)-charindex('_',reverse(plp01.elob_name))+2,charindex('_',reverse(plp01.elob_name)))=
substring(plp17.elob_name, len(plp17.elob_name)-charindex('_',reverse(plp17.elob_name))+2,charindex('_',reverse(plp17.elob_name)))

left join elob_plparameterbase plp19 on plp01.elob_PL_ResultId=plp19.elob_PL_ResultId and plp19.elob_name like 'fix_number_services_%'
and substring(plp01.elob_name, len(plp01.elob_name)-charindex('_',reverse(plp01.elob_name))+2,charindex('_',reverse(plp01.elob_name)))=
substring(plp19.elob_name, len(plp19.elob_name)-charindex('_',reverse(plp19.elob_name))+2,charindex('_',reverse(plp19.elob_name)))

left join elob_plparameterBase plp_moc on plr.elob_pandl_id=plp_moc.elob_PL_ResultId and plp_moc.elob_name ='Mapa kompetencji usługa' 
left join elob_plparameterbase plp22 on plp01.elob_PL_ResultId=plp22.elob_PL_ResultId and plp22.elob_name like 'fix_sla_option_%'
and substring(plp01.elob_name, len(plp01.elob_name)-charindex('_',reverse(plp01.elob_name))+2,charindex('_',reverse(plp01.elob_name)))=
substring(plp22.elob_name, len(plp22.elob_name)-charindex('_',reverse(plp22.elob_name))+2,charindex('_',reverse(plp22.elob_name)))

left join elob_plparameterbase plp23 on plp01.elob_PL_ResultId=plp23.elob_PL_ResultId and plp23.elob_name like 'fix_sla_charges_%'
and substring(plp01.elob_name, len(plp01.elob_name)-charindex('_',reverse(plp01.elob_name))+2,charindex('_',reverse(plp01.elob_name)))=
substring(plp23.elob_name, len(plp23.elob_name)-charindex('_',reverse(plp23.elob_name))+2,charindex('_',reverse(plp23.elob_name)))
---wartosc sprzedazy 2021-07-30
left join elob_plparameterbase plp24 on plp01.elob_PL_ResultId=plp24.elob_PL_ResultId and plp24.elob_name like 'fix_sum_payments_%'
and substring(plp01.elob_name, len(plp01.elob_name)-charindex('_',reverse(plp01.elob_name))+2,charindex('_',reverse(plp01.elob_name)))=
substring(plp24.elob_name, len(plp24.elob_name)-charindex('_',reverse(plp24.elob_name))+2,charindex('_',reverse(plp24.elob_name)))
left join elob_plparameterbase plp_sc on plr.elob_pandl_id=plp_sc.elob_PL_ResultId and plp_sc.elob_name='kanał sprzedaży'
left join elob_plparameter plp_msg on plr.elob_pandl_id=plp_msg.elob_PL_ResultId and plp_msg.elob_name='fix_marigin_cash_and_installment'

where
---utworzone (zakłada sie ze przeliczone od 01.06.2022, zatwierdzone
ap.createdon >='2022-06-01' and ap.statuscode='2'
and a.elob_sowid not in 
('1111111111R002','R5260016437R3917243','1111111111R001','1150006300','1181344530','1310185810','4435271286',
'5045876177','4796154768','5950002447','4054054037','1881881916','9059058982','1874726586','2475019362','4179755066','2354243830',
'R5452597594R2214940','3960520680','2225984122','6341624259','6861176577','4172162185','7742728255','9784731062','4676855126',
'4295543163','4459698814','K8812336','7457613270','1188665941','K1281365','1747077061','2710444807','9999990001','4845164070',
'K5645273','K10417615','3142171411','K6247219','1323738903','3493865659','4519403893','8157104056','5058094689','3466973534',
'2591030458','1152683560','9819448230','K11977016','6172139589','2814770928','9453262082','4543379141','1796179235','K10640910',
'1655844127','2773365029','1071043224','3265360032','2835892163','K7300890','K8812345','9542755828','1031331454','1230944254',
'K11622354','6328998356','9372344309','2780769638','7829430851','7730588587','1561637818','1477498797','7355954375','5524157670',
'5663427009','2533327785','K11200615','K6608149','S22011687378014','8163869987','8247230801','K4440434','K5995889','1751293612',
'K7632452','3451234512','5123451234')
                """

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