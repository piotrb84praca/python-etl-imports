import pandas as pd
from sqlalchemy import text
from datetime import datetime, timedelta
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyPils:
    def __init__(self):
            self.mssql_conn = ConnectFort()
            self.oracle_engine = ConnectOracleDevEngine()           
            self.start_pos = 0
            self.batch_size = 15000
            self.logger = ImportLogger()
            self.oracle_connection = self.oracle_engine.connect()            
            self.clear_date = '2023-06-01' #(datetime.now() - relativedelta(years=2)).strftime("%Y-%m-%d") #2yr
        
    def deleteFromOracle(self):        
        
                self.logger.end_import('Dane od: '+ self.clear_date);     
                with self.oracle_connection.begin():
                    self.oracle_connection.execute(text("DELETE FROM IMPORT_EASY_PILS where mo_createdon >= :clear_date OR mo_createdon is null OR mo_createdon =''"),  {"clear_date": self.clear_date})
                
    def importToOracle(self,df): 
               
                 # Prepare insert statement
                insert_stmt = text("""
                               INSERT INTO IMPORT_EASY_PILS (
                                                NIP,
                                                ID_SOW,
                                                OWNER,
                                                OWNER_CODE,
                                                PROJECT_ID,
                                                PROJECT_OWNER,
                                                PROJECT_OWNER_CODE,
                                                EASY_ORDER,
                                                PIL_ID,
                                                REVE_TOTAL,
                                                REVE_INVOICES,
                                                REVE_OPERATORS,
                                                MARGIN,
                                                MARGIN_TOTAL,
                                                INFO_FULL,
                                                INFO,
                                                MO_ID,
                                                MO_STATUS,
                                                MO_STATE,
                                                SALE_TYPE,
                                                CONTRACT,
                                                CONTRACT_TYPE,
                                                CONTRACT_STATE,
                                                CONTRACT_DATE,
                                                CONTRACT_VERIFICATION,
                                                CONTRACT_PERIOD,
                                                PRODUCT,
                                                PRODUCT_QUANTITY,
                                                PRODUCT_DURATION,
                                                PRODUCT_REVE_MONTHLY,
                                                PRODUCT_REVE_MISC,
                                                COST_VARIABLE,
                                                COST_COMISION,
                                                COST_OTHER,
                                                MO_CREATEDON
                                            ) VALUES (
                                                :NIP,
                                                :ID_SOW,
                                                :OWNER,
                                                :OWNER_CODE,
                                                :PROJECT_ID,
                                                :PROJECT_OWNER,
                                                :PROJECT_OWNER_CODE,
                                                :EASY_ORDER,
                                                :PIL_ID,
                                                :REVE_TOTAL,
                                                :REVE_INVOICES,
                                                :REVE_OPERATORS,
                                                :MARGIN,
                                                :MARGIN_TOTAL,
                                                :INFO_FULL,
                                                :INFO,
                                                :MO_ID,
                                                :MO_STATUS,
                                                :MO_STATE,
                                                :SALE_TYPE,
                                                :CONTRACT,
                                                :CONTRACT_TYPE,
                                                :CONTRACT_STATE,
                                                :CONTRACT_DATE,
                                                :CONTRACT_VERIFICATION,
                                                :CONTRACT_PERIOD,
                                                :PRODUCT,
                                                :PRODUCT_QUANTITY,
                                                :PRODUCT_DURATION,
                                                :PRODUCT_REVE_MONTHLY,
                                                :PRODUCT_REVE_MISC,
                                                :COST_VARIABLE,
                                                :COST_COMISION,
                                                :COST_OTHER,
                                                :MO_CREATEDON
                                            )""")
    
                # Insert new data into Oracle using executemany for bulk insert
                self.oracle_connection.execute(insert_stmt, df.to_dict(orient='records'))
                # Commit the transaction
                self.oracle_connection.commit()
    
    def parseData(self, df):

                    df['MARGIN_TOTAL'] = df['MARGIN_TOTAL'].apply(
                            lambda x: round(float(x), 2) if x not in ['#N/A', ''] else 0
                    )
                    df['INFO_FULL'] = ''#df['INFO_FULL'].apply(lambda x: x.replace("&", " AND ").replace("'", "")[:1000])
                    df['INFO'] = df['INFO'].apply(lambda x: x.replace("&", " AND ").replace("'", "")[:450])
                    

                    return df
        
    def run(self):
            try:
                
                self.logger.start_import(self.__class__.__name__)
    
                # Connect to MSSQL
                mssql_connection = self.mssql_conn.connect()

                self.logger.end_import('Dane od 1: '+ self.clear_date);     
                # Extract data from MSSQL Query 1
                query = """ 
                SELECT distinct 
                isnull(a.elob_nip,'') as [NIP]
                ,a.elob_sowid as [ID_SOW]
                ,su.FullName as [OWNER]
                ,isnull(su.elob_salespersonid,'') as [OWNER_CODE]
                ,o.elob_OpportunityID as [PROJECT_ID]
                ,suo.fullname as [PROJECT_OWNER]
                ,isnull(suo.elob_salespersonid,'') as [PROJECT_OWNER_CODE]
                ,isnull(ord.OrderNumber,'') as [EASY_ORDER]                
                ,pl.elob_PL_Id as [PIL_ID]
                ,cast(pls.elob_mobile_CommittedRevenuekPLN*1000 as float) as [REVE_TOTAL]
                ,cast(pls.elob_mobile_reve_invoices as float) as [REVE_INVOICES]
                ,cast(pls.elob_mobile_reve_operators as float) as [REVE_OPERATORS]
                ,cast(pls.elob_mobile_ContributionMargin as float) as [MARGIN]                
                ,cast(pls.elob_mobile_cost_variablecosts as float) as [COST_VARIABLE]
                ,cast(pls.elob_mobile_cost_other as float) as [COST_OTHER]
                ,cast(pls.elob_mobile_cost_comision as float) as [COST_COMISION]                ,case 
                    when ts_old.field_code is not null then ts_old.field_value --PB2022.03.25
                    when ts1.field_code is null and ts.field_code is not null then ts.field_value 
                    when ts1.field_code is not null and ts.field_code is null then ts1.field_value
                    else '' end as [MARGIN_TOTAL]   ---marża z PiL Tool        
                ,isnull(replace(replace(plp01.elob_ParameterValue,char(10),''),char(13),''),'') as [INFO_FULL]
                ,isnull(replace(replace(substring((plp01.elob_ParameterValue),176,500),char(10),''),char(13),''),'') as [INFO] --500 znaków po intro
                ----
                ,isnull(mo.elob_mobileordernumber,'') as [MO_ID]
                ,isnull(mapmo.Value,'') as [MO_STATUS] --mo.statuscode
                ,case when psb.StageName is null then ' ' else psb.StageName end as [MO_STATE]
                ,isnull(mapmo1.value,'') as [SALE_TYPE] --,mo.elob_mobileordertype                
                ,isnull(c.ContractNumber,'') as [CONTRACT]
                ,isnull(mapc.value,'') as [CONTRACT_STATE]
                ,isnull(mapc1.value,'') as [CONTRACT_TYPE]
                ,isnull(format(c.elob_SignedbyCustomeron+1,'yyyy-MM-dd'),'') as [CONTRACT_DATE]
                ,isnull(format(c.elob_revisiondate,'yyyy-MM-dd'),'') as [CONTRACT_VERIFICATION]
                ,isnull(cast(c.elob_Durationtimeinmonths as int),'') as [CONTRACT_PERIOD] --niestety nie jest uzupełniane dla mobile.                
                ,isnull(mop.elob_name,'') as [PRODUCT]
                ,isnull(cast(mop.elob_quantity as int),'') as [PRODUCT_QUANTITY]
                ,case when mop.elob_contractduration is null then '' 
                    when mop.elob_contractduration=100 then cast(mop.elob_specifydurationinmonths as int)
                    when mop.elob_contractduration<100 then cast(mop.elob_contractduration as int) else ''
                    end as [PRODUCT_DURATION]
                ,case when mop.elob_priceperunitmonth  is null then '' else cast(mop.elob_priceperunitmonth as float) end as [PRODUCT_REVE_MONTHLY]
                ,case when mop.elob_upfrontpayment is null then '' else cast(mop.elob_upfrontpayment as float) end as [PRODUCT_REVE_MISC],
                CONVERT(char(10),mo.CreatedOn,126) as MO_CREATEDON
            FROM
                elob_plbase pl
                join OpportunityBase o on pl.elob_Project=o.OpportunityId
                join accountbase a on o.customerid=a.accountid
                left join salesorderbase ord on o.OpportunityId=ord.OpportunityId --zam. EASy
                ------
                left join elob_mobileorderBase mo on ord.SalesOrderId=mo.elob_order-- zam. mobile
                left join elob_mobileorderproductBase mop on mo.elob_mobileorderId=mop.elob_mobileorder

                ---------
                left join ProcessStageBase psb on mo.stageid=psb.ProcessStageId

                left join stringmapbase mapmo on mo.statuscode=mapmo.AttributeValue and mapmo.ObjectTypeCode=10034 and mapmo.[LangId]=1033 --mozna zmienić na pl 1045, ale mniej jane będzie
                and mapmo.AttributeName='statuscode'
                left join stringmapbase mapmo1 on mo.elob_mobileordertype=mapmo1.AttributeValue and mapmo1.ObjectTypeCode=10034 and mapmo1.[LangId]=1033 --nie zamieniac
                and mapmo1.AttributeName='elob_mobileordertype'
                -----

                left join elob_plresultBase pls on pl.elob_PLstatistic=pls.elob_plresultId--stat 
                left join elob_plsalesresultBase plr on pls.elob_pandl_id=plr.elob_pandl_id
                left join elob_plparameterBase plp01 on plr.elob_pandl_id=plp01.elob_PL_ResultId and plp01.elob_name ='komentarz do metryczki'

                ---piltool
                left join FORT_Produkcja_V2.dbo.piltool_miscellaneous_field ts on pl.elob_PL_Id collate Polish_CI_AS=ts.sfa_pil_id
                and ts.field_code in ('MOBILE_M2M|TOTAL|CF|TOTAL_CF_ZL_DISCOUNTED_CASH_FLOW_DPV') 
                left join FORT_Produkcja_V2.dbo.piltool_miscellaneous_field ts1 on pl.elob_PL_Id collate Polish_CI_AS=ts1.sfa_pil_id
                and ts1.field_code in ('MOBILE_M2M|TOTAL|CF|TOTAL_CF_ZL_DISCOUNTED_CASH_FLOW_DPV')
                -- PB2022.03.25 dodanie wartosci old
                left join FORT_Produkcja_V2.dbo.piltool_miscellaneous_field ts_old on pl.elob_PL_Id collate Polish_CI_AS=ts_old.sfa_pil_id
               	and ts_old.field_code in ('MOBILE_M2M|TOTAL|CF|TOTAL_CF_ZL_DISCOUNTED_CASH_FLOW_DPV_old') 
                ---umowa
                left join contractbase c on mo.elob_contract=c.ContractId

                ---userzy
                join systemuserbase su on a.ownerid=su.SystemUserId
                join systemuserbase suo on o.ownerid=suo.SystemUserId

                ---mapsy do umowy
                left join stringmapbase mapc on c.statuscode=mapc.AttributeValue and mapc.ObjectTypeCode=1010 and mapc.[LangId]=1045 and mapc.AttributeName='statuscode' --zostawić pl
                left join stringmapbase mapc1 on c.elob_ContractType=mapc1.AttributeValue and mapc1.ObjectTypeCode=1010 and mapc1.[LangId]=1045 and mapc1.AttributeName='elob_ContractType'

            WHERE (
                pl.elob_markedasFinalON >= '2019-01-01' 
                OR  elob_pl_id in ( '215902',
                                    '267246',
                                    '278100',
                                    '331412',
                                    '361557')
                ) and --PB2021.10.06 odciecie bo zwracał 2mln wierszy
                (mo.CreatedOn>= '"""+self.clear_date+"""' OR  mo.CreatedOn is null) and --data utw MO od 01.01.2019
                pl.elob_PLType=2 and pl.elob_FinalPL=1 --mobile finalny
                and 
                a.elob_sowid not in --bez testowych klientów
                    ('1111111111R002',
                    'R5260016437R3917243',
                    '1111111111R001',
                    '1150006300',
                    '1181344530',
                    '1310185810',
                    '4435271286',
                    '5045876177',
                    '4796154768',
                    '5950002447',
                    '4054054037',
                    '1881881916',
                    '9059058982',
                    '1874726586',
                    '2475019362',
                    '4179755066',
                    '2354243830',
                    'R5452597594R2214940',
                    '3960520680',
                    '2225984122',
                    '6341624259',
                    '6861176577',
                    '4172162185',
                    '7742728255',
                    '9784731062',
                    '4676855126',
                    '4295543163',
                    '4459698814',
                    'K8812336',
                    '7457613270',
                    '1188665941',
                    'K1281365',
                    '1747077061',
                    '2710444807',
                    '9999990001',
                    '4845164070',
                    'K5645273',
                    'K10417615',
                    '3142171411',
                    'K6247219',
                    '1323738903',
                    '3493865659',
                    '4519403893',
                    '8157104056',
                    '5058094689',
                    '3466973534',
                    '2591030458',
                    '1152683560',
                    '9819448230',
                    'K11977016',
                    '6172139589',
                    '2814770928',
                    '9453262082',
                    '4543379141',
                    '1796179235',
                    'K10640910',
                    '1655844127',
                    '2773365029',
                    '1071043224',
                    '3265360032',
                    '2835892163',
                    'K7300890',
                    'K8812345',
                    '9542755828',
                    '1031331454',
                    '1230944254',
                    'K11622354',
                    '6328998356',
                    '9372344309',
                    '2780769638',
                    '7829430851',
                    '7730588587',
                    '1561637818',
                    '1477498797',
                    '7355954375',
                    '5524157670',
                    '5663427009',
                    '2533327785',
                    'K11200615',
                    'K6608149',
                    'S22011687378014',
                    '8163869987',
                    '8247230801',
                    'K4440434',
                    'K5995889',
                    '1751293612',
                    'K7632452',
                    '3451234512',
                    '5123451234') 
                """

                cursor = mssql_connection.cursor() 
                cursor.execute(query)    
                data = cursor.fetchall()
                self.logger.end_import('Dane od 2: '+ self.clear_date);    
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

