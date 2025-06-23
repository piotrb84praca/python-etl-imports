import pandas as pd
from sqlalchemy import text
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyEContracts:
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
                    cursor.execute("BEGIN KLIK.CLEAN_TABLE('IMPORT_EASY_ECONTRACT_STATUS'); END;")   
                
    def importToOracle(self,df): 
               
                 # Prepare insert statement
                insert_stmt = text("""
                    INSERT INTO KLIK.IMPORT_EASY_ECONTRACT_STATUS ( 
                        ORDER_NUMBER,
                        NIP,
                        ID_SOW,
                        CONTRACT_NUMBER,
                        CONTRACT_STATUS,
                        CONTRACT_TYPE,
                        DOC_TYPE,
                        SENT_TO_CUSTOMER_DATE,
                        ECONTRACT_STATUS,
                        CUSTOMER_SIGNED_DATE,       
                        MODIFIED_ON   )
                    VALUES (
                         :ORDER_NUMBER,
                         :NIP,
                         :ID_SOW,
                         :CONTRACT_NUMBER,
                         :CONTRACT_STATUS,
                         :CONTRACT_TYPE,
                         :DOC_TYPE,
                         :SENT_TO_CUSTOMER_DATE,
                         :ECONTRACT_STATUS,
                         :CUSTOMER_SIGNED_DATE,
                         :MODIFIED_ON
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
                query = """ SELECT distinct 
                            so.elob_ordernumber as ORDER_NUMBER
                            ,a.elob_nip as NIP
                            ,a.elob_sowid as ID_SOW
                            ,c.contractnumber as CONTRACT_NUMBER
                            ,mapc3.value as CONTRACT_STATUS
                            ,mapc.value as CONTRACT_TYPE
                            ,mapc2.value as DOC_TYPE
                            
                            ,case when c.elob_Senttocustomeron between '2017-10-29' and '2018-03-25' then format(dateadd(HH,1,c.elob_Senttocustomeron),'yyyy-MM-dd HH:mm')
                            when c.elob_Senttocustomeron between '2018-03-25' and '2018-10-28' then format(dateadd(HH,2,c.elob_Senttocustomeron),'yyyy-MM-dd HH:mm')
                            when c.elob_Senttocustomeron between '2018-10-28' and '2019-03-31' then format(dateadd(HH,1,c.elob_Senttocustomeron),'yyyy-MM-dd HH:mm')
                            when c.elob_Senttocustomeron between '2019-03-31' and '2019-10-27' then format(dateadd(HH,2,c.elob_Senttocustomeron),'yyyy-MM-dd HH:mm')
                            when c.elob_Senttocustomeron between '2019-10-27' and '2020-03-29' then format(dateadd(HH,1,c.elob_Senttocustomeron),'yyyy-MM-dd HH:mm')
                            else isnull(format(dateadd(HH,2,c.elob_Senttocustomeron ),'yyyy-MM-dd HH:mm'),'') end as SENT_TO_CUSTOMER_DATE
                            ,isnull(mapc1.value,'') as ECONTRACT_STATUS
                            ,case when c.elob_SignedbyCustomeron between '2017-10-29' and '2018-03-25' then format(dateadd(HH,1,c.elob_SignedbyCustomeron),'yyyy-MM-dd HH:mm')
                            when c.elob_SignedbyCustomeron between '2018-03-25' and '2018-10-28' then format(dateadd(HH,2,c.elob_SignedbyCustomeron),'yyyy-MM-dd HH:mm')
                            when c.elob_SignedbyCustomeron between '2018-10-28' and '2019-03-31' then format(dateadd(HH,1,c.elob_SignedbyCustomeron),'yyyy-MM-dd HH:mm')
                            when c.elob_SignedbyCustomeron between '2019-03-31' and '2019-10-27' then format(dateadd(HH,2,c.elob_SignedbyCustomeron),'yyyy-MM-dd HH:mm')
                            when c.elob_SignedbyCustomeron between '2019-10-27' and '2020-03-29' then format(dateadd(HH,1,c.elob_SignedbyCustomeron),'yyyy-MM-dd HH:mm')
                            else isnull(format(dateadd(HH,2,c.elob_SignedbyCustomeron ),'yyyy-MM-dd HH:mm'),'') end as CUSTOMER_SIGNED_DATE
                            ,format(c.ModifiedOn,'yyyy-MM-dd') as MODIFIED_ON
                            from
                            elob_sokxorder2Base so   
                            join stringmapbase mapso on so.statuscode=mapso.AttributeValue and mapso.AttributeName='statuscode' and mapso.[langid]=1033 and mapso.ObjectTypeCode=10069
                            join stringmapbase mapso1 on so.elob_operation=mapso1.AttributeValue and mapso1.AttributeName='elob_operation' and mapso1.[langid]=1045 and mapso1.ObjectTypeCode=10069
                            left join stringmapbase mapso2 on so.elob_path=mapso2.AttributeValue and mapso2.AttributeName='elob_path' and mapso2.[langid]=1033 and mapso2.ObjectTypeCode=10069
                            join accountbase a on so.elob_customer=a.accountid
                            join stringmapbase mapa on a.elob_BusinessSegment=mapa.AttributeValue and mapa.ObjectTypeCode=1 and mapa.[langid]=1033 and mapa.AttributeName='elob_BusinessSegment'
                            join elob_soldproductBase sp on so.elob_soldproduct=sp.elob_soldproductId
                            join elob_soldproductBase sp2 on sp.elob_soldproductId=sp2.elob_ParentSoldProduct
                            join contractbase c on so.elob_contract=c.contractid
                            join stringmapbase mapc on c.elob_ContractType=mapc.AttributeValue and mapc.ObjectTypeCode=1010 and mapc.[LangId]=1045 and mapc.AttributeName='elob_ContractType'
                            left join stringmapbase mapc2 on c.elob_transferdocumenttype=mapc2.AttributeValue and mapc2.ObjectTypeCode=1010 and mapc2.[LangId]=1045 and mapc2.AttributeName='elob_transferdocumenttype'
                            left join stringmapbase mapc1 on c.elob_econtractstatus=mapc1.AttributeValue and mapc1.ObjectTypeCode=1010 and mapc1.[LangId]=1045 and mapc1.AttributeName='elob_econtractstatus'
                            join stringmapbase mapc3 on c.statuscode=mapc3.AttributeValue and mapc3.ObjectTypeCode=1010 and mapc3.[LangId]=1045 and mapc3.AttributeName='statuscode'
                            join systemuserbase so_owner on so.ownerid=so_owner.SystemUserId
                            left join systemuserbase su_db on so.elob_DoubleBooking=su_db.SystemUserId
                            where
                            so.createdon>='2019-07-01' and so.statuscode not in ('100000001') 
                            and so.elob_ordernumber is not null
                            and (so_owner.SiteId in ('E56255EC-B3C0-E811-80E5-0050560100C4','3CE67C4F-B4C8-E811-80E0-0050560100C3','C9A81BDF-60AD-E911-80EC-0050560100C4')
                            or su_db.siteid in ('E56255EC-B3C0-E811-80E5-0050560100C4','3CE67C4F-B4C8-E811-80E0-0050560100C3','C9A81BDF-60AD-E911-80EC-0050560100C4'))
                            and c.elob_transferdocumenttype in ('743940002','743940003')
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

