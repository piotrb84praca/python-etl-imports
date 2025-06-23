import pandas as pd
from sqlalchemy import text
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyContracts:
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
                    cursor.execute("BEGIN KLIK.CLEAN_TABLE('IMPORT_EASY_CONTRACT_STATUS'); END;")   
                    #self.oracle_connection.execute(text("DELETE FROM IMPORT_EASY_CONTRACT_STATUS"))
                
    def importToOracle(self,df): 
               
                 # Prepare insert statement
                insert_stmt = text("""
                    INSERT INTO IMPORT_EASY_CONTRACT_STATUS (CONTRACT_NUMBER,
                        CONTRACT_STATUS,
                        CONTRACT_TYPE,
                        ORDER_NUMBER,
                        CONTRACT_ID,
                        VERIFICATION_DATE,
                        ARCH_DATE)
                    VALUES (:CONTRACT_NUMBER, :CONTRACT_STATUS, :CONTRACT_TYPE, :ORDER_NUMBER, :CONTRACT_ID,:VERIFICATION_DATE,:ARCH_DATE)
                """)
    
                # Insert new data into Oracle using executemany for bulk insert
                self.oracle_connection.execute(insert_stmt, df.to_dict(orient='records'))
                # Commit the transaction
                self.oracle_connection.commit()

                # Log the number of rows affected
                #self.logger.add_to_import(f"""Rows imported {self.start_pos} """) 
                
        
    def importAgregateToOracle(self):
                    
                with self.oracle_connection.begin():
                    self.oracle_connection.execute(text("TRUNCATE TABLE import_easy_contract_status_d"))

                insert_stmt = text("""
                   INSERT INTO import_easy_contract_status_d (contract_id, contract_number, verification_date,  contract_status)
            		SELECT DISTINCT contract_id, contract_number, verification_date,  contract_status
            		FROM import_easy_contract_status
                """)
    
                self.oracle_connection.execute(insert_stmt)
                # Commit the transaction
                self.oracle_connection.commit()

                self.logger.add_to_import('import_easy_contract_status_d agregated')  

    def parseData(self, df):

                    #df['CONTRACT_ID'] = str(df['CONTRACT_ID'])  # uuid to string
                    return df
        
    def run(self):
            try:
                
                self.logger.start_import(self.__class__.__name__)
    
                # Connect to MSSQL
                mssql_connection = self.mssql_conn.connect()
                self.deleteFromOracle()
                
                # Extract data from MSSQL Query 1
                query = """
               SELECT DISTINCT
               REPLACE((CONVERT(CHAR(40),c.ContractId )),' ','' ) as CONTRACT_ID,
                c.ContractNumber as CONTRACT_NUMBER
                ,upper(mapc.Value) as CONTRACT_STATUS
                ,upper(cat.elob_name) as CONTRACT_TYPE
                ,case 
                    when c.elob_CategoryId='C285A2B5-3BC1-E411-80C7-00505601285C' and so.elob_ordernumber is not null then so.elob_ordernumber
                    when c.elob_CategoryId='15F8CEC6-3BC1-E411-80C7-00505601285C' and moi.elob_msisdnnumber is not null then moi.elob_msisdnnumber
                    else '' 
                end as ORDER_NUMBER,
                isnull(format(c.elob_revisiondate,'yyyy-MM-dd'),'') as VERIFICATION_DATE,
                isnull(format(ca.elob_ArchivingDate+1, 'yyyy-MM-dd'), '') as ARCH_DATE
            FROM
                contractbase c 
                join elob_categoryBase cat on c.elob_CategoryId=cat.elob_categoryId
                join accountbase a on c.customerid=a.accountid
                left join elob_sokxorder2Base so on c.contractid=so.elob_contract
                left join elob_mobileorderBase mo on c.contractid=mo.elob_contract
                left join elob_mobileorderitemBase moi on mo.elob_mobileorderid=moi.elob_mobileorder
                join stringmapbase mapc on c.statuscode=mapc.AttributeValue and mapc.ObjectTypeCode=1010 and mapc.[LangId]=1045 and mapc.AttributeName='statuscode'
                left join elob_contractarchive ca on ca.elob_Contract=c.ContractId and ca.elob_comments like 'ZARCHIWIZOWANO%'
            WHERE
                c.createdon >='2023-01-01' 
                AND a.elob_sowid not in 
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
        
                self.importAgregateToOracle()    
                self.logger.end_import(len(data))
                
                
            except Exception as e:
                self.logger.log_error(str(e))
                if self.mssql_conn.connection:
                    self.mssql_conn.close()
                if self.oracle_engine.connection:
                    self.oracle_engine.close()

