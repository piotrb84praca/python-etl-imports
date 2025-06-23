import pandas as pd
from sqlalchemy import text
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyEOrder:
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
                    cursor.execute("BEGIN KLIK.CLEAN_TABLE('IMPORT_EASY_EORDER_STATUS'); END;")   
                
    def importToOracle(self,df): 
               
                 # Prepare insert statement
                insert_stmt = text("""
                    INSERT INTO KLIK.IMPORT_EASY_EORDER_STATUS ( 
                        ID_SOW,
                        ORDER_NUMBER,
                        CREATED_ON,
                        MODIFIED_ON,
                        ORDER_STATUS,
                        SERVICE,
                        SERVICE_TYPE,
                        REALIZATION_TYPE,
                        CONTRACT_SIGNED_BY,
                        CONTRACT_IS_ECONTRACT,
                        CONTRACT_IS_ECONTRACT_AUTO,
                        CONTRACT_NUMBER,
                        CONTRACT_THREE_SIDED,
                        CONTRACT_TYPE,
                        TRANSFER_DOC_TYPE,
                        LAST_WORK_ORDER_NUMBER,
                        IS_REALIZED_BY_ECONTRACT,
                        BARRIERS)
                    VALUES (
                        :ID_SOW,
                        :ORDER_NUMBER,
                        :CREATED_ON,
                        :MODIFIED_ON,
                        :ORDER_STATUS,
                        :SERVICE,
                        :SERVICE_TYPE,
                        :REALIZATION_TYPE,
                        :CONTRACT_SIGNED_BY,
                        :CONTRACT_IS_ECONTRACT,
                        :CONTRACT_IS_ECONTRACT_AUTO,
                        :CONTRACT_NUMBER,
                        :CONTRACT_THREE_SIDED,
                        :CONTRACT_TYPE,
                        :TRANSFER_DOC_TYPE,
                        :LAST_WORK_ORDER_NUMBER,
                        :IS_REALIZED_BY_ECONTRACT,
                        :BARRIERS
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
                query = """  SELECT distinct 
                a.elob_sowid as ID_SOW
                ,so.elob_ordernumber as ORDER_NUMBER
                ,format(so.createdon,'yyyy-MM-dd') as CREATED_ON
                ,format(so.ModifiedOn,'yyyy-MM-dd') as MODIFIED_ON
                ,mapso.value as ORDER_STATUS
                ,sp2.elob_name as SERVICE
                ,case when konty.rodz is null then ' ' else konty.rodz end as SERVICE_TYPE
                --,konty.elob_Index
                ,isnull(mapso3.Value,'') as REALIZATION_TYPE
                ,isnull(mapso2.Value,'') as CONTRACT_SIGNED_BY
                ,isnull(m01.value,'') as CONTRACT_IS_ECONTRACT
                ,isnull(m02.value,'') as CONTRACT_IS_ECONTRACT_AUTO
                ,isnull(c.ContractNumber,'') as CONTRACT_NUMBER
                ,isnull(spr_3.[3-strony],'') as CONTRACT_THREE_SIDED
                ,isnull(mapc.value,'') as  CONTRACT_TYPE
                ,isnull(mapc2.value,'') as TRANSFER_DOC_TYPE
                ,isnull(wo.elob_sokxnumber,'') as LAST_WORK_ORDER_NUMBER
                ,case 
                when p.ProductNumber='is.a.148' and so.statuscode not in ('743940001') then 'W trakcie - Utrzymanie'
                when p.ProductNumber='is.a.148' and so.statuscode in ('743940001') then 'Zrealizowano - Utrzymanie'
                when c.elob_transferdocumenttype=743940002 and so.statuscode not in ('743940001') then N'Realizacja e-umową'
                when c.elob_transferdocumenttype=743940002 and so.statuscode  in ('743940001') then N'Zrealizowano e-umową' --//opis z umowy, że realizacja e-umową
                when c.elob_transferdocumenttype=743940003 and so.statuscode not in ('743940001') then N'Realizacja e-umową automat'
                when c.elob_transferdocumenttype=743940003 and so.statuscode  in ('743940001') then N'Zrealizowano e-umową automat' --//opis z umowy, że realizacja e-umową automat
                when so.statuscode in ('743940001') and so.elob_contractsignedby is not null then N'Zrealizowano - Maximus' --//jest PT więc nie ma możliwości
                when so.statuscode not in ('743940001') and so.elob_contractsignedby is not null then N'W trakcie - Maximus' --//jest PT więc nie ma możliwości
                when so.statuscode  in ('743940001') and
                (par_1.elob_ParameterValue is not null 
                or par_2.elob_ParameterValue is not null 
                or c.elob_Acceptancefornoncontractmode in ('1','0') 
                or c.elob_ContractType not in ('1','3')
                or spr_3.[3-strony]='Tak')
                then N'Zrealizowano - Brak warunków' 
                when so.statuscode not in ('743940001') and
                (par_1.elob_ParameterValue is not null 
                or par_2.elob_ParameterValue is not null 
                or c.elob_Acceptancefornoncontractmode in ('1','0') --tryb bezumowny
                or c.elob_ContractType not in ('1','3')
                or spr_3.[3-strony]='Tak')
                then N'W trakcie - Brak warunków' 
                when so.statuscode not in ('743940001') and par_1.elob_ParameterValue is null and  par_2.elob_ParameterValue is null --//brak uzupełnionych parametrów usługi (1 check czy spełnia warunki)
                and c.elob_ContractType in ('1','3')  --//umowa standardowa lub aneks standardowy (2 check czy spełnia warunki)
                and (spr_3.[3-strony]='Nie' or spr_3.[3-strony] is null) --//Umowa nie może być 3-stronna, pole ma być na Nie lub puste (3 check)
                and (c.elob_acceptancefornoncontractmode is null or c.elob_acceptancefornoncontractmode=2) --//brak zgody na tryb bezumowny  (4 check)
                and (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null)  --//nie poszło e-umową ale moze pójść (5 check)
                and (wo.new_SOKXOrderType not in ('2') or  wo.new_SOKXOrderType is null) --//Nie wygenerowano AU do zamówienia (6 check)
                then N'W trakcie - Warunki na e-umowę są'
                when so.statuscode in ('743940001') and par_1.elob_ParameterValue is null and  par_2.elob_ParameterValue is null --//brak uzupełnionych parametrów usługi (1 check czy spełnia warunki)
                and c.elob_ContractType in ('1','3')  --//umowa standardowa lub aneks standardowy (2 check czy spełnia warunki)
                and (spr_3.[3-strony]='Nie' or spr_3.[3-strony] is null) --//Umowa nie może być 3-stronna, pole ma być na Nie lub puste (3 check)
                and (c.elob_acceptancefornoncontractmode is null or c.elob_acceptancefornoncontractmode=2) --//brak zgody na tryb bezumowny  (4 check)
                and (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null)  --//nie poszło e-umową ale moze pójść (5 check)
                and (wo.new_SOKXOrderType not in ('2') or  wo.new_SOKXOrderType is null) --//Nie wygenerowano AU do zamówienia (6 check)
                --then N'Zrealizowano - Warunki na e-umowę są'
                then N'Zrealizowano - Była możliwość realizacji e-umową'
                when so.statuscode in ('743940001') and c.contractid is null --//brak umowy (chack 1)
                and (wo.new_SOKXOrderType not in ('2') or  wo.new_SOKXOrderType is null)  --//brak zlecenia au (check 2)
                and par_1.elob_ParameterValue is null and  par_2.elob_ParameterValue is null  --//brak uzupełnionych parametrów (i ok!)- check3
                then N'Zrealizowano - Była możliwość realizacji e-umową'
                when so.statuscode not in ('743940001') and c.contractid is null --//brak umowy (chack 1)
                and (wo.new_SOKXOrderType not in ('2') or  wo.new_SOKXOrderType is null)  --//brak zlecenia au (check 2)
                and par_1.elob_ParameterValue is null and  par_2.elob_ParameterValue is null  --//brak uzupełnionych parametrów (i ok!)- check3
                then N'W trakcie - Warunki na e-umowę są' --nie ma umowy
                when so.statuscode in ('743940001') and wo.new_SOKXOrderType  in ('2') --//wygenerowane AU
                and par_1.elob_ParameterValue is null and  par_2.elob_ParameterValue is null--// nieuzupełnione parametry
                and ((c.elob_ContractType in ('1','3') --//umowa standardowa i aneks standardowy
                and (spr_3.[3-strony]='Nie' or spr_3.[3-strony] is null)) or c.ContractId is null) --//umowa nie jest trójstronna lub pusto w polu + lub jeśli brak umowy
                then N'Zrealizowano - Była możliwość realizacji e-umową'
                when so.statuscode not in ('743940001') and wo.new_SOKXOrderType  in ('2') --//wygenerowane AU
                and par_1.elob_ParameterValue is null and  par_2.elob_ParameterValue is null--// nieuzupełnione parametry
                and ((c.elob_ContractType in ('1','3') --//umowa standardowa i aneks standardowy
                and (spr_3.[3-strony]='Nie' or spr_3.[3-strony] is null)) or c.ContractId is null) --//umowa nie jest trójstronna lub pusto w polu + lub jeśli brak umowy
                then N'W trakcie - Była możliwość realizacji e-umową'
                when so.statuscode in ('743940001') and so.elob_econtract=0 and elob_availableforecontractautomatic=0 and so.elob_contractsignedby is null then N'Zrealizowano - Brak warunków' --//e-umowa i e umow automat na nie, bez PT
                when so.statuscode not in ('743940001') and so.elob_econtract=0 and elob_availableforecontractautomatic=0 and so.elob_contractsignedby is null then N'W trakcie - Brak warunków' --//e-umowa i e umow automat na nie, bez PT
                else '' end as IS_REALIZED_BY_ECONTRACT
                ,case when c.elob_ContractType not in ('1','3') and (spr_3.[3-strony]='Nie' or spr_3.[3-strony] is null) and par_1.elob_ParameterValue is null and par_2.elob_ParameterValue is null  
                and so.elob_contractsignedby is null and (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null)
                and (c.elob_Acceptancefornoncontractmode not in ('1','0') or c.elob_Acceptancefornoncontractmode  is null) then N'Bariera - Umowa niestandardowa'
                when spr_3.[3-strony]='Tak' and c.elob_ContractType  in ('1','3')  and par_1.elob_ParameterValue is null and par_2.elob_ParameterValue is null and so.elob_contractsignedby is null and (c.elob_Acceptancefornoncontractmode not in ('1','0') and (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null)
                or c.elob_Acceptancefornoncontractmode  is null)then N'Bariera - umowa trójstronna'
                when par_1.elob_ParameterValue is not null and (c.elob_ContractType in ('1','3') or c.contractid is null) and (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null)
                and (spr_3.[3-strony]='Nie' or spr_3.[3-strony] is null)  and par_2.elob_ParameterValue is null and so.elob_contractsignedby is null and (c.elob_Acceptancefornoncontractmode not in ('1','0') or c.elob_Acceptancefornoncontractmode  is null)  then N'Bariera - Migrowane numery'
                when par_2.elob_ParameterValue is not null and (c.elob_ContractType in ('1','3') or c.contractid is null) and (spr_3.[3-strony]='Nie' or spr_3.[3-strony] is null) 
                and par_1.elob_ParameterValue is null and so.elob_contractsignedby is null and (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null)
                and (c.elob_Acceptancefornoncontractmode not in ('1','0') or c.elob_Acceptancefornoncontractmode  is null) then N'Bariera - Numer zlecenia powiązanego'
                when c.elob_Acceptancefornoncontractmode in ('1','0') and (spr_3.[3-strony]='Nie' or spr_3.[3-strony] is null) and par_1.elob_ParameterValue is null and par_2.elob_ParameterValue is null 
                and (c.elob_ContractType in ('1','3') or c.contractid is null) and  (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null)
                then N'Bariera - Tryb bezumowny'
                when c.elob_ContractType not in ('1','3') and spr_3.[3-strony]='Tak' and (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null) then N'Bariera - Inne'
                when c.elob_ContractType not in ('1','3') and par_2.elob_ParameterValue is not null and (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null) then N'Bariera - Inne'
                when c.elob_ContractType not in ('1','3') and par_1.elob_ParameterValue is not null and (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null) then N'Bariera - Inne'
                when c.elob_ContractType not in ('1','3') and c.elob_Acceptancefornoncontractmode in ('1','0') and (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null) then N'Bariera - Inne'
                when par_1.elob_ParameterValue is not null and par_2.elob_ParameterValue is not null then N'Bariera -Inne'
                when par_1.elob_ParameterValue is not null and  c.elob_Acceptancefornoncontractmode in ('1','0') and (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null) then N'Bariera -Inne'
                when par_2.elob_ParameterValue is not null and  c.elob_Acceptancefornoncontractmode in ('1','0') and (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null) then N'Bariera -Inne'
                when spr_3.[3-strony]='Tak' and par_2.elob_ParameterValue is not null and (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null) then N'Bariera - Inne'
                when spr_3.[3-strony]='Tak' and par_1.elob_ParameterValue is not null and (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null) then N'Bariera - Inne'
                when spr_3.[3-strony]='Tak' and  c.elob_Acceptancefornoncontractmode in ('1','0') and (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null) then N'Bariera - Inne'
                else ''
                end as BARRIERS

                from
                elob_sokxorder2Base so
                join stringmapbase mapso on so.statuscode=mapso.AttributeValue and mapso.AttributeName='statuscode' and mapso.[langid]=1033 and mapso.ObjectTypeCode=10069
                join stringmapbase mapso1 on so.elob_operation=mapso1.AttributeValue and mapso1.AttributeName='elob_operation' and mapso1.[langid]=1045 and mapso1.ObjectTypeCode=10069
                left join stringmapbase mapso2 on so.elob_contractsignedby=mapso2.AttributeValue and mapso2.AttributeName='elob_contractsignedby' and mapso2.[langid]=1033
                join stringmapbase mapso3 on so.elob_Realizationmode=mapso3.AttributeValue and mapso3.AttributeName='elob_Realizationmode' and mapso3.[langid]=1045 and mapso3.ObjectTypeCode=10069
                join stringmapbase mapso4 on so.elob_path=mapso4.AttributeValue and mapso4.AttributeName='elob_path' and mapso4.[langid]=1033 and mapso4.ObjectTypeCode=10069
                left join stringmapbase m01 on so.elob_econtract=m01.AttributeValue and m01.ObjectTypeCode=10069 and m01.[LangId]=1045 and m01.AttributeName='elob_econtract'
                left join stringmapbase m02 on so.elob_availableforecontractautomatic=m02.AttributeValue and m02.ObjectTypeCode=10069 and m02.[LangId]=1045 and m02.AttributeName='elob_availableforecontractautomatic'
                join accountbase a on so.elob_customer=a.accountid
                join stringmapbase mapa on a.elob_BusinessSegment=mapa.AttributeValue and mapa.ObjectTypeCode=1 and mapa.[LangId]=1033 and mapa.AttributeName='elob_BusinessSegment'
                join elob_soldproductbase sp on so.elob_soldproduct=sp.elob_soldproductId
                join productbase p on sp.elob_Product=p.ProductId
                left join elob_productprocessbase pp on so.elob_process=pp.elob_productprocessId
                left join contractbase c on so.elob_contract=c.contractid

                left join stringmapbase mapc on c.elob_ContractType=mapc.AttributeValue and mapc.ObjectTypeCode=1010 and mapc.[LangId]=1045 and mapc.AttributeName='elob_ContractType'
                left join stringmapbase mapc1 on c.elob_acceptancefornoncontractmode=mapc1.AttributeValue and mapc1.ObjectTypeCode=1010 and mapc1.[LangId]=1045 and mapc1.AttributeName='elob_acceptancefornoncontractmode'
                left join stringmapbase mapc2 on c.elob_transferdocumenttype=mapc2.AttributeValue and mapc2.ObjectTypeCode=1010 and mapc2.[LangId]=1045 and mapc2.AttributeName='elob_transferdocumenttype'
                left join elob_workorder2Base wo  on so.elob_LastWorkWorder=wo.elob_workorder2Id--ostatne zlecenie au jak ma nr nie pokazywac ( w teorii)
                left join stringmapbase mapwo on wo.new_SOKXOrderType=mapwo.AttributeValue and mapwo.ObjectTypeCode=10080 and mapwo.[LangId]=1045 and mapwo.AttributeName='new_SOKXOrderType'
                left join stringmapbase mapwo1 on wo.elob_sokxwodecision=mapwo1.AttributeValue and mapwo1.ObjectTypeCode=10080 and mapwo1.[LangId]=1045 and mapwo1.AttributeName='elob_sokxwodecision'
                join elob_soldproductBase sp2 on sp.elob_soldproductId=sp2.elob_ParentSoldProduct

                left join
                (
                select spp.elob_SoldProduct,spp.elob_ParameterValue

                from
                elob_soldproductparameterBase spp
                join elob_productparametersBase pp on spp.elob_Parameter=pp.elob_productparametersId
                where
                pp.elob_name like 'Migrowane numery od OA (wykaz)%'
                ) par_1 on sp2.elob_soldproductId=par_1.elob_SoldProduct


                left join (
                select spp.elob_SoldProduct,spp.elob_ParameterValue

                from
                elob_soldproductparameterBase spp
                join elob_productparametersBase pp on spp.elob_Parameter=pp.elob_productparametersId
                where
                pp.elob_name like 'Numer zlecenia powiązanego'

                ) par_2 on sp2.elob_soldproductId=par_2.elob_SoldProduct

                ---sprawdzanie trójstronnosci
                left join

                (select distinct  c.contractid, 
                case when c.CustomerId=a.AccountId then 'Nie' else 'Tak' end as [3-strony]

                from
                contractbase c 
                join elob_billingaccountBase ba on c.elob_billingaccount=ba.elob_billingaccountId
                join accountbase a on ba.elob_Account=a.AccountId
                ) spr_3 on c.ContractId=spr_3.ContractId
                ---uzytkownicy tworca -nie zapinamy sie
                --jak BO koszalin wlascicielem, to mogą to być TAMowe zamówienia, nie wiem jak dla GO Fiber
                ---KONTYNUACJA WZNOWIENIE UMOWY
                left join
                (
                select sp2.elob_soldproductId,ppv.elob_value as rodz
                ,ppv.elob_Index
                from
                elob_soldproductBase sp2 --on sp.elob_soldproductId=sp2.elob_ParentSoldProduct
                join elob_soldproductparameterBase spp on sp2.elob_soldproductId=spp.elob_SoldProduct 
                and spp.elob_Parameter='7EF087F7-BD6B-E511-80CE-00505601285C'
                join elob_productparametersBase pp on spp.elob_Parameter=pp.elob_productparametersId
                join elob_productparametervalueBase ppv on pp.elob_productparametersId=ppv.elob_Parameter and spp.elob_ParameterValue=ppv.elob_Index --and ppv.elob_Index=pp.elob_variant
                where
                spp.elob_ParameterValue in ('19','4','11')
                )konty on sp2.elob_soldproductId=konty.elob_soldproductId
                ------//UZYTKOWNICY
                --//wł zam sokx.
                join systemuserbase so_owner on so.ownerid=so_owner.SystemUserId
                left join sitebase so_site on so_owner.siteid=so_site.SiteId
                join BusinessUnitBase so_o_bu on so_owner.BusinessUnitId=so_o_bu.BusinessUnitId
                left join systemuserbase so_o_man on so_owner.ParentSystemUserId=so_o_man.SystemUserId
                --//
                --//db
                left join systemuserbase su_db on so.elob_DoubleBooking=su_db.SystemUserId
                left join sitebase su_db_s on su_db.siteid=su_db_s.siteid
                left join businessunitbase su_db_bu on su_db.BusinessUnitId=su_db_bu.BusinessUnitId
                left join systemuserbase su_db_man on su_db.ParentSystemUserId=su_db_man.SystemUserId


                where 
                --warunek na nieanulowane i --niezakończone
                so.elob_ordernumber  is not null and so.statuscode not in ('100000001')--,'743940001') and 
                and so.elob_operation not in ('4') and 
                p.productnumber in --('is.a.148')
                ('ID.A.36','ID.A.438','ID.A.439','ID.A.440','ID.A.441','ID.A.479','SD.A.53','SD.A.68','SD.A.78','SD.A.79','SD.A.7','SD.A.80','SD.A.81','SD.A.86','SD.A.87','SD.A.88')

                and so.createdon>='2019-07-01'
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
                and (so_owner.SiteId in ('E56255EC-B3C0-E811-80E5-0050560100C4','3CE67C4F-B4C8-E811-80E0-0050560100C3','C9A81BDF-60AD-E911-80EC-0050560100C4')
                or su_db.siteid in ('E56255EC-B3C0-E811-80E5-0050560100C4','3CE67C4F-B4C8-E811-80E0-0050560100C3','C9A81BDF-60AD-E911-80EC-0050560100C4'))

                UNION
                select distinct --top 1000
                a.elob_sowid as [ID SOW klienta]

                ,so.elob_ordernumber as [Numer zam. SOKX]
                ,format(so.createdon,'yyyy-MM-dd') as [Data utworzenia zam. SOKX]

                ,format(so.ModifiedOn,'yyyy-MM-dd') as [Data ost. modyfikacji zam. sokx]
                ,mapso.value as [Status zamówienia SOKX]
                ,sp2.elob_name as [Usługa]

                ,case when konty.rodz is null then ' ' else konty.rodz end as [Rodzaj usługi (dot. kontynuacja/wznowienie umowy)]

                ,isnull(mapso3.Value,'') as [Tryb realizacji] --nie może być tryb bezumowny!  brać z umowy

                ,isnull(mapso2.Value,'') as [Umowa podpisana przez]---podpisane przez pt
                ,isnull(m01.value,'') as [Dostępny tryb e-umowa]--\\elob_econtract
                ,isnull(m02.value,'') as [Dostępny tryb e-umowa automat]--\\elob_availableforecontractautomatic

                ,isnull(c.ContractNumber,'') as [Numer umowy]
                ,isnull(spr_3.[3-strony],'') as [Umowa trójstronna]
                ,isnull(mapc.value,'') as [Typ umowy]
                ,isnull(mapc2.value,'') as [Transfer dokument type]
                ,isnull(wo.elob_sokxnumber,'') as [Numer ostatniego zlecenia]
                ,case 
                when p.ProductNumber='is.a.148' and so.statuscode not in ('743940001') then 'W trakcie - Utrzymanie'
                when p.ProductNumber='is.a.148' and so.statuscode in ('743940001') then 'Zrealizowano - Utrzymanie'
                when c.elob_transferdocumenttype=743940002 and so.statuscode not in ('743940001') then N'Realizacja e-umową'
                when c.elob_transferdocumenttype=743940002 and so.statuscode  in ('743940001') then N'Zrealizowano e-umową' --//opis z umowy, że realizacja e-umową
                when c.elob_transferdocumenttype=743940003 and so.statuscode not in ('743940001') then N'Realizacja e-umową automat'
                when c.elob_transferdocumenttype=743940003 and so.statuscode  in ('743940001') then N'Zrealizowano e-umową automat' --//opis z umowy, że realizacja e-umową automat

                when so.statuscode  in ('743940001') and
                (par_1.elob_ParameterValue is not null 
                or par_2.elob_ParameterValue is not null 
                or c.elob_Acceptancefornoncontractmode in ('1','0') 
                or c.elob_ContractType not in ('1','3')
                or spr_3.[3-strony]='Tak')
                then N'Zrealizowano - Brak warunków' 
                ----
                when so.statuscode not in ('743940001') and
                (par_1.elob_ParameterValue is not null 
                or par_2.elob_ParameterValue is not null 
                or c.elob_Acceptancefornoncontractmode in ('1','0') 
                or c.elob_ContractType not in ('1','3')
                or spr_3.[3-strony]='Tak')
                then N'W trakcie - Brak warunków' 
                ----
                when so.statuscode not in ('743940001') and par_1.elob_ParameterValue is null and  par_2.elob_ParameterValue is null --//brak uzupełnionych parametrów usługi (1 check czy spełnia warunki)
                and c.elob_ContractType in ('1','3')  --//umowa standardowa lub aneks standardowy (2 check czy spełnia warunki)
                and (spr_3.[3-strony]='Nie' or spr_3.[3-strony] is null) --//Umowa nie może być 3-stronna, pole ma być na Nie lub puste (3 check)
                and (c.elob_acceptancefornoncontractmode is null or c.elob_acceptancefornoncontractmode=2) --//brak zgody na tryb bezumowny  (4 check)
                and (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null)  --//nie poszło e-umową ale moze pójść (5 check)
                and (wo.new_SOKXOrderType not in ('2') or  wo.new_SOKXOrderType is null) --//Nie wygenerowano AU do zamówienia (6 check)
                then N'W trakcie - Warunki na e-umowę są'

                when so.statuscode in ('743940001') and par_1.elob_ParameterValue is null and  par_2.elob_ParameterValue is null --//brak uzupełnionych parametrów usługi (1 check czy spełnia warunki)
                and c.elob_ContractType in ('1','3')  --//umowa standardowa lub aneks standardowy (2 check czy spełnia warunki)
                and (spr_3.[3-strony]='Nie' or spr_3.[3-strony] is null) --//Umowa nie może być 3-stronna, pole ma być na Nie lub puste (3 check)
                and (c.elob_acceptancefornoncontractmode is null or c.elob_acceptancefornoncontractmode=2) --//brak zgody na tryb bezumowny  (4 check)
                and (c.elob_transferdocumenttype not in ('743940002','743940003') or c.elob_transferdocumenttype is null)  --//nie poszło e-umową ale moze pójść (5 check)
                and (wo.new_SOKXOrderType not in ('2') or  wo.new_SOKXOrderType is null) --//Nie wygenerowano AU do zamówienia (6 check)
                --then N'Zrealizowano - Warunki na e-umowę są'
                then N'Zrealizowano - Była możliwość realizacji e-umową'
                --when spr_3.[3-strony]='Tak' then N'Brak warunków' --//umowa trójstronna na tak =brak warunków
                ---------------
                when so.statuscode in ('743940001') and c.contractid is null --//brak umowy (chack 1)
                and (wo.new_SOKXOrderType not in ('2') or  wo.new_SOKXOrderType is null)  --//brak zlecenia au (check 2)
                and par_1.elob_ParameterValue is null and  par_2.elob_ParameterValue is null  --//brak uzupełnionych parametrów (i ok!)- check3
                --then N'Zrealizowano - Warunki na e-umowę są' --nie ma umowy
                then N'Zrealizowano - Była możliwość realizacji e-umową'

                when so.statuscode not in ('743940001') and c.contractid is null --//brak umowy (chack 1)
                and (wo.new_SOKXOrderType not in ('2') or  wo.new_SOKXOrderType is null)  --//brak zlecenia au (check 2)
                and par_1.elob_ParameterValue is null and  par_2.elob_ParameterValue is null  --//brak uzupełnionych parametrów (i ok!)- check3
                then N'W trakcie - Warunki na e-umowę są' --nie ma umowy


                -------------
                when so.statuscode in ('743940001') and wo.new_SOKXOrderType  in ('2') --//wygenerowane AU
                and par_1.elob_ParameterValue is null and  par_2.elob_ParameterValue is null--// nieuzupełnione parametry
                and ((c.elob_ContractType in ('1','3') --//umowa standardowa i aneks standardowy
                and (spr_3.[3-strony]='Nie' or spr_3.[3-strony] is null)) or c.ContractId is null) --//umowa nie jest trójstronna lub pusto w polu + lub jeśli brak umowy
                then N'Zrealizowano - Była możliwość realizacji e-umową'

                when so.statuscode not in ('743940001') and wo.new_SOKXOrderType  in ('2') --//wygenerowane AU
                and par_1.elob_ParameterValue is null and  par_2.elob_ParameterValue is null--// nieuzupełnione parametry
                and ((c.elob_ContractType in ('1','3') --//umowa standardowa i aneks standardowy
                and (spr_3.[3-strony]='Nie' or spr_3.[3-strony] is null)) or c.ContractId is null) --//umowa nie jest trójstronna lub pusto w polu + lub jeśli brak umowy
                then N'W trakcie - Była możliwość realizacji e-umową'
                -----
                when so.statuscode in ('743940001') and so.elob_econtract=0 and elob_availableforecontractautomatic=0 and so.elob_contractsignedby is null then N'Zrealizowano - Brak warunków' --//e-umowa i e umow automat na nie, bez PT
                when so.statuscode not in ('743940001') and so.elob_econtract=0 and elob_availableforecontractautomatic=0 and so.elob_contractsignedby is null then N'W trakcie - Brak warunków' --//e-umowa i e umow automat na nie, bez PT
                when so.statuscode in ('743940001') and so.elob_econtract=0 and elob_availableforecontractautomatic=0  and so.elob_contractsignedby is not null then N'Zrealizowano - Maximus' --//jest PT więc nie ma możliwości
                when so.statuscode not in ('743940001') and so.elob_econtract=0 and elob_availableforecontractautomatic=0  and so.elob_contractsignedby is not null then N'W trakcie - Maximus' --//jest PT więc nie ma możliwości


                else '' end as [Czy realizacja e-umową]
                ,N'Bariera - Kontynuacja/wznowienie umowy' as [Bariery]

                from
                elob_sokxorder2Base so
                ---mapsy do sokx
                join stringmapbase mapso on so.statuscode=mapso.AttributeValue and mapso.AttributeName='statuscode' and mapso.[langid]=1033 and mapso.ObjectTypeCode=10069
                join stringmapbase mapso1 on so.elob_operation=mapso1.AttributeValue and mapso1.AttributeName='elob_operation' and mapso1.[langid]=1045 and mapso1.ObjectTypeCode=10069
                left join stringmapbase mapso2 on so.elob_contractsignedby=mapso2.AttributeValue and mapso2.AttributeName='elob_contractsignedby' and mapso2.[langid]=1033
                join stringmapbase mapso3 on so.elob_Realizationmode=mapso3.AttributeValue and mapso3.AttributeName='elob_Realizationmode' and mapso3.[langid]=1045 and mapso3.ObjectTypeCode=10069
                join stringmapbase mapso4 on so.elob_path=mapso4.AttributeValue and mapso4.AttributeName='elob_path' and mapso4.[langid]=1033 and mapso4.ObjectTypeCode=10069
                left join stringmapbase m01 on so.elob_econtract=m01.AttributeValue and m01.ObjectTypeCode=10069 and m01.[LangId]=1045 and m01.AttributeName='elob_econtract'
                left join stringmapbase m02 on so.elob_availableforecontractautomatic=m02.AttributeValue and m02.ObjectTypeCode=10069 and m02.[LangId]=1045 and m02.AttributeName='elob_availableforecontractautomatic'


                ---
                join accountbase a on so.elob_customer=a.accountid
                join stringmapbase mapa on a.elob_BusinessSegment=mapa.AttributeValue and mapa.ObjectTypeCode=1 and mapa.[LangId]=1033 and mapa.AttributeName='elob_BusinessSegment'
                join elob_soldproductbase sp on so.elob_soldproduct=sp.elob_soldproductId
                join productbase p on sp.elob_Product=p.ProductId
                left join elob_productprocessbase pp on so.elob_process=pp.elob_productprocessId
                left join contractbase c on so.elob_contract=c.contractid

                ---mapsy do umowy
                left join stringmapbase mapc on c.elob_ContractType=mapc.AttributeValue and mapc.ObjectTypeCode=1010 and mapc.[LangId]=1045 and mapc.AttributeName='elob_ContractType'
                left join stringmapbase mapc1 on c.elob_acceptancefornoncontractmode=mapc1.AttributeValue and mapc1.ObjectTypeCode=1010 and mapc1.[LangId]=1045 and mapc1.AttributeName='elob_acceptancefornoncontractmode'
                left join stringmapbase mapc2 on c.elob_transferdocumenttype=mapc2.AttributeValue and mapc2.ObjectTypeCode=1010 and mapc2.[LangId]=1045 and mapc2.AttributeName='elob_transferdocumenttype'
                ---ostatnie zlecenie
                left join elob_workorder2Base wo  on so.elob_LastWorkWorder=wo.elob_workorder2Id--ostatne zlecenie au jak ma nr nie pokazywac ( w teorii)
                left join stringmapbase mapwo on wo.new_SOKXOrderType=mapwo.AttributeValue and mapwo.ObjectTypeCode=10080 and mapwo.[LangId]=1045 and mapwo.AttributeName='new_SOKXOrderType'
                left join stringmapbase mapwo1 on wo.elob_sokxwodecision=mapwo1.AttributeValue and mapwo1.ObjectTypeCode=10080 and mapwo1.[LangId]=1045 and mapwo1.AttributeName='elob_sokxwodecision'

                ----parametr do usługi
                join elob_soldproductBase sp2 on sp.elob_soldproductId=sp2.elob_ParentSoldProduct

                left join
                (
                select spp.elob_SoldProduct,spp.elob_ParameterValue

                from
                elob_soldproductparameterBase spp
                join elob_productparametersBase pp on spp.elob_Parameter=pp.elob_productparametersId
                where
                pp.elob_name like 'Migrowane numery od OA (wykaz)%'
                ) par_1 on sp2.elob_soldproductId=par_1.elob_SoldProduct


                left join (
                select spp.elob_SoldProduct,spp.elob_ParameterValue

                from
                elob_soldproductparameterBase spp
                join elob_productparametersBase pp on spp.elob_Parameter=pp.elob_productparametersId
                where
                pp.elob_name like 'Numer zlecenia powiązanego'

                ) par_2 on sp2.elob_soldproductId=par_2.elob_SoldProduct

                ---sprawdzanie trójstronnosci
                left join

                (select distinct  c.contractid, 
                case when c.CustomerId=a.AccountId then 'Nie' else 'Tak' end as [3-strony]

                from
                contractbase c 
                join elob_billingaccountBase ba on c.elob_billingaccount=ba.elob_billingaccountId
                join accountbase a on ba.elob_Account=a.AccountId
                ) spr_3 on c.ContractId=spr_3.ContractId
                ---uzytkownicy tworca -nie zapinamy sie
                --jak BO koszalin wlascicielem, to mogą to być TAMowe zamówienia, nie wiem jak dla GO Fiber
                ---KONTYNUACJA WZNOWIENIE UMOWY
                join
                (
                select sp2.elob_soldproductId,ppv.elob_value as rodz
                ,ppv.elob_Index
                from
                elob_soldproductBase sp2 --on sp.elob_soldproductId=sp2.elob_ParentSoldProduct
                join elob_soldproductparameterBase spp on sp2.elob_soldproductId=spp.elob_SoldProduct 
                and spp.elob_Parameter='7EF087F7-BD6B-E511-80CE-00505601285C'
                join elob_productparametersBase pp on spp.elob_Parameter=pp.elob_productparametersId
                join elob_productparametervalueBase ppv on pp.elob_productparametersId=ppv.elob_Parameter and spp.elob_ParameterValue=ppv.elob_Index --and ppv.elob_Index=pp.elob_variant
                where
                spp.elob_ParameterValue in ('19','4','11')
                )konty on sp2.elob_soldproductId=konty.elob_soldproductId
                ------//UZYTKOWNICY
                --//wł zam sokx.
                join systemuserbase so_owner on so.ownerid=so_owner.SystemUserId
                left join sitebase so_site on so_owner.siteid=so_site.SiteId
                join BusinessUnitBase so_o_bu on so_owner.BusinessUnitId=so_o_bu.BusinessUnitId
                left join systemuserbase so_o_man on so_owner.ParentSystemUserId=so_o_man.SystemUserId
                --//
                --//db
                left join systemuserbase su_db on so.elob_DoubleBooking=su_db.SystemUserId
                left join sitebase su_db_s on su_db.siteid=su_db_s.siteid
                left join businessunitbase su_db_bu on su_db.BusinessUnitId=su_db_bu.BusinessUnitId
                left join systemuserbase su_db_man on su_db.ParentSystemUserId=su_db_man.SystemUserId


                where 
                --warunek na nieanulowane i --niezakończone
                so.elob_ordernumber  is not null and so.statuscode not in ('100000001')--,'743940001') and 
                and so.elob_operation not in ('4') and 
                p.productnumber in ('is.a.148')
                and (so_owner.SiteId in ('E56255EC-B3C0-E811-80E5-0050560100C4','3CE67C4F-B4C8-E811-80E0-0050560100C3','C9A81BDF-60AD-E911-80EC-0050560100C4')
                or su_db.siteid in ('E56255EC-B3C0-E811-80E5-0050560100C4','3CE67C4F-B4C8-E811-80E0-0050560100C3','C9A81BDF-60AD-E911-80EC-0050560100C4'))
                and
                --so.createdon between '2019-09-01' and '2019-10-01'
                so.createdon>='2019-07-01'
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

