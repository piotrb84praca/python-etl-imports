import pandas as pd
from sqlalchemy import text
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyFixOrders:
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
                    cursor.execute("BEGIN KLIK.CLEAN_TABLE('IMPORT_EASY_FIX_ORDERS'); END;")   
                
    def importToOracle(self,df): 
               
                 # Prepare insert statement
                insert_stmt = text("""
                    INSERT INTO KLIK.IMPORT_EASY_FIX_ORDERS ( IDSOW_KLIENTA,
                    nip_klienta,
                    segment_klienta,
                    wlasciciel_klienta,
                    login_wlasciciela_klienta,
                    kod_bscs,
                    numer_umowy,
                    wlasciciel_umowy,
                    login_wlasciciela_um,
                    kod_bscs_wl_um,
                    status_umowy,
                    typ_umowy,
                    kategoria_umowy,
                    podkategoria_umowy,
                    data_podpisania_umowy,
                    data_weryfikacji_umowy,
                    data_z_sh,
                    ost_data_modyf_umowy,
                    reprezentatnt_orange,
                    login_rep_orange,
                    kod_bscs_rep_orange,
                    projekt_powiazany_bezp_z_um,
                    id_pil_fix_final_z_ppbzu,
                    marza_z_pilfinal_z_ppbzu,
                    marza_uwzgl_prow_final_z_ppbzu,
                    reve_z_pila_final_z_ppbzu,
                    mapa_kompetencji_og,
                    nr_zam_sokx,
                    status_zamowienia_sokx,
                    numer_zgody,
                    oferta_niestandardowa,
                    wlasciciel_zamowienia,
                    login_wlasciciela_zam,
                    kod_bscs_wl_zam,
                    operacja,
                    produkt_na_zam,
                    rodzaj_uslugi,
                    id_projektu,
                    inicjatywa,
                    wlasciciel_projektu,
                    login_wl_projektu,
                    kod_bscs_wl_pro,
                    id_pil_finalnego,
                    marza_z_pil,
                    marza_uwgledniajaca_prowizje,
                    reve_z_pila,
                    mapa_kompetencji_og1,
                    um_barkod,
                    data_arch_um,
                    status_tiger,
                    stawki_na_umowie,
                    db_kod,
                    db_nazwisko,
                    id_pila_finalnego_z_projektu,
                    numer_dolaczonego_pila_bcase,
                    typ_pila,
                    finalny_pil,
                    marza,
                    calkowita_wartosc_przychodu,
                    mapa_kompetencji_og2,
                    flaga_koronawirus,
                    PROJEKT_POWIAZANY_BEZP_Z_UM_FK,
                    LOGIN_WL_PILA_FIN_Z_PROJEKTU,
                    LOGIN_WL_DOLACZONEGO_PILA_BCS,
                    nr_ost_zlec_au,
                    data_wys_zlec_au,
                    odp_ze_zlec_au,
                    data_odp_na_zlec_au,
                    lokalizacja_kod_pocztowy,
                    lokalizacja_miasto,
                    lokalizacja_ulica,
                    lokalizacja_nr_budynku,
                    proces_zam_sokx,
                    sciezka_zam_sokx,
                    data_zakonczenia_testow,
                    administracja,
                    login_administracja)
                    VALUES (
                           :IDSOW_KLIENTA,
                            :NIP_KLIENTA,
                            :SEGMENT_KLIENTA,
                            :WLASCICIEL_KLIENTA,
                            :LOGIN_WLASCICIELA_KLIENTA,
                            :KOD_BSCS,
                            :NUMER_UMOWY,
                            :WLASCICIEL_UMOWY,
                            :LOGIN_WLASCICIELA_UM,
                            :KOD_BSCS_WL_UM,
                            :STATUS_UMOWY,
                            :TYP_UMOWY,
                            :KATEGORIA_UMOWY,
                            :PODKATEGORIA_UMOWY,
                            :DATA_PODPISANIA_UMOWY,
                            :DATA_WERYFIKACJI_UMOWY,
                            :DATA_Z_SH,
                            :OST_DATA_MODYF_UMOWY,
                            :REPREZENTANT_ORANGE,
                            :LOGIN_REP_ORANGE,
                            :KOD_BSCS_REP_ORANGE,
                            :PROJEKT_POWIAZANY_BEZP_Z_UM,
                            :ID_PIL_FIX_FINAL_Z_PPBZU,
                            :MARZA_Z_PILFINAL_Z_PPBZU,
                            :MARZA_UWZGL_PROW_FINAL_Z_PPBZU,
                            :REVE_Z_PILA_FINAL_Z_PPBZU,
                            :MAPA_KOMPETENCJI_OG,
                            :NR_ZAM_SOKX,
                            :STATUS_ZAMOWIENIA_SOKX,
                            :NUMER_ZGODY,
                            :OFERTA_NIESTANDARDOWA,
                            :WLASCICIEL_ZAMOWIENIA,
                            :LOGIN_WLASCICIELA_ZAM,
                            :KOD_BSCS_WL_ZAM,
                            :OPERACJA,
                            :PRODUKT_NA_ZAM,
                            :RODZAJ_USLUGI,
                            :ID_PROJEKTU,
                            :INICJATYWA,
                            :WLASCICIEL_PROJEKTU,
                            :LOGIN_WL_PROJEKTU,
                            :KOD_BSCS_WL_PO,
                            :ID_PIL_FINALNEGO,
                            :MARZA_Z_PIL,
                            :MARZA_UWGLEDNIAJACA_PROWIZJE,
                            :REVE_Z_PILA,
                            :MAPA_KOMPETENCJI_OG1,
                            :UM_BARKOD,
                            :DATA_ARCH_UM,
                            :STATUS_TIGER,
                            :STAWKI_NA_UMOWIE,
                            :DB_KOD,
                            :DB_NAZWISKO,
                            :ID_PILA_FINALNEGO_Z_PROJEKTU,
                            :NUMER_DOLACZONEGO_PILA_BCASE,
                            :TYP_PILA,
                            :FINALNY_PIL,
                            :MARZA,
                            :CALKOWITA_WARTOSC_PRZYCHODU,
                            :MAPA_KOMPETENCJI_OG2,
                            :FLAGA_KORONAWIRUS,
                            :PROJEKT_POWIAZANY_BEZP_Z_UM_FK,
                            :LOGIN_WL_PILA_FIN_Z_PROJEKTU,
                            :LOGIN_WL_DOLACZONEGO_PILA_BCS,
                            :NR_OST_ZLEC_AU,
                            :DATA_WYS_ZLEC_AU,
                            :ODP_ZE_ZLEC_AU,
                            :DATA_ODP_NA_ZLEC_AU,
                            :LOKALIZACJA_KOD_POCZTOWY,
                            :LOKALIZACJA_MIASTO,
                            :LOKALIZACJA_ULICA,
                            :LOKALIZACJA_NR_BUDYNKU,
                            :PROCES_ZAM_SOKX,
                            :SCIEZKA_ZAM_SOKX,
                            :DATA_ZAKONCZENIA_TESTOW,
                            :ADMINISTRACJA,
                            :LOGIN_ADMINISTRACJA


                    )
                """)
    
                # Insert new data into Oracle using executemany for bulk insert
                self.oracle_connection.execute(insert_stmt, df.to_dict(orient='records'))
                # Commit the transaction
                self.oracle_connection.commit()

                # Log the number of rows affected
                #self.logger.add_to_import(f"""Rows imported {self.start_pos} """) 
                
        


    def parseData(self, df):
        
                    df['MARZA_Z_PILFINAL_Z_PPBZU'] = pd.to_numeric(df['MARZA_Z_PILFINAL_Z_PPBZU'], errors='coerce').fillna(0).astype(float)
                    df['MARZA_UWZGL_PROW_FINAL_Z_PPBZU'] = pd.to_numeric(df['MARZA_UWZGL_PROW_FINAL_Z_PPBZU'], errors='coerce').fillna(0).astype(float)
                    df['REVE_Z_PILA_FINAL_Z_PPBZU'] = pd.to_numeric(df['REVE_Z_PILA_FINAL_Z_PPBZU'], errors='coerce').fillna(0).astype(float)
        
                    df['MARZA_Z_PIL'] =  pd.to_numeric(df['MARZA_Z_PIL'], errors='coerce').fillna(0).astype(float)
                    df['MARZA_UWGLEDNIAJACA_PROWIZJE'] = pd.to_numeric(df['MARZA_UWGLEDNIAJACA_PROWIZJE'], errors='coerce').fillna(0).astype(float)
                    df['REVE_Z_PILA'] =  pd.to_numeric(df['REVE_Z_PILA'], errors='coerce').fillna(0).astype(float)
        
                    df['MARZA'] =  pd.to_numeric(df['MARZA'], errors='coerce').fillna(0).astype(float)
                    df['CALKOWITA_WARTOSC_PRZYCHODU'] =  pd.to_numeric(df['CALKOWITA_WARTOSC_PRZYCHODU'], errors='coerce').fillna(0).astype(float)
        
                    df['MAPA_KOMPETENCJI_OG2'] =  df['MAPA_KOMPETENCJI_OG2'].replace('.', ',')
                    df['LOKALIZACJA_ULICA'] =  df['LOKALIZACJA_ULICA'].replace('\'', '')


        
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
a.elob_sowid AS [IDSOW_KLIENTA],
ISNULL(a.elob_nip, '') AS [NIP_KLIENTA],
MAPA.Value AS [SEGMENT_KLIENTA],
SUA.FULLNAME AS [WLASCICIEL_KLIENTA],
SUBSTRING(SUA.DomainName, 4, 100) AS [LOGIN_WLASCICIELA_KLIENTA],
ISNULL(SUA.elob_salespersonid, '') AS [KOD_BSCS],
C.ContractNumber AS [NUMER_UMOWY],
SUC.FULLNAME AS [WLASCICIEL_UMOWY],
ISNULL(SUBSTRING(SUC.DomainName, 4, 100), '') AS [LOGIN_WLASCICIELA_UM],
ISNULL(SUC.elob_salespersonid, '') AS [KOD_BSCS_WL_UM],
MAPC.Value AS [STATUS_UMOWY],
MAPC1.Value AS [TYP_UMOWY],
CB.elob_name AS [KATEGORIA_UMOWY],
CB1.elob_name AS [PODKATEGORIA_UMOWY],
ISNULL(FORMAT(C.elob_SignedbyCustomeron + 1, 'yyyy-MM-dd'), '') AS [DATA_PODPISANIA_UMOWY],
FORMAT(C.elob_revisiondate, 'yyyy-MM-dd') AS [DATA_WERYFIKACJI_UMOWY],
FORMAT(SH.createdon, 'yyyy-MM-dd') AS [DATA_Z_SH],
FORMAT(C.ModifiedOn, 'yyyy-MM-dd') AS [OST_DATA_MODYF_UMOWY],
ISNULL(SUCC.FULLNAME, '') AS [REPREZENTANT_ORANGE],
ISNULL(SUBSTRING(SUCC.DomainName, 4, 100), '') AS [LOGIN_REP_ORANGE],
ISNULL(SUCC.elob_salespersonid, '') AS [KOD_BSCS_REP_ORANGE],
ISNULL(OC.elob_OpportunityID, '') AS [PROJEKT_POWIAZANY_BEZP_Z_UM],
ISNULL(PL1.elob_PL_Id, '') AS [ID_PIL_FIX_FINAL_Z_PPBZU],
CASE WHEN PL1.elob_pl_id IS NULL THEN ' ' ELSE CAST(PLS1.elob_fix_margin_on_contract AS FLOAT) END AS [MARZA_Z_PILFINAL_Z_PPBZU],
CASE WHEN PLP011.elob_parametervalue IS NULL THEN ' ' ELSE CAST(PLP011.elob_parametervalue AS FLOAT) END AS [MARZA_UWZGL_PROW_FINAL_Z_PPBZU],
CASE WHEN PL1.elob_pl_id IS NULL THEN ' ' ELSE CAST(PLS1.elob_fix_total_revenue_new AS FLOAT) END AS [REVE_Z_PILA_FINAL_Z_PPBZU],
CASE WHEN PLP013.elob_name IS NULL AND PLP014.elob_name IS NULL THEN ''
     WHEN PLP013.elob_name IS NOT NULL AND PLP014.elob_name IS NULL THEN 'OG'
     WHEN PLP013.elob_name IS NULL AND PLP014.elob_name IS NOT NULL THEN 'MOc'
     ELSE 'MOc' END AS [MAPA_KOMPETENCJI_OG],
ISNULL(SUSA.FULLNAME, '') AS [ADMINISTRACJA],
ISNULL(SUBSTRING(SUSA.DomainName, 4, 100), '') AS [LOGIN_ADMINISTRACJA],
ISNULL(SO.elob_ordernumber, '') AS [NR_ZAM_SOKX],
CASE WHEN SO.statuscode IS NULL THEN ' ' ELSE MAPSO.Value END AS [STATUS_ZAMOWIENIA_SOKX],
ISNULL(REPLACE(P.elob_value, '    ', ''), '') AS [NUMER_ZGODY],
CASE WHEN P1.elob_value=63 THEN 'NIE' WHEN P1.elob_value=62 THEN 'TAK' ELSE ' ' END AS [OFERTA_NIESTANDARDOWA],
ISNULL(SUSO.FULLNAME, '') AS [WLASCICIEL_ZAMOWIENIA],
ISNULL(SUBSTRING(SUSO.DomainName, 4, 100), '') AS [LOGIN_WLASCICIELA_ZAM],
ISNULL(SUSO.elob_salespersonid, '') AS [KOD_BSCS_WL_ZAM],
CASE WHEN SO.elob_operation IS NULL THEN ' ' ELSE MAPSO1.Value END AS [OPERACJA],
ISNULL(SP.elob_name, '') AS [PRODUKT_NA_ZAM],
ISNULL(PPV.elob_value, '') AS [RODZAJ_USLUGI],
ISNULL(MAPSO2.value, '') AS [SCIEZKA_ZAM_SOKX],
ISNULL(PPS.elob_name, '') AS [PROCES_ZAM_SOKX],
ISNULL(O.elob_OpportunityID, '') AS [ID_PROJEKTU],
ISNULL(MAPO.value, '') AS [INICJATYWA],
ISNULL(SUO.FULLNAME, '') AS [WLASCICIEL_PROJEKTU],
CASE WHEN SUO.DomainName IS NULL THEN ' ' ELSE SUBSTRING(SUO.DomainName, 4, 100) END AS [LOGIN_WL_PROJEKTU],
ISNULL(SUO.elob_salespersonid, '') AS [KOD_BSCS_WL_PO],
ISNULL(PL.elob_PL_Id, '') AS [ID_PIL_FINALNEGO],

CASE WHEN PL.elob_pl_id IS NULL THEN ' ' ELSE CAST(PLS.elob_fix_margin_on_contract AS FLOAT) END AS [MARZA_Z_PIL],
CASE WHEN PLP01.elob_parametervalue IS NULL THEN ' ' ELSE CAST(PLP01.elob_parametervalue AS FLOAT) END AS [MARZA_UWGLEDNIAJACA_PROWIZJE],
CASE WHEN PL.elob_pl_id IS NULL THEN ' ' ELSE CAST(PLS.elob_fix_total_revenue_new AS FLOAT) END AS [REVE_Z_PILA],

CASE WHEN PLP03.ELOB_NAME IS NULL AND PLP04.ELOB_NAME IS NULL THEN ''
     WHEN PLP03.ELOB_NAME IS NOT NULL AND PLP04.ELOB_NAME IS NULL THEN 'OG'
     WHEN PLP03.ELOB_NAME IS NULL AND PLP04.ELOB_NAME IS NOT NULL THEN 'MOc'
     ELSE 'MOc' END AS [MAPA_KOMPETENCJI_OG1]

-- UMOWA
,ISNULL(CASE WHEN CA.ELOB_Barcode='null' THEN '' ELSE CA.ELOB_Barcode END, '') AS [UM_BARKOD]
,ISNULL(FORMAT(CA.ELOB_ARCHIVINGDATE+1, 'yyyy-MM-dd'), '') AS [DATA_ARCH_UM]
,ISNULL(SMCA.VALUE, '') AS [STATUS_TIGER]
,ISNULL(MAPC2.VALUE, '') AS [STAWKI_NA_UMOWIE]
,ISNULL(SUDB.ELOB_SALESPERSONID, '') AS [DB_KOD]
,ISNULL(SUDB.FULLNAME, '') AS [DB_NAZWISKO]

--- NOWE POLA
,ISNULL(PL01.ELOB_PL_ID, '') AS [ID_PILA_FINALNEGO_Z_PROJEKTU]
,ISNULL(SUBSTRING(SUO_PL01.DOMAINNAME, 4, 100), '') AS [LOGIN_WL_PILA_FIN_Z_PROJEKTU]
,ISNULL(SUBSTRING(SUO_PL02.DOMAINNAME, 4, 100), '') AS [LOGIN_WL_DOLACZONEGO_PILA_BCS]
,ISNULL(C.ELOB_ATTACHEDPLNUMBER, '') AS [NUMER_DOLACZONEGO_PILA_BCASE]
,CASE WHEN PL01.ELOB_PLTYPE=1 AND PL02.ELOB_PLTYPE IS NULL THEN 'Fix'
      WHEN PL02.ELOB_PLTYPE=1 AND PL01.ELOB_PLTYPE IS NULL THEN 'Fix'
      WHEN PL01.ELOB_PLTYPE=2 AND PL02.ELOB_PLTYPE IS NULL THEN 'Mobile'
      WHEN PL01.ELOB_PLTYPE=2 AND PL02.ELOB_PLTYPE IS NULL THEN 'Mobile'
      WHEN PL01.ELOB_PLTYPE=1 AND PL02.ELOB_PLTYPE IS NOT NULL THEN 'Fix'
      WHEN PL01.ELOB_PLTYPE=2 AND PL02.ELOB_PLTYPE IS NOT NULL THEN 'Mobile'
      ELSE '' END AS [TYP_PILA]
,CASE WHEN PL01.ELOB_FINALPL=0 AND PL02.ELOB_FINALPL IS NULL THEN 'NIE'
      WHEN PL01.ELOB_FINALPL=1 AND PL02.ELOB_FINALPL IS NULL THEN 'TAK'
      WHEN PL02.ELOB_FINALPL=1 AND PL01.ELOB_FINALPL IS NULL THEN 'TAK'
      WHEN PL02.ELOB_FINALPL=0 AND PL01.ELOB_FINALPL IS NULL THEN 'NIE'
      WHEN PL01.ELOB_FINALPL=0 AND PL02.ELOB_FINALPL IS NOT NULL THEN 'NIE'
      WHEN PL01.ELOB_FINALPL=1 AND PL02.ELOB_FINALPL IS NOT NULL THEN 'TAK'
      ELSE '' END AS [FINALNY_PIL]
,CASE WHEN PLS01.ELOB_FIX_MARGIN_ON_CONTRACT IS NULL AND PLS02.ELOB_FIX_MARGIN_ON_CONTRACT IS NOT NULL THEN CAST(PLS02.ELOB_FIX_MARGIN_ON_CONTRACT AS FLOAT)
      WHEN PLS01.ELOB_FIX_MARGIN_ON_CONTRACT IS NOT NULL AND PLS02.ELOB_FIX_MARGIN_ON_CONTRACT IS NULL THEN CAST(PLS01.ELOB_FIX_MARGIN_ON_CONTRACT AS FLOAT)
      WHEN PLS01.ELOB_FIX_MARGIN_ON_CONTRACT IS NOT NULL AND PLS02.ELOB_FIX_MARGIN_ON_CONTRACT IS NOT NULL THEN CAST(PLS01.ELOB_FIX_MARGIN_ON_CONTRACT AS FLOAT)
      ELSE '' END AS [MARZA]
,CASE WHEN PLS01.ELOB_FIX_TOTAL_REVENUE_NEW IS NULL AND PLS02.ELOB_FIX_TOTAL_REVENUE_NEW IS NOT NULL THEN CAST(PLS02.ELOB_FIX_TOTAL_REVENUE_NEW AS FLOAT)
      WHEN PLS01.ELOB_FIX_TOTAL_REVENUE_NEW IS NOT NULL AND PLS02.ELOB_FIX_TOTAL_REVENUE_NEW IS NULL THEN CAST(PLS01.ELOB_FIX_TOTAL_REVENUE_NEW AS FLOAT)
      WHEN PLS01.ELOB_FIX_TOTAL_REVENUE_NEW IS NOT NULL AND PLS02.ELOB_FIX_TOTAL_REVENUE_NEW IS NOT NULL THEN CAST(PLS01.ELOB_FIX_TOTAL_REVENUE_NEW AS FLOAT)
      ELSE '' END AS [CALKOWITA_WARTOSC_PRZYCHODU],


CASE WHEN PLP031.elob_name IS NULL AND PLP032.elob_name IS NULL AND PLP041.elob_name IS NULL AND PLP042.elob_name IS NULL THEN ''
     WHEN PLP031.elob_name IS NOT NULL AND PLP032.elob_name IS NULL AND PLP041.elob_name IS NULL AND PLP042.elob_name IS NULL THEN 'OG'
     WHEN PLP031.elob_name IS NULL AND PLP032.elob_name IS NOT NULL AND PLP041.elob_name IS NULL AND PLP042.elob_name IS NULL THEN 'OG'
     WHEN PLP031.elob_name IS NULL AND PLP032.elob_name IS NULL AND PLP041.elob_name IS NULL AND PLP042.elob_name IS NOT NULL THEN 'MOc'
     WHEN PLP031.elob_name IS NULL AND PLP032.elob_name IS NULL AND PLP041.elob_name IS NOT NULL AND PLP042.elob_name IS NULL THEN 'MOc'
     WHEN PLP031.elob_name IS NOT NULL AND PLP032.elob_name IS NOT NULL AND PLP041.elob_name IS NOT NULL AND PLP042.elob_name IS NOT NULL THEN 'MOc'
     ELSE '' END AS [MAPA_KOMPETENCJI_OG2],
     
CASE WHEN OC.elob_coronavirus=1 THEN 'TAK' WHEN OC.elob_coronavirus=0 THEN 'NIE' ELSE '' END AS [PROJEKT_POWIAZANY_BEZP_Z_UM_FK],
CASE WHEN O.elob_coronavirus=1 THEN 'TAK' WHEN O.elob_coronavirus=0 THEN 'NIE' ELSE '' END AS [FLAGA_KORONAWIRUS],
ISNULL(OST_AU.elob_sokxnumber, '') AS [NR_OST_ZLEC_AU],
ISNULL(FORMAT(OST_AU.elob_executiondate, 'yyyy-MM-dd'), '') AS [DATA_WYS_ZLEC_AU],
ISNULL(OST_AU.odp, '') AS [ODP_ZE_ZLEC_AU],
ISNULL(FORMAT(OST_AU.elob_decisiondate, 'yyyy-MM-dd'), '') AS [DATA_ODP_NA_ZLEC_AU],
ISNULL(ZPC.elob_name, '') AS [LOKALIZACJA_KOD_POCZTOWY],
CI.elob_name AS [LOKALIZACJA_MIASTO],
CASE WHEN S.elob_name IS NULL THEN ' ' ELSE S.elob_name END AS [LOKALIZACJA_ULICA],
CASE WHEN ADR.elob_buildingnumberdict IS NULL THEN ADR.elob_BuildingNumber ELSE BB.elob_number END AS [LOKALIZACJA_NR_BUDYNKU],
ISNULL(FORMAT(SO.elob_testcompletiondate, 'yyyy-MM-dd'), '') AS [DATA_ZAKONCZENIA_TESTOW]



from contractbase c
--category
--C285A2B5-3BC1-E411-80C7-00505601285C fix
--15F8CEC6-3BC1-E411-80C7-00505601285C mobile

left join elob_statushistoryBase sh on c.contractid=sh.elob_contractid
JOIN (SELECT distinct elob_contractid,
min(createdon) as createdon
FROM elob_statushistoryBase
where elob_statusreasonid in ('6','100000002','743940004','743940000')

GROUP BY elob_contractid)
b ON sh.elob_contractid = b.elob_contractid and sh.createdon = b.createdon

join elob_categoryBase cb on c.elob_CategoryId=cb.elob_categoryId and cb.elob_categoryId='C285A2B5-3BC1-E411-80C7-00505601285C'

left join elob_categorybase cb1 on c.elob_Subcategory=cb1.elob_categoryId
join accountbase a on c.CustomerId=a.AccountId
left join stringmapbase mapa on a.elob_BusinessSegment=mapa.AttributeValue and mapa.AttributeName='elob_BusinessSegment' and mapa.[LangId]=1033 and mapa.ObjectTypeCode=1

--projekt bezposrednio z umowa
left join opportunitybase oc on c.contractid=oc.elob_contract
/* do weryfikacji - dwa ponizsze joiny zamiast powyzszego
left join elob_contract_opportunityBase co on c.contractid=co.contractid --brakujacy kawalek
left join opportunitybase oc on co.opportunityid=oc.OpportunityId
*/

left join elob_plbase pl1 on oc.OpportunityId=pl1.elob_Project and pl1.elob_PLType=1 and pl1.elob_FinalPL=1
left join elob_plresultBase pls1 on pl1.elob_PLstatistic=pls1.elob_plresultId--stat
left join elob_plsalesresultBase plr1 on pls1.elob_pandl_id=plr1.elob_pandl_id
left join elob_plparameterBase plp011 on plr1.elob_pandl_id=plp011.elob_PL_ResultId and plp011.elob_name ='fix_margin_80'
---MOC i OG
left join elob_plparameterBase plp013 on plr1.elob_pandl_id=plp013.elob_PL_ResultId and plp013.elob_name ='Oferta Gotowa'  --OG
left join elob_plparameterBase plp014 on plr1.elob_pandl_id=plp014.elob_PL_ResultId and plp014.elob_name ='Mapa kompetencji usluga'  --MoC

--
left join stringmapbase mapc on c.statuscode=mapc.AttributeValue and mapc.ObjectTypeCode=1010 and mapc.[LangId]=1045 and mapc.AttributeName='statuscode'
left join stringmapbase mapc1 on c.elob_ContractType=mapc1.AttributeValue and mapc1.ObjectTypeCode=1010 and mapc1.[LangId]=1045 and mapc1.AttributeName='elob_ContractType'
left join stringmapbase mapc2 on c.elob_contractratetype=mapc2.AttributeValue and mapc2.ObjectTypeCode=1010 and mapc2.[LangId]=1045 and mapc2.AttributeName='elob_contractratetype'

--fix
left join elob_sokxorder2Base so on c.ContractId=so.elob_Contract and so.elob_ordernumber is not null
left join OpportunityBase o on so.elob_project=o.OpportunityId
left join stringmapbase mapo on o.elob_initiative=mapo.AttributeValue and mapo.ObjectTypeCode=3 and mapo.[LangId]=1033 and mapo.AttributeName='elob_initiative'
left join elob_soldproductBase sp on so.elob_soldproduct=sp.elob_soldproductId
left join elob_parameterBase p on so.elob_sokxorder2Id=p.elob_SOKXOrder and p.elob_name='Numer zgody'
left join elob_parameterBase p1 on so.elob_sokxorder2Id=p1.elob_SOKXOrder and p1.elob_name ='Oferta niestandardowa'
left join elob_addressBase adr on so.elob_location=adr.elob_addressId
---adr
left join elob_cityBase ci on adr.elob_city=ci.elob_cityId
left join elob_streetBase s on adr.elob_Street=s.elob_streetId
left join elob_zipconnectionsBase zpc on adr.elob_zipconnections=zpc.elob_zipconnectionsId
left join elob_buildingBase bb on adr.elob_buildingnumberdict=bb.elob_buildingId
-- sprr
left join elob_soldproductBase sp2 on sp.elob_soldproductId=sp2.elob_ParentSoldProduct
left join elob_soldproductparameterBase spp on sp2.elob_soldproductId=spp.elob_SoldProduct and spp.elob_Parameter='7EF087F7-BD6B-E511-80CE-00505601285C'
left join elob_productparametersBase pp on spp.elob_Parameter=pp.elob_productparametersId
left join elob_productparametervalueBase ppv on pp.elob_productparametersId=ppv.elob_Parameter and spp.elob_ParameterValue=ppv.elob_Index --and ppv.elob_Index=pp.elob_variant

left join elob_soldproductparametervalueBase spv on spp.elob_soldproductparameterId=spv.elob_SoldProductParameter and spp.elob_ParameterValue=ppv.elob_Index

-- maps sp
left join stringmapbase mapso on so.statuscode=mapso.AttributeValue and mapso.AttributeName='statuscode' and mapso.[langid]=1033 and mapso.ObjectTypeCode=10069
left join stringmapbase mapso1 on so.elob_operation=mapso1.AttributeValue and mapso1.AttributeName='elob_operation' and mapso1.[LangId]=1033 and mapso1.ObjectTypeCode=10069
--24032022
left join stringmapbase mapso2 on so.elob_path=mapso2.AttributeValue and mapso2.ObjectTypeCode=10069 and mapso2.LangId=1033 and mapso2.AttributeName='elob_path'
left join elob_productprocessbase pps on so.elob_process=pps.elob_productprocessId
-- pil
left join elob_plbase pl on o.OpportunityId=pl.elob_Project and pl.elob_FinalPL=1 and pl.elob_PLType=1 --pil fix finalny
left join elob_plresultBase pls on pl.elob_PLstatistic=pls.elob_plresultId--stat
left join elob_plsalesresultBase plr on pls.elob_pandl_id=plr.elob_pandl_id
left join elob_plparameterBase plp01 on plr.elob_pandl_id=plp01.elob_PL_ResultId and plp01.elob_name ='fix_margin_80'
---MOC i OG
left join elob_plparameterBase plp03 on plr.elob_pandl_id=plp03.elob_PL_ResultId and plp03.elob_name ='Oferta Gotowa'  --OG
left join elob_plparameterBase plp04 on plr.elob_pandl_id=plp04.elob_PL_ResultId and plp04.elob_name ='Mapa kompetencji usluga'  --MoC

-- userzy
left join SystemUserBase suso on so.OwnerId=suso.SystemUserId
left join SystemUserBase susa on so.elob_salesadministrationuser=susa.SystemUserId
left join systemuserbase suo on o.OwnerId=suo.SystemUserId
join systemuserbase sua on a.ownerid=sua.SystemUserId

join systemuserbase suc on c.ownerid=suc.SystemUserId
left join elob_contributorBase cc on c.contractid=cc.elob_Contract
left join SystemUserBase succ on cc.elob_OrangeRepresentative=succ.SystemUserId

left join SystemUserBase sudb on sudb.SystemUserId=so.elob_DoubleBooking

-- Tiger
left join elob_contractarchive ca on ca.elob_Contract=c.ContractId
left join StringMapBase smca on smca.AttributeValue=ca.elob_tigerstatus and smca.ObjectTypeCode='10024' and smca.AttributeName='elob_tigerstatus' and smca.LangId='1045'

--NOWE KOLUMNY do UM
--z proj
left join elob_plbase pl01 on c.elob_plnumber=pl01.ActivityId
left join elob_plresultBase pls01 on pl01.elob_PLstatistic=pls01.elob_plresultId--stat
left join elob_plsalesresultBase plr01 on pls01.elob_pandl_id=plr01.elob_pandl_id
--2021-01-11
left join activitypointerbase pl01_ap on pl01.activityid=pl01_ap.ActivityId
left join systemuserbase suo_pl01 on pl01_ap.OwnerId=suo_pl01.SystemUserId

left join elob_plparameterBase plp031 on plr01.elob_pandl_id=plp031.elob_PL_ResultId and plp031.elob_name ='Oferta Gotowa'  --OG
left join elob_plparameterBase plp041 on plr01.elob_pandl_id=plp041.elob_PL_ResultId and plp041.elob_name ='Mapa kompetencji usluga'  --MoC
----z plaucha
left join elob_plbase pl02 on c.elob_attachedplnumber=pl02.elob_pl_id
left join elob_plresultBase pls02 on pl02.elob_PLstatistic=pls02.elob_plresultId--stat
left join elob_plsalesresultBase plr02 on pls02.elob_pandl_id=plr02.elob_pandl_id
left join elob_plparameterBase plp032 on plr02.elob_pandl_id=plp032.elob_PL_ResultId and plp032.elob_name ='Oferta Gotowa'  --OG
left join elob_plparameterBase plp042 on plr02.elob_pandl_id=plp042.elob_PL_ResultId and plp042.elob_name ='Mapa kompetencji usluga'  --MoC
--2021-01-11
left join activitypointerbase pl02_ap on pl02.activityid=pl02_ap.ActivityId
left join systemuserbase suo_pl02 on pl02_ap.OwnerId=suo_pl02.SystemUserId
--2022-02-16
left join (
select wo.elob_SOKXOrderWorkOrderId, wo.elob_sokxnumber, mapwo.Value as odp, mapwo1.value as typ_zlec
,ROW_NUMBER() OVER(PARTITION BY wo.elob_SOKXOrderWorkOrderId ORDER BY wo.createdon deSC) as rn,
wo.elob_decisiondate,wo.elob_executiondate
from
elob_workorder2Base wo
left join stringmapbase mapwo on wo.elob_sokxwodecision=mapwo.AttributeValue and mapwo.ObjectTypeCode=10080 and
mapwo.LangId=1045 and mapwo.AttributeName='elob_sokxwodecision'
left join stringmapbase mapwo1 on wo.new_SOKXOrderType=mapwo1.AttributeValue and mapwo1.ObjectTypeCode=10080 and
mapwo1.LangId=1045 and mapwo1.AttributeName='new_SOKXOrderType'
where
wo.new_SOKXOrderType=2
)ost_au on so.elob_sokxorder2Id=ost_au.elob_SOKXOrderWorkOrderId and ost_au.rn=1

 
where year(c.elob_revisiondate) >= '2021'
and c.statuscode in ('6','100000002', '743940004', '743940000')
and a.elob_sowid not in ---klienci testowi wycieci w miare mozliwosci
('1111111111R002','R5260016437R3917243','R5452597594R2214940','1111111111R001','1181344530','1310185810','4435271286',
'5045876177','4796154768','5950002447','4054054037','1881881916','9059058982','1874726586','2475019362',
'4179755066','2354243830','3960520680','2225984122','6341624259','6861176577','4172162185','7742728255',
'9784731062','4676855126','4295543163','4459698814','K8812336','7457613270','1188665941','K1281365',
'1747077061','2710444807','9999990001','4845164070','K5645273','K10417615','3142171411','K6247219',
'1323738903','3493865659','4519403893','8157104056','5058094689','3466973534','2591030458','1152683560',
'9819448230','K11977016','6172139589','2814770928','9453262082','4543379141','1796179235','K10640910',
'1655844127','2773365029','1071043224','3265360032','2835892163','K7300890','K8812345','9542755828',
'1031331454','1230944254','K11622354','6328998356','9372344309','2780769638','7829430851','7730588587',
'1561637818','1477498797','7355954375','5524157670','5663427009','2533327785','K11200615','K6608149',
'S22011687378014','8163869987','8247230801','K4440434','K5995889','1751293612','K7632452','3451234512','5123451234')
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

