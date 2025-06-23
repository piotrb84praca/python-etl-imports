import pandas as pd
from sqlalchemy import text
from classes.ConnectOracleDevEngine import ConnectOracleDevEngine
from classes.ConnectFort import ConnectFort
from classes.ImportLogger import ImportLogger  

class importEasyProjects:
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
                    cursor.execute("BEGIN KLIK.CLEAN_TABLE('IMPORT_EASY_PROJECTS'); END;")
                    #self.oracle_connection.execute(text("TRUNCATE TABLE IMPORT_EASY_PROJECTS"))
                
    def importToOracle(self,df): 
               
                 # Prepare insert statement
                insert_stmt = text("""INSERT INTO IMPORT_EASY_PROJECTS (
                                    PROJECT_ID,
                                    PROJECT_NUMBER,
                                    ID_SOW,
                                    CUSTOMER_NAME,
                                    PROJECT_NAME,
                                    PROJECT_CREATE_DATE,
                                    PROJECT_STEP,
                                    PROJECT_OWNER,
                                    PROJECT_OWNER_LOGIN,
                                    EST_BILING_DATE,
                                    EST_CLOSE_DATE,
                                    PROJECT_STATE,
                                    PROJECT_STATUS,
                                    MARGIN,
                                    REVENUE,
                                    ESTIMATED_VALUE,
                                    LAST_STEP_CHANGE_DATE,
                                    CREATED_BY,
                                    CREATED_BY_LOGIN,
                                    PRODUCT_CATEGORY,
                                    CUSTOMER_BUDGET,
                                    SALE_TYPE,
                                    PIL_ID,
                                    PIL_FINAL,
                                    PIL_STATUS,
                                    INITIATIVE
                                    )
                            VALUES 
                                    (
                					:PROJECT_ID,
                                    :PROJECT_NUMBER,
                                    :ID_SOW,
                                    :CUSTOMER_NAME,
                                    :PROJECT_NAME,
                                    :PROJECT_CREATE_DATE,
                                    :PROJECT_STEP,
                                    :PROJECT_OWNER,
                                    :PROJECT_OWNER_LOGIN,
                                    :EST_BILING_DATE,
                                    :EST_CLOSE_DATE,
                                    :PROJECT_STATE,
                                    :PROJECT_STATUS,
                                    :MARGIN,
                                    :REVENUE,
                                    :ESTIMATED_VALUE,
                                    :START_DATE,
                                    :CREATED_BY,
                                    :CREATED_BY_LOGIN,
                                    :CATEGORY,
                                    :BUDGET_AMOUNT,
                                    :PROJECT_TYPE,
                                    :PL_ID,
                                    :PL_FINAL,
                                    :PL_STATUS,
                                    :INITIATIVE
                					)""")
    
                # Insert new data into Oracle using executemany for bulk insert
                self.oracle_connection.execute(insert_stmt, df.to_dict(orient='records'))
                # Commit the transaction
                self.oracle_connection.commit()
    
    def parseData(self, df):

                    df['BUDGET_AMOUNT'] = df['BUDGET_AMOUNT'].fillna(0)
                    df['PL_ID'] = df['PL_ID'].fillna('null')
                    df['PL_STATUS'] = df['PL_STATUS'].fillna('null')
        
                    return df
        
    def run(self):
            try:
                
                self.logger.start_import(self.__class__.__name__)
    
                # Connect to MSSQL
                mssql_connection = self.mssql_conn.connect()

                
                # Extract data from MSSQL Query 1
                query = """ SELECT 
                                REPLACE((CONVERT(CHAR(40),o.opportunityId )),' ','' )  as PROJECT_ID,
                                o.elob_OpportunityID as PROJECT_NUMBER,
                                CASE 
                                    WHEN a.elob_sowid like 'R%R___' THEN a.elob_sowid
                                    WHEN a.elob_sowid like 'R%R%' THEN substring(a.elob_sowid , 2, CHARINDEX ('R',a.elob_sowid,2)-2)
                                    ELSE a.elob_sowid
                                END as ID_SOW,
                                a.Name as CUSTOMER_NAME,
                                o.name as PROJECT_NAME,
                                substring(CONVERT(varchar,DateAdd(hour,2,o.CreatedOn),120),0,11) as PROJECT_CREATE_DATE,
                                elob_salesstage.Value as PROJECT_STEP,
                                o.owneridname as PROJECT_OWNER,
                    			substring( sp_o.DomainName, 4, 20) as PROJECT_OWNER_LOGIN,
                                CAST(DATEADD(hour, 2, o.elob_estimatedbillingdate) AS Date) as EST_BILING_DATE,
                                CAST(DATEADD(hour, 2, o.EstimatedCloseDate) AS Date) as EST_CLOSE_DATE,
                                StateCodePLTable.value as PROJECT_STATE,
                                StatusCodePLTable.value as PROJECT_STATUS,
                                CAST(DATEADD(hour, 2, st.startdate) AS Date) as START_DATE,
                                crby.FullName CREATED_BY,
                                substring(crby.DomainName, 4, 20) as CREATED_BY_LOGIN, 
                                [elob_productcategoryPLTable].value CATEGORY,
                                o.BudgetAmount BUDGET_AMOUNT,
                                projectType.value PROJECT_TYPE,
                                pl.elob_PL_Id PL_ID,
                                apbdic.value PL_STATUS,
                                pl.elob_FinalPL PL_FINAL,
                                SUM(CASE
                                        WHEN pls.elob_mobilediscountedcashmargin IS NOT NULL AND pls.elob_mobilediscountedcashmargin <> 0 THEN CAST(pls.elob_mobilediscountedcashmargin AS DECIMAL(12,2))
                                        when pl.elob_PLType = 1 THEN cast(isnull(pls.elob_fix_margin_on_contract,0) as float) * cast(isnull(pls.elob_fix_total_revenue_new,0) as float) /100
                                        ELSE isnull(cast(pls.elob_mobile_ContributionMargin as float), 0) * (cast(isnull(cast(plp02.elob_ParameterValue as float), 0)+ isnull(cast(plp03.elob_ParameterValue as float), 0) as float))/100
                                    END) as MARGIN,
                                SUM(CASE
                                        WHEN pl.elob_PLType = 1 THEN cast(isnull(pls.elob_fix_total_revenue_new,0) as float)
                                        ELSE cast(isnull(cast(plp02.elob_ParameterValue as float), 0)+ isnull(cast(plp03.elob_ParameterValue as float), 0) as float)
                                    END) as REVENUE,
                                SUM(isnull(o.EstimatedValue,0)) as ESTIMATED_VALUE,
                               
                               
                                CASE
                               WHEN InitiativeTable.value is not null then cast(InitiativeTable.value as varchar)
                               WHEN o.elob_initiative=2 then cast('Cybersecurity Storm' as varchar) 
                               ELSE cast(o.elob_initiative as varchar) 
                               END  as INITIATIVE
                            FROM 
                                Opportunity o
                                JOIN Account a ON (o.accountid = a.accountid)
                                JOIN SystemUser sp_o ON (o.ownerid = sp_o.SystemUserId)
                                JOIN SystemUser crby ON (o.CreatedBy = crby.SystemUserId)
                                LEFT JOIN ( SELECT MAX(pl.elob_PL_Id) elob_pl_id, pl.elob_Project
                                                        FROM elob_plbase pl
                                                        WHERE pl.elob_FinalPL = 1
                                                        GROUP BY pl.elob_Project) plfinal ON (plfinal.elob_Project = o.OpportunityId)
                                LEFT JOIN ( SELECT MAX(pl.elob_PL_Id) elob_pl_id, pl.elob_Project
                                                        FROM elob_plbase pl
                                                                JOIN ActivityPointerBase apb ON (apb.ActivityId = pl.ActivityId)
                                                        WHERE pl.elob_FinalPL = 0
                                                                AND apb.statuscode = 2
                                                        GROUP BY pl.elob_Project) placcept ON (placcept.elob_Project = o.OpportunityId)
                    
                                LEFT JOIN ( SELECT MAX(pl.elob_PL_Id) elob_pl_id, pl.elob_Project
                                                        FROM elob_plbase pl
                                                                JOIN ActivityPointerBase apb ON (apb.ActivityId = pl.ActivityId)
                                                        WHERE pl.elob_FinalPL = 0
                                                                AND apb.statuscode = 743940000
                                                        GROUP BY pl.elob_Project) plcalc ON (plcalc.elob_Project = o.OpportunityId)
                                LEFT JOIN elob_plbase pl ON (pl.elob_pl_id = CASE WHEN plfinal.elob_pl_id IS NOT NULL THEN plfinal.elob_pl_id
                                                                                WHEN placcept.elob_pl_id IS NOT NULL THEN placcept.elob_pl_id
                                                                                WHEN plcalc.elob_pl_id IS NOT NULL THEN plcalc.elob_pl_id
                                                                                ELSE NULL 
                                                                             END)
                                LEFT JOIN StringMap [elob_salesstage] ON
                                                                ([elob_salesstage].AttributeName = 'elob_salesstage'
                                                                and [elob_salesstage].ObjectTypeCode = 3
                                                                and [elob_salesstage].AttributeValue = o.elob_SalesStage
                                                                and [elob_salesstage].LangId = 1033 )
                                LEFT JOIN StringMap StateCodePLTable ON ([StateCodePLTable].AttributeName = 'StateCode'
                                                                and [StateCodePLTable].ObjectTypeCode = 3
                                                                and [StateCodePLTable].AttributeValue = o.StateCode
                                                                and [StateCodePLTable].LangId = 1045)
                                LEFT JOIN StringMap InitiativeTable ON ([InitiativeTable].AttributeName = 'elob_initiative'
                                                                and [InitiativeTable].ObjectTypeCode = 3
                                                                and [InitiativeTable].AttributeValue = o.elob_initiative
                                                                and [InitiativeTable].LangId = 1033)                                      
                                LEFT JOIN StringMap StatusCodePLTable ON 
                                                ([StatusCodePLTable].AttributeName = 'StatusCode'
                                                and [StatusCodePLTable].ObjectTypeCode = 3
                                                and [StatusCodePLTable].AttributeValue = o.StatusCode
                                                and [StatusCodePLTable].LangId = 1045)
                                LEFT JOIN elob_plresultBase pls on pl.elob_PLstatistic=pls.elob_plresultId
                                LEFT JOIN elob_plsalesresultBase plr on pls.elob_pandl_id=plr.elob_pandl_id
                                LEFT JOIN elob_plparameterBase plp02 on plr.elob_pandl_id=plp02.elob_PL_ResultId and plp02.elob_name like 'mobile_reve_invoices'
                                LEFT JOIN elob_plparameterBase plp03 on plr.elob_pandl_id=plp03.elob_PL_ResultId and plp03.elob_name like 'mobile_reve_operators'
                                LEFT JOIN ( SELECT max(st.elob_startdate) startdate, elob_projectid
                                            FROM elob_statushistoryBase st
                                            GROUP BY elob_projectid) st ON ( st.elob_projectid = o.OpportunityId)
                                LEFT JOIN StringMap [elob_productcategoryPLTable] on 
                                                    ([elob_productcategoryPLTable].AttributeName = 'elob_productcategory'
                                                    and [elob_productcategoryPLTable].ObjectTypeCode = 3
                                                    and [elob_productcategoryPLTable].AttributeValue = o.elob_productcategory
                                                    and [elob_productcategoryPLTable].LangId = 1045)
                                LEFT JOIN StringMap projectType ON (projectType.AttributeName = 'elob_projecttype'
                                                                and projectType.ObjectTypeCode = 3
                                                                and projectType.AttributeValue = o.elob_ProjectType
                                                                and projectType.LangId = 1045)
                                LEFT JOIN ActivityPointerBase apb ON (apb.ActivityId = pl.ActivityId)
                                LEFT JOIN [B2B_MSCRM].[dbo].[StringMapBase] apbdic ON (apbdic.attributeValue = apb.StatusCode
                                                                                        AND apbdic.AttributeName = 'statuscode'
                                                                                        AND apbdic.LangId = 1045
                                                                                        AND apbdic.objecttypecode = 10046 )
                            WHERE
                                a.elob_businesssegment IN (1,2,3,4)
                                AND o.CreatedOn >= '2019-01-01'
                            GROUP BY
                                o.opportunityId,
                                o.elob_OpportunityID,
                                CASE 
                                    WHEN a.elob_sowid like 'R%R___' THEN a.elob_sowid
                                    WHEN a.elob_sowid like 'R%R%' THEN substring(a.elob_sowid , 2, CHARINDEX ('R',a.elob_sowid,2)-2)
                                    ELSE a.elob_sowid
                                END,
                                a.Name,
                                o.name,
                                substring(CONVERT(varchar,DateAdd(hour,2,o.CreatedOn),120),0,11),
                                elob_salesstage.Value,
                                o.owneridname,
                                CAST(DATEADD(hour, 2, o.elob_estimatedbillingdate) AS Date),
                                CAST(DATEADD(hour, 2, o.EstimatedCloseDate) AS Date),
                                StateCodePLTable.value,
                                StatusCodePLTable.value,
                                CAST(DATEADD(hour, 2, st.startdate) AS Date),
                    			crby.FullName ,
                                crby.DomainName ,
                    			substring( sp_o.DomainName, 4, 20),
                                [elob_productcategoryPLTable].value,
                                o.BudgetAmount,
                                projectType.value,
                                pl.elob_PL_Id,
                                apbdic.value,
                                pl.elob_FinalPL,
                                pls.elob_mobilediscountedcashmargin,
                                InitiativeTable.value,
                                o.elob_initiative """

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

