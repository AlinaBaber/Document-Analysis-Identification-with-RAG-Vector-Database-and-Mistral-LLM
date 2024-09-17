import os
import glob
import pandas as pd
import mysql.connector
import pyodbc
import dateutil.parser as dparser
import imutils
import json

class PatientDataRetrieval:
    def __init__(self):
        self.khi_cnxn = None
        self.iwp_cnxn = None
        self.pi_cnxn = None
        self.pharmacy_cnxn = None
        self.ALL_SERVERS_PATIENT_DATA = []
        self.khi_server_db_url = ""
        self.iwp_server_db_url = ""
        self.pi_server_db_url = ""
        self.pharmacy_server_db_url = ""
        self.khi_database = ""
        self.iwp_database = ""
        self.pi_database = ""
        self.pharmacy_database = ""
        self.khi_username = ""
        self.iwp_username = ""
        self.pi_username = ""
        self.pharmacy_username = ""
        self.khi_password = ""
        self.iwp_password = ""
        self.pi_password = ""
        self.pharmacy_password = ""
        


        with open("CONFIG.JSON", "r") as json_data:
            config  = json_data.read()

        config = json.loads(config)

        DB_CONNECTION_ESTABLISHED = False

        # AI PROCESS DB CREDENITIALS 
        AI_PROCESS_DB_HOST = config["AI_PROCESS_DB_HOST"]
        AI_PROCESS_DB_USER = config["AI_PROCESS_DB_USER"] 
        AI_PROCESS_DB_PASS = config["AI_PROCESS_DB_PASS"] 
        AI_PROCESS_DB_DATABASE = config["AI_PROCESS_DB_DATABASE"] 

        ai_process_db = ""
        ai_process_db_cursor = ""

        try:
            # --- AI PROCESS DB -- #
            ai_process_db = mysql.connector.connect(host=AI_PROCESS_DB_HOST, user=AI_PROCESS_DB_USER, passwd=AI_PROCESS_DB_PASS, db=AI_PROCESS_DB_DATABASE)
            ai_process_db_cursor = ai_process_db.cursor()
            DB_CONNECTION_ESTABLISHED = True
            print("CONNECTED AI DB")
        except:
            pass

        try:
            # ----KHI - DB---#
            self.khi_server_db_url =  config["KHI_MEDFLOW_DB_HOST"] 
            self.khi_database =  config["KHI_MEDFLOW_DB_DATABASE"] 
            self.khi_username =  config["KHI_MEDFLOW_DB_USER"] 
            self.khi_password =  config["KHI_MEDFLOW_DB_PASS"]
            khi_file_upload_user = config["KHI_MEDFLOW_DB_FILE_UPLOAD_USER"]
            khi_cnxn =  pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.khi_server_db_url+';DATABASE='+self.khi_database+';UID='+self.khi_username+';PWD='+ self.khi_password)
            print("CONNECTED KHI DB")
        except Exception as e:
            print("Inside KHI Exception:", e)
            


        try:
            # ----IWP - DB---#
            self.iwp_server_db_url = config["IWP_MEDFLOW_DB_HOST"]
            self.iwp_database = config["IWP_MEDFLOW_DB_DATABASE"] 
            self.iwp_username = config["IWP_MEDFLOW_DB_USER"] 
            self.iwp_password = config["IWP_MEDFLOW_DB_PASS"]
            iwp_file_upload_user = config["IWP_MEDFLOW_DB_FILE_UPLOAD_USER"]
            iwp_cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.iwp_server_db_url+';DATABASE='+self.iwp_database+';UID='+self.iwp_username+';PWD='+ self.iwp_password)
            print("CONNECTED IWP DB")
        except Exception as e:
            print("Inside IWP Exception:", e)

        try:
            # ----PI - DB---#
            self.pi_server_db_url = config["PI_MEDFLOW_DB_HOST"]
            self.pi_database = config["PI_MEDFLOW_DB_DATABASE"] 
            self.pi_username = config["PI_MEDFLOW_DB_USER"] 
            self.pi_password = config["PI_MEDFLOW_DB_PASS"]
            pi_file_upload_user = config["PI_MEDFLOW_DB_FILE_UPLOAD_USER"]
            pi_cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.pi_server_db_url+';DATABASE='+self.pi_database+';UID='+self.pi_username+';PWD='+ self.pi_password)
            print("CONNECTED PI DB")
        except Exception as e:
            print("Inside PI Exception:", e) 

        try:
            # ----PI - DB---#
            self.pharmacy_server_db_url = config["PHARMACY_MEDFLOW_DB_HOST"]
            self.pharmacy_database = config["PHARMACY_MEDFLOW_DB_DATABASE"] 
            self.pharmacy_username = config["PHARMACY_MEDFLOW_DB_USER"] 
            self.pharmacy_password = config["PHARMACY_MEDFLOW_DB_PASS"]
            pharmacy_file_upload_user = config["PHARMACY_MEDFLOW_DB_FILE_UPLOAD_USER"]
            pharmacy_cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.pharmacy_server_db_url+';DATABASE='+self.pharmacy_database+';UID='+self.pharmacy_username+';PWD='+ self.pharmacy_password)
            print("CONNECTED PHARMACY DB")
        except Exception as e:
            print("Inside PHARMANCY Exception:", e) 

    def get_data_from_query(self, DB_QUERY):
        db_message_exceptions = ""
    
        khi_datadb_df = []  

        ## KHI DB Connection if not exist
        khi_db_exception = False
        
        try:

            khi_datadb_df = pd.read_sql(DB_QUERY, self.khi_cnxn)
            khi_server_portal = "KHI"
            self.khi_cnxn.close()
        except Exception as e:

            try:

                self.khi_cnxn =  pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.khi_server_db_url+';DATABASE='+self.khi_database+';UID='+self.khi_username+';PWD='+ self.khi_password)
                khi_datadb_df = pd.read_sql(DB_QUERY, self.khi_cnxn)
                khi_server_portal = "KHI"
                self.khi_cnxn.close()
            except Exception as e:
                khi_db_exception = True
                db_message_exceptions = db_message_exceptions  + " KHI DB Exception - Message : " +  str(e)

        
        
        iwp_datadb_df = [] 
        
        ## IWP DB Connection if not exist
        iwp_db_exception = False
        try:

            iwp_datadb_df = pd.read_sql(DB_QUERY, self.iwp_cnxn)
            iwp_server_portal = "IWP"
            self.iwp_cnxn.close()
        except Exception as e:

            try:

                self.iwp_cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.iwp_server_db_url+';DATABASE='+self.iwp_database+';UID='+self.iwp_username+';PWD='+ self.iwp_password)
                iwp_datadb_df = pd.read_sql(DB_QUERY, self.iwp_cnxn)
                iwp_server_portal = "IWP"
                self.iwp_cnxn.close()
            except Exception as e:
                iwp_db_exception = True
                db_message_exceptions = db_message_exceptions + " IWP DB Exception - Message : " + str(e)

        
        pi_datadb_df = []
        
        ## PI DB Connection if not exist    
        pi_db_exception = False   
        try:

            pi_datadb_df = pd.read_sql(DB_QUERY, self.pi_cnxn)
            pi_server_portal = "PI"
            self.pi_cnxn.close()
        except Exception as e:

            try:

                self.pi_cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.pi_server_db_url+';DATABASE='+self.pi_database+';UID='+self.pi_username+';PWD='+ self.pi_password)
                pi_datadb_df = pd.read_sql(DB_QUERY, self.pi_cnxn)
                pi_server_portal = "PI"
                self.pi_cnxn.close()
            except Exception as e:
                pi_db_exception = True
                db_message_exceptions = db_message_exceptions + " PI DB Exception - Message : " +  str(e)

        
        pharmacy_datadb_df = []
        
        ## PHARMACY DB Connection if not exist
        pharmacy_db_exception = False

        try:

            pharmacy_datadb_df = pd.read_sql(DB_QUERY, self.pharmacy_cnxn)
            pharmacy_server_portal = "PHARMACY"
            self.pharmacy_cnxn.close()
            
        except Exception as e:

            try:

                self.pharmacy_cnxn =  pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.pharmacy_server_db_url+';DATABASE='+self.pharmacy_database+';UID='+self.pharmacy_username+';PWD='+ self.pharmacy_password)
                pharmacy_datadb_df = pd.read_sql(DB_QUERY, self.pharmacy_cnxn)
                pharmacy_server_portal = "PHARMACY"
                self.pharmacy_cnxn.close()
            except Exception as e:
                pharmacy_db_exception = True
                db_message_exceptions = db_message_exceptions  + " PHARMACY DB Exception - Message : " +  str(e)



        pdList = []

        # KHI DATA
        if khi_db_exception == False and not (isinstance(khi_datadb_df, list)) and not (khi_datadb_df.empty):
            found_data = True
            khi_datadb_df["SERVER"] = "KHI"
            pdList.append(khi_datadb_df)
            
        # IWP DATA
        if iwp_db_exception == False and not (isinstance(iwp_datadb_df, list)) and not (iwp_datadb_df.empty) :
            found_data = True
            iwp_datadb_df["SERVER"] = "IWP"
            pdList.append(iwp_datadb_df)

        # PI DATA
        if pi_db_exception == False and not (isinstance(pi_datadb_df, list)) and not (pi_datadb_df.empty) :
            found_data = True
            pi_datadb_df["SERVER"] = "PI"
            pdList.append(pi_datadb_df)

        # PHARMACY DATA
        if pharmacy_db_exception == False and not (isinstance(pharmacy_datadb_df, list)) and not (pharmacy_datadb_df.empty) :
            found_data = True
            pharmacy_datadb_df["SERVER"] = "PHARMACY"
            pdList.append(pharmacy_datadb_df)

        df_data = []
        print("PDList",pdList)
        
        if len(pdList) > 0:
            df_data = pd.concat(pdList)
            return df_data

    def getdata(self, query):
        df_data = self.get_data_from_query(query)
        if df_data is not None and not df_data.empty:
            if len(df_data["ADJNUMBER"].str.lower().unique()) == 1 and  len(df_data["DATEOFBIRTH"].unique()) == 1 or len(df_data["PATIENTREGID"].unique()) == 1  and  len(df_data["DATEOFBIRTH"].unique()) == 1 or len(df_data["ADJNUMBER"].str.lower().unique()) == 1 and  len(df_data["SSN"].unique()) == 1 or len(df_data["PATIENTREGID"].unique()) == 1  and  len(df_data["SSN"].unique()) == 1 or len(df_data["ADJNUMBER"].str.lower().unique()) == 1 and  len(df_data["DATE_OF_INJURY_START"].unique()) == 1 or len(df_data["PATIENTREGID"].unique()) == 1  and  len(df_data["DATE_OF_INJURY_START"].unique()) == 1 or len(df_data["ADJNUMBER"].str.lower().unique()) == 1 and  len(df_data["PATIENTNAME"].str.lower().unique()) == 1 or len(df_data["PATIENTREGID"].unique()) == 1  and  len(df_data["PATIENTNAME"].str.lower().unique()) == 1 or len(df_data["ADJNUMBER"].str.lower().unique()) == 1 and  len(df_data["FIRSTNAME"].str.lower().unique()) == 1 or len(df_data["PATIENTREGID"].unique()) == 1  and  len(df_data["FIRSTNAME"].str.lower().unique()) == 1 or len(df_data["ADJNUMBER"].str.lower().unique()) == 1 and  len(df_data["LASTNAME"].str.lower().unique()) == 1 or len(df_data["PATIENTREGID"].unique()) == 1  and  len(df_data["LASTNAME"].str.lower().unique()) == 1 :
                df_data = df_data.drop_duplicates(["PATIENTREGID","SERVER"], keep='last')
                for index, row in df_data.iterrows():
                    self.ALL_SERVERS_PATIENT_DATA.append((row["PATIENTREGID"],row["SERVER"]))

    def mrn_extraction_mailing_docs_func(self, provider_name, adj_number_list, adj_number, current_claim_number,
                                        ssn_number, current_firstname, current_lastname, doi_date, dob_date, dos_date,
                                        external_mrn, is_mrn_level):
        DB_QUERY = ""
    
        try:

            INITIAL_QUERY  = """
            
                        SELECT distinct b.Flag,b.CASEID
                                ,a.PATIENTREGID,(a.LASTNAME+', '+a.FIRSTNAME) PATIENTNAME
                                ,a.LASTNAME
                                ,a.FIRSTNAME
                                ,a.SSN
                                ,inj.DATE_OF_INJURY_START
                                ,inj.DATE_OF_INJURY_END
                                ,a.DATEOFBIRTH
                                ,a.TRACKING_NO 
                                ,bi.BUSINESSID
                                ,bi.BUSINESSREGID 
                                ,b.WCABNUMBER
                                , b.ADJNUMBER
                                --, s.STATUS
                                , ISNULL(b.INACTIVE,0) INACTIVE 
                                ,ct.CASETYPE 
                                ,il.COMPANY INSURANCEFIRM 
                            FROM PATIENT a join CASEINFO b on  b.patientregid=a.patientregid
                                left outer join INJURY inj  on b.CASEID = inj.CASEID 
                                JOIN STUDY_TO_BE_REQUESTED s on s.patientregid=a.patientregid 
                                                                and s.BUSINESSREGID =b.BUSINESSREGID 
                                                                AND b.CASEID = s.CASEID  
                                JOIN BUSINESSINFO bi on  b.BUSINESSREGID = bi.BUSINESSREGID 
                                left JOIN INSURANCE i on a.PATIENTREGID = i.PATIENTREGID 
                                left JOIN VIEW_INSURANCECOMPANYLOOKUP il on i.INSURANCE_LOOKUPID = il.INSURANCE_LOOKUPID 
                                left JOIN BILL b1 on b1.APPOINTMENTTYPE = s.APPOINTMENTTYPEID 
                                                        and b1.patientregid=a.patientregid 
                                                        and b1.caseid=b.caseid
                                JOIN APPOINTMENTTYPE a1  on s.APPOINTMENTTYPEID = a1.APPOINTMENTTYPEID 
                                JOIN CASETYPE ct on ct.CASETYPEID=b.CASETYPEID 
                                LEFT JOIN ATTORNEYINFORMATIONLOOKUP atl on atl.ATTORNEY_LOOKUPID=b.ATTORNEY
                                WHERE 1=1  
                            
                            """
            
            if is_mrn_level == False:
                
                if provider_name:
                    
                    WHERE_INITIAL_QUERY = """ AND ( bi.businessname = '""" + str(provider_name) + """' or  bi.businessname like '%""" + str(provider_name) + """%' ) AND (  """
                    Final_DB_QUERY = INITIAL_QUERY + WHERE_INITIAL_QUERY 
                    
                else:
                    Final_DB_QUERY = ""
                
            else:
                WHERE_INITIAL_QUERY = """ AND (  """


                Final_DB_QUERY = INITIAL_QUERY + WHERE_INITIAL_QUERY
        except Exception as e:
            pass
        
        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and adj_number_list :
            for adjdata in adj_number_list:
                ADJ_NUMBER_UNIQUE = adjdata.replace(" ","")
                if ADJ_NUMBER_UNIQUE:
                    adjnumber = str(ADJ_NUMBER_UNIQUE.replace("#","").replace(":","").replace(";","").replace(".","").replace("*","").replace("-","").strip())
                    if len(adjnumber) > 3:
                        DB_QUERY = Final_DB_QUERY + " bi.active = 1  and  ( b.ADJNUMBER = '" + "" + str(adjnumber) +"' or  lower(b.ADJNUMBER) = '" + "" + str(adjnumber) +"' ) ) "
                        self.getdata(DB_QUERY)
        
        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and adj_number :
            adjnumber= str(adj_number.replace("#","").replace(":","").replace(";","").replace(".","").replace("*","").replace("-","").strip())
            if len(adjnumber) > 3:
                DB_QUERY = Final_DB_QUERY + " bi.active = 1  and  ( b.ADJNUMBER = '" + "" + str(adjnumber) +"' or  lower(b.ADJNUMBER) = '" + "" + str(adjnumber) +"' )  ) "
                self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and current_claim_number and current_lastname and current_firstname:
            dashed_claim_number = current_claim_number
            without_dashed_claim_number = current_claim_number.replace("-","")
            DB_QUERY = Final_DB_QUERY + " bi.active = 1  and i.CLAIM_NUMBER like '"+str(dashed_claim_number)+"%' and ( (  a.FIRSTNAME like '"+current_firstname+"%' and  a.LASTNAME like '"+current_lastname+"%'  ) or (  a.FIRSTNAME like '"+current_lastname+"%' and  a.LASTNAME like '"+current_firstname+"%'  ) ) or bi.active = 1  and i.CLAIM_NUMBER like '"+str(without_dashed_claim_number)+"%' and ( (  a.FIRSTNAME like '"+current_firstname+"%' and  a.LASTNAME like '"+current_lastname+"%'  ) or (  a.FIRSTNAME like '"+current_lastname+"%' and  a.LASTNAME like '"+current_firstname+"%'  ) ) )"
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and current_claim_number and doi_date:
            dashed_claim_number = current_claim_number
            without_dashed_claim_number = current_claim_number.replace("-","")
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and  ( i.CLAIM_NUMBER like '"+str(dashed_claim_number)+"%' or i.CLAIM_NUMBER like '"+str(without_dashed_claim_number)+"%' ) and (inj.DATE_OF_INJURY_START ='"+doi_date+"' or inj.DATE_OF_INJURY_END ='"+doi_date+"') ) "
            self.getdata(DB_QUERY)                    

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and current_claim_number and doi_date:
            dashed_claim_number = current_claim_number[:-4] if len(current_claim_number) >= 16 else current_claim_number
            without_dashed_claim_number = current_claim_number[:-4].replace("-","")  if len(current_claim_number) >= 16 else current_claim_number.replace("-","") 
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and  ( i.CLAIM_NUMBER like '"+str(dashed_claim_number)+"%' or i.CLAIM_NUMBER like '"+str(without_dashed_claim_number)+"%' ) and (inj.DATE_OF_INJURY_START ='"+doi_date+"' or inj.DATE_OF_INJURY_END ='"+doi_date+"') ) "
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and current_claim_number and dob_date:
            dashed_claim_number = current_claim_number
            without_dashed_claim_number = current_claim_number.replace("-","")
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and  ( i.CLAIM_NUMBER like '"+str(dashed_claim_number)+"%' or i.CLAIM_NUMBER like '"+str(without_dashed_claim_number)+"%' ) and a.DATEOFBIRTH ='"+dob_date+"' ) "
            self.getdata(DB_QUERY) 

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and current_claim_number and dob_date:

            dashed_claim_number = current_claim_number[:-4] if len(current_claim_number) >= 16 else current_claim_number
            without_dashed_claim_number = current_claim_number[:-4].replace("-","")  if len(current_claim_number) >= 16 else current_claim_number.replace("-","") 
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and  ( i.CLAIM_NUMBER like '"+str(dashed_claim_number)+"%' or i.CLAIM_NUMBER like '"+str(without_dashed_claim_number)+"%' ) and a.DATEOFBIRTH ='"+dob_date+"' ) "
            self.getdata(DB_QUERY) 

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and dob_date and dos_date:
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and  a.DATEOFBIRTH ='"+dob_date+"' and b1.DATEOFSERVICE ='"+dos_date+"'  )"
            self.getdata(DB_QUERY) 


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and dos_date and current_lastname and current_firstname:
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and b1.DATEOFSERVICE ='"+dos_date+"' and ( (  a.FIRSTNAME like '"+current_firstname+"%' and a.LASTNAME like '"+current_lastname+"%'  ) or (  a.FIRSTNAME like '"+current_lastname+"%' and  a.LASTNAME like '"+current_firstname+"%'  ) ) )"
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and current_claim_number and adj_number:
            dashed_claim_number = current_claim_number
            without_dashed_claim_number = current_claim_number.replace("-","")
            adjnumber = str(adj_number.replace("#","").replace(":","").replace(";","").replace(".","").replace("*","").replace("-","").strip())
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and i.CLAIM_NUMBER like '"+str(dashed_claim_number)+"%' and b.ADJNUMBER = '" + "" +str(adjnumber)+"' or bi.active = 1 and i.CLAIM_NUMBER like '"+str(without_dashed_claim_number)+"%' and b.ADJNUMBER = '" + "" +str(adjnumber)+"' ) "
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and current_claim_number and ssn_number and  not ( ssn_number =="999999999" ) and  not ( ssn_number =="9999" ):
            dashed_claim_number = current_claim_number
            without_dashed_claim_number = current_claim_number.replace("-","")
            ssnnumber = ssn_number.replace("-","")
            ssnnumber_last_four = ssn_number.replace("-","")[-4:]
            DB_QUERY = Final_DB_QUERY + " bi.active = 1  and ( a.SSN = '" + str(ssnnumber) +"' or a.SSN like '%"+str(ssnnumber_last_four)+"' )  and ( i.CLAIM_NUMBER like '"+str(dashed_claim_number)+"%' or i.CLAIM_NUMBER like '"+str(without_dashed_claim_number)+"%') ) "
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and adj_number and current_lastname and current_firstname:
            adjnumber= str(adj_number.replace("#","").replace(":","").replace(";","").replace(".","").replace("*","").replace("-","").strip())
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and b.ADJNUMBER = '" + "" +str(adjnumber)+"' and ( (  a.FIRSTNAME like '"+current_firstname+"%' and  a.LASTNAME like '"+current_lastname+"%'  ) or (  a.FIRSTNAME like '"+current_lastname+"%' and  a.LASTNAME like '"+current_firstname+"%'  ) ) ) "
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and dob_date and adj_number :
            adjnumber= str(adj_number.replace("#","").replace(":","").replace(";","").replace(".","").replace("*","").replace("-","").strip())
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and a.DATEOFBIRTH ='"+dob_date+"' and b.ADJNUMBER = '" + "" + str(adjnumber) +"' )"
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and doi_date and ssn_number and  not ( ssn_number =="999999999" ) and  not ( ssn_number =="9999" ):
            ssnnumber = ssn_number.replace("-","")
            ssnnumber_last_four = ssn_number.replace("-","")[-4:]
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and  (inj.DATE_OF_INJURY_START ='"+doi_date+"' or inj.DATE_OF_INJURY_END ='"+doi_date+"')  and ( a.SSN = '" + str(ssnnumber) +"' or a.SSN like '%"+str(ssnnumber_last_four)+"' )  )"
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and dob_date and ssn_number and  not ( ssn_number =="999999999" ) and  not ( ssn_number =="9999" ):

            ssnnumber = ssn_number.replace("-","")
            ssnnumber_last_four = ssn_number.replace("-","")[-4:]
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and  a.DATEOFBIRTH ='"+dob_date+"' and ( a.SSN = '" + str(ssnnumber) +"' or a.SSN like '%"+str(ssnnumber_last_four)+"' )  )"
            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and adj_number and doi_date:
            adjnumber= str(adj_number.replace("#","").replace(":","").replace(";","").replace(".","").replace("*","").replace("-","").strip())
            DB_QUERY = Final_DB_QUERY + " bi.active = 1  and b.ADJNUMBER = '" + "" + str(adjnumber) +"'   and (inj.DATE_OF_INJURY_START ='"+doi_date+"' or inj.DATE_OF_INJURY_END ='"+doi_date+"') ) "
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and dob_date and current_lastname and current_firstname:
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and a.DATEOFBIRTH ='"+dob_date+"' and ( (  a.FIRSTNAME like '"+current_firstname+"%' and  a.LASTNAME like '"+current_lastname+"%'  ) or (  a.FIRSTNAME like '"+current_lastname+"%' and  a.LASTNAME like '"+current_firstname+"%'  ) ) )"
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and ssn_number and  not ( ssn_number =="999999999" ) and  not ( ssn_number =="9999" ) and current_lastname and current_firstname:
            ssnnumber = ssn_number.replace("-","")
            ssnnumber_last_four = ssn_number.replace("-","")[-4:]
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and ( a.SSN = '" + str(ssnnumber) +"' or a.SSN like '%"+str(ssnnumber_last_four)+"' )  and ( (  a.FIRSTNAME like '"+current_firstname+"%' and  a.LASTNAME like '"+current_lastname+"%'  ) or (  a.FIRSTNAME like '"+current_lastname+"%' and  a.LASTNAME like '"+current_firstname+"%'  ) ) )"
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and doi_date and current_lastname and current_firstname:
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and inj.DATE_OF_INJURY_START ='"+doi_date+"' and ( (  a.FIRSTNAME like '"+current_firstname+"%' and  a.LASTNAME like '"+current_lastname+"%'  ) or (  a.FIRSTNAME like '"+current_lastname+"%' and  a.LASTNAME like '"+current_firstname+"%'  ) ) or bi.active = 1 and inj.DATE_OF_INJURY_END ='"+doi_date+"' and ( (  a.FIRSTNAME like '"+current_firstname+"%' and  a.LASTNAME like '"+current_lastname+"%'  ) or (  a.FIRSTNAME like '"+current_lastname+"%' and  a.LASTNAME like '"+current_firstname+"%'  ) )  )"
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and dob_date and doi_date:
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and  a.DATEOFBIRTH ='"+dob_date+"' and inj.DATE_OF_INJURY_START ='"+doi_date+"' or bi.active = 1 and  a.DATEOFBIRTH ='"+dob_date+"' and inj.DATE_OF_INJURY_END ='"+doi_date+"' )"
            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and ssn_number and  not ( ssn_number =="999999999" ) and  not ( ssn_number =="9999" ) :
            ssnnumber = ssn_number.replace("-","")
            DB_QUERY = Final_DB_QUERY + " bi.active = 1  and   a.SSN = '" + str(ssnnumber) +"' ) "
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and current_claim_number :
            dashed_claim_number = current_claim_number
            without_dashed_claim_number = current_claim_number.replace("-","")
            DB_QUERY = Final_DB_QUERY + " bi.active = 1  and  ( i.CLAIM_NUMBER like '"+str(dashed_claim_number)+"%' or i.CLAIM_NUMBER like '"+str(without_dashed_claim_number)+"%' )) "
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and current_claim_number :
            dashed_claim_number = current_claim_number.replace("-001","")
            without_dashed_claim_number = current_claim_number.replace("-001","").replace("-","")
            DB_QUERY = Final_DB_QUERY + " bi.active = 1  and  ( i.CLAIM_NUMBER like '"+str(dashed_claim_number)+"%' or i.CLAIM_NUMBER like '"+str(without_dashed_claim_number)+"%' )) "
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and current_claim_number :
            dashed_claim_number = current_claim_number.replace("-0001","")
            without_dashed_claim_number = current_claim_number.replace("-0001","").replace("-","")
            DB_QUERY = Final_DB_QUERY + " bi.active = 1  and  ( i.CLAIM_NUMBER like '"+str(dashed_claim_number)+"%' or i.CLAIM_NUMBER like '"+str(without_dashed_claim_number)+"%' )) "
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and doi_date and current_firstname and dos_date:
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and inj.DATE_OF_INJURY_START ='"+doi_date+"' and b1.DATEOFSERVICE ='"+dos_date+"' and ( (  a.FIRSTNAME like '"+current_firstname+"%'  ) or (  a.LASTNAME like '"+current_firstname+"%'  ) ) or bi.active = 1 and inj.DATE_OF_INJURY_END ='"+doi_date+"' and ( (  a.FIRSTNAME like '"+current_firstname+"%'  ) or (  a.LASTNAME like '"+current_firstname+"%'  ) )  )"
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and dob_date and current_lastname and current_firstname and external_mrn:
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and a.DATEOFBIRTH ='"+dob_date+"' and b.EXTERNALMRN_CASELEVEL='"+external_mrn+"' and ( (  a.FIRSTNAME like '"+current_firstname+"%' and  a.LASTNAME like '"+current_lastname+"%'  ) or (  a.FIRSTNAME like '"+current_lastname+"%' and  a.LASTNAME like '"+current_firstname+"%'  ) ) )"
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and current_claim_number and dos_date:
            dashed_claim_number = current_claim_number
            without_dashed_claim_number = current_claim_number.replace("-","")
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and  ( i.CLAIM_NUMBER like '"+str(dashed_claim_number)+"%' or i.CLAIM_NUMBER like '"+str(without_dashed_claim_number)+"%' ) and b1.DATEOFSERVICE ='"+dos_date+"' ) "
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and current_claim_number and dos_date:

            dashed_claim_number = current_claim_number[:-4] if len(current_claim_number) >= 16 else current_claim_number
            without_dashed_claim_number = current_claim_number[:-4].replace("-","")  if len(current_claim_number) >= 16 else current_claim_number.replace("-","") 

            if len(dashed_claim_number) > 3 or len(without_dashed_claim_number) > 3:

                DB_QUERY = Final_DB_QUERY + " bi.active = 1 and  ( i.CLAIM_NUMBER like '"+str(dashed_claim_number)+"%' or i.CLAIM_NUMBER like '"+str(without_dashed_claim_number)+"%' ) and b1.DATEOFSERVICE ='"+dos_date+"' ) "
                self.getdata(DB_QUERY)
#         if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and current_firstname and current_lastname:
#             DB_QUERY = Final_DB_QUERY + " bi.active = 1 and (a.FIRSTNAME like '" + current_firstname + "%' and a.LASTNAME like '" + current_lastname + "%') )"
#             self.getdata(DB_QUERY)
        
        
        return self.ALL_SERVERS_PATIENT_DATA
