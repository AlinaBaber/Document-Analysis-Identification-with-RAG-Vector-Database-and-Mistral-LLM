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
            if isinstance(adj_number, list):
                pass
            else:
                adjnumber= str(adj_number.replace("#","").replace(":","").replace(";","").replace(".","").replace("*","").replace("-","").strip())
                if len(adjnumber) > 3:
                    DB_QUERY = Final_DB_QUERY + " bi.active = 1  and  ( b.ADJNUMBER = '" + "" + str(adjnumber) +"' or  lower(b.ADJNUMBER) = '" + "" + str(adjnumber) +"' )  ) "
                    self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and current_claim_number and current_lastname and current_firstname:
            #Change
            if isinstance(current_claim_number, list):
                # Handle the case where current_claim_number is a list
                conditions = []
                current_claim_number2 = set(current_claim_number) if isinstance(current_claim_number, list) else {current_claim_number}
                for claim_number in current_claim_number2:
                    if claim_number:
                        dashed_claim_number = claim_number
                        without_dashed_claim_number = claim_number.replace("-", "")
                        condition = f"(bi.active = 1 and i.CLAIM_NUMBER like '{dashed_claim_number}%' and ((a.FIRSTNAME like '{current_firstname}%' and a.LASTNAME like '{current_lastname}%') or (a.FIRSTNAME like '{current_lastname}%' and a.LASTNAME like '{current_firstname}%')) or bi.active = 1 and i.CLAIM_NUMBER like '{without_dashed_claim_number}%' and ((a.FIRSTNAME like '{current_firstname}%' and a.LASTNAME like '{current_lastname}%') or (a.FIRSTNAME like '{current_lastname}%' and a.LASTNAME like '{current_firstname}%')))"
                        conditions.append(condition)
                DB_QUERY = Final_DB_QUERY + " ".join(conditions)
            
            else:
                dashed_claim_number = current_claim_number
                without_dashed_claim_number = current_claim_number.replace("-","")
                DB_QUERY = Final_DB_QUERY + " bi.active = 1  and i.CLAIM_NUMBER like '"+str(dashed_claim_number)+"%' and ( (  a.FIRSTNAME like '"+current_firstname+"%' and  a.LASTNAME like '"+current_lastname+"%'  ) or (  a.FIRSTNAME like '"+current_lastname+"%' and  a.LASTNAME like '"+current_firstname+"%'  ) ) or bi.active = 1  and i.CLAIM_NUMBER like '"+str(without_dashed_claim_number)+"%' and ( (  a.FIRSTNAME like '"+current_firstname+"%' and  a.LASTNAME like '"+current_lastname+"%'  ) or (  a.FIRSTNAME like '"+current_lastname+"%' and  a.LASTNAME like '"+current_firstname+"%'  ) ) )"
                self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and current_claim_number and doi_date:
            if isinstance(current_claim_number, list):
                claim_conditions = []
                current_claim_number2 = set(current_claim_number) if isinstance(current_claim_number, list) else {current_claim_number}
                for claim_number in current_claim_number2:
                    if claim_number:
                        dashed_claim_number = claim_number
                        without_dashed_claim_number = claim_number.replace("-", "")
                        claim_condition = f"(i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%')"
                        claim_conditions.append(claim_condition)
                claim_condition_str = " or ".join(claim_conditions)
            else:
                claim_condition_str = ""
                if isinstance(current_claim_number, str):
                    dashed_claim_number = current_claim_number
                    without_dashed_claim_number = current_claim_number.replace("-", "")
                    claim_condition_str = f"(i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%')"

            if isinstance(doi_date, list):
                date_conditions = []
                doi_date = set(doi_date) if isinstance(doi_date, list) else {doi_date}
                for date_value in doi_date:
                    date_condition = f"(inj.DATE_OF_INJURY_START = '{date_value}' or inj.DATE_OF_INJURY_END = '{date_value}')"
                    date_conditions.append(date_condition)
                date_condition_str = " or ".join(date_conditions)
            else:
                date_condition_str = f"(inj.DATE_OF_INJURY_START = '{doi_date}' or inj.DATE_OF_INJURY_END = '{doi_date}')"

            DB_QUERY = Final_DB_QUERY + f" bi.active = 1 and ({claim_condition_str}) and ({date_condition_str})"
            self.getdata(DB_QUERY)
              

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and current_claim_number and doi_date:
            if isinstance(current_claim_number, list):
                claim_conditions = []
                current_claim_number2 = set(current_claim_number) if isinstance(current_claim_number, list) else {current_claim_number}
                for claim_number in current_claim_number2:
                    if claim_number:
                        dashed_claim_number = claim_number[:-4] if len(claim_number) >= 16 else claim_number
                        without_dashed_claim_number = claim_number[:-4].replace("-", "") if len(claim_number) >= 16 else claim_number.replace("-", "")
                        claim_condition = f"(i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%')"
                        claim_conditions.append(claim_condition)
                claim_condition_str = " or ".join(claim_conditions)
            else:
                claim_condition_str = ""
                if isinstance(current_claim_number, str):
                    dashed_claim_number = current_claim_number[:-4] if len(current_claim_number) >= 16 else current_claim_number
                    without_dashed_claim_number = current_claim_number[:-4].replace("-", "") if len(current_claim_number) >= 16 else current_claim_number.replace("-", "")
                    claim_condition_str = f"(i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%')"

            if isinstance(doi_date, list):
                date_conditions = []
                doi_date2 = set(doi_date) if isinstance(doi_date, list) else {doi_date}
                for date_value in doi_date2:
                    date_condition = f"(inj.DATE_OF_INJURY_START = '{date_value}' or inj.DATE_OF_INJURY_END = '{date_value}')"
                    date_conditions.append(date_condition)
                date_condition_str = " or ".join(date_conditions)
            else:
                date_condition_str = f"(inj.DATE_OF_INJURY_START = '{doi_date}' or inj.DATE_OF_INJURY_END = '{doi_date}')"

            DB_QUERY = Final_DB_QUERY + f" bi.active = 1 and ({claim_condition_str}) and ({date_condition_str})"
            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and current_claim_number and dob_date:
            if isinstance(current_claim_number, list):
                claim_conditions = []
                current_claim_number2 = set(current_claim_number) if isinstance(current_claim_number, list) else {current_claim_number}
                for claim_number in current_claim_number2:
                    if claim_number:
                        dashed_claim_number = claim_number
                        without_dashed_claim_number = claim_number.replace("-", "")
                        claim_condition = f"(i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%')"
                        claim_conditions.append(claim_condition)
                claim_condition_str = " or ".join(claim_conditions)
            else:
                claim_condition_str = ""
                if isinstance(current_claim_number, str):
                    dashed_claim_number = current_claim_number
                    without_dashed_claim_number = current_claim_number.replace("-", "")
                    claim_condition_str = f"(i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%')"

            DB_QUERY = Final_DB_QUERY + f" bi.active = 1 and ({claim_condition_str}) and a.DATEOFBIRTH = '{dob_date}'"
            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and current_claim_number and dob_date:
            if isinstance(current_claim_number, list):
                claim_conditions = []
                current_claim_number2 = set(current_claim_number) if isinstance(current_claim_number, list) else {current_claim_number}
                for claim_number in current_claim_number2:
                    if claim_number:
                        dashed_claim_number = claim_number[:-4] if len(claim_number) >= 16 else claim_number
                        without_dashed_claim_number = claim_number[:-4].replace("-", "") if len(claim_number) >= 16 else claim_number.replace("-", "")
                        claim_condition = f"(i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%')"
                        claim_conditions.append(claim_condition)
                claim_condition_str = " or ".join(claim_conditions)
            else:
                claim_condition_str = ""
                if isinstance(current_claim_number, str):
                    dashed_claim_number = current_claim_number[:-4] if len(current_claim_number) >= 16 else current_claim_number
                    without_dashed_claim_number = current_claim_number[:-4].replace("-", "") if len(current_claim_number) >= 16 else current_claim_number.replace("-", "")
                    claim_condition_str = f"(i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%')"

            DB_QUERY = Final_DB_QUERY + f" bi.active = 1 and ({claim_condition_str}) and a.DATEOFBIRTH = '{dob_date}'"
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and dob_date and dos_date:
            DB_QUERY = Final_DB_QUERY + f" bi.active = 1 and a.DATEOFBIRTH = '{dob_date}'"

            if isinstance(dos_date, list):
                dos_conditions = []
                dos_date2 = set(dos_date) if isinstance(dos_date, list) else {dos_date}
                for date_value in dos_date2:
                    dos_condition = f"b1.DATEOFSERVICE = '{date_value}'"
                    dos_conditions.append(dos_condition)
                dos_condition_str = " or ".join(dos_conditions)

                DB_QUERY += f" and ({dos_condition_str})"
            else:
                DB_QUERY += f" and b1.DATEOFSERVICE = '{dos_date}'"

            self.getdata(DB_QUERY)



        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and dos_date and current_lastname and current_firstname:
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and ("

            if isinstance(dos_date, list):
                dos_conditions = []
                dos_date2 = set(dos_date) if isinstance(dos_date, list) else {dos_date}
                for date_value in dos_date2:
                    dos_condition = f"b1.DATEOFSERVICE = '{date_value}'"
                    dos_conditions.append(dos_condition)
                dos_condition_str = " or ".join(dos_conditions)
                DB_QUERY += f"({dos_condition_str})"
            else:
                DB_QUERY += f"b1.DATEOFSERVICE = '{dos_date}'"

            name_conditions = [
                f"(a.FIRSTNAME like '{current_firstname}%' and a.LASTNAME like '{current_lastname}%')",
                f"(a.FIRSTNAME like '{current_lastname}%' and a.LASTNAME like '{current_firstname}%')"
            ]
            name_condition_str = " and ".join(name_conditions)

            DB_QUERY += f" and ({name_condition_str}))"

            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and current_claim_number and adj_number:
            if isinstance(current_claim_number, list):
                claim_conditions = []
                current_claim_number2 = set(current_claim_number) if isinstance(current_claim_number, list) else {current_claim_number}
                for claim_number in current_claim_number2:
                    if claim_number:
                        dashed_claim_number = claim_number
                        without_dashed_claim_number = claim_number.replace("-", "")
                        condition = f"(bi.active = 1 and i.CLAIM_NUMBER like '{dashed_claim_number}%' and b.ADJNUMBER = '{adj_number}' or bi.active = 1 and i.CLAIM_NUMBER like '{without_dashed_claim_number}%' and b.ADJNUMBER = '{adj_number}')"
                        claim_conditions.append(condition)

                claim_condition_str = " or ".join(claim_conditions)
            else:
                claim_condition_str = ""
                if isinstance(current_claim_number, str):
                    dashed_claim_number = current_claim_number
                    without_dashed_claim_number = current_claim_number.replace("-", "")
                    adj_number_cleaned = str(adj_number.replace("#", "").replace(":", "").replace(";", "").replace(".", "").replace("*", "").replace("-", "").strip())

                    claim_condition_str = f"(bi.active = 1 and i.CLAIM_NUMBER like '{dashed_claim_number}%' and b.ADJNUMBER = '{adj_number_cleaned}' or bi.active = 1 and i.CLAIM_NUMBER like '{without_dashed_claim_number}%' and b.ADJNUMBER = '{adj_number_cleaned}')"

                DB_QUERY = Final_DB_QUERY + f" {claim_condition_str}"
                self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and current_claim_number and ssn_number and not (ssn_number == "999999999") and not (ssn_number == "9999"):
            if isinstance(current_claim_number, list):
                claim_conditions = []
                current_claim_number2 = set(current_claim_number) if isinstance(current_claim_number, list) else {current_claim_number}
                for claim_number in current_claim_number2:
                    if claim_number:
                        dashed_claim_number = claim_number
                        without_dashed_claim_number = claim_number.replace("-", "")
                        ssnnumber = ssn_number.replace("-", "")
                        ssnnumber_last_four = ssn_number.replace("-", "")[-4:]

                        condition = f"(bi.active = 1 and (a.SSN = '{ssnnumber}' or a.SSN like '%{ssnnumber_last_four}') and (i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%'))"
                        claim_conditions.append(condition)

                claim_condition_str = " or ".join(claim_conditions)
            else:
                claim_condition_str = ""
                if isinstance(current_claim_number, str):
                    dashed_claim_number = current_claim_number
                    without_dashed_claim_number = current_claim_number.replace("-", "")
                    ssnnumber = ssn_number.replace("-", "")
                    ssnnumber_last_four = ssn_number.replace("-", "")[-4:]

                    claim_condition_str = f"(bi.active = 1 and (a.SSN = '{ssnnumber}' or a.SSN like '%{ssnnumber_last_four}') and (i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%'))"

            DB_QUERY = Final_DB_QUERY + f" {claim_condition_str}"
            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and adj_number and current_lastname and current_firstname:
            if isinstance(adj_number, list):
                adj_conditions = []
                adj_number2 = set(adj_number) if isinstance(adj_number, list) else {adj_number}
                for number in adj_number2:
                    adjnumber = str(number.replace("#", "").replace(":", "").replace(";", "").replace(".", "").replace("*", "").replace("-", "").strip())
                    name_conditions = [
                        f"(a.FIRSTNAME like '{current_firstname}%' and a.LASTNAME like '{current_lastname}%')",
                        f"(a.FIRSTNAME like '{current_lastname}%' and a.LASTNAME like '{current_firstname}%')"
                    ]
                    name_condition_str = " or ".join(name_conditions)

                    adj_condition = f"(bi.active = 1 and b.ADJNUMBER = '{adjnumber}' and ({name_condition_str}))"
                    adj_conditions.append(adj_condition)

                adj_condition_str = " or ".join(adj_conditions)
            else:
                adjnumber = str(adj_number.replace("#", "").replace(":", "").replace(";", "").replace(".", "").replace("*", "").replace("-", "").strip())
                name_conditions = [
                    f"(a.FIRSTNAME like '{current_firstname}%' and a.LASTNAME like '{current_lastname}%')",
                    f"(a.FIRSTNAME like '{current_lastname}%' and a.LASTNAME like '{current_firstname}%')"
                ]
                name_condition_str = " or ".join(name_conditions)

                adj_condition_str = f"(bi.active = 1 and b.ADJNUMBER = '{adjnumber}' and ({name_condition_str}))"

            DB_QUERY = Final_DB_QUERY + f" {adj_condition_str}"
            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and dob_date and adj_number :
            adjnumber= str(adj_number.replace("#","").replace(":","").replace(";","").replace(".","").replace("*","").replace("-","").strip())
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and a.DATEOFBIRTH ='"+dob_date+"' and b.ADJNUMBER = '" + "" + str(adjnumber) +"' )"
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and doi_date and ssn_number and not (ssn_number == "999999999") and not (ssn_number == "9999"):
            ssnnumber = ssn_number.replace("-", "")
            ssnnumber_last_four = ssn_number.replace("-", "")[-4:]

            if isinstance(doi_date, list):
                doi_conditions = []
                doi_date2 = set(doi_date) if isinstance(doi_date, list) else {doi_date}
                for date_value in doi_date2:
                    doi_condition = f"(inj.DATE_OF_INJURY_START = '{date_value}' or inj.DATE_OF_INJURY_END = '{date_value}')"
                    doi_conditions.append(doi_condition)
                doi_condition_str = " or ".join(doi_conditions)
            else:
                doi_condition_str = f"(inj.DATE_OF_INJURY_START = '{doi_date}' or inj.DATE_OF_INJURY_END = '{doi_date}')"

            DB_QUERY = Final_DB_QUERY + f" bi.active = 1 and ({doi_condition_str}) and (a.SSN = '{ssnnumber}' or a.SSN like '%{ssnnumber_last_four}')"
            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and dob_date and ssn_number and not (ssn_number == "999999999") and not (ssn_number == "9999"):
            dob_condition = f"a.DATEOFBIRTH = '{dob_date}'"

            if isinstance(ssn_number, list):
                ssn_conditions = []
                ssn_number2 = set(ssn_number) if isinstance(ssn_number, list) else {ssn_number}
                for ssn_value in ssn_number2:
                    ssn_value = ssn_value.replace("-", "")
                    ssn_last_four = ssn_value[-4:]
                    ssn_condition = f"(a.SSN = '{ssn_value}' or a.SSN like '%{ssn_last_four}')"
                    ssn_conditions.append(ssn_condition)
                ssn_condition_str = " or ".join(ssn_conditions)
            else:
                ssn_value = ssn_number.replace("-", "")
                ssn_last_four = ssn_value[-4:]
                ssn_condition_str = f"(a.SSN = '{ssn_value}' or a.SSN like '%{ssn_last_four}')"

            DB_QUERY = Final_DB_QUERY + f" bi.active = 1 and {dob_condition} and ({ssn_condition_str})"
            self.getdata(DB_QUERY)



        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and adj_number and doi_date:
            if isinstance(doi_date, list):
                doi_conditions = []
                doi_date2 = set(doi_date) if isinstance(doi_date, list) else {doi_date}
                for date_value in doi_date2:
                    doi_condition = f"(inj.DATE_OF_INJURY_START = '{date_value}' or inj.DATE_OF_INJURY_END = '{date_value}')"
                    doi_conditions.append(doi_condition)
                doi_condition_str = " or ".join(doi_conditions)
            else:
                doi_condition_str = f"(inj.DATE_OF_INJURY_START = '{doi_date}' or inj.DATE_OF_INJURY_END = '{doi_date}')"

            DB_QUERY = Final_DB_QUERY + f" bi.active = 1 and b.ADJNUMBER = '{adjnumber}' and ({doi_condition_str})"
            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0  and dob_date and current_lastname and current_firstname:
            DB_QUERY = Final_DB_QUERY + " bi.active = 1 and a.DATEOFBIRTH ='"+dob_date+"' and ( (  a.FIRSTNAME like '"+current_firstname+"%' and  a.LASTNAME like '"+current_lastname+"%'  ) or (  a.FIRSTNAME like '"+current_lastname+"%' and  a.LASTNAME like '"+current_firstname+"%'  ) ) )"
            self.getdata(DB_QUERY)

        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and ssn_number and not (ssn_number == "999999999") and not (ssn_number == "9999") and current_lastname and current_firstname:
            if isinstance(ssn_number, list):
                ssn_conditions = []
                ssn_number2 = set(ssn_number) if isinstance(ssn_number, list) else {ssn_number}
                for ssn_value in ssn_number2:
                    ssn_value = ssn_value.replace("-", "")
                    ssn_last_four = ssn_value[-4:]
                    name_conditions = [
                        f"(a.FIRSTNAME like '{current_firstname}%' and a.LASTNAME like '{current_lastname}%')",
                        f"(a.FIRSTNAME like '{current_lastname}%' and a.LASTNAME like '{current_firstname}%')"
                    ]
                    name_condition_str = " or ".join(name_conditions)

                    ssn_condition = f"(bi.active = 1 and (a.SSN = '{ssn_value}' or a.SSN like '%{ssn_last_four}') and ({name_condition_str}))"
                    ssn_conditions.append(ssn_condition)

                ssn_condition_str = " or ".join(ssn_conditions)
            else:
                ssn_condition_str = ""
                if isinstance(ssn_number, str):
                    ssn_number = ssn_number.replace("-", "")
                    ssn_last_four = ssn_number[-4:]
                    name_conditions = [
                        f"(a.FIRSTNAME like '{current_firstname}%' and a.LASTNAME like '{current_lastname}%')",
                        f"(a.FIRSTNAME like '{current_lastname}%' and a.LASTNAME like '{current_firstname}%')"
                    ]
                    name_condition_str = " or ".join(name_conditions)

                    ssn_condition_str = f"(bi.active = 1 and (a.SSN = '{ssn_number}' or a.SSN like '%{ssn_last_four}') and ({name_condition_str}))"

            DB_QUERY = Final_DB_QUERY + f" {ssn_condition_str}"
            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and doi_date and current_lastname and current_firstname:
            name_conditions = [
                f"(a.FIRSTNAME like '{current_firstname}%' and a.LASTNAME like '{current_lastname}%')",
                f"(a.FIRSTNAME like '{current_lastname}%' and a.LASTNAME like '{current_firstname}%')"
            ]
            name_condition_str = " or ".join(name_conditions)

            if isinstance(doi_date, list):
                doi_conditions = []
                doi_date2 = set(doi_date) if isinstance(doi_date, list) else {doi_date}
                for date_value in doi_date2:
                    doi_condition = f"(bi.active = 1 and (inj.DATE_OF_INJURY_START = '{date_value}' and ({name_condition_str}) or inj.DATE_OF_INJURY_END = '{date_value}' and ({name_condition_str})))"
                    doi_conditions.append(doi_condition)
                doi_condition_str = " or ".join(doi_conditions)
            else:
                doi_condition_str = f"(bi.active = 1 and (inj.DATE_OF_INJURY_START = '{doi_date}' and ({name_condition_str}) or inj.DATE_OF_INJURY_END = '{doi_date}' and ({name_condition_str})))"

            DB_QUERY = Final_DB_QUERY + f" {doi_condition_str}"
            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and dob_date and doi_date:
            dob_condition = f"a.DATEOFBIRTH = '{dob_date}'"

            if isinstance(doi_date, list):
                doi_conditions = []
                doi_date2 = set(doi_date) if isinstance(doi_date, list) else {doi_date}
                for date_value in doi_date2:
                    doi_condition = f"(bi.active = 1 and (inj.DATE_OF_INJURY_START = '{date_value}' or inj.DATE_OF_INJURY_END = '{date_value}') and {dob_condition})"
                    doi_conditions.append(doi_condition)
                doi_condition_str = " or ".join(doi_conditions)
            else:
                doi_condition_str = f"(bi.active = 1 and (inj.DATE_OF_INJURY_START = '{doi_date}' or inj.DATE_OF_INJURY_END = '{doi_date}') and {dob_condition})"

            DB_QUERY = Final_DB_QUERY + f" {doi_condition_str}"
            self.getdata(DB_QUERY)



        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and ssn_number and not (ssn_number == "999999999") and not (ssn_number == "9999"):
            if isinstance(ssn_number, list):
                ssn_conditions = []
                ssn_number2 = set(ssn_number) if isinstance(ssn_number, list) else {ssn_number}
                for ssn_value in ssn_number2:
                    ssn_value = ssn_value.replace("-", "")
                    ssn_condition = f"(bi.active = 1 and a.SSN = '{ssn_value}')"
                    ssn_conditions.append(ssn_condition)
                ssn_condition_str = " or ".join(ssn_conditions)
            else:
                ssn_number = ssn_number.replace("-", "")
                ssn_condition_str = f"(bi.active = 1 and a.SSN = '{ssn_number}')"

            DB_QUERY = Final_DB_QUERY + f" {ssn_condition_str}"
            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and current_claim_number:
            if isinstance(current_claim_number, list):
                claim_conditions = []
                current_claim_number2 = set(current_claim_number) if isinstance(current_claim_number, list) else {current_claim_number}
                for claim_value in current_claim_number2:
                    if claim_value:
                        dashed_claim_number = claim_value
                        without_dashed_claim_number = claim_value.replace("-", "")
                        claim_condition = f"(bi.active = 1 and (i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%'))"
                        claim_conditions.append(claim_condition)
                claim_condition_str = " or ".join(claim_conditions)
            else:
                claim_condition_str = ""
                if isinstance(current_claim_number, str):
                    dashed_claim_number = current_claim_number
                    without_dashed_claim_number = current_claim_number.replace("-", "")
                    claim_condition_str = f"(bi.active = 1 and (i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%'))"

            DB_QUERY = Final_DB_QUERY + f" {claim_condition_str}"
            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and current_claim_number:
            if isinstance(current_claim_number, list):
                claim_conditions = []
                current_claim_number2= set(current_claim_number) if isinstance(current_claim_number, list) else {current_claim_number}
                for claim_value in current_claim_number2:
                    if claim_value:
                        dashed_claim_number = claim_value.replace("-001", "")
                        without_dashed_claim_number = claim_value.replace("-001", "").replace("-", "")
                        claim_condition = f"(bi.active = 1 and (i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%'))"
                        claim_conditions.append(claim_condition)
                claim_condition_str = " or ".join(claim_conditions)
            else:
                claim_condition_str = ""
                if isinstance(current_claim_number, str):
                    dashed_claim_number = current_claim_number.replace("-001", "")
                    without_dashed_claim_number = current_claim_number.replace("-001", "").replace("-", "")
                    claim_condition_str = f"(bi.active = 1 and (i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%'))"

            DB_QUERY = Final_DB_QUERY + f" {claim_condition_str}"
            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and current_claim_number:
            if isinstance(current_claim_number, list):
                current_claim_number2 = set(current_claim_number) if isinstance(current_claim_number, list) else {current_claim_number}
                claim_conditions = []
                for claim_value in current_claim_number2:
                    if claim_value:
                        dashed_claim_number = claim_value.replace("-0001", "")
                        without_dashed_claim_number = claim_value.replace("-0001", "").replace("-", "")
                        claim_condition = f"(bi.active = 1 and (i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%'))"
                        claim_conditions.append(claim_condition)
                claim_condition_str = " or ".join(claim_conditions)
            else:
                claim_condition_str = ""
                if isinstance(current_claim_number, str):
                    dashed_claim_number = current_claim_number.replace("-0001", "")
                    without_dashed_claim_number = current_claim_number.replace("-0001", "").replace("-", "")
                    claim_condition_str = f"(bi.active = 1 and (i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%'))"

            DB_QUERY = Final_DB_QUERY + f" {claim_condition_str}"
            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and doi_date and current_firstname and dos_date:
            name_conditions = [
                f"(a.FIRSTNAME like '{current_firstname}%')",
                f"(a.LASTNAME like '{current_firstname}%')"
            ]
            name_condition_str = " or ".join(name_conditions)

            if isinstance(doi_date, list):
                doi_conditions = []
                doi_date2 = set(doi_date) if isinstance(doi_date, list) else {doi_date}
                for date_value in doi_date2:
                    if isinstance(dos_date, list):
                        dos_conditions = []
                        for dos_value in dos_date:
                            doi_condition = f"(bi.active = 1 and inj.DATE_OF_INJURY_START = '{date_value}' and b1.DATEOFSERVICE = '{dos_value}' and ({name_condition_str}))"
                            dos_conditions.append(doi_condition)
                        dos_condition_str = " or ".join(dos_conditions)
                    else:
                        dos_condition_str = f"(bi.active = 1 and inj.DATE_OF_INJURY_START = '{date_value}' and b1.DATEOFSERVICE = '{dos_date}' and ({name_condition_str}))"
                    doi_conditions.append(dos_condition_str)
            else:
                if isinstance(dos_date, list):
                    dos_conditions = []
                    dos_date2= set(dos_date) if isinstance(dos_date, list) else {dos_date}
                    for dos_value in dos_date2:
                        dos_condition_str = f"(bi.active = 1 and inj.DATE_OF_INJURY_START = '{doi_date}' and b1.DATEOFSERVICE = '{dos_value}' and ({name_condition_str}))"
                        dos_conditions.append(dos_condition_str)
                    dos_condition_str = " or ".join(dos_conditions)
                else:
                    dos_conditions = []
                    dos_condition_str = f"(bi.active = 1 and inj.DATE_OF_INJURY_START = '{doi_date}' and b1.DATEOFSERVICE = '{dos_date}' and ({name_condition_str}))"
                dos_conditions.append(dos_condition_str)

            dos_condition_str = " or ".join(dos_conditions)

            DB_QUERY = Final_DB_QUERY + f" {dos_condition_str}"
            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and dob_date and current_lastname and current_firstname and external_mrn:
            name_conditions = [
                f"(a.FIRSTNAME like '{current_firstname}%')",
                f"(a.LASTNAME like '{current_lastname}%')"
            ]
            name_condition_str = " or ".join(name_conditions)

            if isinstance(dob_date, list):
                dob_conditions = []
                dob_date2= set(dob_date) if isinstance(dob_date, list) else {dob_date}
                for dob_value in dob_date2:
                    if isinstance(external_mrn, list):
                        external_mrn_conditions = []
                        for mrn_value in external_mrn:
                            dob_condition = f"(bi.active = 1 and a.DATEOFBIRTH = '{dob_value}' and b.EXTERNALMRN_CASELEVEL = '{mrn_value}' and ({name_condition_str}))"
                            external_mrn_conditions.append(dob_condition)
                        external_mrn_condition_str = " or ".join(external_mrn_conditions)
                    else:
                        external_mrn_condition_str = f"(bi.active = 1 and a.DATEOFBIRTH = '{dob_value}' and b.EXTERNALMRN_CASELEVEL = '{external_mrn}' and ({name_condition_str}))"
                    dob_conditions.append(external_mrn_condition_str)
            else:
                if isinstance(external_mrn, list):
                    external_mrn_conditions = []
                    external_mrn2 = set(external_mrn) if isinstance(external_mrn, list) else {external_mrn}
                    for mrn_value in external_mrn2:
                        external_mrn_condition_str = f"(bi.active = 1 and a.DATEOFBIRTH = '{dob_date}' and b.EXTERNALMRN_CASELEVEL = '{mrn_value}' and ({name_condition_str}))"
                        external_mrn_conditions.append(external_mrn_condition_str)
                    external_mrn_condition_str = " or ".join(external_mrn_conditions)
                else:
                    external_mrn_condition_str = f"(bi.active = 1 and a.DATEOFBIRTH = '{dob_date}' and b.EXTERNALMRN_CASELEVEL = '{external_mrn}' and ({name_condition_str}))"
                dob_conditions.append(external_mrn_condition_str)

            dob_condition_str = " or ".join(dob_conditions)

            DB_QUERY = Final_DB_QUERY + f" {dob_condition_str}"
            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and current_claim_number and dos_date:
            if isinstance(current_claim_number, list):
                claim_conditions = []
                current_claim_number2 = set(current_claim_number) if isinstance(current_claim_number, list) else {current_claim_number}
                for claim_value in current_claim_number2:
                    if claim_value:
                        if isinstance(dos_date, list):
                            dos_conditions = []
                            dos_date2 = set(dos_date) if isinstance(dos_date, list) else {dos_date}
                            for dos_value in dos_date2:
                                dashed_claim_number = claim_value
                                without_dashed_claim_number = claim_value.replace("-", "")
                                claim_condition = f"(bi.active = 1 and (i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%') and b1.DATEOFSERVICE = '{dos_value}')"
                                dos_conditions.append(claim_condition)
                            dos_condition_str = " or ".join(dos_conditions)
                        else:
                            dashed_claim_number = claim_value
                            without_dashed_claim_number = claim_value.replace("-", "")
                            dos_condition_str = f"(bi.active = 1 and (i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%') and b1.DATEOFSERVICE = '{dos_date}')"
                        claim_conditions.append(dos_condition_str)
                claim_condition_str = " or ".join(claim_conditions)
            else:
                if isinstance(dos_date, list):
                    dos_conditions = []
                    dos_date2 = set(dos_date) if isinstance(dos_date, list) else {dos_date}
                    for dos_value in dos_date2:
                        dashed_claim_number = current_claim_number
                        without_dashed_claim_number = current_claim_number.replace("-", "")
                        dos_condition_str = f"(bi.active = 1 and (i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%') and b1.DATEOFSERVICE = '{dos_value}')"
                        dos_conditions.append(dos_condition_str)
                    claim_condition_str = " or ".join(dos_conditions)
                else:
                    dashed_claim_number = current_claim_number
                    without_dashed_claim_number = current_claim_number.replace("-", "")
                    claim_condition_str = f"(bi.active = 1 and (i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%') and b1.DATEOFSERVICE = '{dos_date}')"

            DB_QUERY = Final_DB_QUERY + f" {claim_condition_str}"
            self.getdata(DB_QUERY)


        if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and current_claim_number and dos_date:
            if isinstance(current_claim_number, list):
                current_claim_number2 = set(current_claim_number) if isinstance(current_claim_number, list) else {current_claim_number}
                claim_conditions = []
                for claim_value in current_claim_number2:
                    if claim_value:
                        if len(claim_value) >= 16:
                            dashed_claim_number = claim_value[:-4]
                            without_dashed_claim_number = claim_value[:-4].replace("-", "")
                        else:
                            dashed_claim_number = claim_value
                            without_dashed_claim_number = claim_value.replace("-", "")

                        if len(dashed_claim_number) > 3 or len(without_dashed_claim_number) > 3:
                            claim_condition = f"(bi.active = 1 and (i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%') and b1.DATEOFSERVICE = '{dos_date}')"
                            claim_conditions.append(claim_condition)

                claim_condition_str = " or ".join(claim_conditions)
            else:
                if len(current_claim_number) >= 16:
                    dashed_claim_number = current_claim_number[:-4]
                    without_dashed_claim_number = current_claim_number[:-4].replace("-", "")
                else:
                    dashed_claim_number = current_claim_number
                    without_dashed_claim_number = current_claim_number.replace("-", "")

                if len(dashed_claim_number) > 3 or len(without_dashed_claim_number) > 3:
                    claim_condition_str = f"(bi.active = 1 and (i.CLAIM_NUMBER like '{dashed_claim_number}%' or i.CLAIM_NUMBER like '{without_dashed_claim_number}%') and b1.DATEOFSERVICE = '{dos_date}')"
                else:
                    claim_condition_str = ""

            DB_QUERY = Final_DB_QUERY + f" {claim_condition_str}"
            self.getdata(DB_QUERY)

#         if len(self.ALL_SERVERS_PATIENT_DATA) == 0 and current_firstname and current_lastname:
#             DB_QUERY = Final_DB_QUERY + " bi.active = 1 and (a.FIRSTNAME like '" + current_firstname + "%' and a.LASTNAME like '" + current_lastname + "%') )"
#             self.getdata(DB_QUERY)
        
        
        return self.ALL_SERVERS_PATIENT_DATA