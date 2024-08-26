import snowflake.connector
from sqlalchemy import create_engine
import pymssql
import pandas as pd
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key,load_der_private_key
import os
import logging
from datetime import datetime, timedelta, timezone,date,time
from dateutil.relativedelta import relativedelta
import openpyxl
from openpyxl import load_workbook
import io
from io import BytesIO
import requests
import base64

#Config logging
script_dir = os.path.dirname(os.path.realpath(__file__))
logging.basicConfig(
    filename=os.path.join(script_dir,'logs.log'),
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
try:
    #Load Secrets
    sql_server_ip = os.getenv('generations_sql_server_ip')
    db = os.getenv('generations_db_name')
    username = os.getenv('generations_sql_user')
    passw = os.getenv('generations_sql_pass')
    table = os.getenv('generations_sql_table')
    private_key_path = r'' + os.getenv('pem_key_path')
    pem_pass = os.getenv('pem_pass')
    snow_account = os.getenv('snowflake_account')
    snow_ft_db = os.getenv('snowflake_fivetran_db')
    snow_max_db = os.getenv('snowflake_maxio_db')
    snow_etl_role = os.getenv('snowflake_etl_role')
    snow_etl_user = os.getenv('snowflake_etl_user')
    snow_etl_wh = os.getenv('snow_etl_wh')
    snow_final_table = os.getenv('generations_snow_table')
    graph_secret = os.getenv('graph_secret')
    graph_client = os.getenv('graph_client')
    graph_tenant = os.getenv('graph_tenant')

    #Dynamically handle date in query
    last_month = datetime.now() - relativedelta(months=1)
    date = format(last_month, '%b-%y')

    exp = 'SQLEXPRESS'

    server_address = f'{sql_server_ip}\\{exp}'

    #Fetch generations usage data from SQL server
    with pymssql.connect(server=server_address, user=username, password=passw, database=db) as conn:

        cursor = conn.cursor()
        cursor.execute(f"select * from [{db}].[dbo].[{table}] where monthyear like '{date}'")
        result = cursor.fetchall()
        column_headers = [description[0] for description in cursor.description]
        result_with_headers = [column_headers] + result

    raw_df = pd.DataFrame(result, columns=column_headers)

    #Encode private key pass
    password = pem_pass.encode()

    #Read in private key
    with open(private_key_path, 'rb') as key_file:
        key_data = key_file.read()

    #Load the private key as PEM
    private_key = load_pem_private_key(key_data, password=password)

    #Extract the private key bytes in PKCS8 format
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    #Init snowflake session to fetch Salesforce ID
    ctx = snowflake.connector.connect(
        user=snow_etl_user,
        account=snow_account,
        private_key=private_key_bytes,
        role=snow_etl_role,
        warehouse=snow_etl_wh)

    cs = ctx.cursor()

    #Fetch details from SFDC
    script = """
    select
    id,
    legacy_id_c,
    name
    from PC_FIVETRAN_DB.SALESFORCE.ACCOUNT
    where is_deleted = false and legacy_id_c is not null
    """
    payload = cs.execute(script)
    sfdc = pd.DataFrame.from_records(iter(payload), columns=[x[0] for x in payload.description])

    #Merge SQL and Snowflake results
    merged_df = raw_df.merge(sfdc, left_on='VendorID',right_on='LEGACY_ID_C',how='left')

    merged_df.rename(columns={'ID':'HHAXUNIQUEID','NAME':'VENDORNAME','Quantity':'VALUE','VendorID':'VENDORID'},inplace=True)

    merged_df.drop(columns=['LEGACY_ID_C','id'],inplace=True)

    import_file = merged_df

    backup_file = import_file

    #Add environment code
    merged_df['ENVIRONMENTCODE'] = 'GEN'

    #Build import file and backup email file
    import_file = merged_df

    email_backup = import_file

    col_order_backup =  ['VENDORID','ENVIRONMENTCODE','VENDORNAME','HHAXUNIQUEID','COMPONENT','VALUE','Total','MONTHYEAR']
    col_order_import = ['VENDORID','ENVIRONMENTCODE','VENDORNAME','HHAXUNIQUEID','COMPONENT','VALUE','MONTHYEAR']

    int_import_file = import_file.drop(columns=['CostPer', 'Total'])
    final_import_file = int_import_file[col_order_import]

    final_backup_file = backup_file[col_order_backup]

    #Connection string for snowflake import
    connection_string = f"snowflake://{snow_etl_user}@{snow_account}/{snow_max_db}/PUBLIC?warehouse={snow_etl_wh}&role={snow_etl_role}&authenticator=externalbrowser"

    #Instantiate SQLAlchemy engine with the private key
    engine = create_engine(
        connection_string,
        connect_args={
            "private_key": private_key_bytes})

    chunk_size = 10000
    chunks = [x for x in range(0, len(int_import_file), chunk_size)] + [len(int_import_file)]
    table_name = 'GEN_MONTHLY_BILLING' 

    #Load import file
    for i in range(len(chunks) - 1):
        int_import_file[chunks[i]:chunks[i + 1]].to_sql(table_name, engine, if_exists='append', index=False)

    excel_buffer = io.BytesIO()

    csv_mappings = {
        'Usage Backup': final_backup_file
    }

    #Write the pandas dataframe backup file to a single excel file, store in memory
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as excel_writer:
        for sheet_name, dataframe in csv_mappings.items():
                dataframe.to_excel(excel_writer, sheet_name=sheet_name, index=False, header=True)

    excel_buffer.seek(0)

    #Encode excel file in base64
    attachment_base64 = base64.b64encode(excel_buffer.read()).decode('utf-8')

    #Load from and to email addresses for backup email
    with open(r'C:\Users\mdunlap\Desktop\Generations Monthly Usage\from.txt', 'r') as from_email_wrapper:
        from_email_temp = from_email_wrapper.read().replace('\n', '')

    email_from = from_email_temp

    with open(r'C:\Users\mdunlap\Desktop\Generations Monthly Usage\to.txt', 'r') as to_email_wrapper:
        to_email_temp = to_email_wrapper.read().replace('\n', '')

    email_to = to_email_temp

    client_id = graph_client
    client_secret = graph_secret
    tenant_id = graph_tenant

    #Build the email automation for the backup file
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://graph.microsoft.com/.default'
    }
    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    access_token = response.json().get('access_token')

    from_email = email_from
    to_email = [email_to]
    subject = 'Generations Usage Backup File' + ' '+ '-' +' '+ date
    body = 'Generations Usage Backup File' + ' '+ '-' + ' '+ date

    email_recipients = [{"emailAddress": {"address": email}} for email in to_email]

    attachment = {
        '@odata.type': '#microsoft.graph.fileAttachment',
        'name': f'Generations Usage - {date}.xlsx',
        'contentType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'contentBytes': attachment_base64
    }

    send_mail_url = f'https://graph.microsoft.com/v1.0/users/{from_email}/sendMail'
    email_msg = {
        'message': {
            'subject': subject,
            'body': {
                'contentType': "Text",
                'content': body
            },
            'toRecipients': email_recipients,
            'attachments': [attachment]
        }
    }

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    response = requests.post(send_mail_url, headers=headers, json=email_msg)
    response.raise_for_status()

    logging.getLogger().setLevel(logging.INFO)
    logging.info('Success')

except Exception as e:
    logging.exception('Operation failed due to an error')
logging.getLogger().setLevel(logging.ERROR)
