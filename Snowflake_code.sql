CREATE OR REPLACE TABLE ELIGIBLE_USER(
"global_identifier" STRING,
"user_id" INTEGER,
"date_created" TIMESTAMP,
"date_updated" TIMESTAMP,
"display" STRING
);

INSERT INTO ELIGIBLE_USER VALUES ('473181443564354',1639129,'2025-06-14 6:01:06','2025-06-15 6:01:06', '1872'),
                                 ('573181443564354',2618129,'2025-06-14 6:01:06','2025-06-15 6:01:06', '2872'),
                                 ('173181443564354',1139129,'2025-06-14 6:01:06','2025-06-15 6:01:06', '4872'),
                                 ('273181443564354',1239129,'2025-06-14 6:01:06','2025-06-15 6:01:06', '2872'),
                                 ('373181443564354',1339129,'2025-06-14 6:01:06','2025-06-15 6:01:06', '1272'),
                                 ('413181443564354',1439129,'2025-06-14 6:01:06','2025-06-15 6:01:06', '1372'),
                                 ('423181443564354',1539129,'2025-06-14 6:01:06','2025-06-15 6:01:06', '1472'),
                                 ('433181443564354',1839129,'2025-06-14 6:01:06','2025-06-15 6:01:06', '1672'),
                                 ('443181443564354',1739129,'2025-06-14 6:01:06','2025-06-15 6:01:06', '1772'),
                                 ('453181443564354',1939129,'2025-06-14 6:01:06','2025-06-15 6:01:06', '1972');

SELECT * FROM ELIGIBLE_USER;



CREATE OR REPLACE STAGE splitfile_internal_stage;
CREATE OR REPLACE STAGE bal_internal_stage;

CREATE or replace file format my_csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
COMPRESSION = none;

CREATE OR REPLACE SECRET SFTP_EC2_SECRET
TYPE = PASSWORD
USERNAME = 'user'
PASSWORD = 'pass';

CREATE OR REPLACE SECRET pgp_public_key
    TYPE = GENERIC_STRING
    SECRET_STRING = "-----BEGIN PGP PUBLIC KEY BLOCK-----

mDMEaEAt7BYJKwYBBAHaRw8BAQdAraXkrdTmf6GfI+hqpmBzFAVg5PzGoHG82lD4
MwC/VYa0Jk1vaGl0IFJhaiAobXkga2V5KSA8bW9oaXRAZXhhbXBsZS5vcmc+iJME
ExYKADsWIQSjrnarM2mk2ki9AQ2KIarJi/8GFwUCaEAt7AIbAwULCQgHAgIiAgYV
CgkICwIEFgIDAQIeBwIXgAAKCRCKIarJi/8GF3cXAQDBpR5gXs9ZA4cIgzCvlCtA
w2bjOw1C1H4CZTDL8FZoeQEA5/lbcyMxUh5LpQmrHrLIdpWO+u0uP1iGZNoTCexz
OAO4OARoQC3sEgorBgEEAZdVAQUBAQdAgUiqJxCmjpJAl93fAfUjVl6kAdBvGjE4
pIvtQcgQIE4DAQgHiHgEGBYKACAWIQSjrnarM2mk2ki9AQ2KIarJi/8GFwUCaEAt
7AIbDAAKCRCKIarJi/8GF6zCAQClPduMf1SI1rytsikD6+VvN8VPeW7lxr95IN3k
z2JrjwD/Uy5amYnxm7LYkxJpy2CZlFNQ/ukJC6rDc3eghnk6xwc=
=nXoa
-----END PGP PUBLIC KEY BLOCK-----";


DESC SECRET pgp_public_key;
DESC SECRET SFTP_EC2_SECRET;

GRANT USAGE ON SECRET pgp_public_key TO ROLE ACCOUNTADMIN;

CREATE OR REPLACE NETWORK RULE sftp_ec2_network_rule
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('your-local-host-where-your-sftp-server-is-hosted:2222');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION my_sftp_ec2_integration
ALLOWED_NETWORK_RULES = ('sftp_ec2_network_rule')
ALLOWED_AUTHENTICATION_SECRETS = ('SFTP_EC2_SECRET','pgp_public_key')
ENABLED = TRUE;

GRANT USAGE ON INTEGRATION my_sftp_ec2_integration TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SECRET SFTP_EC2_SECRET TO ROLE ACCOUNTADMIN;

list @splitfile_internal_stage;
list @bal_internal_stage;

REMOVE @splitfile_internal_stage;
REMOVE @bal_internal_stage;

CREATE OR REPLACE TABLE dataJobDetail (
    jobId   varchar(255),
    jobStartTime    TIMESTAMP,
    jobEndTime  TIMESTAMP,
    sequenceNumber  INTEGER,
    status  varchar(255),
    tableName   varchar(255),
    dataFilename    VARIANT,
    lastFetchedDataTime TIMESTAMP,
    totalRecordCount    INTEGER,
    message varchar(255)
);



--Data migration
CREATE OR REPLACE PROCEDURE submit_data(
table_name STRING,
host STRING,
port INTEGER,
start_date_time TIMESTAMP,
end_date_time TIMESTAMP
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'main'
PACKAGES = ('snowflake-snowpark-python')
EXECUTE AS CALLER
AS
$$
from datetime import datetime, timezone
import ast
import logging
import uuid

job_start_time = datetime.utcnow()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def main(session, table_name, host, port, start_date_time, end_date_time):
    try:
        seq_number = None
        
        # Defining the trace_id
        trace_id = str(uuid.uuid4()) 

        # Get sequence number of last successful job from dataJobDetail table
        seq_number = session.sql(f"""
            SELECT sequenceNumber FROM dataJobDetail WHERE jobEndTime = (SELECT MAX(jobEndTime) FROM dataJobDetail WHERE status = 'SUCCESS' AND tableName = '{table_name}')
        """).collect()[0][0]
        logger.info(f"[{trace_id}] Sequence number of last successful job is {seq_number}")
        
        # Initialize the job in dataJobDetail
        session.sql(f"""
            INSERT INTO dataJobDetail 
            SELECT '{trace_id}', '{job_start_time}', NULL, {seq_number}, 'INITIAL','{table_name}', NULL, NULL, NULL, 'Job initialised succesfully'
        """).collect()
        logger.info(f"[{trace_id}] Initialize the job in dataJobDetail table.")
        
        # Data pull + split Files creation
        logger.info(f"[{trace_id}] CALL get_data() to get data from source table {table_name}")
        session.sql(f"""
            CALL get_data( 
                '{table_name}',  
                {f"TIMESTAMP '{start_date_time}'" if start_date_time else "NULL"}, 
                {f"TIMESTAMP '{end_date_time}'" if end_date_time else "NULL"},
                '{trace_id}'
            )
        """).collect()

        # Reformatting the datafiles to add BNC checks details
        logger.info(f"[{trace_id}] CALL reformat_datafile() for the reformatting of the datafiles.")
        session.sql(f"CALL reformat_datafile('{table_name}','{trace_id}')").collect()

        # Test SFTP connection
        logger.info(f"[{trace_id}] CALL Testing test_sftp_connect() for SFTP connection.")
        # session.sql(f"CALL test_sftp_connect('{host}', {port}, '{trace_id}')").collect()

        # Transmit datafiles
        logger.info(f"[{trace_id}] CALL transmit_bnc_datafiles() for transmission of datafile to SFTP inbox.")
        session.sql(f"CALL transmit_bnc_datafiles('bal_internal_stage','{host}', {port}, '{trace_id}')").collect()
        
    except Exception as e:
        session.sql(f"CALL job_exception_handler('{trace_id}', 'FAILED', 'Job failed because of exception')").collect()
        logger.error(f"[{trace_id}] Error fetching data from Snowflake: {e}")
        raise
    return 
$$;

CALL submit_data('Eligible_User', '13.127.120.68', 2222, '2025-06-13 01:40:01', '2025-06-17 01:40:01');


--Copy data from table to a csv file in @my_internal_stage and a backup file in @backup_internal_stage;
CREATE OR REPLACE PROCEDURE get_data(
table_name STRING,
start_date_time TIMESTAMP,
end_date_time TIMESTAMP,
trace_id VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'main'
PACKAGES = ('snowflake-snowpark-python')
EXECUTE AS CALLER
AS
$$
from datetime import datetime, timezone
import csv
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
now = datetime.utcnow()
today_run_system_date = now.strftime("%Y-%m-%dT%H-%M-%S")

def main(session, table_name, start_date_time, end_date_time, trace_id):
    try:
        # Remove all the files from @splitfile_internal_stage stage
        logger.info(f"[{trace_id}] Remove all the files from @splitfile_internal_stage stage.")
        session.sql("REMOVE @splitfile_internal_stage").collect() 

        # Define where clause according to the type of job you want
        new_filename = f"ResyData_{table_name}_{today_run_system_date}"  
        where_clause = ""
        if start_date_time is not None and end_date_time is not None:
            where_clause = f"""
                where "date_updated" >= '{start_date_time}' AND "date_updated" <= '{end_date_time}'
            """

        # Copy data from table to @splitfile_internal_stage stage
        logger.info(f"[{trace_id}] Copy the data from table {table_name} to the csv in @splitfile_internal_stage after splitting the files according to MAX_FILE_SIZE.") 
        session.sql(f"""
            COPY INTO @splitfile_internal_stage/{new_filename} 
            FROM (
                SELECT * FROM {table_name} {where_clause} 
            ) 
            FILE_FORMAT = my_csv_format 
            OVERWRITE = True 
            MAX_FILE_SIZE = 1
        """).collect()
        
    except Exception as e:
        session.sql(f"CALL job_exception_handler('{trace_id}', 'FAILED', 'Job failed while pulling data from source table')").collect()
        logger.error(f"[{trace_id}] Error fetching data from Snowflake: {e}")
        raise
        
    return ""
$$;


-- Read the data from the file and split it if it exceeds 200bytes and save them to @splitfile_internal_stage
CREATE OR REPLACE PROCEDURE reformat_datafile(
table_name STRING, 
trace_id VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'main'
PACKAGES = ('snowflake-snowpark-python')
EXECUTE AS CALLER
AS
$$
from datetime import datetime
import _snowflake
import csv
import re
import io
import logging
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def main(session, table_name, trace_id):
    try:
        # Remove all the files from @bal_internal_stage
        logger.info(f"[{trace_id}] Remove all the files from @bal_internal_stage.")
        session.sql('REMOVE @bal_internal_stage').collect()

        # Fetch all the files which matches the pattern from @splitfile_internal_stage
        filename_prefix = f"ResyData_{table_name}"
        pattern = filename_prefix+'.*\\.csv'
        logger.info(f"[{trace_id}] Take list of all the files which matches the pattern - {pattern} from @splitfile_internal_stage")
        all_files_in_stage = session.sql('LIST @splitfile_internal_stage').collect()
        files = [file.name.split('/')[1] for file in all_files_in_stage if re.match(pattern, file.name.split('/')[1])]
        logger.info(f"[{trace_id}] List : {files}")

        # If there are no files set job as 'NO_DATA'
        if not files:
            logger.info(f"[{trace_id}] No new data to upload")
            session.sql(f"CALL job_exception_handler('{trace_id}', 'NO_DATA', 'There is no new data to upload')").collect()
            raise Exception("no CSV files found")
            
        files.sort()
        num = 0
        total_records = 0
        last_fetched_data_time = ''
        for file in files:
            # Read the splitfile and increment the seq no 
            file_stream = session.file.get_stream(f"@splitfile_internal_stage/{file}")
            num += 1
            
            # Extract date and no. of rows from the file 
            originalDate = file.replace('.','_').split('_')[-5]
            date_part, time_part = originalDate.split('T')
            hh, mm, ss = time_part.split('-')
            #formattedDate = f"{date_part}T{hh}:{mm}:{ss}.{ms}Z"
            formattedDate = f"{date_part} {hh}:{mm}:{ss}"
            rows = sum(1 for _ in file_stream)   # Extract the rows (excluding the header)
            total_records += rows
            file_stream.seek(0)

            # Add header and trailer
            header = f"H,{formattedDate},{file}"
            content = file_stream.read().decode('utf-8')
            trailer = f"T,{rows}"
            modifiedContent = header + '\n' + content + trailer

            if(len(files) == num):
                last_fetched_data_time = content.strip().split('\n')[-1].split(',')[-2]

            # Put the files into the bal_internal_stage stage after adding the BNC details
            file_obj = io.BytesIO(modifiedContent.encode('utf-8'))
            session.file.put_stream(file_obj, f'@bal_internal_stage/{file}', overwrite = True, auto_compress = False)
            logger.info(f"[{trace_id}] {file} uploaded to @bal_internal_stage with BNC details where Header : {header}, Trailer : {trailer}")

        # Update the job with these details usinf trace_id
        logger.info(f"[{trace_id}] Update the job with sequenceNumber : sequenceNumber + {num}, totalRecordCount : {total_records}, lastFetchedDataTime : {last_fetched_data_time}")
        session.sql(f"""
            UPDATE dataJobDetail 
            SET  sequenceNumber = sequenceNumber + {num},
                 dataFilename = {files},
                 totalRecordCount = {total_records},
                 lastFetchedDataTime = TIMESTAMP '{last_fetched_data_time}'
            WHERE jobId = '{trace_id}';
        """).collect()
        
    except Exception as e:
        session.sql(f"CALL job_exception_handler('{trace_id}', 'FAILED', 'Job failed while reformatting datafiles')").collect()
        logger.error(f"[{trace_id}] Error fetching data from Snowflake: {e}")
        raise
        
    return ""
$$


--Test sftp connection
CREATE OR REPLACE PROCEDURE test_sftp_connect(
host STRING, 
port INTEGER,
trace_id VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
EXTERNAL_ACCESS_INTEGRATIONS = (my_sftp_ec2_integration)
HANDLER = 'main'
PACKAGES = ('paramiko', 'snowflake-snowpark-python')
SECRETS = ('cred' = SFTP_EC2_SECRET)
EXECUTE AS CALLER
AS
$$
import _snowflake
import paramiko
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def main(session, host, port, trace_id):
    try:
        USERNAME = _snowflake.get_username_password('cred').username
        PASSWORD = _snowflake.get_username_password('cred').password
        transport = paramiko.Transport((host, port))
        transport.connect(username=USERNAME, password=PASSWORD)
        logger.info(f"[{trace_id}] Estabilishing SFTP connection")
        sftp = paramiko.SFTPClient.from_transport(transport)
        temp = "Successfully connected to remote SFT server"
        sftp.close()
        transport.close()
        logger.info(f"[{trace_id}] SFTP connection was successfull")
        
    except Exception as e:
        session.sql(f"CALL job_exception_handler('{trace_id}', 'SFTP_FAILED', 'Job failed because of failed SFTP connection')").collect()
        logger.error(f"[{trace_id}] Error fetching data from Snowflake: {e}")
        raise
    return temp
$$



--Creating the Balancing file and then SFTPing it to the server after encrypting it
CREATE OR REPLACE PROCEDURE transmit_bnc_datafiles(
my_stage STRING, 
host STRING, 
port INTEGER, 
trace_id VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
EXTERNAL_ACCESS_INTEGRATIONS = (my_sftp_ec2_integration)
HANDLER = 'main'
PACKAGES = ('paramiko', 'snowflake-snowpark-python', 'pandas', 'pgpy')
SECRETS = ('cred' = SFTP_EC2_SECRET, 'pgpy' = pgp_public_key )
EXECUTE AS CALLER
AS
$$
import glob
from datetime import datetime
import shutil
import _snowflake
import paramiko
import os
import io
import pandas
import csv
import pgpy
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def main(session, my_stage, host, port, trace_id):
    try:
        files = session.sql(f"LIST @{my_stage}").collect()
        if not files:
            raise Exception("no files found")
            
        USERNAME = _snowflake.get_username_password('cred').username
        PASSWORD = _snowflake.get_username_password('cred').password
        pgp_public_key = _snowflake.get_generic_secret_string('pgpy')

        pgp_pubkey, _ = pgpy.PGPKey.from_blob(pgp_public_key)
        transport = paramiko.Transport((host, port))
        logger.info(f"[{trace_id}] Estabilishing SFTP connection")
        transport.connect(username=USERNAME, password=PASSWORD)
        logger.info(f"[{trace_id}] SFTP connection Estabilished")
        sftp = paramiko.SFTPClient.from_transport(transport)
        
        logger.info(f"[{trace_id}] Starting the transmission of datafiles after PGP encryption")
        for file in files:
            file_name = file['name']
            content = session.file.get_stream(f"@{file_name}").read()
            message = pgpy.PGPMessage.new(content)
            encrypted_msg = str(pgp_pubkey.encrypt(message))
            with sftp.open(f'{file_name.split("/",1)[1]}.gpg', 'w') as temp_file:
                temp_file.write(encrypted_msg)
            logger.info(f"[{trace_id}] {file_name.split('/',1)[1]}.gpg sent successfully")
        
        sftp.close()
        transport.close()
        
        logger.info(f"[{trace_id}] Closing SFTP connection and updating status as SUCCESS for the job")
        
        session.sql(f"""
            UPDATE dataJobDetail 
            SET status = 'SUCCESS', 
                message = 'Files uploaded to sftp inbox successfully', 
                jobEndTime = CURRENT_TIMESTAMP() 
            WHERE jobId = '{trace_id}';
        """).collect()
        
    except Exception as e:
        session.sql(f"CALL job_exception_handler('{trace_id}', 'SFTP_FAILED', 'Job failed while transmitting to SFTP inbox')").collect()
        logger.error(f"[{trace_id}] Error fetching data from Snowflake: {e}")
        raise

    return "BNC files are transferred"
$$



CREATE OR REPLACE PROCEDURE job_exception_handler(
trace_id VARCHAR,
status STRING,
message STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'main'
PACKAGES = ('snowflake-snowpark-python')
EXECUTE AS CALLER
AS
$$
def main(session, trace_id, status, message):
    try:
        session.sql(f"""
                UPDATE dataJobDetail 
                SET status = '{status}', 
                    message = '{message}', 
                    jobEndTime = CURRENT_TIMESTAMP() 
                WHERE jobId = '{trace_id}';
            """).collect()
    except Exception as e:
        print(f"{e}")
        raise
    return ""
$$;
