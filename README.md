# Snowflake-to-sftp-server

HOW TO SEND CSV FILES FROM SNOWFLAKE’S TABLE TO A SFTP SERVER

Snowflake is a cloud-based data warehousing platform known for its scalability and flexibility. It separates storage and compute resources, enabling independent scaling and cost optimization. With a focus on ease of use, it facilitates secure data sharing and collaboration between organizations.

STEP 1 – CREATE WAREHOUSE
A warehouse provides the required resources, such as CPU, memory, and temporary storage, to perform the following operations in a Snowflake session.
1.	For POC purpose we have used the COMPUTE_WH warehouse which is a default virtual warehouse that is automatically created when a new snowflake account is provisioned.
2.	Now we can run queries, load data and perform transformations on it.
 

STEP 2 – CREATE DATABASE AND TABLES
You can create multiple databases under a single warehouse.
1.	To create a database using snowflake’s UI you just have to go to data tab on the list of options on left and select database in it and then click create database and then you can give the details. 

2.	Other way is through the SQL worksheets, go to project tab on the left and then click on worksheets there is an icon of creating the worksheet. From here you just have to write SQL to create the database in conventional way Here.

3.	We have created a table named as ELIGIBLE_USER with following fields in the PUBLIC Schema.

4.	We have another table dataJobDetail which will store the job status once we start the job.
•	jobId – random unique string .
•	jobStartTime – Job’s starting time.
•	jobEndTime – Job’s ending time.
•	sequenceNumber – To store the Number of the last successful job, we will increment this number after every successful job after adding the no. of split files to it.
•	Status – SUCCESS, FAILURE, NO_DATA and  SFTP_FAILURE.
•	tableName – for eg. ELIGIBLE_USER
•	dataFilename – list of names of all the split files which are going to be sent.
•	lastFetchedDataTime – It will capture the time of the last date_updated  column from the row in the last successful job.
•	totalRecordCount – It will capture the total number of records for this job.
•	Message – This will inform us about the reason of the job failure or was successful.
  

Note:- From now on we’ll doing everything in PRACTICE database with PUBLIC schema.

STEP 3 – CREATE CSV FORMAT, STAGES, SECRETS AND CONFIGURATION FOR EXTERNAL ACCESS INTEGRATION
1.	Create stages bal_internal_stage which will store the files which are split on the basis of MAX_FILE_SIZE =1  from ELIGIBLE_USER table and splitfile_internal_stage which will store the files after adding the few details like date, filename and total number of records in the file. Read..
2.	Create the csv format my_csv_format. Read..
 
3.	Create secrets to store the username, password and gpg_public_key. Read..
4.	We are using GPG encryption before sending files to the sftp server. Video..
 
5.	Grant all the usage to accountAdmin role so that  you can access these configs in your python procedure.
6.	Now lets do the config for the external access integration:
•	Create the NETWORK RULE to allow egress traffic to the sftp host. Read..
•	Create the EXTERNAL ACCESS INTEGRATION to integrate all the configs like network rule and secrets Read..
 


STEP 4 – CREATE STORED PROCEDURES 
Using snowflake’s procedure we can write code in multiple languages like python, java, javascript etc. We will be using python for our use case. You can read more about procedures here.
We are having 6 stored procedures, lets go through them one by one.
1.	Submit_data – It is having arguments table_name for eg. ELIGIBLE_USER, host(SFTP HOST), port(SFTP PORT), start_date_time and end_date_time for ad hoc requests if we want to send data from some custom time period. This is the main procedure and from here we are calling all the other procedures stepwise.
 
2.	get_data –This procedure will Copy data from table to a csv file in @my_internal_stage and a backup file in @backup_internal_stage
3.	read_data - Read the data from the file and split it if it exceeds 200bytes and save them to @splitfile_internal_stage
4.	test_sftp_connect - Test sftp connection
5.	transmit_bnc_datafiles - Creating the Balancing file and then SFTPing it to the server after encrypting it
6.	job_exception_handler - Update the job to the current status.


