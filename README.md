# Automate GDPR User Data Erasure Requests

### About
ConnectSphere is a fast-growing professional networking platform serving millions of users across Europe. With strict GDPR obligations, especially the “Right to be Forgotten,” it must ensure complete and accurate deletion of user data across multiple systems.

### Challenge
User data is scattered across numerous microservices—profiles, articles, comments, messages, and support systems. Manually deleting this data is risky, slow, and non-scalable. Missing even a single record could result in major fines and loss of user trust. ConnectSphere needed an automated, reliable, and auditable erasure pipeline.

### Solution
As a Senior Data Engineer, I built an automated GDPR deletion engine using ADLS Gen2 and Azure Databricks on a lakehouse architecture. Daily data extracts are ingested as incremental loads and processed into Delta tables. The pipeline identifies and deletes all user-associated data while generating an immutable audit trail to prove compliance.

### Details
The data is pulled from Kafka topics and uploaded to blob storage through ADF pipelines. Here we are using Databricks to successfully connect and ingest data into delta tables using pyspark codes.

 ADLS             |  Blob files
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/3f82ec7a-f83e-4371-bfb5-9796a83b53ab" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/2c10f08f-dbbb-40c0-a899-6ef7b31b64f5" />


After creation of external data sources, a Config file is setup to parameterize the file paths and variables. All the notebooks utilize it and thus avoid any hard codings. The historical loads is a one time noteboooks which is utilized in creating the historical data available in the delta tables.
 Configuration Files             |  Historical loads
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/fdac527a-5189-4236-99f2-1b7e054f15e6" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/a432ab41-2ac2-47b7-ba22-1706d919e7d0" />

The daily incremental job works on Upserting data into the respective delta table which intelligently inserts or updates records based on the primary keys of a table. The daily pipeline is scheduled at a certain time post data is available in the blob storage. The file runs and updates the tables with the incremental data on daily basis.
 Daily Incremental job             |  Pipeline run
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/98840fe1-4463-4303-92e0-def0816ab7b9" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/d23eacfb-4e1e-420c-9303-3ac46c8884fa" />

The snippet shows the logic of the user data deletion which is the core part of this case which implements the user's Right to be forgotten. The notebook is parameterized using notebook widgets and the values can be updated in the pipeline for an on demand manual deletion or it can be automated via REST APIs post call and integrated with the platform to avoid any manual dependencies. At a later stage, the notebook also deletes all the files from the table directory that are no longer in the latest state of the transaction log for the table and are older than a retention threshold. By doing this we are removing any possibility of time travelling thus ensuring all user data has been completely wiped.
 User data Deletion             |  Vaccum files
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/5aecdf64-5d34-440e-8677-bcad9aaf5e24" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/fcd4e729-41ee-4019-ae44-44bee5d9c67d" />

This below image provides evidence of the final, critical output of the GDPR process: the immutable audit log. The screenshot shows a query against the audit_log Delta Table, displaying a clear and permanent record of the erasure operation. Each row captures the essential details required for compliance, including the specific user ID processed, the actions taken (DELETE, ANONYMIZE), the tables affected, the number of records impacted, and a precise timestamp. This audit trail is the definitive proof that the "Right to be Forgotten" request was fulfilled systematically and completely.
### Audit Trial
<img width="940" height="449" alt="image" src="https://github.com/user-attachments/assets/e0cb1fcc-fb07-42a6-94a2-c9d3835dff01" />
