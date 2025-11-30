# Automate GDPR User Data Erasure Requests

### About
ConnectSphere is a fast-growing professional networking platform serving millions of users across Europe. With strict GDPR obligations, especially the “Right to be Forgotten,” it must ensure complete and accurate deletion of user data across multiple systems.

### Challenge
User data is scattered across numerous microservices—profiles, articles, comments, messages, and support systems. Manually deleting this data is risky, slow, and non-scalable. Missing even a single record could result in major fines and loss of user trust. ConnectSphere needed an automated, reliable, and auditable erasure pipeline.

### Solution
As a Senior Data Engineer, I built an automated GDPR deletion engine using ADLS Gen2 and Azure Databricks on a lakehouse architecture. Daily data extracts are ingested as incremental loads and processed into Delta tables. The pipeline identifies and deletes all user-associated data while generating an immutable audit trail to prove compliance.


 ADLS             |  Blob files
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/3f82ec7a-f83e-4371-bfb5-9796a83b53ab" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/2c10f08f-dbbb-40c0-a899-6ef7b31b64f5" />

The data is pulled from Kafka topics and uploaded to blob storage through ADF pipelines. Here we are using Databricks to successfully connect and ingest data into delta tables using pyspark codes.


 Configuration Files             |  Historical loads
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/fdac527a-5189-4236-99f2-1b7e054f15e6" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/a432ab41-2ac2-47b7-ba22-1706d919e7d0" />


 Daily Incremental job             |  Pipeline run
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/98840fe1-4463-4303-92e0-def0816ab7b9" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/d23eacfb-4e1e-420c-9303-3ac46c8884fa" />


 User data Deletion             |  Vaccum files
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/5aecdf64-5d34-440e-8677-bcad9aaf5e24" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/fcd4e729-41ee-4019-ae44-44bee5d9c67d" />



### Audit Trial
<img width="940" height="449" alt="image" src="https://github.com/user-attachments/assets/e0cb1fcc-fb07-42a6-94a2-c9d3835dff01" />
