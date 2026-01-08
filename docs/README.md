# Building an ETL pipeline with Apache Airflow and Visualizing AWS Redshift data using Microsoft Power BI

### Check the reference article here: <a href="https://aws.plainenglish.io/uber-expenses-tracking-with-airflow-redshift-powerbi-27688a686f60">Building an ETL data pipeline with Apache Airflow and Visualizing AWS Redshift data using Microsoft Power BI</a>

<p align="justify">
Have you heard phrases like <strong>Hungry? You're in the right place</strong> or <strong>Request a trip, hop in, and relax.</strong>? Both phrases are very common in our daily lives, they represent the emblems of the two most important businesses with <a href="https://qz.com/1889602/uber-q2-2020-earnings-eats-is-now-bigger-than-rides/"> millionaire revenues </a> from UBER. <strong>Have you ever thought about how much money you spend on these services?</strong> The goal of this project is to track the expenses of <a href="https://www.uber.com/">Uber Rides</a> and <a href="https://www.ubereats.com/">Uber Eats</a> through data engineering processes using technologies such as <a href="https://airflow.apache.org/">Apache Airflow</a>, <a href="https://aws.amazon.com/es/redshift/">AWS Redshift</a> and <a href="https://powerbi.microsoft.com/es-es/">Power BI</a>. This repository demonstrates a quick and easy way to automate the entire pipeline step by step.
</p>

# Architecture - Uber expenses tracking

![alt text](https://wittline.github.io/uber-expenses-tracking/Images/architecture.png)

## What are the data sources?

<p align="justify"> 
Every time an Uber Eats or Uber Rides service has ended, you receive a payment receipt in your email. This receipt contains the information about the details of the service and is attached to the email with the extension <strong>.eml</strong>. Both receipts belong to the details sent by Uber about Eats and Rides services; these serve as our original data sources. For this implementation, these receipts are downloaded from the email to a local environment or S3 bucket.
</p>

### Uber Rides receipt example

![alt text](https://wittline.github.io/uber-expenses-tracking/Images/rides_receipt_example.png)

### Uber Eats receipt example

![alt text](https://wittline.github.io/uber-expenses-tracking/Images/eats_receipt_example.png)

## Data modelling

<p align="justify"> 
Once the details for each type of receipt have been detected, it is easy to identify the features, entities, and relations of the model. This data model contains the expenses of both services separated into different fact tables, sharing dimensions between these facts. Therefore, the proposed model follows a constellation scheme.
</p>

![alt text](https://wittline.github.io/uber-expenses-tracking/Images/dwh_schema.jpg)

## Infrastructure as Code (IaC) in AWS

<p align="justify"> 
The aim of this section is to create a Redshift cluster on AWS and keep it available for use by the Airflow DAG. In addition to preparing the infrastructure, the file <strong>AWS-IAC-IAM-EC2-S3-Redshift.ipynb</strong> provides an alternative staging zone in S3.

Below are the steps carried out in this process:
</p>

- Install <a href="https://www.stanleyulili.com/git/how-to-install-git-bash-on-windows/">git-bash for windows</a>. Open **git bash** and clone this repository to access the **dags** folder, the **docker-compose.yaml** file, and other required assets.

``` 
sruthi@dev-machine MINGW64 /c
$ git clone https://github.com/SruthiGade/uber-expenses-tracking.git
```
- Create a new User in AWS with **AdministratorAccess** and obtain your security credentials.
- Configure your AWS Credentials in your local machine using the <a href="https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html ">AWS CLI</a>.
- Setup local environment with Google Colab:
  - Open CMD and run: 
      ```
      C:\>jupyter notebook --NotebookApp.allow_origin='https://colab.research.google.com' --port=8888 --NotebookApp.port_retries=0
      ```
  - Copy the localhost URL for use in Colab.
  - Go to <a href="https://colab.research.google.com/"> Google Colab </a> and create a new Notebook.
  - Connect to local runtime and paste the Backend URL.
  - Upload and execute ***AWS-IAC-IAM-EC2-S3-Redshift.ipynb*** to:
    - Create required S3 buckets (uber-tracking-expenses-bucket-s3 and airflow-runs-receipts).
    - Move Uber receipts to S3.
    - Load parameters from the dwh.cfg file.
    - Create clients for IAM, EC2, and Redshift.
    - Create the IAM Role for Redshift S3 ReadOnly access.
    - Provision the Redshift cluster and monitor status until Available.
    - Configure TCP port access for the cluster endpoint.
    - Validate the connection.

#### Content of ***AWS-IAC-IAM-EC2-S3-Redshift.ipynb***

> Libraries

 ```python
import pandas as pd
import glob
import os
import boto3
import json
import configparser
from botocore.exceptions import ClientError
import psycopg2
```

> Cloning repository, Buckets creation, folders and uploading the local files to S3

```python
def bucket_s3_exists(b):
    s3 = boto3.resource('s3')
    return s3.Bucket(b) in s3.buckets.all()

def create_s3_bucket(b, folders):
    client = boto3.client('s3')
    client.create_bucket(Bucket=b, CreateBucketConfiguration={'LocationConstraint': 'us-east-2'})
    if folders != '':
        fls = folders.split(',')
        for f in fls:
            client.put_object(Bucket= b, Body='', Key=f + '/')

def upload_files_to_s3(file_name, b, folder, object_name, args=None):
    
    client = boto3.client('s3')

    if object_name is None:
        object_name = folder + "/{fname}".format(fname= os.path.basename(file_name)) 

    response = client.upload_file(file_name, b, object_name , ExtraArgs = args)

    return response


ACLargs = {'ACL':'authenticated-read' }
bucket_names = {'uber-tracking-expenses-bucket-s3': 'unprocessed_receipts', 'airflow-runs-receipts':'eats,rides'}

print('Creating the S3 buckets...')
for k in bucket_names:
    if not bucket_s3_exists(k):
       create_s3_bucket(k, bucket_names[k])    

print('S3 buckets created')

print('Uploading the local receipts files to S3...')
files = glob.glob("localpath/receipts/*")

for file in files:
    print("Uploading file:", file)
    print(upload_files_to_s3(file, 'uber-tracking-expenses-bucket-s3', 'unprocessed_receipts', None, ACLargs))


print('Files uploaded successfully')
 ```

> Loading all the Params from the dwh.cfg file

 ```python
config = configparser.ConfigParser()
config.read_file(open('/Uber-expenses-tracking/IAC/dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", 
                   "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, 
                  DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })
 ```
 
> Creating clients for IAM, EC2 and Redshift resources

 ```python
ec2 = boto3.resource('ec2',
                       region_name="us-east-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-east-2'
                  )

redshift = boto3.client('redshift',
                       region_name="us-east-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
 ```
 
> Creating the IAM Role for Redshift S3 Access
 
 ```python
try:
    print("Creating new IAM Role") 
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )    
except Exception as e:
    print(e)
    
print("Attaching Policy")
iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
print(roleArn)
 ```

> Creating Redshift Cluster

 ```python
try:
    response = redshift.create_cluster(        
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        IamRoles=[roleArn]  
    )
except Exception as e:
    print(e)
 ```

> Redshift Cluster Details

 ```python
def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", 
                  "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)
 ```
 
> Redshift Cluster endpoint and role ARN

 ```python
DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
 ```

> Incoming TCP port configuration

 ```python
try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)
 ```

> Checking the connection

 ```python
conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print('Connecting to RedShift...')
conn = psycopg2.connect(conn_string)
print('Connected to Redshift')
 ```

## Building an ETL data pipeline with Apache Airflow

<p align="justify">
This project focuses on the data integration process of Uber receipts into a centralized data model. While prior knowledge of Airflow is beneficial, the provided configuration allows for a streamlined deployment.
</p>

### Docker environment

<p align="justify">
This project utilizes a <a href="https://www.docker.com/">Docker</a> container for local orchestration. Apache Airflow components (metadata database, scheduler, and webserver) are deployed using Docker Compose with a Celery executor.
</p>

- Ensure <a href="https://docs.docker.com/docker-for-windows/install/">Docker Desktop</a> is installed.
- Navigate to the project directory in git bash:

```linux 
sruthi@dev-machine MINGW64 /c
$ cd Uber-expenses-tracking/code
```

```linux 
sruthi@dev-machine MINGW64 /c/Uber-expenses-tracking/code
$ echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
$ docker-compose up airflow-init
$ docker-compose up
```

<p align="justify">
Once running, access the Airflow GUI at <a href="http://localhost:8080/">http://localhost:8080</a>. The default credentials are "airflow" for both user and password.
</p>

Configuration steps:
- In the Airflow GUI, go to Admin -> Variables and import **variables.json** from the repository.
- Go to Admin -> Connections and configure your AWS and Redshift credentials.

![alt text](https://wittline.github.io/uber-expenses-tracking/Images/variables.png)

### Running the DAG

- Identify the scheduler container ID:
```linux
$ docker ps
```
- Trigger the DAG via CLI:
```linux
$ docker exec [CONTAINER_ID] airflow dags trigger Uber_tracking_expenses
```
- Alternatively, use the Airflow GUI to trigger the execution manually.

### DAG Details

<p align="justify">
The DAG performs the following core operations:
</p>

<ul>
<li><strong>Start_UBER_Business:</strong> Separates Uber Eats and Uber Rides receipts from the S3 bucket for parallel processing.</li>
<li><strong>Processing Tasks:</strong> Condenses processed receipts into single datasets (eats_receipts.csv, items_eats_receipts.csv, and rides_receipts.csv) stored in the airflow-runs-receipts bucket.</li>
<li><strong>Redshift Schema Creation:</strong> Executes SQL commands to create dimension, fact, and staging tables.</li>
<li><strong>Data Loading:</strong> Uses the COPY command to move data from S3 to Redshift staging tables (staging_rides, staging_eats, staging_eats_items).</li>
<li><strong>DWH Transformation:</strong> Populates dimension and fact tables from staging data. Note that Redshift focuses on analytical query performance and does not strictly enforce foreign key constraints.</li>
<li><strong>Quality Check:</strong> Validates record counts in the DWH tables before dropping staging tables.</li>
</ul>

![alt text](https://wittline.github.io/uber-expenses-tracking/Images/dag.PNG)

## Visualizing AWS Redshift data using Microsoft Power BI

<p align="justify">
Connect Power BI Desktop to the AWS Redshift cluster to visualize the processed data.
</p>

- Open Power BI Desktop and sign in.
- Select Get Data -> Database -> Amazon Redshift.
- Provide the Server endpoint and Database name. Use "Import" mode.
- Build dashboards using the provided **report_receipts.pbix** template as a guide.
- Publish the report to the Power BI service for mobile consumption and sharing.

![powerBi_uber_services6](https://user-images.githubusercontent.com/8701464/115949563-97e22b00-a49b-11eb-92ab-5459b4469f5f.gif)

## Contributing and Feedback
Feedback and contributions are welcome to improve the pipeline efficiency or visualization features.

## Project Maintenance
- **Maintainer:** Sruthi Gade
- **Role:** Business Data Analyst
- **Original Concept:** Ramses Alexander Coraspe Valdez (2021)

## About the Developer
Sruthi Gade is a Business Data Analyst with over 3 years of experience in translating complex business requirements into actionable, data-driven insights. With a strong background in SQL, Python, and Power BI, she specializes in building scalable analytics solutions and automated ETL pipelines within cloud environments. Her work focuses on improving operational efficiency and supporting executive decision-making through advanced data modeling and predictive analysis.

- **Email:** gade.sruhthi02@gmail.com
- **Skills:** SQL, Python, R, Pandas, NumPy, Power BI, Tableau, AWS Redshift

## License
This project is licensed under the terms of the Apache License.