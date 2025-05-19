import configparser
import pandas as pd
import boto3
import json
from botocore.exceptions import ClientError

   
#Load DWH Params from a file
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

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

DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")
    
#Create clients for EC2, S3, IAM and Redshift
ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )

redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    
#Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)
def create_iam_role(role_name=DWH_IAM_ROLE_NAME):
    #1.1 Create the role, 
    
    try:
        print("1.1 Creating a new IAM Role...\n") 
        iam.create_role(
            Path='/',
            RoleName=role_name,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)

def attach_role_policy(role_name=DWH_IAM_ROLE_NAME):
    #1.2 Attaching Policy
    try:
        print("1.2 Attaching Policy...\n")
        iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
    except Exception as e:
        print(e)

#Create Redshift cluster and get IAM role ARN
def create_redshift_cluster(role_name=DWH_IAM_ROLE_NAME):
    try:
        print("1.3 Get the IAM role ARN...\n")
        roleArn = iam.get_role(RoleName=role_name)['Role']['Arn']
        
        print("1.4 Creating Redshift Cluster...\n")
        redshift.create_cluster(
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)
        
#Check clusteer status if it is available
def check_cluster_status(seconds=15):
    endpoint = ''
    rolearn = ''
    while redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']=='creating':
        print('Creating cluster...\n')
        time.sleep(seconds)
        
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    if myClusterProps['ClusterStatus']=='available':
        print("Redshift cluster available")
        endpoint = myClusterProps['Endpoint']['Address']
        rolearn = myClusterProps['IamRoles'][0]['IamRoleArn']
#         myClusterProps['ClusterStatus'] == 'available'
    else:
        print("Error while creating Redshift cluster!")
        
    config['DWH']['DWH_ENDPOINT'] = endpoint
    print("Set  DWH_ENDPOINT to 'dwh.cfg' ...")
    with open('dwh.cfg', 'w') as configfile:    
        config.write(configfile)
        
    config['IAM_ROLE']['ARN'] = rolearn
    print("Set  ARN to 'dwh.cfg' ...")
    with open('dwh.cfg', 'w') as configfile:    
        config.write(configfile)
               
    return endpoint, rolearn

#Open an incoming TCP port to access the cluster ednpoint
def open_tcp_port(cluster_name=DWH_CLUSTER_IDENTIFIER, from_port=int(DWH_PORT), to_port=int(DWH_PORT)):
    print("1.5 Opening an incoming TPC port to acess the cluster endpoint...\n")
    try:
        metadata = redshift.describe_clusters(ClusterIdentifier=cluster_name)
        redshift_metadata = metadata.get("Clusters", "No Clusters Found")[0]
        vpc_id = redshift_metadata.get("VpcId", "No VpcId Found")
        
        vpc = ec2.Vpc(id=vpc_id)
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=from_port,
            ToPort=to_port
        )
    except Exception as e:
        print(e)
        

def main():
    create_iam_role()
    attach_role_policy()
    create_redshift_cluster()
    check_cluster_status()
    open_tcp_port()


if __name__ == "__main__":
    main()