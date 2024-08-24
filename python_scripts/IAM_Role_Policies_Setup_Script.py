# import required libraries
import configparser
import boto3
import pandas as pd
import json
import time

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

try:
    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')
    SESSION_TOKEN = config.get('AWS','SESSION_TOKEN')
    
    GLUE_IAM_ROLE_NAME = config.get("GLUE", "GLUE_IAM_ROLE_NAME")
    GLUE_PORT               = config.get("GLUE","GLUE_PORT")
    GLUE_ENDPOINT           = config.get("GLUE","GLUE_ENDPOINT")
    GLUE_ROLE_ARN           = config.get("GLUE","GLUE_ROLE_ARN")
    GLUE_VPC_ID             = config.get("GLUE","GLUE_VPC_ID")
except:
    pass

# Connection to AWS services (IAM, EC2, REDSHIFT)
iam = boto3.client(
    'iam',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
    aws_session_token=SESSION_TOKEN,
    region_name='us-east-1'
)

ec2_resource = boto3.resource(
    'ec2',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
    aws_session_token=SESSION_TOKEN,
    region_name='us-east-1'
)

ec2_client = boto3.client('ec2',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
    aws_session_token=SESSION_TOKEN,
    region_name='us-east-1')


print("Connection to the AWS services is successful.")
print()


# IAM role and policies
# Detach the existing IAM policies
policies = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess",
    "arn:aws:iam::aws:policy/AmazonEC2FullAccess"
]

print("Detaching policies from the role:")
for policy_arn in policies:
    try:
        iam.detach_role_policy(
            RoleName=GLUE_IAM_ROLE_NAME,
            PolicyArn=policy_arn
        )
        print(f"Policy {policy_arn} detached successfully.")
    except Exception as e:
        print(f"Error in detaching the policy {policy_arn}:")
        print(e)

# Wait for a few seconds to ensure policies are detached
time.sleep(15)

# Delete the existing IAM role
print("Deleting the existing IAM role:")
try:
    iam.delete_role(RoleName=GLUE_IAM_ROLE_NAME)
    print("Deletion of Role is successful.")
except Exception as e:
    print("Error in deleting the role:")
    print(e)

# Wait for a few seconds to ensure roles are deleted
time.sleep(15)
print()

# Define the policy document
ASSUME_ROLE_POLICY_DOCUMENT = {
    'Version': '2012-10-17',
    'Statement': [
        {
            'Effect': 'Allow',
            'Principal': {'Service': 'glue.amazonaws.com'},
            'Action': 'sts:AssumeRole'
        }
    ]
}

# Create the IAM role
try:
    glue_role = iam.create_role(
        Path='/',
        RoleName=GLUE_IAM_ROLE_NAME,
        Description="Allows AWS Glue to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(ASSUME_ROLE_POLICY_DOCUMENT)
    )
    print("Creation of Role is successful.")
except Exception as e:
    print("Error in creating the role:")
    print(e)

# Wait for a few seconds to ensure roles are created
time.sleep(15)

# Attach the necessary policies
for policy_arn in policies:
    try:
        iam.attach_role_policy(
            RoleName=GLUE_IAM_ROLE_NAME,
            PolicyArn=policy_arn
        )
        print(f"Policy {policy_arn} attached successfully.")
    except Exception as e:
        print(f"Error in attaching the policy {policy_arn}:")
        print(e)

# Wait for a few seconds to ensure policies are attached
time.sleep(15)

# Extract the ARN of the created role
try:
    GLUE_ROLE_ARN = glue_role['Role']['Arn']
    GLUE_IAM_ROLE_NAME = glue_role['Role']['RoleName']
    #print(f"The ARN of the created role is: {role_arn}")
    #print(f"The Name of the created role is: {role_arn}")
except Exception as e:
    print("Error in extracting the IAM role ARN or Name:")
    print(e)


# Fetch the VPC ID
def get_vpc_id():
    response = ec2_client.describe_vpcs()
    if response['Vpcs']:
        return response['Vpcs'][0]['VpcId']
    else:
        raise Exception("No VPCs found")

# Create a VPC Endpoint for S3
def create_vpc_endpoint(vpc_id):
    response = ec2_client.create_vpc_endpoint(
        VpcId=vpc_id,
        ServiceName='com.amazonaws.us-east-1.s3',  # Change the region as needed
        VpcEndpointType='Gateway'
    )
    return response['VpcEndpoint']['VpcEndpointId']

try:
    vpc_id = get_vpc_id()
    GLUE_ENDPOINT = create_vpc_endpoint(vpc_id)
    print(f"VPC Endpoint ID: {GLUE_ENDPOINT}")
except Exception as e:
    print("Error in creating VPC endpoint:")
    print(e)

# Wait for a few seconds to ensure endpoint is created
time.sleep(15)
    
# Security group parameters configuration
try:
    GLUE_VPC_ID = get_vpc_id()
    vpc = ec2_resource.Vpc(id=GLUE_VPC_ID)
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    
    # Define the rule parameters
    cidr_ip = '0.0.0.0/0'
    ip_protocol = 'tcp'
    from_port = int(GLUE_PORT)
    to_port = int(GLUE_PORT)

    # Check existing ingress rules
    existing_rules = defaultSg.ip_permissions
    rule_exists = False

    for rule in existing_rules:
        # Check if the rule matches the desired parameters
        if (rule['IpProtocol'] == ip_protocol and
                rule['FromPort'] == from_port and
                rule['ToPort'] == to_port and
                any(cidr['CidrIp'] == cidr_ip for cidr in rule['IpRanges'])):
            rule_exists = True
            break

    if rule_exists:
        print(f"Rule already exists: ALLOW {ip_protocol} from {cidr_ip} on port {from_port}.")
        print()
    else:
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp=cidr_ip,
            IpProtocol=ip_protocol,
            FromPort=from_port,
            ToPort=to_port
        )
        print("Security group configuration is successful.")
        print()

except Exception as e:
    print("Error in configuring security group:")
    print(e)
    print()


# Update endpoint in the config file
print(GLUE_ENDPOINT)
print(GLUE_VPC_ID)
print(GLUE_IAM_ROLE_NAME)
print(GLUE_ROLE_ARN)
GLUE_VPC_ID = config.set("GLUE", "GLUE_VPC_ID", GLUE_VPC_ID)
GLUE_ENDPOINT = config.set("GLUE", "GLUE_ENDPOINT", GLUE_ENDPOINT)
GLUE_IAM_ROLE_NAME = config.set("GLUE", "GLUE_IAM_ROLE_NAME", GLUE_IAM_ROLE_NAME)
GLUE_ROLE_ARN = config.set("GLUE", "GLUE_ROLE_ARN", GLUE_ROLE_ARN)


# Write the endpoint & VPC ID back to the config file
with open('dwh.cfg', 'w') as configfile:
    config.write(configfile)

print("End")