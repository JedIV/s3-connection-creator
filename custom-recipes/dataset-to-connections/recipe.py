import dataiku
from dataiku.customrecipe import *
from dataiku import pandasutils as pdu
import pandas as pd
import boto3
from typing import Dict, List
from ast import literal_eval
from boto_connections import Aws_Roles, get_boto3_iam_client

# get input and output datasets
input_A_names = get_input_names_for_role('input_list')
output_A_names = get_output_names_for_role('main_output')

# get admin connection
# set connection with role to load bucket information here.
# the role should have the following policy attached:
#{
#    "Version": "2012-10-17",
#    "Statement": [
#        {
#            "Sid": "VisualEditor0",
#            "Effect": "Allow",
#            "Action": [
#                "iam:GetRole",
#                "iam:GetPolicyVersion",
#                "iam:GetPolicy",
#                "iam:ListAttachedRolePolicies",
#                "iam:ListRoles",
#                "iam:ListRolePolicies"
#            ],
#            "Resource": "*"
#        }
#    ]
#}

connection = get_recipe_config()['connection']

# Read recipe inputs
role_groups = dataiku.Dataset(input_A_names[0])
role_groups_df = role_groups.get_dataframe()
role_groups_df["groups"] = role_groups_df["groups"].apply(literal_eval)

# dictionary of roles and groups
rg_list = role_groups_df.to_dict('records')

# boto 3 client, authenticated with the credentials in the admin connection defined in the plugin GUI
client = get_boto3_iam_client(connection)

# Get instance of AWS_Roles class from the boto_connections library
role_generator = Aws_Roles(client)

# list of roles and the groups associated with each role. You can enter them here, or load them as an input dataframe
roles = rg_list

# Dataiku API client
dku_client = dataiku.api_client()

# generate the full list of role/bucket/groups combinations
policy_map = role_generator.get_policies_for_roles(roles)
policy_list = role_generator.get_policy_role_list(policy_map)
full_list = role_generator.get_buckets_policy_role_list(policy_list)

# Combine the initial list of roles/groups with the new list of role/bucket/groups
roles = roles + full_list

clean_roles = []


# generate connections
for role in roles:

    if "bucket" in role:
        name = role["role"] + "_" + role["bucket"]
        bucket = role["bucket"]
    else:
        name = role["role"]
        bucket = ''
        
    # get the arn of each role
    role["arn"] = role_generator.get_arn_for_role(role["role"])
    role["connection"] = name
    
    # create parameters dictionay for each connection
    params = {'credentialsMode'                   : 'STS_ASSUME_ROLE',
              'defaultManagedPath'                : '/dataiku',
              'hdfsInterface'                     : 'S3A',
              'encryptionMode'                    : 'NONE',
              'chbucket'                          : bucket,
              'switchToRegionFromBucket'          : True,
              'usePathMode'                       : False,
              'stsRoleToAssume'                   : role["arn"],
              'metastoreSynchronizationMode'      : 'NO_SYNC',
              'customAWSCredentialsProviderParams': [],
              'dkuProperties'                     : [],
              'namingRule'                        : {}}
    
    # try to build or update each connection
    try:
        if name not in dku_client.list_connections():
            new_connection = dku_client.create_connection(name, 
                                                          type='EC2', 
                                                          params=params, 
                                                          usable_by='ALLOWED', 
                                                          allowed_groups= role["groups"])
        else:
            new_connection = dku_client.get_connection(name)
        role["result"] = "success"
        definition = new_connection.get_definition()
        definition['usableBy'] = 'ALLOWED'
        definition['allowedGroups'] = role["groups"]
        definition['detailsReadability'] = {'readableBy'   : 'ALLOWED', 
                                            'allowedGroups': role["groups"]}
        new_connection.set_definition(definition)
    except Exception as e: role["result"] = e


    clean_roles.append(role)

# output last run to a dataset
list_of_connections = dataiku.Dataset(output_A_names[0])
list_of_connections.write_with_schema(pd.DataFrame(clean_roles))

