# Code for custom code recipe dataset-to-connections (imported from a Python recipe)

# To finish creating your custom recipe from your original PySpark recipe, you need to:
#  - Declare the input and output roles in recipe.json
#  - Replace the dataset names by roles access in your code
#  - Declare, if any, the params of your custom recipe in recipe.json
#  - Replace the hardcoded params values by acccess to the configuration map

# See sample code below for how to do that.
# The code of your original recipe is included afterwards for convenience.
# Please also see the "recipe.json" file for more information.

# import the classes for accessing DSS objects from the recipe
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import *

# Inputs and outputs are defined by roles. In the recipe's I/O tab, the user can associate one
# or more dataset to each input and output role.
# Roles need to be defined in recipe.json, in the inputRoles and outputRoles fields.

# To  retrieve the datasets of an input role named 'input_A' as an array of dataset names:
input_A_names = get_input_names_for_role('input_A_role')
# The dataset objects themselves can then be created like this:
input_A_datasets = [dataiku.Dataset(name) for name in input_A_names]

# For outputs, the process is the same:
output_A_names = get_output_names_for_role('main_output')
output_A_datasets = [dataiku.Dataset(name) for name in output_A_names]


# The configuration consists of the parameters set up by the user in the recipe Settings tab.

# Parameters must be added to the recipe.json file so that DSS can prompt the user for values in
# the Settings tab of the recipe. The field "params" holds a list of all the params for wich the
# user will be prompted for values.

# The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
connection = get_recipe_config()['connection']

# For optional parameters, you should provide a default value in case the parameter is not present:
my_variable = get_recipe_config().get('parameter_name', None)

# Note about typing:
# The configuration of the recipe is passed through a JSON object
# As such, INT parameters of the recipe are received in the get_recipe_config() dict as a Python float.
# If you absolutely require a Python int, use int(get_recipe_config()["my_int_param"])


#############################
# Your original recipe
#############################

# -*- coding: utf-8 -*-
import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
from boto_connections import Aws_Roles, get_boto3_iam_client

import boto3
from typing import Dict, List
from ast import literal_eval

# Read recipe inputs
role_groups = dataiku.Dataset("role_groups")
role_groups_df = role_groups.get_dataframe()
role_groups_df["groups"] = role_groups_df["groups"].apply(literal_eval)

rg_list = role_groups_df.to_dict('records')


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

client = get_boto3_iam_client(connection)

role_generator = Aws_Roles(client)

# list of roles and the groups associated with each role. You can enter them here, or load them as an input dataframe
roles = rg_list

dku_client = dataiku.api_client()

policy_map = role_generator.get_policies_for_roles(roles)
policy_list = role_generator.get_policy_role_list(policy_map)
full_list = role_generator.get_buckets_policy_role_list(policy_list)

roles = roles + full_list

clean_roles = []

for role in roles:

    if "bucket" in role:
        name = role["role"] + "_" + role["bucket"]
        bucket = role["bucket"]
    else:
        name = role["role"]
        bucket = ''
    
    role["arn"] = role_generator.get_arn_for_role(role["role"])
    role["connection"] = name
    params = {'credentialsMode': 'STS_ASSUME_ROLE',
      'defaultManagedPath': '/dataiku',
      'hdfsInterface': 'S3A',
      'encryptionMode': 'NONE',
      'chbucket': bucket,
      'switchToRegionFromBucket': True,
      'usePathMode': False,
      'stsRoleToAssume': role["arn"],
      'metastoreSynchronizationMode': 'NO_SYNC',
      'customAWSCredentialsProviderParams': [],
      'dkuProperties': [],
      'namingRule': {}}
    try:
        new_connection = dku_client.create_connection(name, type='EC2', params=params, usable_by='ALLOWED', allowed_groups= role["groups"])
        role["result"] = "success"
        definition = new_connection.get_definition()
        definition['detailsReadability'] = {'readableBy': 'ALLOWED', 'allowedGroups': role["groups"]}
        new_connection.set_definition(definition)
    except Exception as e: role["result"] = e


    clean_roles.append(role)

list_of_connections = dataiku.Dataset(output_A_names[0])
list_of_connections.write_with_schema(pd.DataFrame(clean_roles))

