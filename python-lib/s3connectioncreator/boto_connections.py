import boto3
from typing import Dict, List
import dataiku

def get_boto3_iam_client(connection):
    client = dataiku.api_client()
    connection_info = client.get_connection(connection).get_info()
    input_creds = connection_info['resolvedAWSCredential']

    client = boto3.client(
        'iam',
        aws_access_key_id=input_creds['accessKey'],
        aws_secret_access_key=input_creds['secretKey'],
        aws_session_token=input_creds['sessionToken']
    )
    return client

class Aws_Roles:
    def __init__(self, client):
        self.client = client
    
    def get_arn_for_role(self, role):
        arn = self.client.get_role(RoleName=role)['Role']['Arn']
        return arn

    def get_policies_for_roles(self, role_names: List[str]) -> Dict[str, List[Dict[str, str]]]:
        """ Create a mapping of role names and any policies they have attached to them by 
            paginating over list_attached_role_policies() calls for each role name. 
            Attached policies will include policy name and ARN.
        """
        policy_map = {}
        policy_paginator = self.client.get_paginator('list_attached_role_policies')
        print(role_names)
        for name in role_names:
            arn = self.client.get_role(RoleName=name["role"])['Role']['Arn']
            role_policies = []
            for response in policy_paginator.paginate(RoleName=name["role"]):
                role_policies.extend(response.get('AttachedPolicies'))
                for policy in role_policies:
                    policy['arn'] = arn
                    policy['groups'] = name['groups']
            policy_map.update({name["role"]: role_policies})
        return policy_map

    def get_policy_role_list(self, policy_map):
        policy_list = []
        for i in policy_map.keys():
            for j in policy_map[i]:
                print(j)
                policy_list.append({"role"   : i, 
                                    "policy" : j['PolicyArn'], 
                                    "arn"    : j['arn'], 
                                    "groups" : j['groups']})
        return policy_list

    def get_policy_details_statement(self, policy_arn):
        policy = self.client.get_policy(
            PolicyArn = policy_arn
        )
        policy_version = self.client.get_policy_version(
            PolicyArn = policy_arn, 
            VersionId = policy['Policy']['DefaultVersionId']
        )
        return policy_version['PolicyVersion']['Document']['Statement']

    def get_bucket_list(self, policy_details_statement):
        bucket_list = []
        for s in policy_details_statement:
            print(s)
            if "s3" in s['Action']:
                for resource in s['Resource']:
                    bucket_list.append(resource.split(":::")[1].split("/")[0])

        bucket_list = list(set(bucket_list))
        return bucket_list

    def get_buckets_policy_role_list(self, policy_list):
        full_info = []
        for policy in policy_list:
            for bucket in self.get_bucket_list(self.get_policy_details_statement(policy["policy"])):
                full_info.append({"role"   : policy["role"],
                                  "policy" : policy["policy"],
                                  "arn"    : policy["arn"],
                                  "groups" : policy["groups"],
                                  "bucket" : bucket})

        return full_info