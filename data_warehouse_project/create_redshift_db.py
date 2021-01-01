import configparser
import json
import time

import boto3
import pandas as pd


def create_aws_client_general(client_name, iam_config, iam_config_section):
    """
    A helper function for creating boto3 aws clients
    :param client_name: name of client to create (e.g. 's3', 'redshift')
    :param iam_config: config read from iam.config file
    :param iam_config_section: section of iam_config whose access keys should apply
    :return: boto3 client that was created
    """
    return boto3.client(client_name,
                        aws_access_key_id=iam_config.get(iam_config_section, 'ACCESS_ID_KEY'),
                        aws_secret_access_key=iam_config.get(iam_config_section, 'ACCESS_ID_SECRET'))


def create_iam_client(iam_config):
    """
    Creates an iam client
    :param iam_config: config loaded from iam.cfg
    :return: the iam client created from this parameter
    """
    return create_aws_client_general('iam', iam_config, 'ROOT_USER')


def create_redshift_client(iam_config):
    """
    Creates a redshift client
    :param iam_config: config loaded from iam.cfg
    :return: the redshift client created.
    """
    return boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=iam_config.get('ROOT_USER', 'ACCESS_ID_KEY'),
                        aws_secret_access_key=iam_config.get('ROOT_USER', 'ACCESS_ID_SECRET'))


def create_redshift_reader_role(iam_client, dwh_config):
    """
    Creates a redshift reader role
    :param iam_client: boto3 client for iam service
    :param dwh_config: config read from 'dwh.config' file
    :return: the response from role creation
    """
    return iam_client.create_role(
        Path='/',
        RoleName=dwh_config.get('IAM_ROLE', 'ROLENAME'),
        Description="Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {'Service': 'redshift.amazonaws.com'}
                            }],
             'Version': '2012-10-17'}))


def create_redshift_cluster(role, redshift_client, dwh_config):
    """
    Created a redshift cluster
    :param role: arn of role to assign to redshift cluster
    :param redshift_client: boto3 aws redshift client
    :param dwh_config: config read from dwh.cfg file
    :return: The response from creating the cluster.
    """
    try:
        return redshift_client.create_cluster(
            # HW
            ClusterType=dwh_config.get('CLUSTER', 'CLUSTER_TYPE'),
            NodeType=dwh_config.get('CLUSTER', 'NODE_TYPE'),
            NumberOfNodes=int(dwh_config.get('CLUSTER', 'NUM_NODES')),

            # Identifiers & Credentials
            DBName=dwh_config.get('CLUSTER', 'DB_NAME'),
            ClusterIdentifier=dwh_config.get('CLUSTER', 'IDENTIFIER'),
            MasterUsername=dwh_config.get('CLUSTER', 'DB_USER'),
            MasterUserPassword=dwh_config.get('CLUSTER', 'DB_PASSWORD'),

            # Roles (for s3 access)
            IamRoles=[role])
    except redshift_client.exceptions.ClusterAlreadyExistsFault:
        print("Cluster already exists, continuing")


# Shamelessly copied from exercise on infrastrucure as code in udacity data warehouse lesson.
def pretty_redshift_props(props):
    """
    :param props: cluster props
    :return: dataframe of cluster props, for nicer printing.
    """
    pd.set_option('display.max_colwidth', None)
    keys_to_show = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keys_to_show]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def main():
    """
    Creates a redshift cluster based on the info in dwh.config and iam.config.
    Waits for the cluster to come up, throws if it doesn't do so in about 10 minutes,
    else prints out some properties of the cluster.
    :return: Nothing
    """
    dwh_config = configparser.ConfigParser()
    dwh_config.read('dwh.cfg')

    iam_config = configparser.ConfigParser()
    iam_config.read('iam.cfg')

    iam_client = create_iam_client(iam_config)
    redshift_client = create_redshift_client(iam_config)

    simple_role_name = dwh_config.get('IAM_ROLE', 'ROLENAME')
    try:
        create_redshift_reader_role(iam_client, dwh_config)
        iam_client.attach_role_policy(
            RoleName=simple_role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']
    except iam_client.exceptions.EntityAlreadyExistsException:
        print("Role already exists, continuing")

    role = iam_client.get_role(RoleName=simple_role_name)['Role']['Arn']
    create_redshift_cluster(role, redshift_client, dwh_config)
    cluster_identifier = dwh_config.get('CLUSTER', 'IDENTIFIER')

    for i in range(20):
        try:
            cluster_props = redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
            cluster_endpoints = cluster_props['Endpoint']['Address']
            cluster_role_arn = cluster_props['IamRoles'][0]['IamRoleArn']
            print("DWH_ENDPOINT :: ", cluster_endpoints)
            print("DWH_ROLE_ARN :: ", cluster_role_arn)
            break  # if we made it here, no need to retry
        except Exception:
            if i < 19:
                print("Waiting for cluster to come up...")
                time.sleep(30.0)
                continue
            else:
                raise

    print(pretty_redshift_props(cluster_props))

if __name__ == "__main__":
    main()
