import boto3
import configparser
import json
import time

def create_iam_role(aws_session, config):
    '''
    Creates IAM role

    Parameters:
        aws_session: Authenticated AWS session
        config: Configuration object

    Returns:
        IAM role ARN
    '''
    iam_role_name = config.get('IAM_ROLE', 'NAME')

    print(f'Creating IAM role {iam_role_name}')

    # create IAM client
    iam = aws_session.client('iam')

    # create IAM role
    iam.create_role(
        RoleName=iam_role_name,
        Description='Allows Redshift clusters to call AWS services',
        AssumeRolePolicyDocument=json.dumps({
            'Version': '2012-10-17',
            'Statement': [{
                'Effect': 'Allow',
                'Principal': {
                    'Service': 'redshift.amazonaws.com',
                },
                'Action': 'sts:AssumeRole',
            }]
        })
    )

    # attach role policy to IAM role to give role full access to S3
    iam.attach_role_policy(
        RoleName=iam_role_name,
        PolicyArn=config.get('IAM_ROLE', 'POLICY_ARN'),
    )

    # get IAM role ARN
    iam_role_arn = iam.get_role(RoleName=iam_role_name)['Role']['Arn']

    print(f'Created IAM role {iam_role_name} with ARN {iam_role_arn}')

    # return IAM role ARN
    return iam_role_arn

def create_redshift_cluster(iam_role_arn, aws_session, config):
    '''
    Creates Redshift cluster

    Parameters:
        iam_role_arn: ARN of an IAM role that can create a Redshift cluster
        aws_session: Authenticated AWS session
        config: Configuration object

    Returns:
        Redshift cluster URL
    '''
    cluster_identifier = config.get('CLUSTER', 'IDENTIFIER')

    print(f'Creating Redshift cluster {cluster_identifier}')

    # create Redshift client
    redshift = aws_session.client('redshift')

    # create Redshift cluster
    redshift.create_cluster(
        # hardware
        ClusterType=config.get('CLUSTER', 'TYPE'),
        NodeType=config.get('CLUSTER', 'NODE_TYPE'), 
        NumberOfNodes=int(config.get('CLUSTER','NUM_NODES')),

        # identifiers
        ClusterIdentifier=cluster_identifier,
        DBName=config.get('CLUSTER', 'DB'),

        # credentials
        MasterUsername=config.get('CLUSTER', 'DB_USER'),
        MasterUserPassword=config.get('CLUSTER', 'DB_PASSWORD'),

        # roles (to allow cluster to access S3 cluster)
        IamRoles=[iam_role_arn]  
    )

    # wait while Redshift cluster finishes creating
    cluster_endpoint_address = None
    while cluster_endpoint_address is None: 
        response_props = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
        # get Reshift cluster status
        cluster_status = next((k, v) for k,v in response_props.items() if k == 'ClusterStatus')
        if cluster_status[1] == 'available':
            # get Redshift cluster endpoint
            cluster_endpoint = next((k, v) for k,v in response_props.items() if k == 'Endpoint')
            cluster_endpoint_address = cluster_endpoint[1]['Address']
        else:
            print(f'Creating...')

            # wait for 15 seconds
            time.sleep(15)

    print(f'Created Redshift cluster {cluster_identifier} at endpoint address {cluster_endpoint_address}')

    # return 
    return cluster_endpoint_address

def main():
    '''
    Script entry point:
    - Imports config file
    - Creates AWS session
    - Creates IAM role for Redshift to access S3
    - Creates Redshift cluster with IAM role
    - Prints Reshift cluster URL and IAM role ARN

    Parameters:
        None

    Returns:
        None
    '''
    # import config file
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    # create AWS session
    aws_session = boto3.Session(
        aws_access_key_id=config.get('AWS', 'KEY'), 
        aws_secret_access_key=config.get('AWS', 'SECRET'), 
        region_name='us-west-2'
    )

    # create IAM role for Redshift to access S3
    iam_role_arn = create_iam_role(aws_session, config)

    # create Redshift cluster with IAM role
    redshift_cluster_endpoint = create_redshift_cluster(iam_role_arn, aws_session, config)

    print(f'''
        Edit dwh.cfg with the following values:
        [CLUSTER] HOST={redshift_cluster_endpoint}
        [IAM_ROLE] ARN={iam_role_arn}
    ''')

if __name__ == '__main__':
    main()