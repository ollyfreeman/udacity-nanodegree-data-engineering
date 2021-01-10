import boto3
import configparser
import json
import time

def delete_iam_role(aws_session, config):
    '''
    Deletes IAM role

    Parameters:
        aws_session: Authenticated AWS session
        config: Configuration object

    Returns:
        None
    '''
    iam_role_name = config.get('IAM_ROLE', 'NAME')

    print(f'Deleting IAM role {iam_role_name}')

    # create IAM client
    iam = aws_session.client('iam')

    # detch role policy from IAM
    iam.detach_role_policy(
        RoleName=iam_role_name,
        PolicyArn=config.get('IAM_ROLE', 'POLICY_ARN'),
    )

    # delete IAM role
    iam.delete_role(RoleName=iam_role_name)

    print(f'Deleted IAM role {iam_role_name}')

def delete_redshift_cluster(aws_session, config):
    '''
    Deletes Redshift cluster

    Parameters:
        aws_session: Authenticated AWS session
        config: Configuration object

    Returns:
        None
    '''
    cluster_identifier = config.get('CLUSTER', 'IDENTIFIER')

    print(f'Deleting Redshift cluster {cluster_identifier}')

    # create Redshift client
    redshift = aws_session.client('redshift')

    # delete Redshift cluster
    redshift.delete_cluster(
        ClusterIdentifier=cluster_identifier, 
        SkipFinalClusterSnapshot=True
    )

    print(f'Deleted Redshift cluster {cluster_identifier}')

def main():
    '''
    Script entry point:
    - Imports config file
    - Creates AWS session
    - Deletes Redshift cluster
    - Deletes IAM role

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

    # delete Redshift cluster
    delete_redshift_cluster(aws_session, config)

    # delete IAM role
    delete_iam_role(aws_session, config)

if __name__ == '__main__':
    main()