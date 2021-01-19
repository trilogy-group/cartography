import logging
import json

from .util import get_botocore_config
from cartography.util import aws_handle_regions
from cartography.util import run_cleanup_job
from cartography.util import timeit

logger = logging.getLogger(__name__)


@timeit
@aws_handle_regions
def get_dhcp_options_data(boto3_session, region):
    client = boto3_session.client('ec2', region_name=region, config=get_botocore_config())
    paginator = client.get_paginator('describe_dhcp_options')
    dhcp_options = []
    for page in paginator.paginate():
        dhcp_options.extend(page['DhcpOptions'])
    return dhcp_options


def load_dhcp_options(neo4j_session, data, region, aws_account_id, aws_update_tag):

    for dhcp_options in data:
        import_dhcp_options = ("""
        MERGE (dhcp:DHCPOptions{id: {DhcpOptionsId}})
        ON CREATE SET dhcp.firstseen = timestamp()
        SET dhcp.lastupdated = {aws_update_tag},
        dhcp.dhcpoptionsid = {DhcpOptionsId},
        dhcp.region = {region},
        dhcp.dhcp_configurations = '%s'
        WITH dhcp
        MATCH (awsAccount:AWSAccount{id: {aws_account_id}})
        MERGE (awsAccount)-[r:RESOURCE]->(dhcp)
        ON CREATE SET r.firstseen = timestamp()
        SET r.lastupdated = {aws_update_tag}
        """ % json.dumps(dhcp_options['DhcpConfigurations']))
    
        neo4j_session.run(
            import_dhcp_options, DhcpOptionsId=dhcp_options['DhcpOptionsId'], aws_update_tag=aws_update_tag,
            region=region, aws_account_id=aws_account_id,
        )


@timeit
def cleanup_dhcp_options(neo4j_session, common_job_parameters):
    run_cleanup_job('aws_import_dhcp_options_cleanup.json', neo4j_session, common_job_parameters)


@timeit
def sync_dhcp_options(
    neo4j_session, boto3_session, regions, current_aws_account_id, aws_update_tag,
    common_job_parameters,
):
    for region in regions:
        logger.debug("Syncing EC2 internet gateways for region '%s' in account '%s'.", region, current_aws_account_id)
        data = get_dhcp_options_data(boto3_session, region)
        load_dhcp_options(neo4j_session, data, region, current_aws_account_id, aws_update_tag)
    cleanup_dhcp_options(neo4j_session, common_job_parameters)
