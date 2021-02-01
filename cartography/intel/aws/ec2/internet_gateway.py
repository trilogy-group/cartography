import logging
from string import Template

from .util import get_botocore_config
from cartography.util import aws_handle_regions
from cartography.util import run_cleanup_job
from cartography.util import timeit

logger = logging.getLogger(__name__)


@timeit
@aws_handle_regions
def get_internet_gateway_data(boto3_session, region):
    client = boto3_session.client('ec2', region_name=region, config=get_botocore_config())
    paginator = client.get_paginator('describe_internet_gateways')
    internet_gateways = []
    for page in paginator.paginate():
        internet_gateways.extend(page['InternetGateways'])
    return internet_gateways

@timeit
def load_attachments(neo4j_session, internet_gateways, aws_update_tag):
    ingest_internet_gateway_vpc_relations = """
    UNWIND {InternetGateways} as internet_gateway
    UNWIND internet_gateway.Attachments as attachment
    MATCH (igw:InternetGateway{id: internet_gateway.InternetGatewayId}),
        (vpc:AWSVpc{id: attachment.VpcId})
    MERGE (igw)-[r:ATTACHED_TO]->(vpc)
    ON CREATE SET r.firstseen = timestamp()
    SET r.state = attachment.State,
    r.lastupdated = {aws_update_tag}
    """

    neo4j_session.run(
        ingest_internet_gateway_vpc_relations,
        InternetGateways=internet_gateways,
        aws_update_tag=aws_update_tag,
    )


def load_internet_gateways(neo4j_session, data, region, aws_account_id, aws_update_tag):

    ingest_internet_gateways = """
    UNWIND {internet_gateways} as internet_gateway
    MERGE (igw:InternetGateway{id: internet_gateway.InternetGatewayId})
    ON CREATE SET igw.firstseen = timestamp()
    SET igw.lastupdated = {aws_update_tag},
    igw.region = {region},
    igw.internetgatewayid = internet_gateway.InternetGatewayId
    """

    ingest_internet_gateway_aws_account_relations = """
    UNWIND {internet_gateways} as internet_gateway
    MATCH (igw:InternetGateway{id: internet_gateway.InternetGatewayId}), (aws:AWSAccount{id: {aws_account_id}})
    MERGE (aws)-[r:RESOURCE]->(igw)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}
    """

    neo4j_session.run(
        ingest_internet_gateways, internet_gateways=data, aws_update_tag=aws_update_tag,
        region=region
    )

    neo4j_session.run(
        ingest_internet_gateway_aws_account_relations, internet_gateways=data, aws_update_tag=aws_update_tag,
        aws_account_id=aws_account_id
    )

    load_attachments(
        neo4j_session,
        internet_gateways=data,
        aws_update_tag=aws_update_tag,
    )


@timeit
def cleanup_internet_gateways(neo4j_session, common_job_parameters):
    run_cleanup_job('aws_ingest_internet_gateways_cleanup.json', neo4j_session, common_job_parameters)


@timeit
def sync_internet_gateways(
    neo4j_session, boto3_session, regions, current_aws_account_id, aws_update_tag,
    common_job_parameters,
):
    for region in regions:
        logger.info("Syncing EC2 internet gateways for region '%s' in account '%s'.", region, current_aws_account_id)
        data = get_internet_gateway_data(boto3_session, region)
        load_internet_gateways(neo4j_session, data, region, current_aws_account_id, aws_update_tag)
    cleanup_internet_gateways(neo4j_session, common_job_parameters)
