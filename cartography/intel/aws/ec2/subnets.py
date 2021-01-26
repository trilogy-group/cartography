import logging

from .util import get_botocore_config
from cartography.util import aws_handle_regions
from cartography.util import run_cleanup_job
from cartography.util import timeit

logger = logging.getLogger(__name__)


@timeit
@aws_handle_regions
def get_subnet_data(boto3_session, region):
    client = boto3_session.client('ec2', region_name=region, config=get_botocore_config())
    paginator = client.get_paginator('describe_subnets')
    subnets = []
    for page in paginator.paginate():
        subnets.extend(page['Subnets'])
    return subnets


@timeit
def load_ipv4_cidr_association(neo4j_session, subnets, aws_update_tag):
    ingest_statement = """
    UNWIND {Subnets} as subnet_data
    MATCH (subnet:EC2Subnet{subnetid: subnet_data.SubnetId})
    MERGE (new_block:AWSCidrBlock:AWSIpv4CidrBlock{id: subnet_data.SubnetId + '|' + subnet_data.CidrBlock})
    ON CREATE SET new_block.firstseen = timestamp()
    SET new_block.cidr_block = subnet_data.CidrBlock,
    new_block.lastupdated = {aws_update_tag}
    WITH subnet, new_block
    MERGE (subnet)-[r:BLOCK_ASSOCIATION]->(new_block)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}
    """
    neo4j_session.run(
        ingest_statement,
        Subnets=subnets,
        aws_update_tag=aws_update_tag,
    )


def _get_ipv6_cidr_association_statement():
    INGEST_IPV6_CIDR_TEMPLATE = """
    UNWIND {Subnets} as subnet_data
    UNWIND subnet_data.Ipv6CidrBlockAssociationSet as block_data
        MATCH (subnet:EC2Subnet{subnetid: subnet_data.SubnetId})
        MERGE (new_block:AWSCidrBlock:AWSIpv6CidrBlock{id: subnet_data.SubnetId + '|' + block_data.Ipv6CidrBlock})
        ON CREATE SET new_block.firstseen = timestamp()
        SET new_block.association_id = block_data.AssociationId,
        new_block.cidr_block = block_data.Ipv6CidrBlock,
        new_block.block_state = block_data.Ipv6CidrBlockState.State,
        new_block.block_state_message = block_data.Ipv6CidrBlockState.StatusMessage,
        new_block.ipv6_pool = block_data.Ipv6Pool,
        new_block.network_border_group = block_data.NetworkBorderGroup,
        new_block.lastupdated = {aws_update_tag}
        WITH subnet, new_block
        MERGE (subnet)-[r:BLOCK_ASSOCIATION]->(new_block)
        ON CREATE SET r.firstseen = timestamp()
        SET r.lastupdated = {aws_update_tag}
    """
    return INGEST_IPV6_CIDR_TEMPLATE


@timeit
def load_ipv6_cidr_association_set(neo4j_session, subnets, aws_update_tag):
    ingest_statement = _get_ipv6_cidr_association_statement()

    neo4j_session.run(
        ingest_statement,
        Subnets=subnets,
        aws_update_tag=aws_update_tag,
    )


def load_subnets(neo4j_session, data, region, aws_account_id, aws_update_tag):

    ingest_subnets = """
    UNWIND {subnets} as subnet
    MERGE (snet:EC2Subnet{subnetid: subnet.SubnetId})
    ON CREATE SET snet.firstseen = timestamp()
    SET snet.lastupdated = {aws_update_tag}, snet.name = subnet.CidrBlock, snet.cidr_block = subnet.CidrBlock,
    snet.available_ip_address_count = subnet.AvailableIpAddressCount, snet.default_for_az = subnet.DefaultForAz,
    snet.map_customer_owned_ip_on_launch = subnet.MapCustomerOwnedIpOnLaunch, snet.outpost_arn = subnet.OutpostArn,
    snet.map_public_ip_on_launch = subnet.MapPublicIpOnLaunch, snet.subnet_arn = subnet.SubnetArn, snet.vpc_id = subnet.VpcId,
    snet.availability_zone = subnet.AvailabilityZone, snet.availability_zone_id = subnet.AvailabilityZoneId,
    snet.subnetid = subnet.SubnetId
    """

    ingest_subnet_vpc_relations = """
    UNWIND {subnets} as subnet
    MATCH (snet:EC2Subnet{subnetid: subnet.SubnetId}), (vpc:AWSVpc{id: subnet.VpcId})
    MERGE (snet)-[r:MEMBER_OF_AWS_VPC]->(vpc)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}
    """

    ingest_subnet_aws_account_relations = """
    UNWIND {subnets} as subnet
    MATCH (snet:EC2Subnet{subnetid: subnet.SubnetId}), (aws:AWSAccount{id: {aws_account_id}})
    MERGE (aws)-[r:RESOURCE]->(snet)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}
    """

    neo4j_session.run(
        ingest_subnets, subnets=data, aws_update_tag=aws_update_tag,
        region=region, aws_account_id=aws_account_id,
    )

    load_ipv4_cidr_association(
        neo4j_session,
        subnets=data,
        aws_update_tag=aws_update_tag,
    )

    load_ipv6_cidr_association_set(
        neo4j_session,
        subnets=data,
        aws_update_tag=aws_update_tag,
    )

    neo4j_session.run(
        ingest_subnet_vpc_relations, subnets=data, aws_update_tag=aws_update_tag,
        region=region, aws_account_id=aws_account_id,
    )
    neo4j_session.run(
        ingest_subnet_aws_account_relations, subnets=data, aws_update_tag=aws_update_tag,
        region=region, aws_account_id=aws_account_id,
    )


@timeit
def cleanup_subnets(neo4j_session, common_job_parameters):
    run_cleanup_job('aws_ingest_subnets_cleanup.json', neo4j_session, common_job_parameters)


@timeit
def sync_subnets(
    neo4j_session, boto3_session, regions, current_aws_account_id, aws_update_tag,
    common_job_parameters,
):
    for region in regions:
        logger.info("Syncing EC2 subnets for region '%s' in account '%s'.", region, current_aws_account_id)
        data = get_subnet_data(boto3_session, region)
        load_subnets(neo4j_session, data, region, current_aws_account_id, aws_update_tag)
    cleanup_subnets(neo4j_session, common_job_parameters)
