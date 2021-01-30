import logging

from .util import get_botocore_config
from cartography.util import aws_handle_regions
from cartography.util import run_cleanup_job
from cartography.util import timeit

logger = logging.getLogger(__name__)


@timeit
@aws_handle_regions
def get_route_table_data(boto3_session, region):
    client = boto3_session.client('ec2', region_name=region, config=get_botocore_config())
    paginator = client.get_paginator('describe_route_tables')
    route_tables = []
    for page in paginator.paginate():
        route_tables.extend(page['RouteTables'])
    return route_tables


def _get_implicit_subnet_association_statement():
    INGEST_IMPLICIT_SUBNET_ASSOCIATIONS_TEMPLATE ="""
    UNWIND {RouteTables} as route_table
    UNWIND route_table.Associations as association
        MATCH (rtable:RouteTable{id: route_table.RouteTableId})
        MATCH (n:EC2Subnet{vpc_id: route_table.VpcId}) WHERE NOT EXISTS((n)-[:SUBNET_ASSOCIATION]->(:RouteTable))
        FOREACH(ignoreMe IN CASE association.Main when true THEN [1] ELSE [] END |
            MERGE(n)-[r:SUBNET_ASSOCIATION]->(rtable)
            ON CREATE SET r.firstseen = timestamp()
            SET r.is_implicit = true,
            r.lastupdated = {aws_update_tag}
        )
    """
    return INGEST_IMPLICIT_SUBNET_ASSOCIATIONS_TEMPLATE


def _get_explicit_subnet_association_statement():
    INGEST_EXPLICIT_SUBNET_ASSOCIATIONS_TEMPLATE ="""
    UNWIND {RouteTables} as route_table
    UNWIND route_table.Associations as association
        MATCH (rtable:RouteTable{id: route_table.RouteTableId})
        MATCH (snet:EC2Subnet{subnetid: association.SubnetId})
        FOREACH(ignoreMe IN CASE association.Main when true THEN [] ELSE [1] END |
            MERGE(snet)-[r:SUBNET_ASSOCIATION]->(rtable)
            ON CREATE SET r.firstseen = timestamp()
            SET r.is_implicit = false,
            r.lastupdated = {aws_update_tag}
        )
    """
    return INGEST_EXPLICIT_SUBNET_ASSOCIATIONS_TEMPLATE


@timeit
def load_subnet_associations(neo4j_session, route_tables, aws_update_tag):
    explicit_subnet_association_statement = _get_explicit_subnet_association_statement()
    implicit_subnet_association_statement = _get_implicit_subnet_association_statement()

    neo4j_session.run(explicit_subnet_association_statement,
        RouteTables=route_tables,
        aws_update_tag=aws_update_tag,
    )

    neo4j_session.run(implicit_subnet_association_statement,
        RouteTables=route_tables,
        aws_update_tag=aws_update_tag,
    )


def _get_routes_statement():
    INGEST_ROUTE_TEMPLATE ="""
    UNWIND {RouteTables} as route_table
    UNWIND route_table.Routes as route_data
        MERGE (new_route:Route{id: route_table.RouteTableId + '|' + 
        Case route_data.DestinationCidrBlock when null then
        (Case route_data.DestinationIpv6CidrBlock when null then route_data.DestinationPrefixListId else
        route_data.DestinationIpv6CidrBlock end)
        else route_data.DestinationCidrBlock end})
        ON CREATE SET new_route.firstseen = timestamp()
        SET new_route.destination_cidr_block = route_data.DestinationCidrBlock,
        new_route.destination_ipv6_cidr_block = route_data.DestinationIpv6CidrBlock,
        new_route.destination_prefix_list_id = route_data.DestinationPrefixListId,
        new_route.egress_only_internet_gateway_id = route_data.EgressOnlyInternetGatewayId,
        new_route.gateway_id = route_data.GatewayId,
        new_route.instance_id = route_data.InstanceId,
        new_route.nat_gateway_id = route_data.NatGatewayId,
        new_route.transit_gateway_id = route_data.TransitGatewayId,
        new_route.network_interface_id = route_data.NetworkInterfaceId,
        new_route.vpc_peering_connection_id = route_data.VpcPeeringConnectionId,
        new_route.lastupdated = {aws_update_tag}

        WITH new_route, route_table
        MATCH (rtable:RouteTable{id: route_table.RouteTableId})
        MERGE (new_route)-[r:ROUTE_OF]->(rtable)
        ON CREATE SET r.firstseen = timestamp()
        SET r.lastupdated = {aws_update_tag}

        WITH new_route
        FOREACH(ignoreMe IN CASE new_route.destination_cidr_block when null THEN [] ELSE [1] END |
            MERGE (new_block:AWSCidrBlock:AWSIpv4CidrBlock{id: new_route.id + '|' + 'cidr'})
            ON CREATE SET new_block.firstseen = timestamp()
            SET new_block.cidr_block = new_route.destination_cidr_block,
            new_block.lastupdated = {aws_update_tag}
            MERGE (new_route)-[r:BLOCK_ASSOCIATION]->(new_block)
            ON CREATE SET r.firstseen = timestamp()
            SET r.lastupdated = {aws_update_tag}
        )

        WITH new_route
        FOREACH(ignoreMe IN CASE new_route.destination_ipv6_cidr_block when null THEN [] ELSE [1] END |
            MERGE (new_block:AWSCidrBlock:AWSIpv6CidrBlock{id: new_route.id + '|' + 'cidr'})
            ON CREATE SET new_block.firstseen = timestamp()
            SET new_block.cidr_block = new_route.destination_ipv6_cidr_block,
            new_block.lastupdated = {aws_update_tag}
            MERGE (new_route)-[r:BLOCK_ASSOCIATION]->(new_block)
            ON CREATE SET r.firstseen = timestamp()
            SET r.lastupdated = {aws_update_tag}
        )

        WITH new_route
        OPTIONAL MATCH (tgw:AWSTransitGateway{tgw_id: new_route.transit_gateway_id})
        WITH tgw, new_route
        FOREACH(ignoreMe IN CASE tgw WHEN null THEN [] ELSE [1] END |
            MERGE (new_route)-[r:ROUTE_TARGET]->(tgw)
            ON CREATE SET r.firstseen = timestamp()
            SET r.lastupdated = {aws_update_tag}
        )

        WITH new_route
        OPTIONAL MATCH (network_interface:NetworkInterface{id: new_route.network_interface_id})
        WITH network_interface, new_route
        FOREACH(ignoreMe IN CASE network_interface WHEN null THEN [] ELSE [1] END |
            MERGE (new_route)-[r:ROUTE_TARGET]->(network_interface)
            ON CREATE SET r.firstseen = timestamp()
            SET r.lastupdated = {aws_update_tag}
        )

        WITH new_route
        OPTIONAL MATCH (igw:InternetGateway{id: new_route.gateway_id})
        WITH igw, new_route
        FOREACH(ignoreMe IN CASE igw WHEN null THEN [] ELSE [1] END |
            MERGE (new_route)-[r:ROUTE_TARGET]->(igw)
            ON CREATE SET r.firstseen = timestamp()
            SET r.lastupdated = {aws_update_tag}
        )
        """
        # ToDo: Add ROUTE_TARGET relation with other types of target of routes.
    return INGEST_ROUTE_TEMPLATE


@timeit
def load_routes(neo4j_session, route_tables, aws_update_tag):
    ingest_statement = _get_routes_statement()
    neo4j_session.run(
        ingest_statement,
        RouteTables=route_tables,
        aws_update_tag=aws_update_tag,
    )


def load_route_tables(neo4j_session, data, region, aws_account_id, aws_update_tag):

    ingest_route_tables = """
    UNWIND {route_tables} as route_table
    MERGE (rtable:RouteTable{id: route_table.RouteTableId})
    ON CREATE SET rtable.firstseen = timestamp()
    SET rtable.lastupdated = {aws_update_tag},
    rtable.region = {region},
    rtable.vpc_id = route_table.VpcId,
    rtable.route_table_id = route_table.RouteTableId
    """

    ingest_route_table_vpc_relations = """
    UNWIND {route_tables} as route_table
    MATCH (rtable:RouteTable{id: route_table.RouteTableId}), (vpc:AWSVpc{id: route_table.VpcId})
    MERGE (rtable)-[r:MEMBER_OF_AWS_VPC]->(vpc)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}
    """

    ingest_route_table_aws_account_relations = """
    UNWIND {route_tables} as route_table
    MATCH (rtable:RouteTable{id: route_table.RouteTableId}), (aws:AWSAccount{id: {aws_account_id}})
    MERGE (aws)-[r:RESOURCE]->(rtable)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}
    """

    neo4j_session.run(ingest_route_tables, route_tables=data, aws_update_tag=aws_update_tag,
        region=region)

    neo4j_session.run(ingest_route_table_vpc_relations, route_tables=data,
        aws_update_tag=aws_update_tag)

    neo4j_session.run(ingest_route_table_aws_account_relations, route_tables=data,
        aws_update_tag=aws_update_tag, aws_account_id=aws_account_id)

    load_routes(neo4j_session, route_tables=data, aws_update_tag=aws_update_tag)

    load_subnet_associations(neo4j_session, route_tables=data, aws_update_tag=aws_update_tag)


@timeit
def cleanup_route_tables(neo4j_session, common_job_parameters):
    run_cleanup_job('aws_ingest_route_tables_cleanup.json', neo4j_session, common_job_parameters)


@timeit
def sync_route_tables(
    neo4j_session, boto3_session, regions, current_aws_account_id, aws_update_tag,
    common_job_parameters,
):
    for region in regions:
        logger.debug("Syncing EC2 route_tables for region '%s' in account '%s'.", region, current_aws_account_id)
        data = get_route_table_data(boto3_session, region)
        load_route_tables(neo4j_session, data, region, current_aws_account_id, aws_update_tag)
    cleanup_route_tables(neo4j_session, common_job_parameters)
