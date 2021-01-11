import logging

from cartography.util import aws_handle_regions
from cartography.util import run_cleanup_job
from cartography.util import timeit

logger = logging.getLogger(__name__)


@timeit
@aws_handle_regions
def get_elasticache_clusters(boto3_session, region):
    client = boto3_session.client('elasticache', region_name=region)
    paginator =  client.get_paginator("describe_cache_clusters")
    cache_clusters = []
    for page in paginator.paginate():
        cache_clusters.extend(page['CacheClusters'])
    return cache_clusters

def transform_elasticache_clusters(response_objects):
    """
    Process the Elasticache clusters response objects and return a flattened list of cache clusters with all the necessary fields
    we need to load it into Neo4j
    :param response_objects: The return data from get_elasticache_clusters()
    :return: A list of cache clusters
    """

    cache_clusters = []
    for cache_cluster in response_objects:
        cache_cluster_dict = {}
        cache_cluster_dict["name"] = cache_cluster["CacheClusterId"]
        cache_cluster_dict["no_of_nodes"] = cache_cluster["NumCacheNodes"]
        cache_cluster_dict["security_groups"] = cache_cluster["SecurityGroups"]
        cache_cluster_dict["engine"] = cache_cluster["Engine"]
        cache_cluster_dict["engine_version"] = cache_cluster["EngineVersion"]
        cache_cluster_dict["configuration_endpoint"] = cache_cluster.get("ConfigurationEndpoint")
        cache_cluster_dict["node_type"] = cache_cluster["CacheNodeType"]
        cache_cluster_dict["arn"] = cache_cluster["ARN"]
        cache_cluster_dict["preferred_availability_zone"] = cache_cluster["PreferredAvailabilityZone"]
        cache_cluster_dict["cache_subnet_group_name"] = cache_cluster["CacheSubnetGroupName"]
        cache_cluster_dict["transit_encryption_enabled"] = cache_cluster["TransitEncryptionEnabled"]
        cache_cluster_dict["at_rest_encryption_enabled"] = cache_cluster["AtRestEncryptionEnabled"]

        cache_clusters.append(cache_cluster_dict)

    return cache_clusters


def load_elasticache_clusters(neo4j_session, cache_clusters, region, current_aws_account_id, aws_update_tag):
    query = """
    MERGE (cache:Elasticache{id: {ClusterArn}})
    ON CREATE SET cache.firstseen = timestamp(),
                cache.arn = {ClusterArn},
                cache.name = {ClusterName},
                cache.region = {Region}
    SET cache.lastupdated = {aws_update_tag},
        cache.endpoint = {ConfigurationEndpoint},
        cache.node_quantity = {NodeQuantity},
        cache.node_type = {NodeType},
        cache.engine = {ClusterEngine},
        cache.engine_version = {EngineVersion},
        cache.security_groups = {SecurityGroupIds},
        cache.Preferred_AZ = {PreferredAZ},
        cache.transit_encryption_enabled = {TransitEncryptionEnabled},
        cache.at_rest_encryption_enabled = {AtRestEncryptionEnabled}
    WITH cache
    MATCH (owner:AWSAccount{id: {AWS_ACCOUNT_ID}})
    MERGE (owner)-[r:RESOURCE]->(cache)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}

    """

    for cluster in cache_clusters:
        security_group_ids = []

        for security_group in cluster["security_groups"]:
            security_group_ids.append(security_group["SecurityGroupId"])

        neo4j_session.run(
            query,
            ClusterArn=cluster['arn'],
            ClusterName=cluster['name'],
            SecurityGroupIds=security_group_ids ,
            NodeQuantity=cluster['no_of_nodes'],
            NodeType=cluster['node_type'],
            ClusterEngine=cluster['engine'],
            EngineVersion=cluster['engine_version'],
            PreferredAZ=cluster['preferred_availability_zone'],
            CacheSubnetGroupName=cluster['cache_subnet_group_name'],
            TransitEncryptionEnabled=cluster['transit_encryption_enabled'],
            AtRestEncryptionEnabled=cluster['at_rest_encryption_enabled'],
            ConfigurationEndpoint=cluster.get('configuration_endpoint'),
            Region=region,
            aws_update_tag=aws_update_tag,
            AWS_ACCOUNT_ID=current_aws_account_id
        )


@timeit
def cleanup(neo4j_session, common_job_parameters):
    run_cleanup_job('aws_import_elasticache_clusters_cleanup.json', neo4j_session, common_job_parameters)


@timeit
def sync(neo4j_session, boto3_session, regions, current_aws_account_id, aws_update_tag, common_job_parameters):
    for region in regions:
        logger.info("Syncing Elasticache clusters for region '%s' in account '%s'.", region, current_aws_account_id)

        cache_clusters_responses = get_elasticache_clusters(boto3_session, region)
        cache_clusters = transform_elasticache_clusters(cache_clusters_responses)
        
        load_elasticache_clusters(neo4j_session, cache_clusters, region, current_aws_account_id, aws_update_tag)


    cleanup(neo4j_session, common_job_parameters)




