import logging

from cartography.util import aws_handle_regions
from cartography.util import run_cleanup_job
from cartography.util import timeit

logger = logging.getLogger(__name__)


@timeit
@aws_handle_regions
def get_lambda_layers_data(boto3_session, region):
    """
    Create an Lambda boto3 client and grab all the lambda layers.
    """
    client = boto3_session.client('lambda', region_name=region)
    paginator = client.get_paginator('list_layers')
    lambda_layers = []
    for page in paginator.paginate():
        for each_function in page['Layers']:
            lambda_layers.append(each_function)
    return lambda_layers


@timeit
@aws_handle_regions
def get_lambda_layer_versions_data(boto3_session, region, layer_arn):
    """
    Create an Lambda boto3 client and grab all the versions of a lambda function.
    """
    client = boto3_session.client('lambda', region_name=region)
    paginator = client.get_paginator('list_layer_versions')
    layer_versions = []
    for page in paginator.paginate(LayerName=layer_arn):
        for each_function in page['LayerVersions']:
            layer_versions.append(each_function)
    return layer_versions


def add_layer_to_graph(boto3_session, neo4j_session, lambda_layer, region,
    current_aws_account_id, aws_update_tag):

    ingest_lambda_layers = """
    MERGE (layer:AWSLambdaLayer{id: {LayerArn}})
    ON CREATE SET layer.firstseen = timestamp()
    SET layer.name = {LayerName},
    layer.arn = {LayerArn},
    layer.latest_matching_version_arn = {LatestMatchingVersionArn},
    layer.latest_matching_version_no = {LatestMatchingVersionNo},
    layer.region = {Region},
    layer.lastupdated = {aws_update_tag}
    """
    
    ingest_lambda_layer_versions = """
    UNWIND {LambdaLayerVersions} as lambda_layer_version
    MERGE (lv:AWSLambdaLayerVersion{id: lambda_layer_version.LayerVersionArn})
    ON CREATE SET lv.firstseen = timestamp()
    SET lv.layer_verion_arn = lambda_layer_version.LayerVersionArn,
    lv.version = lambda_layer_version.Version,
    lv.description = lambda_layer_version.Description,
    lv.created_date = lambda_layer_version.CreatedDate,
    lv.compatible_runtimes = lambda_layer_version.CompatibleRuntimes,
    lv.licensce_info = lambda_layer_version.LicenseInfo,
    lv.lastupdated = {aws_update_tag}

    WITH lv
    MATCH (lambda_layer:AWSLambdaLayer{id: {LambdaLayerArn}})
    MERGE (lv)-[r:LAMBDA_LAYER_VERSION_OF]->(lambda_layer)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}
    """

    neo4j_session.run(ingest_lambda_layers,
        LayerName=lambda_layer["LayerName"],
        LayerArn=lambda_layer["LayerArn"],
        LatestMatchingVersionArn=lambda_layer["LatestMatchingVersion"].get("LayerVersionArn"),
        LatestMatchingVersionNo=lambda_layer["LatestMatchingVersion"].get("Version"),
        Region=region,
        aws_update_tag=aws_update_tag,
    )

    versions = get_lambda_layer_versions_data(boto3_session, region, lambda_layer["LayerArn"])

    neo4j_session.run(ingest_lambda_layer_versions,
        LambdaLayerVersions=versions,
        LambdaLayerArn=lambda_layer["LayerArn"],
        aws_update_tag=aws_update_tag,
    )


@timeit
def load_lambda_layers(boto3_session, neo4j_session, data, region, current_aws_account_id, aws_update_tag):
    ingest_lambda_layer_account_statement = """
    MATCH (lambda_layer:AWSLambdaLayer{id: {LayerArn}}), (owner:AWSAccount{id: {AWS_ACCOUNT_ID}})
    MERGE (owner)-[r:RESOURCE]->(lambda_layer)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}
    """

    for lambda_layer in data:
        add_layer_to_graph(boto3_session, neo4j_session, lambda_layer, region, current_aws_account_id,
            aws_update_tag)

        neo4j_session.run(ingest_lambda_layer_account_statement,
            LayerArn=lambda_layer["LayerArn"],
            AWS_ACCOUNT_ID=current_aws_account_id,
            aws_update_tag=aws_update_tag,
        )


@timeit
def cleanup_lambda_layers(neo4j_session, common_job_parameters):
    run_cleanup_job('aws_import_lambda_layer_cleanup.json', neo4j_session, common_job_parameters)


@timeit
def sync_lambda_layers(
    neo4j_session, boto3_session, regions, current_aws_account_id, aws_update_tag,
    common_job_parameters,
):
    for region in regions:
        logger.info("Syncing Lambda Layers for region in '%s' in account '%s'.", region, current_aws_account_id)
        data = get_lambda_layers_data(boto3_session, region)
        load_lambda_layers(boto3_session, neo4j_session, data, region, current_aws_account_id, aws_update_tag)

    cleanup_lambda_layers(neo4j_session, common_job_parameters)


def sync(
        neo4j_session, boto3_session, regions, current_aws_account_id, aws_update_tag,
        common_job_parameters,
):
    sync_lambda_layers(
        neo4j_session, boto3_session, regions, current_aws_account_id, aws_update_tag,
        common_job_parameters,
    )
