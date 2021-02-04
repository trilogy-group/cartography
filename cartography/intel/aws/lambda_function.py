import logging

from cartography.util import aws_handle_regions
from cartography.util import run_cleanup_job
from cartography.util import timeit
from string import Template

logger = logging.getLogger(__name__)


@timeit
@aws_handle_regions
def get_lambda_data(boto3_session, region):
    """
    Create an Lambda boto3 client and grab all the lambda functions.
    """
    client = boto3_session.client('lambda', region_name=region)
    paginator = client.get_paginator('list_functions')
    lambda_functions = []
    for page in paginator.paginate():
        for each_function in page['Functions']:
            lambda_functions.append(each_function)
    return lambda_functions


@timeit
@aws_handle_regions
def get_lambda_versions_data(boto3_session, region, lambda_arn):
    """
    Create an Lambda boto3 client and grab all the versions of a lambda function.
    """
    client = boto3_session.client('lambda', region_name=region)
    paginator = client.get_paginator('list_versions_by_function')
    lambda_versions = []
    for page in paginator.paginate(FunctionName=lambda_arn):
        for each_function in page['Versions']:
            lambda_versions.append(each_function)
    return lambda_versions


@timeit
@aws_handle_regions
def get_lambda_tags(boto3_session, function_arn, region):
    """
    Create an Lambda boto3 client and grab all the tags for a lambda function.
    """
    client = boto3_session.client('lambda', region_name=region)
    response = client.list_tags(Resource=function_arn)
    return response["Tags"]

def _get_lambda_function_query(lambda_version):
    ingest_lambda_functions = Template("""
    MERGE (lambda:$lambda_label{id: {Arn}})
    ON CREATE SET lambda.firstseen = timestamp()
    SET lambda.name = {LambdaName},
    lambda.modifieddate = {LastModified},
    lambda.arn = {Arn},
    lambda.role = {Role},
    lambda.runtime = {Runtime},
    lambda.handler = {Handler},
    lambda.package_type = {PackageType},
    lambda.tracing_config_mode = {TracingConfigMode},
    lambda.vpc_id = {VpcId},
    lambda.region = {Region},
    lambda.version = {Version},
    lambda.description = {Description},
    lambda.timeout = {Timeout},
    lambda.memory = {MemorySize},
    lambda.lastupdated = {aws_update_tag}

    WITH lambda
    UNWIND {LambdaLayers} as layer
        MATCH (lv:AWSLambdaLayerVersion{id: layer.Arn})
        MERGE (lv)-[r:ATTACHED_TO]->(lambda)
        ON CREATE SET r.firstseen = timestamp()
        SET r.lastupdated = {aws_update_tag}

    WITH lambda
    MATCH (vpc:AWSVpc{id: {VpcId}})
    MERGE (vpc)<-[r:MEMBER_OF_AWS_VPC]-(lambda)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}

    WITH lambda
    UNWIND {SubnetIds} as subnet_id
        MATCH (subnet:EC2Subnet{subnetid: subnet_id})
        MERGE (subnet)<-[r:PART_OF_SUBNET]-(lambda)
        ON CREATE SET r.firstseen = timestamp()
        SET r.lastupdated = {aws_update_tag}

    WITH lambda
    UNWIND {SecurityGroupIds} as sgp_id
        MATCH (sgp:EC2SecurityGroup{id: sgp_id})
        MERGE (sgp)<-[r:MEMBER_OF_EC2_SECURITY_GROUP]-(lambda)
        ON CREATE SET r.firstseen = timestamp()
        SET r.lastupdated = {aws_update_tag}

    WITH lambda
    MATCH (role:AWSPrincipal{arn: {Role}})
    MERGE (lambda)-[r:STS_ASSUME_ROLE_ALLOW]->(role)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}
    """)

    lambda_label = "AWSLambda"
    if lambda_version != "$LATEST":
        lambda_label = "AWSLambdaVersion"

    return ingest_lambda_functions.safe_substitute(lambda_label=lambda_label)


def add_lambda_to_graph(neo4j_session, lambda_function, region,
    current_aws_account_id, aws_update_tag):
    ingest_lambda_functions = _get_lambda_function_query(lambda_function["Version"])

    vpc_config = {} if not lambda_function.get("VpcConfig") else lambda_function.get("VpcConfig")
    tracing_config = {} if not lambda_function.get("TracingConfig") else lambda_function.get("TracingConfig")
    neo4j_session.run(
        ingest_lambda_functions,
        LambdaName=lambda_function["FunctionName"],
        Arn=lambda_function["FunctionArn"],
        Runtime=lambda_function.get("Runtime"),
        Handler=lambda_function.get("Handler"),
        PackageType=lambda_function.get("PackageType"),
        TracingConfigMode=tracing_config.get("Mode"),
        Role=lambda_function["Role"],
        Description=lambda_function["Description"],
        Timeout=lambda_function["Timeout"],
        MemorySize=lambda_function["MemorySize"],
        VpcId=vpc_config.get("VpcId"),
        SubnetIds=vpc_config.get("SubnetIds"),
        SecurityGroupIds=vpc_config.get("SecurityGroupIds"),
        LastModified=lambda_function["LastModified"],
        Version=lambda_function["Version"],
        LambdaLayers=lambda_function.get("Layers"),
        Region=region,
        AWS_ACCOUNT_ID=current_aws_account_id,
        aws_update_tag=aws_update_tag,
    )


@timeit
def load_lambda_tags(boto3_session, neo4j_session, function_arn, region, current_aws_account_id, aws_update_tag):
    ingest_lambda_tags = """
    UNWIND {Tags} as input_tag
        MATCH (lambda:AWSLambda{id: {FunctionArn}})
        MERGE(aws_tag:AWSTag:Tag{id:input_tag.Key + ":" + input_tag.Value})
        ON CREATE SET aws_tag.firstseen = timestamp()

        SET aws_tag.lastupdated = {aws_update_tag},
        aws_tag.key = input_tag.Key,
        aws_tag.value =  input_tag.Value,
        aws_tag.region = {Region}

        MERGE (lambda)-[r:TAGGED]->(aws_tag)
        SET r.lastupdated = {aws_update_tag},
        r.firstseen = timestamp()
    """
    tags = get_lambda_tags(boto3_session, function_arn, region)
    lambda_tags = [dict(Key=key, Value=tags[key]) for key in tags.keys()]

    neo4j_session.run(ingest_lambda_tags,
        Tags=lambda_tags,
        FunctionArn=function_arn,
        AWS_ACCOUNT_ID=current_aws_account_id,
        Region=region,
        aws_update_tag=aws_update_tag
    )


def _get_lambda_version_attachment_query():
    ingest_lambda_version_attachment = """
    MATCH (lambda:AWSLambda{id: {FunctionArn}})
    WITH lambda
    UNWIND {LambdaVersions} as lambda_version
        MATCH (lv:AWSLambdaVersion{id: lambda_version.FunctionArn})
        MERGE (lv)-[r:LAMBDA_VERSION_OF]->(lambda)
        ON CREATE SET r.firstseen = timestamp()
        SET r.lastupdated = {aws_update_tag}
    """
    return ingest_lambda_version_attachment


@timeit
def load_lambda_functions(boto3_session, neo4j_session, data, region, current_aws_account_id, aws_update_tag):

    ingest_lambda_version_attachment = _get_lambda_version_attachment_query()

    ingest_lambda_account_statement = """
    MATCH (lambda:AWSLambda{id: {FunctionArn}})
    WITH lambda
    MATCH (owner:AWSAccount{id: {AWS_ACCOUNT_ID}})
    MERGE (owner)-[r:RESOURCE]->(lambda)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}
    """

    for lambda_function in data:
        lambda_function["Version"] = "$LATEST"
        # Add the Latest Version with Unqualified ARN: arn without the suffix
        add_lambda_to_graph(neo4j_session, lambda_function, region, current_aws_account_id,
            aws_update_tag)

        load_lambda_tags(boto3_session, neo4j_session, lambda_function["FunctionArn"],
            region, current_aws_account_id, aws_update_tag)

        neo4j_session.run(ingest_lambda_account_statement,
            FunctionArn=lambda_function["FunctionArn"],
            AWS_ACCOUNT_ID=current_aws_account_id,
            aws_update_tag=aws_update_tag,
        )

        versions = get_lambda_versions_data(boto3_session, region, lambda_function["FunctionArn"])
        # Filter out the Latest as it has already been added!
        versions = [version for version in versions if version["Version"] != "$LATEST"]

        # Add all the versions to the Graph!
        for version in versions:
            add_lambda_to_graph(neo4j_session, version, region, current_aws_account_id,
                aws_update_tag)

        # Add a relation between all the versions and the Latest Version
        neo4j_session.run(ingest_lambda_version_attachment,
            LambdaVersions=versions,
            FunctionArn=lambda_function["FunctionArn"],
            aws_update_tag=aws_update_tag
        )


@timeit
def cleanup_lambda(neo4j_session, common_job_parameters):
    run_cleanup_job('aws_import_lambda_cleanup.json', neo4j_session, common_job_parameters)


@timeit
def sync_lambda_functions(
    neo4j_session, boto3_session, regions, current_aws_account_id, aws_update_tag,
    common_job_parameters,
):
    for region in regions:
        logger.info("Syncing Lambda for region in '%s' in account '%s'.", region, current_aws_account_id)
        data = get_lambda_data(boto3_session, region)
        load_lambda_functions(boto3_session, neo4j_session, data, region, current_aws_account_id, aws_update_tag)

    cleanup_lambda(neo4j_session, common_job_parameters)


def sync(
        neo4j_session, boto3_session, regions, current_aws_account_id, aws_update_tag,
        common_job_parameters,
):
    sync_lambda_functions(
        neo4j_session, boto3_session, regions, current_aws_account_id, aws_update_tag,
        common_job_parameters,
    )
