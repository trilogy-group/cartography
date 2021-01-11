import logging

from .util import get_botocore_config
from cartography.util import aws_handle_regions
from cartography.util import run_cleanup_job
from cartography.util import timeit

logger = logging.getLogger(__name__)


@timeit
@aws_handle_regions
def get_ec2_volumes(boto3_session, region):
    client = boto3_session.client('ec2', region_name=region, config=get_botocore_config())
    paginator = client.get_paginator('describe_volumes')
    volumes = []
    for page in paginator.paginate():
        volumes.extend(page['Volumes'])
    return volumes



@timeit
def load_ec2_volumes(neo4j_session, data, region, current_aws_account_id, aws_update_tag):
    ingest_volume = """
    MERGE (vol:EC2Volume{id: {VolumeId}})
    ON CREATE SET vol.firstseen = timestamp(),
                  vol.region = {Region}
    SET vol.lastupdated = {aws_update_tag},
        vol.state = {State},
        vol.availability_zone = {AvailabilityZone},
        vol.encryted = {Encrypted},
        vol.kms_key_id = {KmsKeyId},
        vol.size = {Size},
        vol.snapshot_id = {SnapshotId},
        vol.iops = {Iops},
        vol.type = {VolumeType},
        vol.fastrestored = {FastRestored},
        vol.multiattach = {MultiAttachEnabled}
    WITH vol
    MATCH (owner:AWSAccount{id: {AWS_ACCOUNT_ID}})
    MERGE (owner)-[r:RESOURCE]->(vol)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}
    WITH vol
    MATCH (inst:EC2Instance{id: {InstanceId}})
    MERGE (vol)-[d:ATTACHED_TO]->(inst)
    ON CREATE SET d.firstseen = timestamp()
    SET d.lastupdated = {aws_update_tag}
    """
    for volume in data:
        volume_id = volume["VolumeId"]
        instance_ids = [attachment["InstanceId"] for attachment in volume.get("Attachments")] if volume.get("Attachments") else []

        for instance_id in instance_ids:
            neo4j_session.run(
                ingest_volume,
                VolumeId=volume_id,
                AvailabilityZone=volume.get('AvailabilityZone'),
                CreateTime=volume.get('CreateTime'),
                Encrypted=volume.get('Encrypted'),
                KmsKeyId=volume.get('KmsKeyId'),
                OutpostArn=volume.get('OutpostArn'),
                Size=volume.get('Size'),
                SnapshotId=volume.get('SnapshotId'),
                State=volume.get('State'),
                Iops=volume.get('Iops'),
                VolumeType=volume.get('VolumeType'),
                FastRestored=volume.get('FastRestored'),
                MultiAttachEnabled=volume.get('MultiAttachEnabled'),
                InstanceId=instance_id,
                Region=region,
                AWS_ACCOUNT_ID=current_aws_account_id,
                aws_update_tag=aws_update_tag,
            )


 


@timeit
def cleanup_ec2_volumes(neo4j_session, common_job_parameters):
    # TODO: Update Cleanup File
    run_cleanup_job('aws_import_ec2_volumes_cleanup.json', neo4j_session, common_job_parameters)


@timeit
def sync_volumes(neo4j_session, boto3_session, regions, current_aws_account_id, aws_update_tag, common_job_parameters):
    for region in regions:
        logger.debug("Syncing EC2 Volumes for region '%s' in account '%s'.", region, current_aws_account_id)
        data = get_ec2_volumes(boto3_session, region)
        load_ec2_volumes(neo4j_session, data, region, current_aws_account_id, aws_update_tag)
    cleanup_ec2_volumes(neo4j_session, common_job_parameters)

