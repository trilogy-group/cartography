
  
import logging
import os
import tempfile
import base64
from kubernetes import client as kube_client, config

from cartography.util import EKSAuth
from cartography.util import aws_handle_regions
from cartography.util import run_cleanup_job
from cartography.util import timeit

logger = logging.getLogger(__name__)


@timeit
@aws_handle_regions
def get_eks_clusters(boto3_session, region):
    client = boto3_session.client('eks', region_name=region)
    clusters = []
    paginator = client.get_paginator('list_clusters')
    for page in paginator.paginate():
        clusters.extend(page['clusters'])
    return clusters
  

@timeit
def get_eks_describe_cluster(boto3_session, region, cluster_name):
    client = boto3_session.client('eks', region_name=region)
    response = client.describe_cluster(name=cluster_name)
    return response['cluster']

@timeit
def get_list_of_pods_in_a_cluster(boto3_session, region, cluster_name):
    client = boto3_session.client('eks', region_name=region)
    response = client.describe_cluster(name=cluster_name )

    eks = EKSAuth(cluster_name, region)
    host = response["cluster"]["endpoint"]
    cert_data = response["cluster"]["certificateAuthority"]["data"]
    token = eks.get_token()

    ca_file = open(os.path.join(tempfile.gettempdir(), "ca.crt"), "wb")
    ca_file.write(base64.b64decode(cert_data))
    ca_file.close()

    configuration = kube_client.Configuration()
    configuration.api_key["authorization"] = token
    configuration.api_key_prefix['authorization'] = 'Bearer'
    configuration .host =host
    configuration.ssl_ca_cert = os.path.join(tempfile.gettempdir(), "ca.crt")

    v1 = kube_client.CoreV1Api(kube_client.ApiClient(configuration))

    try:
        pods_resp = v1.list_pod_for_all_namespaces(watch=False)
        return pods_resp
    except kube_client.rest.ApiException as e:
        logger.error(("An error occurred with kubernetes: %s"), e)
        if (e.reason == "Unauthorized"):
            logger.warning(f"""\n\n\n***
The AWS principal does not have access to {cluster_name} cluster. Please grant read permissions, on the cluster, to the AWS user configured to run the discovery, see sample below:
Example 'eksctl create iamidentitymapping --cluster {cluster_name} --arn arn:aws:iam::04*******26:user/new.user --group system:authenticated --username new.user' 
*** \n\n\n""")

def transform_list_of_pods_in_a_cluster(response_objects):
    pods = []
    for pod in response_objects.items:
        pod_dict = {}
        pod_dict["name"] = pod.metadata.name
        pod_dict["namespace"] = pod.metadata.namespace
        pod_dict["pod_ip"] = pod.status.pod_ip
        pod_dict["labels"] = pod.metadata.labels
        pod_dict["containers"] = []

        for container in pod.spec.containers:
            container_dict = {}
            container_dict["name"] = container.name
            container_dict["ports"] = [ port.container_port for port in container.ports]
            container_dict["resources"] = {
                "cpu_limit": container.resources.limits.get("cpu"),
                "memory_limit": container.resources.limits.get("memory"),
                "cpu_request": container.resources.requests.get("cpu"),
                "memory_request": container.resources.requests.get("memory")
            }
            pod_dict["containers"].append(container_dict)
        pods.append(pod_dict)

    return pods




@timeit
def load_eks_clusters(neo4j_session, cluster_data, region, current_aws_account_id, aws_update_tag):
    query = """
    MERGE (cluster:EKSCluster{id: {ClusterArn}})
    ON CREATE SET cluster.firstseen = timestamp(),
                cluster.arn = {ClusterArn},
                cluster.name = {ClusterName},
                cluster.region = {Region},
                cluster.created_at = {CreatedAt}
    SET cluster.lastupdated = {aws_update_tag},
        cluster.endpoint = {ClusterEndpoint},
        cluster.endpoint_public_access = {ClusterEndointPublic},
        cluster.rolearn = {ClusterRoleArn},
        cluster.version = {ClusterVersion},
        cluster.platform_version = {ClusterPlatformVersion},
        cluster.status = {ClusterStatus},
        cluster.audit_logging = {ClusterLogging}
    WITH cluster
    MATCH (owner:AWSAccount{id: {AWS_ACCOUNT_ID}})
    MERGE (owner)-[r:RESOURCE]->(cluster)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}
    """

    for cd in cluster_data:
        cluster = cluster_data[cd]["cluster_desc"]

        neo4j_session.run(
            query,
            ClusterArn=cluster['arn'],
            ClusterName=cluster['name'],
            ClusterEndpoint=cluster.get('endpoint'),
            ClusterEndointPublic=cluster.get('resourcesVpcConfig', {}).get('endpointPublicAccess'),
            ClusterRoleArn=cluster.get('roleArn'),
            ClusterVersion=cluster.get('version'),
            ClusterPlatformVersion=cluster.get('platformVersion'),
            ClusterStatus=cluster.get('status'),
            CreatedAt=str(cluster.get('createdAt')),
            ClusterLogging=_process_logging(cluster),
            Region=region,
            aws_update_tag=aws_update_tag,
            AWS_ACCOUNT_ID=current_aws_account_id,
        )


def _attach_eks_labels(neo4j_session, labels, pod_name, cluster, region, current_aws_account_id, aws_update_tag):
    query = """ 
    MERGE(label:EKSLabel{id: {LabelId}})
    ON CREATE SET label.firstseen = timestamp(),
                        label.region = {Region}
    SET label.lastupdated = {aws_update_tag},
        label.key = {Key},
        label.value =  {Value}
    WITH label
    MATCH (owner:EKSPod{id: {PodId}})
    MERGE (owner)-[r:LABELED]->(label)
    SET r.lastupdated = {aws_update_tag},
        r.firstseen = timestamp()
    """

    for key in labels:
        neo4j_session.run(
            query,
            LabelId = key +":"+ labels[key],
            Key = key,
            Value = labels[key],
            Region = region,
            PodId = cluster["arn"] + ":" + pod_name,
            PodName = pod_name,
            aws_update_tag = aws_update_tag
        )

#Identify containers by cluster and pod ids
def _attach_eks_containers(neo4j_session, containers, pod_name, cluster, region, current_aws_account_id, aws_update_tag):
    query ="""
    MERGE (container:EKSContainer{id: {ContainerId}})
    ON CREATE SET container.firstseen = timestamp(),
                  container.name = {ContainerName},
                  container.region = {Region}
    SET container.lastupdated = {aws_update_tag},
        container.cluster = {ClusterName},
        container.ports = {ContainerPorts},
        container.cpu_limit = {CPULimit},
        container.memory_limit = {MemoryLimit},
        container.cpu_request = {CPURequest},
        container.memory_request = {MemoryRequest}
    WITH container
    MATCH (owner:EKSPod{id: {PodId}})
    MERGE (owner)-[r:HOSTS]->(container)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}
    """

    for container in containers:
        neo4j_session.run(
            query,
            ContainerId = cluster["arn"] + ":" + pod_name +":"+ container["name"],
            ClusterArn=cluster["arn"],
            ContainerName=container["name"],
            Region = region,
            aws_update_tag = aws_update_tag,
            ClusterName = cluster["name"],
            ContainerPorts = container["ports"],
            CPULimit = container["resources"]["cpu_limit"],
            MemoryLimit = container["resources"]["memory_limit"],
            CPURequest = container["resources"]["cpu_request"],
            MemoryRequest = container["resources"]["memory_request"],
            PodId = cluster["arn"] + ":" + pod_name,
            PodName = pod_name
        )



@timeit
def load_eks_cluster_pods(neo4j_session, cluster_data, region, current_aws_account_id, aws_update_tag):
    query = """
    MERGE (pod:EKSPod{id: {PodId}})
    ON CREATE SET pod.firstseen = timestamp(),
                  pod.name = {PodName},
                  pod.region = {Region}
    SET pod.lastupdated = {aws_update_tag},
        pod.namespace = {PodNamespace},
        pod.ip = {PodIP},
        pod.cluster = {ClusterName}
    WITH pod
    MATCH (owner:EKSCluster{id: {ClusterArn}})
    MERGE (owner)-[r:RESOURCE]->(pod)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}
    """

    for cd in cluster_data:
        pods = cluster_data[cd]["pods"]
        cluster = cluster_data[cd]["cluster_desc"]

        for pod in pods:
            neo4j_session.run(
                query,
                PodId = cluster["arn"] + ":" + pod["name"],
                ClusterArn=cluster['arn'],
                PodName=pod['name'],
                Region=region,
                PodNamespace=pod['namespace'],
                PodIP=pod['pod_ip'],
                ClusterName=cluster["name"],
                aws_update_tag=aws_update_tag
            )
            _attach_eks_labels(neo4j_session, pod["labels"], pod["name"], cluster, region, current_aws_account_id, aws_update_tag )
            _attach_eks_containers(neo4j_session, pod["containers"], pod["name"], cluster, region, current_aws_account_id, aws_update_tag )


def _process_logging(cluster):
    """
    Parse cluster.logging.clusterLogging to verify if
    at least one entry has audit logging set to Enabled.
    """
    logging = False
    cluster_logging = cluster.get('logging', {}).get('clusterLogging')
    if cluster_logging:
        logging = any(filter(lambda x: 'audit' in x['types'] and x['enabled'], cluster_logging))
    return logging


@timeit
def cleanup(neo4j_session, common_job_parameters):
    run_cleanup_job('aws_import_eks_cleanup.json', neo4j_session, common_job_parameters)


@timeit
def sync(neo4j_session, boto3_session, regions, current_aws_account_id, aws_update_tag, common_job_parameters):
    for region in regions:
        logger.info("Syncing EKS for region '%s' in account '%s'.", region, current_aws_account_id)

        clusters = get_eks_clusters(boto3_session, region)

        cluster_data = {}
        for cluster_name in clusters:
            cluster_data[cluster_name] = {}
            cluster_data[cluster_name]["cluster_desc"] = get_eks_describe_cluster(boto3_session, region, cluster_name)
            # pod_responses = get_list_of_pods_in_a_cluster(boto3_session, region, cluster_name)
            # cluster_data[cluster_name]["pods"] = transform_list_of_pods_in_a_cluster(pod_responses)


        load_eks_clusters(neo4j_session, cluster_data, region, current_aws_account_id, aws_update_tag)
        # load_eks_cluster_pods(neo4j_session, cluster_data, region, current_aws_account_id, aws_update_tag)

    cleanup(neo4j_session, common_job_parameters)
    