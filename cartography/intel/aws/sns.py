import logging

from cartography.util import aws_handle_regions
from cartography.util import run_cleanup_job
from cartography.util import timeit

logger = logging.getLogger(__name__)



@aws_handle_regions
@timeit
def get_topics(boto3_session, region):
    client = boto3_session.client('sns', region_name=region)
    response = client.list_topics()
    return response['Topics']


@timeit
def get_topic_subscriptions(boto3_session, region, topic_arn):
    client = boto3_session.client('sns', region_name=region)
    subscriptions = []
    NextToken = ""
    while NextToken is not None:
        # NextToken is used to get the subscriptions in chunks of 100 
        subs_response = client.list_subscriptions_by_topic(
            TopicArn=topic_arn,
            NextToken=NextToken 
        )
        subscriptions.extend(subs_response["Subscriptions"])
        NextToken = subs_response.get("NextToken")
    
    return subscriptions

@timeit
def load_topics(neo4j_session, topics, region, current_aws_account_id, aws_update_tag):
    query = """
    MERGE (topic:SNSTopic{id: {TopicArn}})
    ON CREATE SET topic.firstseen = timestamp(),
                topic.arn = {TopicArn},
                topic.region = {Region}
    SET topic.lastupdated = {aws_update_tag}
    WITH topic
    MATCH (owner:AWSAccount{id: {AWS_ACCOUNT_ID}})
    MERGE (owner)-[r:RESOURCE]->(topic)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag}
    """


    for topic in topics:
        neo4j_session.run(
            query,
            TopicArn=topic['TopicArn'],
            Region=region,
            aws_update_tag=aws_update_tag,
            AWS_ACCOUNT_ID=current_aws_account_id
        )

        _attach_subscriptions(neo4j_session, topic["Subscriptions"], topic["TopicArn"], region, current_aws_account_id, aws_update_tag)


def _attach_subscriptions(neo4j_session, subscriptions, topic_arn, region, current_aws_account_id, aws_update_tag):
    query = """
    MERGE (subs:SNSSubscription{id: {SubscriptionArn}})
    ON CREATE SET subs.firstseen = timestamp(),
                subs.arn = {SubscriptionArn},
                subs.region = {Region}
    SET subs.lastupdated = {aws_update_tag},
        subs.protocol = {SubscriptionProtocol},
        subs.endpoint = {SubscriptionEndpoint},
        subs.topic_arn = {TopicArn}
    WITH subs
    MATCH (owner:AWSAccount{id: {AWS_ACCOUNT_ID}})
    MATCH (topic:SNSTopic{id: {TopicArn}})
    MERGE (owner)-[r:RESOURCE]->(subs)
    MERGE (subs)-[k:SUBSCRIBED_TO]->(topic)
    ON CREATE SET r.firstseen = timestamp(),
                  k.firstseen = timestamp()
    SET r.lastupdated = {aws_update_tag},
        k.lastupdated = {aws_update_tag}
    """


    for subscription in subscriptions:
        neo4j_session.run(
            query,
            SubscriptionArn = subscription['SubscriptionArn'],
            TopicArn=topic_arn,
            SubscriptionProtocol=subscription['Protocol'],
            SubscriptionEndpoint=subscription['Endpoint'],
            Region=region,
            aws_update_tag=aws_update_tag,
            AWS_ACCOUNT_ID=current_aws_account_id
        )


@timeit
def cleanup(neo4j_session, common_job_parameters):
    run_cleanup_job('aws_import_sns_cleanup.json', neo4j_session, common_job_parameters)


@timeit
def sync(neo4j_session, boto3_session, regions, current_aws_account_id, aws_update_tag, common_job_parameters):
    for region in regions:
        logger.info("Syncing SNS for region '%s' in account '%s'.", region, current_aws_account_id)

        topics = get_topics(boto3_session, region)

        for topic in topics:
            topic["Subscriptions"] = get_topic_subscriptions(boto3_session, region, topic["TopicArn"])


        load_topics(neo4j_session, topics, region, current_aws_account_id, aws_update_tag)

    cleanup(neo4j_session, common_job_parameters)
