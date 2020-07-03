# imports
from datetime import datetime
import uuid
import json
import boto3

from os import environ
from Common.LambdaResponse import LambdaResponse
from Common.DataLogger import DataLogger
from Common.Log import Log

# initial common properties
version = environ.get('version') or 'v1'
environment = environ.get('environment') or 'dev'
eventSource = 'C4C'
invocation_timestamp = datetime.utcnow()

# sqs settings
# TODO: get this from secure place
snsClient = boto3.client('sns')
topicArn = environ.get('topicArn') or ''

# initialze logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    try:
        for record in event['Records']:
            try:
                payload = json.loads(record['body'])
                # transfrom record
                record_to_publish = {
                    "employee": {
                        "objectId": payload['ObjectID'],
                        "crmEmpId": payload['EmployeeID'],
                        "crmBPId": payload['BusinessPartnerID'],
                        "firstName": payload['FirstName'],
                        "middleName": payload['MiddleName'],
                        "lastName": payload['LastName'],
                        "emailId": payload['Email'],
                        "countryCode": payload['CountryCode'],
                        "country": payload['CountryCodeText'],
                        "crmActive": payload['UserLockedIndicator'],
                        "crmTeamId": payload['TeamID'],
                        "crmTeamName": payload['TeamName'],
                        "available": payload['UserAvailableIndicator'],
                        "supportedCountries": payload['SupportedCountries'],
                        "supportedLanguages": payload['SupportedLanguages'],
                        "eventSource": eventSource
                    }
                }
                # publish to Employee Topic
                result = snsClient.publish(
                    TopicArn=topicArn,
                    Message=json.dumps(record_to_publish, ensure_ascii=False).encode('utf-8').decode(),
                    Subject="{env}-employee-translator-{ver}".format(env=environment, ver=version),
                    MessageAttributes={
                        'countryCode': {
                            'DataType': 'String',
                            'StringValue': payload['CountryCode']
                        },
                        'eventSource': {
                            'DataType': 'String',
                            'StringValue': eventSource
                        },
                        'correlationId': {
                            'DataType': 'String',
                            'StringValue': record['messageAttributes']['correlationId']['stringValue'] or str(uuid.uuid4())
                        }
                    })
                DataLogger.info(Log(
                    message="Translated employee record",
                    correlation_id=record['messageAttributes']['correlationId']['stringValue'] or str(uuid.uuid4()),
                    invoked_component="{env}-c4c-employee-translator-{ver}".format(env=environment, ver=version),
                    invoker_agent="{env}-c4c-employee-queue-{ver}".format(env=environment, ver=version),
                    target_idp_application="{env}-c4c-employee-topic-{ver}".format(env=environment, ver=version),
                    processing_stage="employee-translator-created",
                    request_payload=json.dumps(payload),
                    response_details=json.dumps(result),
                    invocation_timestamp=str(invocation_timestamp),
                    original_source_app=eventSource))
            except Exception as error:
                DataLogger.info(Log(
                    message="Error translating employee record",
                    error=str(error),
                    correlation_id=record['messageAttributes']['correlationId']['stringValue'] or str(uuid.uuid4()),
                    invoked_component="{env}-c4c-employee-translator-{ver}".format(env=environment, ver=version),
                    invoker_agent="{env}-c4c-employee-queue-{ver}".format(env=environment, ver=version),
                    target_idp_application="{env}-c4c-employee-topic-{ver}".format(env=environment, ver=version),
                    processing_stage="employee-translator-created",
                    request_payload=json.dumps(payload),
                    invocation_timestamp=str(invocation_timestamp),
                    original_source_app=eventSource))
                raise Exception(error)
        return LambdaResponse.ok_response()
    except Exception as error:
        DataLogger.info(Log(
            message="Error translating employee record",
            error=str(error),
            invoked_component="{env}-c4c-employee-translator-{ver}".format(env=environment, ver=version),
            invoker_agent="{env}-c4c-employee-queue-{ver}".format(env=environment, ver=version),
            target_idp_application="{env}-c4c-employee-topic-{ver}".format(env=environment, ver=version),
            processing_stage="employee-translator-created",
            invocation_timestamp=str(invocation_timestamp),
            original_source_app=eventSource))
        raise Exception(error)
