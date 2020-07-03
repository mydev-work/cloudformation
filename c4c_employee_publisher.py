# imports
from datetime import datetime
import requests
import uuid
import json
import logging
import boto3

from os import environ
from Common.LambdaResponse import LambdaResponse
from Common.DataLogger import DataLogger
from Common.Log import Log
from Common.ParameterStoreHelper import ParameterStoreHelper

# initial common properties
version = environ.get('version') or 'v1'
environment = environ.get('environment') or 'dev'
eventSource = 'C4C'
invocation_timestamp = datetime.utcnow()
odataHeaders = {'Content-Type': 'application/json'}

# sqs settings
sqsClient = boto3.client('sqs')
queueUrl = environ.get('queueUrl') or ''

# initialze logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    try:
        # get odata settings
        odataDoman = ParameterStoreHelper.get_value_from_ssm_parameter_store("/{env}/aws_c4c/employee_publisher/{ver}/odata_doman".format(env=environment, ver=version))
        odataUserName = ParameterStoreHelper.get_value_from_ssm_parameter_store("/{env}/aws_c4c/employee_publisher/{ver}/odata_userName".format(env=environment, ver=version))
        odataPassword = ParameterStoreHelper.get_value_from_ssm_parameter_store("/{env}/aws_c4c/employee_publisher/{ver}/odata_password".format(env=environment, ver=version), True)
        odataUrlFormat = ParameterStoreHelper.get_value_from_ssm_parameter_store("/{env}/aws_c4c/employee_publisher/{ver}/odata_urlFormat".format(env=environment, ver=version))

        for record in event['Records']:
            try:
                payload = json.loads(record['body'])
                # request c4c for the details
                result = requests.get(url=odataUrlFormat.format(domain=odataDoman, objectId=payload["data"]["root-entity-id"]), headers=odataHeaders, auth=(odataUserName, odataPassword))
                if(result.status_code != 200):
                    raise Exception(result)
                jsonResult = result.json()
                record_to_publish = {
                    "ObjectID": jsonResult['d']['results']['ObjectID'],
                    "EmployeeID": jsonResult['d']['results']['EmployeeID'],
                    "BusinessPartnerID": jsonResult['d']['results']['BusinessPartnerID'],
                    "FirstName": jsonResult['d']['results']['FirstName'],
                    "MiddleName": jsonResult['d']['results']['MiddleName'],
                    "LastName": jsonResult['d']['results']['LastName'],
                    "Email": jsonResult['d']['results']['Email'],
                    "CountryCode": jsonResult['d']['results']['CountryCode'],
                    "CountryCodeText": jsonResult['d']['results']['CountryCodeText'],
                    "UserLockedIndicator": str(jsonResult['d']['results']['UserLockedIndicator']),
                    "TeamID": getattr(jsonResult['d']['results'], 'TeamID', ''),
                    "TeamName": getattr(jsonResult['d']['results'], 'TeamName', ''),
                    "UserAvailableIndicator": str(getattr(jsonResult['d']['results'], 'UserAvailableIndicator', '')),
                    "SupportedCountries": getattr(jsonResult['d']['results'], 'SupportedCountries', ''),
                    "SupportedLanguages": getattr(jsonResult['d']['results'], 'SupportedLanguages', ''),
                }
                # publish to Employee Queue
                result = sqsClient.send_message(
                    QueueUrl=queueUrl,
                    MessageBody=json.dumps(record_to_publish, ensure_ascii=False).encode('utf-8').decode(),
                    MessageAttributes={
                        'countryCode': {
                            'DataType': 'String',
                            'StringValue': jsonResult['d']['results']['CountryCode']
                        },
                        'eventSource': {
                            'DataType': 'String',
                            'StringValue': eventSource
                        },
                        'correlationId': {
                            'DataType': 'String',
                            'StringValue': payload['event-id'] or str(uuid.uuid4())
                        }
                    })
                DataLogger.info(Log(
                    message="Published employee record",
                    correlation_id=payload['event-id'] or str(uuid.uuid4()),
                    invoked_component="{env}-c4c-employee-event-publisher-{ver}".format(env=environment, ver=version),
                    invoker_agent="{env}-c4c-employee-event-queue-{ver}".format(env=environment, ver=version),
                    target_idp_application="{env}-c4c-employee-queue-{ver}".format(env=environment, ver=version),
                    processing_stage="employee-publisher-created",
                    request_payload=json.dumps(payload),
                    response_details=json.dumps(result),
                    invocation_timestamp=str(invocation_timestamp),
                    original_source_app=eventSource))
            except Exception as error:
                DataLogger.info(Log(
                    message="Error publishing employee record",
                    error=str(error),
                    correlation_id=payload['event-id'] or str(uuid.uuid4()),
                    invoked_component="{env}-c4c-employee-event-publisher-{ver}".format(env=environment, ver=version),
                    invoker_agent="{env}-c4c-employee-event-queue-{ver}".format(env=environment, ver=version),
                    target_idp_application="{env}-c4c-employee-queue-{ver}".format(env=environment, ver=version),
                    processing_stage="employee-publisher-created",
                    request_payload=json.dumps(payload),
                    invocation_timestamp=str(invocation_timestamp),
                    original_source_app=eventSource))
                raise Exception(error)
        return LambdaResponse.ok_response()
    except Exception as error:
        DataLogger.info(Log(
            message="Error publishing employee record",
            error=str(error),
            invoked_component="{env}-c4c-employee-event-publisher-{ver}".format(env=environment, ver=version),
            invoker_agent="{env}-c4c-employee-event-queue-{ver}".format(env=environment, ver=version),
            target_idp_application="{env}-c4c-employee-queue-{ver}".format(env=environment, ver=version),
            processing_stage="employee-publisher-created",
            invocation_timestamp=str(invocation_timestamp),
            original_source_app=eventSource))
        raise Exception(error)
