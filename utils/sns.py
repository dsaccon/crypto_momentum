import os
import boto3


def SNS_call(msg='Hello World', topic='ATG_David_SMS'):
    if "AWS_SNS_KEY" in os.environ and "AWS_SNS_SECRET" in os.environ:
        keyId = os.environ["AWS_SNS_KEY"]
        sKeyId= os.environ["AWS_SNS_SECRET"]
    else:
        raise Exception("Cannot proceed. AWS keys missing")
    if not topic:
        raise Exception("Must provide topic")

    if topic:
        _ARN = 'arn:aws:sns:us-west-2:405366002313:' + topic

    # Create an SNS client
    client = boto3.client(
        "sns",
        aws_access_key_id=keyId,
        aws_secret_access_key=sKeyId,
        region_name="us-west-2"
    )
    # Send SMS to topic subscribers, or number
    if topic:
        client.publish(
            TopicArn=_ARN,
            Message=msg,
            MessageAttributes={
                'AWS.SNS.SMS.SenderID': {
                  'DataType': 'String',
                  'StringValue': 'ATG'
                }
            }
        )
