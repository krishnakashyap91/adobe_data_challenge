from datetime import datetime
from pytz import timezone
import boto3
from botocore.exceptions import ClientError

SENDER = "krishna.kashyap91@gmail.com"
RECIPIENT = "krishna.kashyap91@gmail.com"
AWS_REGION = "us-east-2"
# The subject line for the email.
SUBJECT = "Report Ready!"
CURRENT_DATE = datetime.now(timezone('America/Denver')).strftime('%Y-%m-%d')
FILENAME = f"{CURRENT_DATE}_SearchKeywordPerformance.tab"
# The email body for recipients with non-HTML email clients.
BODY_TEXT = (f"{FILENAME} is avilable in s3://data-adobe-analytics/reports/")
            
# The HTML body of the email.
BODY_HTML = f"""<html>
<head></head>
<body>
  <h1>{FILENAME} is avilable in s3://data-adobe-analytics/reports/</h1>
</body>
</html>
            """  
CHARSET = "UTF-8"
 
def lambda_handler(event, context):
    # Create a new SES resource and specify a region.
    client = boto3.client('ses',region_name=AWS_REGION)
    try:
        # Contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])