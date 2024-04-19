import json
import time
import boto3

# Define the email source
EMAIL_SOURCE = ''
client = boto3.client("ses")



def fdp_detail(payload):
    fdp_id = payload['parameters'][0]['value']
    if fdp_id == 'x-857':
        return {
            "response": {
                "fdpId": fdp_id,
                "createdDate": "21-Jul-2023",
                "lastActivityDate": "25-Jul-2023",
                "status": "Open",
                "damageType": "Corrosion"
            }
        }
    elif fdp_id == 'x-006':
        return {
            "response": {
                "fdpId": fdp_id,
                "createdDate": "20-May-2023",
                "lastActivityDate": "23-Jul-2023",
                "status": "Open",
                "damageType": "Corrosion"
            }
        }
    elif fdp_id == 'x-999':
        return {
            "response": {
                "fdpId": fdp_id,
                "createdDate": "10-Jan-2023",
                "lastActivityDate": "31-Feb-2023",
                "status": "Completed",
                "damageType": "Corrosion"
            }
        }
    else:
        return {
            "response": {
                "fdpId": fdp_id,
                "createdDate": "18-Apr-2023",
                "lastActivityDate": "20-Apr-2023",
                "status": "Open",
                "damageType": "Corrosion"
            }
        }

def open_fdps():
    return {
        "response": [
            {
                "fdpId": "x-006",
                "facEngrId": "1239456843",
                "fdpStatus": "Open"
            },
            {
                "fdpId": "x-857",
                "facEngrId": "1239456843",
                "fdpStatus": "Open"
            },
            {
                "fdpId": "x-334",
                "facEngrId": "1239456843",
                "fdpStatus": "Open"
            }
        ]
    }

def outstanding_paperwork(parameters):
    for parameter in parameters:
        if parameter.get("value", None) == "x-857":
            return {
                "response": {
                    "pendingDocuments": "Vendor Drawing"
                }
            }
        elif parameter.get("value", None) == "x-006":
            return {
                "response": {
                    "pendingDocuments": "Vendor Drawing"
                }
            }
        else:
            return {
                "response": {
                    "pendingDocuments": ""
                }
            }

def send_email(recipients, subject, body, source=EMAIL_SOURCE):
    client = boto3.client("ses")

    # Ensure recipients is a list
    if not isinstance(recipients, list):
        recipients = [recipients]

    response = client.send_email(
        Destination={"ToAddresses": recipients},
        Message={
            "Body": {
                "Text": {
                    "Charset": "UTF-8",
                    "Data": body
                }
            },
            "Subject": {
                "Charset": "UTF-8",
                "Data": subject
            },
        },
        Source=source)
    return response

def send_reminder(payload):
    print(payload)
    # Extract data from payload
    data = {
        "fdpId": "",
        "pendingDocuments": "",
        "pendingDocumentsRequirements": ""
    }
    for p in payload['requestBody']['content']['application/json']['properties']:
        print(p)
        if p['type'] == 'string':
            data[p['name']] = p['value']
        else:
            data[p['name']] = json.loads(p['value'])

    # Compose email body
    email_body = "<br/>Please provide the following documents for your fdp " + str(data['fdpId']) + ": <br/><ul>"
    for d in data['pendingDocuments']:
        for r in data['pendingDocumentsRequirements']:
            if d in r:
                email_body += "<li><b>" + str(d) + "</b>: " + str(r) + "</li>"
    email_body += "</ul>Thanks<br/><br/>"
    email_body += "Best regards,<br/>Gen AI assistant"
    print(email_body)

    # Send email
    recipients = ["taojfeng@amazon.com"]  # Update this to the desired recipient email
    subject = "Reminder: Outstanding Documents Required for Your fdp"
    send_email(recipients, subject, email_body)

    return {
        "response": {
            "email_content": email_body,
            "sendReminderTrackingId": "50e8400-e29b-41d4-a716-446655440000",
            "sendReminderStatus": "InProgress"
        }
    }



def lambda_handler(event, context):
    action = event['actionGroup']
    api_path = event['apiPath']

    if api_path == '/fdps':
        body = open_fdps()
    elif api_path == '/fdps/{fdpId}/outstanding-documents':
        parameters = event['parameters']
        body = outstanding_documents(parameters)
    elif api_path == '/fdps/{fdpId}/detail':
        body = fdp_detail(event)
    elif api_path == '/send-reminder':
        body = send_reminder(event)
    else:
        body = {"{}::{} is not a valid api, try another one.".format(action, api_path)}

    response_body = {
        'application/json': {
            'body': str(body)
        }
    }

    action_response = {
        'actionGroup': event['actionGroup'],
        'apiPath': event['apiPath'],
        'httpMethod': event['httpMethod'],
        'httpStatusCode': 200,
        'responseBody': response_body
    }

    mock_api_response = {'response': action_response}
    return mock_api_response
