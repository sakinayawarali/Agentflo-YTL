import os

import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv()

ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
CONTENT_SID = os.getenv("TWILIO_CONTENT_SID", "")
TO_NUMBER = os.getenv("TWILIO_TO", "")
FROM_NUMBER = os.getenv("TWILIO_FROM", "")
CONTENT_VARIABLES = os.getenv("TWILIO_CONTENT_VARIABLES", "")

url = f"https://api.twilio.com/2010-04-01/Accounts/{ACCOUNT_SID}/Messages.json"

payload = {
    "ContentSid": CONTENT_SID,
    "To": TO_NUMBER,
    "From": FROM_NUMBER,
    "ContentVariables": CONTENT_VARIABLES,
}
headers = {
    "Content-Type": "application/x-www-form-urlencoded",
}

response = requests.request(
    "POST",
    url,
    headers=headers,
    data=payload,
    auth=HTTPBasicAuth(ACCOUNT_SID, AUTH_TOKEN),
)

print(response.text)
