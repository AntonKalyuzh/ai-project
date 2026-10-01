from http.client import HTTPException
import os, sys, time, sqlalchemy
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError, ProgrammingError
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.proxies import WebshareProxyConfig
from google.cloud.sql.connector import Connector, IPTypes
from google.cloud import tasks_v2
from google_auth_oauthlib.flow import Flow
from google.protobuf import duration_pb2, timestamp_pb2
import logging
from urllib.parse import urlparse
import re
import hmac
import secrets
import hashlib
from pathlib import Path
from dotenv import load_dotenv
from ringcentral import SDK
import time, json
from quart_cors import cors
import requests
from twilio.rest import Client
from quart import Quart, request, send_file, websocket,jsonify
from jsonschema import validate, ValidationError
from google.oauth2 import id_token
from google.auth.transport import requests as grequests
import google.auth
from email.message import EmailMessage
import smtplib
from googleapiclient.discovery import build
from openai import OpenAI
import asyncio
import base64
import tempfile
from datetime import datetime, timedelta, timezone
from tools.authorize import Authorize
import random
import pandas
import io
from openpyxl.styles import Font, Alignment, PatternFill  
from google.oauth2 import service_account
from googleapiclient.discovery import build as google_build
from google.oauth2.credentials import Credentials
from google.cloud import storage
import uuid
import twilio
from csv_analysis import analyze_csv, write_analysis_report
from google import genai
from google.genai.types import (
    HttpOptions, GenerateContentConfig, Tool, GoogleSearch,
    AutomaticFunctionCallingConfig,
)
import secrets
from tools.copart_api_wrapper import (
  _get_event_category,
  _extract_transaction_context,
  _extract_event_info,
  _extract_assignment_context,
  _build_response,
  handle_lifecycle_event,
  handle_dispatch_event,
  handle_hold_event,
  handle_release_event,
  handle_service_order_event,
  handle_title_direct_event,
  handle_title_express_event,
  handle_auction_event,
  handle_billing_event,
  dispatch_event,
  RELEASE_EVENTS,
  DISPATCH_EVENTS,
  HOLD_EVENTS,
  SERVICE_ORDER_EVENTS,
  TITLE_DIRECT_EVENTS,
  TITLE_EXPRESS_EVENTS,
  AUCTION_EVENTS,
  BILLING_EVENTS,
  LIFECYCLE_EVENTS,
)

import sqlglot
import aiohttp
import sqlglot.expressions as exp
from tools.live_sale_tracker import get_auto_check

try:
  from pypdf import PdfReader
except ImportError:
  PdfReader = None

# Setup RingCentral SDK
platform = None
try:
   rcsdk = SDK(os.environ.get('RC_APP_CLIENT_ID'), os.environ.get("RC_APP_CLIENT_SECRET"), "https://platform.ringcentral.com")
   platform = rcsdk.platform()
   platform.login( jwt= os.environ.get('RC_USER_JWT') )
except Exception as e:
   logging.error(f"Error setting up RingCentral SDK: {e}")

def setuprcsdk():
   global platform
   if platform is None:
      try:
         rcsdk = SDK(os.environ.get('RC_APP_CLIENT_ID'), os.environ.get("RC_APP_CLIENT_SECRET"), "https://platform.ringcentral.com")
         platform = rcsdk.platform()
         platform.login( jwt= os.environ.get('RC_USER_JWT') )
      except Exception as e:
         logging.error(f"Error setting up RingCentral SDK: {e}")
         return False

load_dotenv()
# Basic configuration
logging.basicConfig(level=logging.INFO)

# Get a logger instance
logger = logging.getLogger(__name__)
USER_INSTANCE_CONNECTION_NAME = os.environ.get("USER_INSTANCE_CONNECTION_NAME")  # "project:region:instance"
USER_DB_NAME = os.environ.get("USER_DB_NAME")
USER_DB_PASS = os.environ.get("USER_DB_PASS")
AVA_INSTANCE_CONNECTION_NAME = os.environ.get("AVA_INSTANCE_CONNECTION_NAME")  # "project:region:instance"
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")  # "project:region:instance"
PB_INSTANCE_CONNECTION_NAME = os.environ.get("PB_INSTANCE_CONNECTION_NAME")
PB_USER = os.environ.get("PB_USER")
PB_PASS = os.environ.get("PB_PASS")
PB_DB_NAME = os.environ.get("PB_DB_NAME")
DB_USER = os.environ.get("DB_USER")   # or IAM user if using IAM DB AuthN
DB_PASS = os.environ.get("DB_PASS")               # omit if using IAM DB AuthN
AVA_DB_NAME = "Ava_main"
DB_NAME = os.environ.get("DB_NAME")
FB_VERIFY_TOKEN       = os.getenv("FB_VERIFY_TOKEN", "set-a-verify-token")
FB_GRAPH_VERSION   = os.getenv("FB_GRAPH_VERSION", "v24.0")
FB_GRAPH           = f"https://graph.facebook.com/{FB_GRAPH_VERSION}"
FB_LL_USER_TOKEN   = os.getenv("FB_LL_USER_ACCESS_TOKEN")  # 60-day user token
FB_ACCOUNT_ID = os.getenv("FB_ACCOUNT_ID", "65436250")  # JCW account id

FB_AVA_USER_TOKEN = os.getenv("FB_AVA_USER_ACCESS_TOKEN")  # 60-day user token for Ava page
FB_PAGE_ID         = os.getenv("FB_PAGE_ID", "305423772513")  # JCW page id

SA_AUTHENTICATION = os.environ.get("SA_AUTHENTICATION")  


OAUTH_CLIENT_ID = os.environ.get("OAUTH_CLIENT_ID")
YT_OAUTH_CLIENT_ID = os.environ.get("YT_OAUTH_CLIENT_ID")
YT_OAUTH_CLIENT_SECRET = os.environ.get("YT_OAUTH_CLIENT_SECRET")
YT_REFRESH_TOKEN = os.environ.get("YT_REFRESH_TOKEN")
import secrets

YOUTUBE_REDIRECT_URI = (
    "https://prism.andrew-chat.com"
    "/api/v1/prism/authorize/youtube/callback"
)

YOUTUBE_SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]

YOUTUBE_STATE_SECRET = os.environ.get(
    "YOUTUBE_STATE_SECRET",
    SA_AUTHENTICATION
)
AUTHORIZED_USERS = os.environ.get("AUTHORIZED_USERS", "").split(",")
OAUTH_CLIENT_SECRET = os.environ.get("OAUTH_CLIENT_SECRET")
GPT_KEY = os.environ.get("GPT_KEY")
GMAIL_NOTIFICATION_CREDENTIALS = os.environ.get("GMAIL_NOTIFICATION_CREDENTIALS")

ACCUTRADE_LOGIN = os.environ.get("ACCUTRADE_LOGIN")
ACCUTRADE_PASSWORD = os.environ.get("ACCUTRADE_PASSWORD")
ACCUTRADE_URL = os.environ.get("ACCUTRADE_URL")
ACCUTRADE_LOGIN_URL = os.environ.get(
  "ACCUTRADE_LOGIN_URL",
  "https://appraiser3.accu-trade.com/auth/get-started"
  "?iss=https%3A%2F%2Fauth0.accu-trade.com%2F",
)
ACCUTRADE_NEW_APPRAISAL_URL = "https://appraiser3.accu-trade.com/appraisal/new"

EVIDENCE_DIR = Path(os.environ.get("EVIDENCE_DIR", "/tmp/evidence"))
ACCUTRADE_OUTPUT_DIR = EVIDENCE_DIR / "accutrade"
CARFAX_OUTPUT_DIR = EVIDENCE_DIR / "carfax"
AUTOCHECK_OUTPUT_DIR = EVIDENCE_DIR / "autocheck"
ERROR_OUTPUT_DIR = EVIDENCE_DIR / "errors"

SERVICE_TOKEN = os.environ.get("EVIDENCE_SERVICE_TOKEN", "")
MAX_CONCURRENCY = int(os.environ.get("EVIDENCE_MAX_CONCURRENCY", "20"))
AUTOCHECK_EVIDENCE_TIMEOUT = float(os.environ.get("AUTOCHECK_EVIDENCE_TIMEOUT", "120"))
HEADLESS = os.environ.get("EVIDENCE_HEADLESS", "true").lower() not in {"0", "false", "no"}

USER_AGENT = (
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_evidence_semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

CLOUD_TASKS_PROJECT = os.environ.get("CLOUD_TASKS_PROJECT", "ai-initiative-project")
CLOUD_TASKS_LOCATION = os.environ.get("CLOUD_TASKS_LOCATION", "us-central1")
CLOUD_TASKS_QUEUE = os.environ.get("CLOUD_TASKS_QUEUE", "prism-relay-buffer")
PRISM_RELAY_PROCESS_URL = os.environ.get(
  "PRISM_RELAY_PROCESS_URL",
  "https://prism.andrew-chat.com/api/v1/prism/relay/process"
)

IG_SETTINGS_TABLE = "`prism-prime`.`ig-autoreply-settings`"
 
IG_MEDIA_FIELDS = ",".join([
    "id", "caption", "media_type", "media_product_type", "media_url",
    "thumbnail_url", "permalink", "timestamp", "comments_count", "like_count",
])
 
IG_DEFAULT_LIMIT = 25
IG_MAX_LIMIT = 50
IG_MAX_OPTIONS = 10
IG_MAX_OPTION_LEN = 900
IG_GRAPH_TIMEOUT = 15
IG_SETTINGS_TTL = 60
 
IG_ALLOWED_PLACEHOLDERS = {"year", "make", "model", "car", "username"}
IG_USE_MAIN_GET = False

webcredentials, project = google.auth.default()
webclient = genai.Client(
    vertexai=True,
    project=project,
    location="us-central1",  # or whatever region you're using
    credentials=webcredentials
)

PRISM_SUGGESTIONS_MODEL = os.environ.get(
  "AVA_SUGGESTIONS_MODEL",
  os.environ.get("AVA_FILTER_MODEL", "gemini-2.5-flash-lite"),
)

gptclient = OpenAI(api_key=GPT_KEY)

car_schema = {
    'type' : "json_schema",
    'json_schema' : {
        'name' : 'car_schema',
        'schema' : {
            'type' : "object",
            'description' : "Car that was generated in the response.",
            'properties' : {
                'vin' : {
                    'type' : "string",
                    'description' : "Vehicle Identification Number"
                    },
                'year' : {
                    'type' : "number"
                    },
                'make' : {
                    'type' : "string"
                    },
                'model' : {
                    'type' : "string"
                    },
                'trim' : {
                    'type' : "string"
                    },
                'mileage' : {
                    'type' : "number",
                    'description' : "Mileage if provided in the message. Otherwise simply 10000 * the year."
                    },
                'condition' : {
                    'type' : "string",
                    'description' : "Description of the condition of the car, including interior and exterior."
                    },
                'background' : {
                    'type' : "string",
                    'description' : "Description of the car's background, such as owner history, carfax, and accidents."
                    },    
                'color' : {
                    'type' : "string",
                    'description' : "Closest possible approximation of the color of the car.",
                    'enum' : ['black', 'yellow' ,'white' ,'red' ,'green' ,'blue', 'silver' ,'gold','grey','teal','tan','cream','unknown']
                    },
                'region' : {
                    'type' : "string",
                    'description' : "General region the car is located in. Goes west cost, south east, north east, midwest, south west, the regions of the US.",
                    'enum' : ['WC','SE','NE','MW','SW']
                    },
                'daysBeforeSale' : {
                    'type' : "number",
                    'description' : "Number of days before the car goes on sale. Default is 30 days if not provided."
                    },
                'source' : {
                    'type' : "string",
                    'description' : "Where the car is coming from. Deduced either from a carfax report that is provided, or from questioning."
                  }
              },
              'required' : ['year', 'make' ,'model'],
              'additionalProperties' : False
        }
    }
}

ava_image_upload_schema = {
  'type': 'object',
  'properties': {
    'type': {
      'type': 'string',
      'enum': [
        'type_c',
        'other',
        'interior',
        'vin',
        'registration',
        'title_back',
        'door_sticker',
        'left',
        'drivers_license',
        'odometer',
        'title_font',
        'back',
        'front',
        'blue_driver',
        'right',
        'marriage_license',
        'release_of_lien',
        'death_certificate',
        'carmax_offer',
        'dealer_license',
        'payoff',
        'divorce_decree',
        'power_of_attorney',
        'business_card',
        'payoff_verification',
        'carvana_offer',
        'dealer_offer',
        'clear_title',
        'vroom_offer',
        'title_check',
        'tda_offer',
        'engine',
        'engine_bay'
      ],
      'description' : 'Best label for what the image is.'
    },
    'damaged': {
      'type': 'boolean'
    },
    'rotation': {
      'type': 'string',
      'enum': ['0', '90', '180', '270']
    },
    'description': {
       'type': 'string',
       'description': 'Description of the image content. Ensure to highlight damage if image is a car.'
    }
  },
  'required': ['type', 'damaged', 'rotation','description'],
  'additionalProperties': False
}

AVA_IMAGE_UPLOAD_PROMPT = """Process the attached image and classify it with exactly one label from the allowed list below.

Allowed labels:
- type_c
- interior
- vin
- registration
- title_back
- door_sticker
- left
- drivers_license
- odometer
- title_font
- back
- front
- blue_driver
- right
- marriage_license
- release_of_lien
- death_certificate
- carmax_offer
- dealer_license
- payoff
- divorce_decree
- power_of_attorney
- business_card
- payoff_verification
- carvana_offer
- dealer_offer
- clear_title
- vroom_offer
- title_check
- tda_offer
- engine
- engine_bay

Return only JSON matching this schema:
{
  \"type\": \"one_of_the_allowed_labels\",
  \"damaged\": true,
  \"rotation\": \"0\",
  \"description\": \"Description of the image content. Describe car damage clearly.\"
}

Rules:
- Return JSON only. No markdown. No explanation.
- If the image is of a car, set showing_damage to true or false based on visible damage.
- If the image is not of a car, set showing_damage to false.
- Set rotation to the string number of degrees needed to rotate the image left so it appears right side up.
- rotation must be one of: \"0\", \"90\", \"180\", \"270\".
- If the image is blurry, label it damaged, and put in the description that it is blurry.
- If the image is of a vin plate, driver license, or title, label it damaged if any part of the document would not be readable due to blurriness or glare.
- Do not add any fields beyond label, showing_damage, and rotation_degrees."""

def ava_helper(message: list, tools: list = []):
  
  response = gptclient.responses.create(
    model="gpt-4o-mini",
    input=message,
    temperature=0.0,             # deterministic, direct replies
    top_p=1.0,
    tools=tools,
    store=False,
  )
  logging.info(response.output_text)
  return response.output_text

ip_type = IPTypes.PRIVATE if os.environ.get("PRIVATE_IP") else IPTypes.PUBLIC
connector = Connector(refresh_strategy="LAZY")

def getconn():
    return connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",  # driver
        user=DB_USER,
        password=DB_PASS,  # drop this if using automatic IAM DB AuthN
        db=DB_NAME,
        ip_type=ip_type,
        # enable_iam_auth=True,  # uncomment for IAM DB AuthN (Postgres)
    )

def getavaconn():
   return connector.connect(
        AVA_INSTANCE_CONNECTION_NAME,
        "pymysql",  # driver
        user=DB_USER,
        password=DB_PASS,  # drop this if using automatic IAM DB AuthN
        db=AVA_DB_NAME,
        ip_type=ip_type,
        # enable_iam_auth=True,  # uncomment for IAM DB AuthN (Postgres)
    )

def create_youtube_state(code_verifier):
    payload = {
        "code_verifier": code_verifier,
        "nonce": secrets.token_urlsafe(16),
    }

    raw = base64.urlsafe_b64encode(
        json.dumps(payload).encode()
    ).decode().rstrip("=")

    signature = hmac.new(
        YOUTUBE_STATE_SECRET.encode(),
        raw.encode(),
        hashlib.sha256,
    ).hexdigest()

    return f"{raw}.{signature}"


def decode_youtube_state(state):
    raw, signature = state.rsplit(".", 1)

    expected = hmac.new(
        YOUTUBE_STATE_SECRET.encode(),
        raw.encode(),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(signature, expected):
        raise ValueError("Invalid OAuth state signature")

    padding = "=" * (-len(raw) % 4)

    payload = json.loads(
        base64.urlsafe_b64decode(raw + padding).decode()
    )

    return payload
   
def getpbconn():
   return connector.connect(
        PB_INSTANCE_CONNECTION_NAME,
        "pymysql",  # driver
        user=PB_USER,
        password=PB_PASS,  # drop this if using automatic IAM DB AuthN
        db=PB_DB_NAME,
        ip_type=ip_type
   )

def getflexconn(instance):
    return connector.connect(
          instance,
          "pymysql",  # driver
          user=PB_USER,
          password=PB_PASS,  # drop this if using automatic IAM DB AuthN
          db=PB_DB_NAME,
          ip_type=ip_type,
      )
logging.info("Loading connection to sql")
engine = sqlalchemy.create_engine("mysql+pymysql://", creator=getconn, pool_pre_ping=True)
avaengine = sqlalchemy.create_engine("mysql+pymysql://", creator=getavaconn, pool_pre_ping=True)
pbengine = sqlalchemy.create_engine("mysql+pymysql://", creator=getpbconn, pool_pre_ping=True)

#app setup
app = cors(Quart(__name__), allow_origin="*")

def read_extension_phone_number_detect_sms_feature():
  try:
    resp = platform.get("/restapi/v1.0/account/~/extension/~/phone-number")
    jsonObj = resp.json()
    for record in jsonObj.records:
      for feature in record.features:
        if feature == "SmsSender":
          # If user has multiple phone numbers, check and decide which number
          # to be used for sending SMS message.
          return record.phoneNumber
    if len(jsonObj.records) == 0:
      return "This user does not own a phone number!"
    else:
      return "None of this user's phone number(s) has the SMS capability!"
  except Exception as e:
    logging.error(e)
    return e

def encode_image(file_path):
    """Encode image to base64 string."""
    with open(file_path, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
    return encoded_string

# Send a text message from a user own phone number to a recipient number
def send_sms(fromNumber, toNumber, message, file_paths):
  try:
      if platform == None:
         setuprcsdk()
      platform.login( jwt= os.environ.get('RC_USER_JWT') )
      builder = rcsdk.create_multipart_builder()
      builder.set_body({
          'from' : { 'phoneNumber': fromNumber },
          'to'   : [ {'phoneNumber': toNumber} ],
          # To send group messaging, add more (max 10 recipients) 'phoneNumber' object. E.g.
          #
          # 'to': [
          #       { 'phoneNumber': "Recipient1-Phone-Number" },
          #       { 'phoneNumber': "Recipient2-Phone-Number" }
          #     ],
          'text' :  message
      })
      if len(file_paths) != 0:
        for file_path in file_paths:
          logging.info(f"Attaching file {file_path}")
          if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                content = f.read()
                attachment = (os.path.basename(file_path), content)
                builder.add(attachment)
      send_request = builder.request('/restapi/v1.0/account/~/extension/~/mms')
      
      resp = platform.send_request(send_request)
      jsonObj = resp.json()
      logging.info("MMS sent. Message id: %s", jsonObj.id)
      check_mms_message_status(jsonObj.id)
  except Exception as e:
      logging.error(e)

def check_mms_message_status(messageId):
    try:
        endpoint = "/restapi/v1.0/account/~/extension/~/message-store/" + str(messageId)
        if platform == None:
          setuprcsdk()
        resp = platform.get(endpoint)
        jsonObj = resp.json()
        logging.info("Message status: " + jsonObj.messageStatus)
        counter = 0
        while jsonObj.messageStatus == "Queued" and counter < 5:
            time.sleep(5)
            endpoint = "/restapi/v1.0/account/~/extension/~/message-store/" + str(messageId)
            resp = platform.get(endpoint)
            jsonObj = resp.json()
            logging.info("Message status: " + jsonObj.messageStatus)
            counter += 1
    except Exception as e:
        logging.error(e)
  
def group_init():
    """Load all buyers as [{'name':..., 'phone':...}, ...]."""
    logging.info("Loading buyers from %s", DB_NAME)
    try:
      with engine.connect() as conn:
            result = conn.execute(text("SELECT name, phone FROM buyers"))
            rows = result.mappings().all()
      if not rows:
          logging.info("No buyers found")
          return []
      group = [{"name": row["name"], "phone": row["phone"]} for row in rows]
      logging.info("Loaded %d buyers", len(group))
      return group
    except Exception as e:
      logging.error(f"Failed to load buyers: {e}")
      return []
    
def ava_ringcentral_check_allowed(id):
  """Check if buyer or lead is allowed to use ava via ringcentral"""
  try:
    with engine.connect() as conn:
      result = conn.execute(text("SELECT * FROM ava_allowed WHERE buyer_id = :buyer_id"), {"buyer_id": id})
      rows = result.mappings().all()
    if not rows:
      logging.info("No buyers found")
      with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM ava_allowed WHERE lead_id = :lead_id"), {"lead_id": id})
            rows = result.mappings().all()
      if not rows:
          logging.info("No leads found")
          return False
    return True
  except Exception as e:
    logging.error(f"Failed to load buyers: {e}")
    return False
    
def create_subscription(extension_id: str):
  if platform == None:
    setuprcsdk()
  platform.login( jwt= os.environ.get('RC_USER_JWT') )
  body = {
    'eventFilters': [
        f'/restapi/v1.0/account/~/extension/{extension_id}/message-store/instant?type=SMS'
    ],
    'expiresIn': 31536000,
    "deliveryMode": {
          "transportType": "WebHook",
          "address": "https://prism.andrew-chat.com/api/v1/prism/ava/ringcentral"
    }
  }
  r = platform.post('/restapi/v1.0/subscription', body)
  return r.id

def delete_subscription(subscription_id: str):
  if platform == None:
    setuprcsdk()
  platform.login( jwt= os.environ.get('RC_USER_JWT') )
  r = platform.delete(f'/restapi/v1.0/subscription/{subscription_id}')
  return r.status_code


GCS_BUCKET_NAME = "ava_main"

def _get_gcs_bucket():
  """Get the GCS bucket for file uploads."""
  client = storage.Client()
  return client.bucket(GCS_BUCKET_NAME)


def _generate_gcs_download_url(blob):
  """Generate a signed URL with ADC, including token-only Cloud Run credentials."""
  credentials, _project = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
  )
  credentials.refresh(grequests.Request())
  service_account_email = (
    os.environ.get("PRISM_SIGNING_SERVICE_ACCOUNT")
    or getattr(credentials, "service_account_email", None)
  )
  if not service_account_email or service_account_email == "default":
    raise RuntimeError(
      "Could not determine the Prism runtime service account. Set "
      "PRISM_SIGNING_SERVICE_ACCOUNT to its email address."
    )
  return blob.generate_signed_url(
    version="v4",
    expiration=timedelta(days=7),
    method="GET",
    service_account_email=service_account_email,
    access_token=credentials.token,
  )

@app.route('/api/v1/prism/ava/files/upload', methods=['POST'])
async def ava_file_upload():
    """Upload a file to the ava_uploads GCS bucket and return a download link."""
    auth_token = request.headers.get('Authorization')
    if auth_token != SA_AUTHENTICATION:
        return "Unauthorized", 403

    streamed_upload = bool(request.headers.get("X-Artifact-Name"))
    temp_path = None
    try:
        if streamed_upload:
            file_name = Path(request.headers["X-Artifact-Name"]).name
            content_type = request.content_type or "application/octet-stream"
            if not file_name:
                return {"error": "X-Artifact-Name is required"}, 400
            with tempfile.NamedTemporaryFile(delete=False) as output:
                temp_path = output.name
                async for chunk in request.body:
                    output.write(chunk)
        else:
            data = await request.get_json(silent=True) or {}
            file_name = data.get("file_name")
            file_content_b64 = data.get("file_content")
            content_type = data.get("content_type", "application/octet-stream")
            if not file_name or not file_content_b64:
                return {"error": "file_name and file_content are required"}, 400
            file_name = Path(file_name).name
            file_content = base64.b64decode(file_content_b64)

        unique_prefix = f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        blob_path = f"generated/{unique_prefix}/{file_name}"

        bucket = _get_gcs_bucket()
        blob = bucket.blob(blob_path)
        if streamed_upload:
            await asyncio.to_thread(
                blob.upload_from_filename,
                temp_path,
                content_type=content_type,
            )
        else:
            await asyncio.to_thread(
                blob.upload_from_string,
                file_content,
                content_type=content_type,
            )

        download_url = await asyncio.to_thread(_generate_gcs_download_url, blob)
        logging.info(f"File '{file_name}' uploaded to gs://{GCS_BUCKET_NAME}/{blob_path}")
        return {
            "status": "success",
            "file_name": file_name,
            "blob_path": blob_path,
            "download_url": download_url,
        }, 200
    except Exception as exc:
        logging.error(f"Failed to upload file to GCS: {exc}")
        return {"error": f"Upload failed: {str(exc)}"}, 500
    finally:
        if temp_path:
            Path(temp_path).unlink(missing_ok=True)


@app.route('/api/v1/prism/ava/files/download/<path:file_name>', methods=['GET'])
async def ava_file_download(file_name):
  """Download a file from the ava_uploads GCS bucket by file name."""
  auth_token = request.headers.get('Authorization') or request.args.get('token')
  if auth_token != SA_AUTHENTICATION:
    return "Unauthorized", 403

  try:
    bucket = _get_gcs_bucket()

    # Search for the file in the generated/ prefix
    blobs = list(bucket.list_blobs(prefix="generated/"))
    target_blob = None
    for blob in blobs:
      if blob.name.endswith(f"/{file_name}") or blob.name == file_name:
        target_blob = blob
        break

    if not target_blob:
      return {"error": f"File '{file_name}' not found"}, 404

    file_content = target_blob.download_as_bytes()

    # Determine content type
    content_type = target_blob.content_type or "application/octet-stream"

    output = io.BytesIO(file_content)
    output.seek(0)

    return await send_file(
      output,
      mimetype=content_type,
      as_attachment=True,
      attachment_filename=file_name
    )

  except Exception as e:
    logging.error(f"Failed to download file from GCS: {e}")
    return {"error": f"Download failed: {str(e)}"}, 500


@app.route('/api/v1/prism/ava/files/list', methods=['GET'])
async def ava_file_list():
  """List all files in the ava_uploads GCS bucket."""
  auth_token = request.headers.get('Authorization') or request.args.get('token')
  if auth_token != SA_AUTHENTICATION:
    return "Unauthorized", 403

  try:
    bucket = _get_gcs_bucket()
    blobs = bucket.list_blobs(prefix="generated/")

    files = []
    for blob in blobs:
      # Skip directory markers
      if blob.name.endswith("/"):
        continue
      signed_url = await asyncio.to_thread(_generate_gcs_download_url, blob)
      files.append({
        "file_name": blob.name.split("/")[-1],
        "blob_path": blob.name,
        "content_type": blob.content_type,
        "size_bytes": blob.size,
        "created": blob.time_created.isoformat() if blob.time_created else None,
        "download_url": signed_url
      })

    return {"files": files, "count": len(files)}, 200

  except Exception as e:
    logging.error(f"Failed to list files from GCS: {e}")
    return {"error": f"List failed: {str(e)}"}, 500


@app.route('/api/v1/prism/ava/files/analyze', methods=['POST'])
async def ava_file_analyze():
  """Analyze a complete generated CSV and optionally publish a derived report."""
  auth_token = request.headers.get('Authorization')
  if auth_token != SA_AUTHENTICATION:
    return {"ok": False, "error": "Unauthorized"}, 403

  data = await request.get_json(silent=True) or {}
  blob_path = str(data.get("blob_path") or "").strip()
  if (
    not blob_path.startswith("generated/")
    or ".." in Path(blob_path).parts
    or not blob_path.lower().endswith(".csv")
  ):
    return {"ok": False, "error": "blob_path must identify a generated CSV artifact"}, 400

  temp_path = None
  report_temp_path = None
  try:
    bucket = _get_gcs_bucket()
    blob = bucket.get_blob(blob_path)
    if blob is None:
      return {"ok": False, "error": "CSV artifact not found"}, 404
    max_bytes = int(os.environ.get("ARTIFACT_ANALYSIS_MAX_BYTES", str(256 * 1024 * 1024)))
    if blob.size is not None and blob.size > max_bytes:
      return {"ok": False, "error": f"CSV artifact exceeds the {max_bytes}-byte analysis limit"}, 413

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as output:
      temp_path = output.name
    await asyncio.to_thread(blob.download_to_filename, temp_path)
    result = await asyncio.to_thread(
      analyze_csv,
      temp_path,
      group_by=data.get("group_by") or [],
      metric_column=str(data.get("metric_column") or ""),
      aggregation=str(data.get("aggregation") or "sum"),
      filters=data.get("filters") or [],
      order=str(data.get("order") or "desc"),
      limit=data.get("limit", 25),
    )
    result["blob_path"] = blob_path
    report_name = str(data.get("report_name") or "").strip()
    if report_name:
      report_file_type = str(data.get("report_file_type") or "xlsx").strip().lower()
      safe_report_name = re.sub(r"[^A-Za-z0-9._ -]+", "_", report_name).strip(" ._") or "report"
      safe_report_name = re.sub(r"\s+", "_", safe_report_name)[:120]
      report_file_name = f"{safe_report_name}.{report_file_type}"
      with tempfile.NamedTemporaryFile(suffix=f".{report_file_type}", delete=False) as report_output:
        report_temp_path = report_output.name
      await asyncio.to_thread(
        write_analysis_report,
        result,
        report_temp_path,
        report_name=report_name,
        summary=str(data.get("report_summary") or ""),
        file_type=report_file_type,
      )
      report_blob_path = (
        f"generated/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}/"
        f"{report_file_name}"
      )
      report_blob = bucket.blob(report_blob_path)
      content_type = (
        "text/csv"
        if report_file_type == "csv"
        else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
      )
      await asyncio.to_thread(
        report_blob.upload_from_filename,
        report_temp_path,
        content_type=content_type,
      )
      result["report"] = {
        "file_name": report_file_name,
        "file_type": report_file_type,
        "blob_path": report_blob_path,
        "download_url": await asyncio.to_thread(_generate_gcs_download_url, report_blob),
        "row_count": result.get("result_row_count", 0),
      }
    return result, 200
  except (TypeError, ValueError) as exc:
    return {"ok": False, "error": str(exc)}, 400
  except Exception as exc:
    logging.exception("Failed to analyze CSV artifact %s", blob_path)
    return {"ok": False, "error": f"CSV analysis failed: {type(exc).__name__}"}, 500
  finally:
    if temp_path:
      Path(temp_path).unlink(missing_ok=True)
    if report_temp_path:
      Path(report_temp_path).unlink(missing_ok=True)


@app.route('/api/v1/prism/ava/filter-vardefs', methods=['GET'])
async def ava_filter_vardefs():
  """Return the canonical Powerbuyer query/filter vocabulary to Ava runtimes."""
  auth_token = request.headers.get('Authorization')
  if auth_token != SA_AUTHENTICATION:
    return {"ok": False, "error": "Unauthorized"}, 403
  path = Path(__file__).parent / "tools" / "powerbuyer_filter_vardefs.md"
  try:
    markdown = await asyncio.to_thread(path.read_text, encoding="utf-8")
  except (OSError, UnicodeError) as exc:
    return {"ok": False, "error": f"Filter vardefs unavailable: {type(exc).__name__}"}, 500
  return {"ok": True, "markdown": markdown, "size_chars": len(markdown)}, 200


@app.route('/api/v1/prism/ava/files/delete/<path:file_name>', methods=['DELETE'])
async def ava_file_delete(file_name):
  """Delete a file from the ava_uploads GCS bucket."""
  auth_token = request.headers.get('Authorization')
  if auth_token != SA_AUTHENTICATION:
    return "Unauthorized", 403

  try:
    bucket = _get_gcs_bucket()

    # Search for the file
    blobs = list(bucket.list_blobs(prefix="generated/"))
    target_blob = None
    for blob in blobs:
      if blob.name.endswith(f"/{file_name}") or blob.name == file_name:
        target_blob = blob
        break

    if not target_blob:
      return {"error": f"File '{file_name}' not found"}, 404

    target_blob.delete()
    logging.info(f"File '{file_name}' deleted from gs://{GCS_BUCKET_NAME}/{target_blob.name}")

    return {"status": "success", "message": f"File '{file_name}' deleted successfully"}, 200

  except Exception as e:
    logging.error(f"Failed to delete file from GCS: {e}")
    return {"error": f"Delete failed: {str(e)}"}, 500


@app.route('/api/v1/prism/authorize/youtube', methods=['GET'])
async def authorize_youtube():

    code_verifier = secrets.token_urlsafe(64)

    state = create_youtube_state(code_verifier)

    client_config = {
        "web": {
            "client_id": YT_OAUTH_CLIENT_ID,
            "client_secret": YT_OAUTH_CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    }

    flow = Flow.from_client_config(
        client_config,
        scopes=YOUTUBE_SCOPES,
        redirect_uri=YOUTUBE_REDIRECT_URI,
        code_verifier=code_verifier,
    )

    authorization_url, _ = flow.authorization_url(
        access_type="offline",
        prompt="consent",
        include_granted_scopes="true",
        state=state,
    )

    return {
        "authorization_url": authorization_url,
    }, 200

@app.route(
    '/api/v1/prism/authorize/youtube/callback',
    methods=['GET']
)
async def authorize_youtube_callback():

    error = request.args.get("error")

    if error:
        return {
            "error": error,
            "error_description": request.args.get("error_description"),
        }, 400

    authorization_code = request.args.get("code")
    state = request.args.get("state")

    if not authorization_code:
        return {"error": "Missing authorization code"}, 400

    if not state:
        return {"error": "Missing OAuth state"}, 400

    try:
        state_data = decode_youtube_state(state)
        code_verifier = state_data["code_verifier"]

    except Exception as e:
        return {
            "error": "Invalid OAuth state",
            "details": str(e),
        }, 400

    client_config = {
        "web": {
            "client_id": YT_OAUTH_CLIENT_ID,
            "client_secret": YT_OAUTH_CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    }

    flow = Flow.from_client_config(
        client_config,
        scopes=YOUTUBE_SCOPES,
        redirect_uri=YOUTUBE_REDIRECT_URI,
        code_verifier=code_verifier,
    )

    try:
        flow.fetch_token(
            code=authorization_code
        )

        credentials = flow.credentials

        return {
            "success": True,
            "refresh_token": credentials.refresh_token,
            "access_token": credentials.token,
            "expires_at": (
                credentials.expiry.isoformat()
                if credentials.expiry
                else None
            ),
        }, 200

    except Exception as e:
        logging.exception("YouTube OAuth token exchange failed")

        return {
            "error": "Token exchange failed",
            "details": str(e),
        }, 500

@app.route('/api/v1/prism/ringcentral/extensions/<page>', methods=['GET'])
async def ringcentral_extensions(page):
  auth_token = request.headers.get('Authorization')
  if auth_token != SA_AUTHENTICATION:
    return "Unauthorized", 403
  try:
    if platform == None:
      setuprcsdk()
    platform.login( jwt= os.environ.get('RC_USER_JWT') )
    resp = platform.get(f"/restapi/v1.0/account/~/extension?page={page}&perpage=100")
    jsonObj = resp.json()
    logging.info(f"Ringcentral response: {jsonObj}")
    extensions = []
    for record in jsonObj.records:
      logging.info(f"Processing extension record: {record.contact}")
      extensions.append({
        "id": record.id,
        "name": record.name,
        #"contact": record.contact.__dict__ if record.contact else None
      })
    return {"extensions": extensions}, 200
  except Exception as e:
    logging.error(f"Failed to fetch RingCentral extensions: {e}")
    return {"error": "Failed to fetch extensions"}, 500

@app.route('/api/v1/prism/ringcentral/sms_log/<extension_id>', methods=['POST'])
async def ringcentral_sms_log(extension_id):
  auth_token = request.headers.get('Authorization')
  if auth_token != SA_AUTHENTICATION:
    return "Unauthorized", 403
  try:
    data = await request.get_json(silent=True) or {}
    if platform == None:
      setuprcsdk()
    platform.login( jwt= os.environ.get('RC_USER_JWT') )
    dateFrom = data.get("date_from")
    dateTo = data.get("date_to")
    resp = platform.get(f"/restapi/v1.0/account/~/extension/{extension_id}/message-store?type=SMS&dateFrom={dateFrom}&dateTo={dateTo}")
    jsonObj = resp.text()
    jsonObj = json.loads(jsonObj)
    messages = []
    for record in jsonObj['records']:
      messages.append(record)
    return {"messages": messages}, 200
  except Exception as e:
    logging.error(f"Failed to fetch SMS log: {e}")
    return {"error": "Failed to fetch SMS log"}, 500
  
@app.route('/api/v1/prism/ringcentral/get_sms/<extension_id>/<message_id>', methods=['GET']) 
async def ringcentral_get_sms(extension_id, message_id):
  auth_token = request.headers.get('Authorization')
  if auth_token != SA_AUTHENTICATION:
    return "Unauthorized", 403
  try:
    if platform == None:
      setuprcsdk()
    platform.login( jwt= os.environ.get('RC_USER_JWT') )
    resp = platform.get(f"/restapi/v1.0/account/~/extension/{extension_id}/message-store/{message_id}")
    jsonObj = resp.json()
    message = {
      "id": jsonObj.id,
      "from": jsonObj.from_.phoneNumber if jsonObj.from_ else None,
      "to": [to.phoneNumber for to in jsonObj.to] if jsonObj.to else [],
      "text": jsonObj.subject if jsonObj.subject else None,
      "time": jsonObj.time
    }
    return {"message": message}, 200
  except Exception as e:
    logging.error(f"Failed to fetch SMS message: {e}")
    return {"error": "Failed to fetch SMS message"}, 500 

@app.route('/api/v2/prism/youtube/analytics', methods=['POST'])
async def youtube_analytics():
    """
    Internal tool for connecting to YouTube Analytics API to review channel/video analytics.

    Auth model:
    - Requires internal Authorization header.
    - Uses server-configured YouTube OAuth credentials for the connected channel.
    - The request-body access_token is accepted for compatibility but is not used by this implementation.

    Actions:
    - channel_analytics: Get overall channel analytics
    - channel_timeseries: Get day-by-day channel metrics
    - video_analytics: Get analytics for a specific video
    - video_timeseries: Get day-by-day metrics for a specific video
    - top_videos: Get top performing videos, enriched with title/thumbnail/publishedAt
    - compare_videos: Compare multiple videos side-by-side
    - audience_demographics: Get audience demographic data
    - traffic_sources: Get traffic source data
    - engagement_metrics: Get engagement metrics
    - recent_videos: Get most recent uploaded videos
    """
    auth_token = request.headers.get('Authorization')
    if auth_token != SA_AUTHENTICATION:
        return {"error": "Unauthorized"}, 403

    data = await request.get_json(silent=True) or {}
    action = data.get("action")

    VALID_ACTIONS = [
        "channel_analytics",
        "channel_timeseries",
        "video_analytics",
        "video_timeseries",
        "top_videos",
        "compare_videos",
        "audience_demographics",
        "traffic_sources",
        "engagement_metrics",
        "recent_videos",
    ]

    if not action:
        return {"error": "action is required", "valid_actions": VALID_ACTIONS}, 400

    if action not in VALID_ACTIONS:
        return {"error": f"unknown action '{action}'", "valid_actions": VALID_ACTIONS}, 400

    def parse_date(value: str, field_name: str):
        try:
            return datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"{field_name} must be in YYYY-MM-DD format")

    def validate_dates(start_date: str, end_date: str):
        start_dt = parse_date(start_date, "start_date")
        end_dt = parse_date(end_date, "end_date")
        if start_dt > end_dt:
            raise ValueError("start_date must be on or before end_date")
        return start_dt, end_dt

    def rows_to_dicts(response):
        headers = [h["name"] for h in response.get("columnHeaders", [])]
        rows = response.get("rows", [])
        return [dict(zip(headers, row)) for row in rows]

    def chunked(seq, size):
        for i in range(0, len(seq), size):
            yield seq[i:i + size]

    def fetch_video_metadata(video_ids, youtube_data_service):
        if not video_ids:
            return {}

        metadata = {}
        for batch in chunked(video_ids, 50):
            resp = youtube_data_service.videos().list(
                part="snippet,contentDetails,statistics",
                id=",".join(batch),
                maxResults=len(batch)
            ).execute()

            for item in resp.get("items", []):
                vid = item.get("id")
                snippet = item.get("snippet", {})
                thumbs = snippet.get("thumbnails", {})
                metadata[vid] = {
                    "video_id": vid,
                    "title": snippet.get("title"),
                    "description": snippet.get("description"),
                    "published_at": snippet.get("publishedAt"),
                    "thumbnail": (
                        thumbs.get("high", {}).get("url")
                        or thumbs.get("medium", {}).get("url")
                        or thumbs.get("default", {}).get("url")
                    ),
                    "url": f"https://www.youtube.com/watch?v={vid}",
                    "channel_title": snippet.get("channelTitle"),
                }
        return metadata

    def get_channel_id_and_title(youtube_data_service):
        channels_response = youtube_data_service.channels().list(
            mine=True,
            part='id,snippet'
        ).execute()

        if not channels_response.get('items'):
            return None, None

        item = channels_response['items'][0]
        return item['id'], item.get('snippet', {}).get('title')

    try:
        # Using server-side refresh token credentials, matching the current implementation.
        credentials = Credentials(
            token=None,
            refresh_token=YT_REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=YT_OAUTH_CLIENT_ID,
            client_secret=YT_OAUTH_CLIENT_SECRET
        )

        youtube_analytics_service = google_build('youtubeAnalytics', 'v2', credentials=credentials)
        youtube_data_service = google_build('youtube', 'v3', credentials=credentials)

        channel_id, channel_title = get_channel_id_and_title(youtube_data_service)
        if not channel_id:
            return {"error": "No channel found for authenticated user"}, 404

        start_date = data.get("start_date", (datetime.utcnow() - timedelta(days=28)).strftime("%Y-%m-%d"))
        end_date = data.get("end_date", datetime.utcnow().strftime("%Y-%m-%d"))
        validate_dates(start_date, end_date)

        if action == "channel_analytics":
            metrics = data.get(
                "metrics",
                ["views", "estimatedMinutesWatched", "subscribersGained", "subscribersLost"]
            )
            response = youtube_analytics_service.reports().query(
                ids=f"channel=={channel_id}",
                startDate=start_date,
                endDate=end_date,
                metrics=",".join(metrics)
            ).execute()

            return {
                "channel_id": channel_id,
                "channel_title": channel_title,
                "start_date": start_date,
                "end_date": end_date,
                "channel_analytics": rows_to_dicts(response),
                "raw": response,
            }, 200

        elif action == "channel_timeseries":
            metrics = data.get(
                "metrics",
                ["views", "estimatedMinutesWatched", "subscribersGained", "subscribersLost"]
            )
            response = youtube_analytics_service.reports().query(
                ids=f"channel=={channel_id}",
                startDate=start_date,
                endDate=end_date,
                metrics=",".join(metrics),
                dimensions="day",
                sort="day"
            ).execute()

            return {
                "channel_id": channel_id,
                "channel_title": channel_title,
                "start_date": start_date,
                "end_date": end_date,
                "channel_timeseries": rows_to_dicts(response),
                "raw": response,
            }, 200

        elif action == "video_analytics":
            video_id = data.get("video_id")
            if not video_id:
                return {"error": "video_id is required for video_analytics action"}, 400

            metrics = data.get(
                "metrics",
                ["views", "likes", "comments", "shares", "estimatedMinutesWatched", "averageViewDuration"]
            )

            response = youtube_analytics_service.reports().query(
                ids=f"channel=={channel_id}",
                startDate=start_date,
                endDate=end_date,
                metrics=",".join(metrics),
                filters=f"video=={video_id}"
            ).execute()

            metadata = fetch_video_metadata([video_id], youtube_data_service).get(video_id, {})

            return {
                "channel_id": channel_id,
                "channel_title": channel_title,
                "start_date": start_date,
                "end_date": end_date,
                "video": metadata,
                "video_analytics": rows_to_dicts(response),
                "raw": response,
            }, 200

        elif action == "video_timeseries":
            video_id = data.get("video_id")
            if not video_id:
                return {"error": "video_id is required for video_timeseries action"}, 400

            metrics = data.get(
                "metrics",
                ["views", "likes", "comments", "shares", "estimatedMinutesWatched", "averageViewDuration"]
            )

            response = youtube_analytics_service.reports().query(
                ids=f"channel=={channel_id}",
                startDate=start_date,
                endDate=end_date,
                metrics=",".join(metrics),
                dimensions="day",
                filters=f"video=={video_id}",
                sort="day"
            ).execute()

            metadata = fetch_video_metadata([video_id], youtube_data_service).get(video_id, {})

            return {
                "channel_id": channel_id,
                "channel_title": channel_title,
                "start_date": start_date,
                "end_date": end_date,
                "video": metadata,
                "video_timeseries": rows_to_dicts(response),
                "raw": response,
            }, 200

        elif action == "top_videos":
            max_results = int(data.get("max_results", 10))
            max_results = max(1, min(max_results, 25))

            sort_by = data.get("sort_by", "views")
            valid_sort = {"views", "estimatedMinutesWatched", "likes", "comments"}
            if sort_by not in valid_sort:
                return {"error": f"invalid sort_by '{sort_by}'", "valid_sort": sorted(valid_sort)}, 400

            response = youtube_analytics_service.reports().query(
                ids=f"channel=={channel_id}",
                startDate=start_date,
                endDate=end_date,
                metrics="views,estimatedMinutesWatched,likes,comments",
                dimensions="video",
                maxResults=max_results,
                sort=f"-{sort_by}"
            ).execute()

            rows = rows_to_dicts(response)
            video_ids = [r["video"] for r in rows if r.get("video")]
            metadata_map = fetch_video_metadata(video_ids, youtube_data_service)

            enriched = []
            for row in rows:
                vid = row.get("video")
                enriched.append({
                    **metadata_map.get(vid, {"video_id": vid}),
                    "views": row.get("views"),
                    "estimatedMinutesWatched": row.get("estimatedMinutesWatched"),
                    "likes": row.get("likes"),
                    "comments": row.get("comments"),
                })

            return {
                "channel_id": channel_id,
                "channel_title": channel_title,
                "start_date": start_date,
                "end_date": end_date,
                "sort_by": sort_by,
                "top_videos": enriched,
                "raw": response,
            }, 200

        elif action == "compare_videos":
            video_ids = data.get("video_ids")
            if not video_ids or not isinstance(video_ids, list) or len(video_ids) < 2:
                return {"error": "video_ids must be a list with at least 2 video IDs"}, 400

            if len(video_ids) > 10:
                return {"error": "compare_videos supports at most 10 video IDs per request"}, 400

            metrics = data.get(
                "metrics",
                ["views", "likes", "comments", "shares", "estimatedMinutesWatched", "averageViewDuration"]
            )

            metadata_map = fetch_video_metadata(video_ids, youtube_data_service)
            comparisons = []

            for video_id in video_ids:
                response = youtube_analytics_service.reports().query(
                    ids=f"channel=={channel_id}",
                    startDate=start_date,
                    endDate=end_date,
                    metrics=",".join(metrics),
                    filters=f"video=={video_id}"
                ).execute()

                comparisons.append({
                    "video": metadata_map.get(video_id, {"video_id": video_id}),
                    "metrics": rows_to_dicts(response),
                    "raw": response,
                })

            return {
                "channel_id": channel_id,
                "channel_title": channel_title,
                "start_date": start_date,
                "end_date": end_date,
                "compare_videos": comparisons,
            }, 200

        elif action == "audience_demographics":
            video_id = data.get("video_id")
            filters = f"video=={video_id}" if video_id else None

            query_params = {
                "ids": f"channel=={channel_id}",
                "startDate": start_date,
                "endDate": end_date,
                "metrics": "viewerPercentage",
                "dimensions": "ageGroup,gender"
            }
            if filters:
                query_params["filters"] = filters

            response = youtube_analytics_service.reports().query(**query_params).execute()

            result = {
                "channel_id": channel_id,
                "channel_title": channel_title,
                "start_date": start_date,
                "end_date": end_date,
                "audience_demographics": rows_to_dicts(response),
                "raw": response,
            }

            if video_id:
                result["video"] = fetch_video_metadata([video_id], youtube_data_service).get(video_id, {"video_id": video_id})

            return result, 200

        elif action == "traffic_sources":
            video_id = data.get("video_id")
            filters = f"video=={video_id}" if video_id else None

            query_params = {
                "ids": f"channel=={channel_id}",
                "startDate": start_date,
                "endDate": end_date,
                "metrics": "views,estimatedMinutesWatched",
                "dimensions": "insightTrafficSourceType"
            }
            if filters:
                query_params["filters"] = filters

            response = youtube_analytics_service.reports().query(**query_params).execute()

            result = {
                "channel_id": channel_id,
                "channel_title": channel_title,
                "start_date": start_date,
                "end_date": end_date,
                "traffic_sources": rows_to_dicts(response),
                "raw": response,
            }

            if video_id:
                result["video"] = fetch_video_metadata([video_id], youtube_data_service).get(video_id, {"video_id": video_id})

            return result, 200

        elif action == "engagement_metrics":
            video_id = data.get("video_id")
            filters = f"video=={video_id}" if video_id else None

            query_params = {
                "ids": f"channel=={channel_id}",
                "startDate": start_date,
                "endDate": end_date,
                "metrics": "likes,comments,shares,subscribersGained,subscribersLost"
            }
            if filters:
                query_params["filters"] = filters

            response = youtube_analytics_service.reports().query(**query_params).execute()

            result = {
                "channel_id": channel_id,
                "channel_title": channel_title,
                "start_date": start_date,
                "end_date": end_date,
                "engagement_metrics": rows_to_dicts(response),
                "raw": response,
            }

            if video_id:
                result["video"] = fetch_video_metadata([video_id], youtube_data_service).get(video_id, {"video_id": video_id})

            return result, 200

        elif action == "recent_videos":
            max_results = int(data.get("max_results", 5))
            max_results = max(1, min(max_results, 25))

            response = youtube_data_service.search().list(
                part="id,snippet",
                channelId=channel_id,
                type="video",
                order="date",
                maxResults=max_results
            ).execute()

            videos = []
            for item in response.get("items", []):
                if item.get("id", {}).get("videoId") is None:
                    continue

                snippet = item["snippet"]
                videos.append({
                    "video_id": item["id"]["videoId"],
                    "title": snippet.get("title"),
                    "description": snippet.get("description"),
                    "published_at": snippet.get("publishedAt"),
                    "thumbnail": snippet.get("thumbnails", {}).get("high", {}).get("url"),
                    "url": f"https://www.youtube.com/watch?v={item['id']['videoId']}",
                    "channel_title": snippet.get("channelTitle"),
                })

            return {
                "channel_id": channel_id,
                "channel_title": channel_title,
                "recent_videos": videos,
                "raw": response,
            }, 200

    except ValueError as e:
        return {"error": str(e)}, 400
    except google.auth.exceptions.TransportError as e:
        logging.error(f"YouTube OAuth transport error: {e}")
        return {"error": "Failed to authenticate with YouTube credentials"}, 401
    except Exception as e:
        logging.exception("YouTube Analytics API error")
        return {"error": str(e)}, 500

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request



@app.route('/api/v1/prism/ava/ringcentral/on/<buyer_extension>', methods=['GET'])
async def ava_ringcentral_on(buyer_extension):
  token = request.args.get("token")
  if not token or token != SA_AUTHENTICATION:
    return "Unauthorized", 401
  return create_subscription(buyer_extension)

@app.route('/api/v1/prism/ava/ringcentral/off/<subscription_id>', methods=['GET'])
async def ava_ringcentral_off(subscription_id):
  token = request.args.get("token")
  if not token or token != SA_AUTHENTICATION:
    return "Unauthorized", 401
    
  return delete_subscription(subscription_id)

@app.route('/api/v1/prism/ava/ringcentral', methods=['POST'])
async def ava_ringcentral():
  # Check Content-Type before parsing JSON
  validation_token = request.headers.get('Validation-Token') 

  if validation_token:
      return "OK", 200, {"Validation-Token": validation_token}
  data = await request.get_json()
  logging.info(f"Received RingCentral webhook data: {data}")
  return "OK", 200 #Temporary block
  """
  if 'body' in data:
    logging.info(f"Message body: {data['body']}")
    try:
      body = await request.get_json()
      logging.info(f"Webhook received {body}")
      body = body['body']
      platform.login( jwt= os.environ.get('RC_USER_JWT') )
      try:
        message = {
          'phone': body['from']['phoneNumber'],
          'message': body['subject'] if 'subject' in body else body['text'],
        }
        if 'attachments' in body and len(body['attachments']) > 0:
          message['file_path'] = download_mms_attachments(body['attachments'])
          if 'attachments' in body and len(body['attachments']) > 0:
            temp_content = [{"type": "input_text", "text": f"User sent the following message: {message['message']}, process the attached images, and reply with the original user message followed by your analysis of the images. Provide car data if applicable in the following schema: {car_schema}"}]
            file_types = []
            for i in range(len(body['attachments'])):
              if body['attachments'][i]['type'] == "MmsAttachment":
                file_types.append(body['attachments'][i]['contentType'])
            for i in range(len(message['file_path'])):
                base64_image=encode_image(message['file_path'][i])
                filetype = file_types[i]
                print(filetype)
                temp_content.append({"type": "input_image", "image_url": f"data:{filetype};base64,{base64_image}"})
            if len(message['file_path']) > 0:
              message['message']
              
              message['message'] = ava_helper([{
                      "role": "user",
                      "content": temp_content,
              }],[])  
          
        else:
          message['file_path'] = []
        try:
          lead_obj, lead_err = main_get({
            "table": "main_leads",
            "retrieval_type": "by_field",
            "field_name": "phone",
            "field_value": body['from']['phoneNumber']
          })
          lead = lead_obj[0] if lead_obj else None
          logging.info(f"Loaded lead: {lead}")
          message['lead'] = lead
        except Exception as e:
          logging.error(f"Failed to load lead: {e}")
          message['lead'] = None
        session_id = requests.put(f'https://andrew.andrew-chat.com/api/v1/session/{body['from']['phoneNumber']}_{body['to']['phoneNumber']}', headers = {"Authorization": SA_AUTHENTICATION}).json()
        if(len(session_id['sessions']) == 0):
            session_id = requests.get(f'https://andrew.andrew-chat.com/api/v1/session/{body['from']['phoneNumber']}_{body['to']['phoneNumber']}', headers = {"Authorization": SA_AUTHENTICATION}).json()['session_id']
            session_id = {
                'id': session_id,
                'state' : {
                  'chat_history': []
                }
            }

            ava_message = "The current time is " + datetime.utcnow().isoformat() + f". As a customer service assistant, handle the message(store information, gather more information about the car/pickup, answer questions) Lead: {message['lead']} Sent the following message check the chat_logs for reference of the conversation: " + message['message']
        else:

          ava_message = "The current time is " + datetime.utcnow().isoformat() + f". As a customer service assistant, handle the message(store information, gather more information about the car/pickup, answer questions) Lead: {message['lead']}, sent the following message: " + message['message']
          session_id = session_id['sessions'][0]
        
        andrew_response = requests.post(f"https://andrew.andrew-chat.com/api/v1/prism", json = {
            "action" : "create",
            "message" : ava_message,
            "user_id" : "prism_bot",
            "session_id" : str(session_id['id'])
}, headers = {"Authorization": SA_AUTHENTICATION})
         # Handle responses, such as function calls and text responses.
        try:
          logging.info(andrew_response)
          json_andrew = andrew_response.json()
          message_assembly = ""
          for chunk in json_andrew:
              if "text" in chunk:
                message_assembly += " " + chunk['text']
          send_sms(body['to']['phoneNumber'], body['from']['phoneNumber'], message_assembly.strip(), [])
        except Exception as e:
          logging.error(f"Failed to parse Ava auto response: {e}")
          return "Bad request", 400
      except Exception as e:
        logging.error(f"Failed to parse incoming message, {e}")
        return "Bad request", 400
      logging.info(f"Incoming message from workflow {message}")
    except Exception as e:
      logging.error(f"Failed to parse incoming message, {e}")
      return "Bad request", 400
  return "OK", 200, {"Validation-Token": validation_token}"""


def andrew_group_init():
    """Load all buyers as [{'name':..., 'phone':...}, ...]."""
    logging.info("Loading buyers from %s", DB_NAME)
    try:
      with engine.connect() as conn:
            result = conn.execute(text("SELECT name, phone FROM andrew_buyers"))
            rows = result.mappings().all()
      if not rows:
          logging.info("No buyers found")
          return []
      group = [{"name": row["name"], "phone": row["phone"]} for row in rows]
      logging.info("Loaded %d buyers", len(group))
      return group
    except Exception as e:
      logging.error(f"Failed to load buyers: {e}")
      return []

def group_add(name: str, phone: str):
    """Insert a buyer. Returns (ok, error_message)."""
    name = (name or "")
    phone = (phone or "").strip()
    if not name or not phone:
        return "Bad request"
    
    logging.info("Adding buyer to group: name=%s, phone=%s", name, phone)
    try:
        with engine.connect() as conn:
            conn.execute(
              text("INSERT INTO buyers (name, phone) VALUES (:name, :phone)"),
              {"name": name, "phone": phone},
            )
            conn.commit()
        send_phone = read_extension_phone_number_detect_sms_feature()
        send_sms(send_phone, phone, "Welcome to the GMTV Buyer Chat Group.",file_paths=[])#Send welcome message to newly added #
        return "Success"
    except IntegrityError as e:
        # Likely UNIQUE violation on phone
        logging.warning("Duplicate phone or integrity error: %s", e)
        return "Buyer with this phone already exists"
    except Exception as e:
        logging.exception("Failed to insert into buyer group: %s", e)
        return "Server error"

def strategy_group(agent: str):
    logging.info("Loading strategy for agent %s", agent)
    try:
      return [{'Strategy': """Here is your strategy step by step.
1) Get standardized values from get_car_context for us in the MMR tools. Update your car object with those standard values.
2) Collect data from each of the following sources, as the become available.:
	MMR_DATA or MMR_WITH_YEAR_MAKE_MODEL if no VIN is available. Run as soon as VIN or Year/Make/Model are available. Sometimes Forecasted MMR will be available, sometimes only Actual MMR.
	get_sales. Search with the make, asking for a list of all sales of the same make.
	GET_LEARNING. As soon as you have Year and Make.
3) Use the other information available to you to determine if the car is above, below, or at average condition.
4) Provide a single final evaluation based on the sale data. Adjust with your insights, the condition of the car, and your recent sales. Show your sales data in the form of tables for the user to read."""}]
    except Exception as e:
      logging.error(f"Failed to load strategies: {e}")
      return []

def create_valuation(prompt):
  logging.info("Creating valuation summary")
  try:
    with engine.connect() as conn:
      result = conn.execute(
          text("INSERT INTO andrew_training_data (prompt) VALUES (:prompt)"),
          {"prompt": prompt}
      )
      conn.commit()
    logging.info("Created valuation")
    return str(result.lastrowid)
  except Exception as e:
    logging.error(f"Failed to create valuation: {e}")
    return "error"

def update_valuation(id: int, **kwargs):
  logging.info(f"Updating valuation {id} with streams")
  try:
    with engine.connect() as conn:
      update_fields = ", ".join([f"{key} = :{key}" for key in kwargs.keys()])
      logging.info(update_fields)
      query = f"UPDATE andrew_training_data SET {update_fields} WHERE id = :id" 
      logging.info(query)     
      result = conn.execute(text(query), {**kwargs, "id": id})
      conn.commit()
    logging.info(f"Updated valuation {id}")
    return "success"
  except Exception as e:
    logging.error(f"Failed to update valuation: {e}")
    return "error"

def get_valuation_batch(start_id: int, end_id: int):
   logging.info("Loading valuation from batch start at %s to %s", start_id, end_id)
   try:
      with engine.connect() as conn:
        result = conn.execute(
          text("SELECT * FROM andrew_training_data WHERE id >= :start_id AND id < :end_id"),
          {"start_id": start_id, "end_id": end_id}
        )
        rows = result.mappings().all()
      return rows
   except Exception as e:
      logging.error(f"Failed to load valuation batch: {e}")
      return "error"

def _valuation_select_sql(select):
  if select in (None, "*"):
    return "*"

  columns = [select] if isinstance(select, str) else list(select)
  if not columns:
    raise ValueError("select must contain at least one column")

  for column in columns:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(column)):
      raise ValueError(f"Invalid valuation select column: {column}")

  return ", ".join(f"`{column}`" for column in columns)


def get_valuation_search(
    property: str,
    value: str,
    batch_size: int,
    date=None,
    select="*",
):
  logging.info("Loading valuation search for %s = %s", property, value)
  try:
    with engine.connect() as conn:
      date_clause = " AND job_date >= :date" if date is not None else ""
      select_sql = _valuation_select_sql(select)
      query = (
        f"SELECT {select_sql} FROM andrew_training_data "
        f"WHERE {property} = :value{date_clause} "
        "ORDER BY job_date DESC "
        "LIMIT :batch_size"
      )
      logging.info(query)
      params = {
        "value": value,
        "batch_size": batch_size,
      }
      if date is not None:
        params["date"] = date
      result = conn.execute(text(query), params)
      rows = result.mappings().all()
    return [dict(row) for row in rows]
  except Exception as e:
    logging.error(f"Failed to load valuation search: {e}")
    return []


def get_valuation_rows_by_ids(ids, select="*"):
  """Load selected valuation columns for a bounded set of primary keys."""
  ids = list(dict.fromkeys(ids or []))
  if not ids:
    return []

  try:
    placeholders = ", ".join(f":id_{index}" for index in range(len(ids)))
    params = {f"id_{index}": value for index, value in enumerate(ids)}
    query = (
      f"SELECT {_valuation_select_sql(select)} "
      "FROM andrew_training_data "
      f"WHERE id IN ({placeholders}) "
      "ORDER BY job_date DESC"
    )

    with engine.connect() as conn:
      rows = conn.execute(text(query), params).mappings().all()
    return [dict(row) for row in rows]
  except Exception as e:
    logging.error(f"Failed to load valuation rows by ids: {e}")
    return []
  
def valuation_get_last_index():
  logging.info("Loading last valuation index")
  try:
    with engine.connect() as conn:
      result = conn.execute(
        text("SELECT id FROM andrew_training_data ORDER BY id DESC LIMIT 1")
      )
      row = result.mappings().first()
    if row:
      return row["id"]
    else:
      return 0
  except Exception as e:
    logging.error(f"Failed to load last valuation index: {e}")
    return "error"

VALID_AVA_TASK_STATUSES = {
    "queued",
    "in_progress",
    "blocked",
    "completed",
    "failed",
    "cancelled",
}


def _ava_json_dumps_or_none(value):
    if value is None:
        return None
    return json.dumps(value, default=str)


def _ava_json_loads_safe(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return value


def _ava_task_row_to_dict(row):
    if not row:
        return None

    item = dict(row)
    item["metadata"] = _ava_json_loads_safe(item.get("metadata"))

    for key in ["created_at", "updated_at", "completed_at", "due_date"]:
        if item.get(key) is not None:
            item[key] = str(item[key])

    return item


def _ava_progress_row_to_dict(row):
    if not row:
        return None

    item = dict(row)
    item["metadata"] = _ava_json_loads_safe(item.get("metadata"))

    if item.get("created_at") is not None:
        item["created_at"] = str(item["created_at"])

    return item


def ava_create_task_record(
    title,
    description=None,
    assigned_to=None,
    priority=None,
    due_date=None,
    metadata=None,
    status="queued",
    progress=0,
):
    if not title or not str(title).strip():
        return {"ok": False, "error": "title is required"}, 400

    if status not in VALID_AVA_TASK_STATUSES:
        return {
            "ok": False,
            "error": "invalid status",
            "valid_statuses": sorted(VALID_AVA_TASK_STATUSES),
        }, 400

    try:
        progress = int(progress or 0)
    except Exception:
        return {"ok": False, "error": "progress must be an integer"}, 400

    if progress < 0 or progress > 100:
        return {"ok": False, "error": "progress must be between 0 and 100"}, 400

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO ava_tasks (
                        title, description, assigned_to, priority, due_date,
                        status, progress, metadata
                    )
                    VALUES (
                        :title, :description, :assigned_to, :priority, :due_date,
                        :status, :progress, :metadata
                    )
                """),
                {
                    "title": str(title).strip(),
                    "description": description,
                    "assigned_to": assigned_to,
                    "priority": priority,
                    "due_date": due_date,
                    "status": status,
                    "progress": progress,
                    "metadata": _ava_json_dumps_or_none(metadata or {}),
                },
            )

            task_id = result.lastrowid

            conn.execute(
                text("""
                    INSERT INTO ava_task_progress (
                        task_id, message, progress, status, metadata
                    )
                    VALUES (
                        :task_id, :message, :progress, :status, :metadata
                    )
                """),
                {
                    "task_id": task_id,
                    "message": "Task created.",
                    "progress": progress,
                    "status": status,
                    "metadata": _ava_json_dumps_or_none({"source": "create"}),
                },
            )

            conn.commit()

            task = conn.execute(
                text("SELECT * FROM ava_tasks WHERE id = :task_id"),
                {"task_id": task_id},
            ).mappings().first()

        return {
            "ok": True,
            "task_id": str(task_id),
            "task": _ava_task_row_to_dict(task),
        }, 201

    except Exception as e:
        logging.exception("Failed to create Ava task")
        return {"ok": False, "error": "create_task_failed", "message": str(e)}, 500


def ava_update_task_record(
    task_id,
    title=None,
    description=None,
    assigned_to=None,
    priority=None,
    due_date=None,
    status=None,
    progress=None,
    metadata=None,
):
    if not task_id:
        return {"ok": False, "error": "task_id is required"}, 400

    if status and status not in VALID_AVA_TASK_STATUSES:
        return {
            "ok": False,
            "error": "invalid status",
            "valid_statuses": sorted(VALID_AVA_TASK_STATUSES),
        }, 400

    if progress is not None:
        try:
            progress = int(progress)
        except Exception:
            return {"ok": False, "error": "progress must be an integer"}, 400

        if progress < 0 or progress > 100:
            return {"ok": False, "error": "progress must be between 0 and 100"}, 400

    updates = {}

    for key, value in {
        "title": title,
        "description": description,
        "assigned_to": assigned_to,
        "priority": priority,
        "due_date": due_date,
        "status": status,
        "progress": progress,
    }.items():
        if value is not None:
            updates[key] = value

    if metadata is not None:
        updates["metadata"] = _ava_json_dumps_or_none(metadata)

    if status in {"completed", "failed", "cancelled"}:
        updates["completed_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    if not updates:
        return {"ok": False, "error": "no update fields provided"}, 400

    set_clause = ", ".join([f"{key} = :{key}" for key in updates])

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"""
                    UPDATE ava_tasks
                    SET {set_clause}
                    WHERE id = :task_id
                """),
                {"task_id": task_id, **updates},
            )

            if result.rowcount == 0:
                conn.rollback()
                return {"ok": False, "error": "task_not_found", "task_id": task_id}, 404

            conn.execute(
                text("""
                    INSERT INTO ava_task_progress (
                        task_id, message, progress, status, metadata
                    )
                    VALUES (
                        :task_id, :message, :progress, :status, :metadata
                    )
                """),
                {
                    "task_id": task_id,
                    "message": "Task updated.",
                    "progress": progress,
                    "status": status,
                    "metadata": _ava_json_dumps_or_none({
                        "source": "update",
                        "updated_fields": list(updates.keys()),
                    }),
                },
            )

            conn.commit()

            task = conn.execute(
                text("SELECT * FROM ava_tasks WHERE id = :task_id"),
                {"task_id": task_id},
            ).mappings().first()

        return {
            "ok": True,
            "task_id": str(task_id),
            "task": _ava_task_row_to_dict(task),
        }, 200

    except Exception as e:
        logging.exception("Failed to update Ava task")
        return {"ok": False, "error": "update_task_failed", "message": str(e)}, 500


def ava_append_task_progress_record(
    task_id,
    message,
    progress=None,
    status=None,
    metadata=None,
):
    if not task_id:
        return {"ok": False, "error": "task_id is required"}, 400

    if not message or not str(message).strip():
        return {"ok": False, "error": "message is required"}, 400

    if status and status not in VALID_AVA_TASK_STATUSES:
        return {
            "ok": False,
            "error": "invalid status",
            "valid_statuses": sorted(VALID_AVA_TASK_STATUSES),
        }, 400

    if progress is not None:
        try:
            progress = int(progress)
        except Exception:
            return {"ok": False, "error": "progress must be an integer"}, 400

        if progress < 0 or progress > 100:
            return {"ok": False, "error": "progress must be between 0 and 100"}, 400

    try:
        with engine.connect() as conn:
            task = conn.execute(
                text("SELECT * FROM ava_tasks WHERE id = :task_id"),
                {"task_id": task_id},
            ).mappings().first()

            if not task:
                return {"ok": False, "error": "task_not_found", "task_id": task_id}, 404

            conn.execute(
                text("""
                    INSERT INTO ava_task_progress (
                        task_id, message, progress, status, metadata
                    )
                    VALUES (
                        :task_id, :message, :progress, :status, :metadata
                    )
                """),
                {
                    "task_id": task_id,
                    "message": str(message).strip(),
                    "progress": progress,
                    "status": status,
                    "metadata": _ava_json_dumps_or_none(metadata or {}),
                },
            )

            update_fields = {}

            if progress is not None:
                update_fields["progress"] = progress

            if status is not None:
                update_fields["status"] = status

            if status in {"completed", "failed", "cancelled"}:
                update_fields["completed_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            if update_fields:
                set_clause = ", ".join([f"{key} = :{key}" for key in update_fields])
                conn.execute(
                    text(f"""
                        UPDATE ava_tasks
                        SET {set_clause}
                        WHERE id = :task_id
                    """),
                    {"task_id": task_id, **update_fields},
                )

            conn.commit()

            updated_task = conn.execute(
                text("SELECT * FROM ava_tasks WHERE id = :task_id"),
                {"task_id": task_id},
            ).mappings().first()

        return {
            "ok": True,
            "task_id": str(task_id),
            "task": _ava_task_row_to_dict(updated_task),
        }, 200

    except Exception as e:
        logging.exception("Failed to append Ava task progress")
        return {"ok": False, "error": "append_progress_failed", "message": str(e)}, 500


def ava_get_task_record(task_id, include_progress_log=True):
    if not task_id:
        return {"ok": False, "error": "task_id is required"}, 400

    try:
        with engine.connect() as conn:
            task = conn.execute(
                text("SELECT * FROM ava_tasks WHERE id = :task_id"),
                {"task_id": task_id},
            ).mappings().first()

            if not task:
                return {"ok": False, "error": "task_not_found", "task_id": task_id}, 404

            payload = _ava_task_row_to_dict(task)

            if include_progress_log:
                rows = conn.execute(
                    text("""
                        SELECT *
                        FROM ava_task_progress
                        WHERE task_id = :task_id
                        ORDER BY created_at ASC, id ASC
                    """),
                    {"task_id": task_id},
                ).mappings().all()

                payload["progress_log"] = [_ava_progress_row_to_dict(row) for row in rows]

        return {
            "ok": True,
            "task_id": str(task_id),
            "task": payload,
        }, 200

    except Exception as e:
        logging.exception("Failed to get Ava task")
        return {"ok": False, "error": "get_task_failed", "message": str(e)}, 500


def ava_poll_task_records(
    status=None,
    assigned_to=None,
    limit=25,
    include_completed=False,
):
    try:
        limit = int(limit or 25)
    except Exception:
        return {"ok": False, "error": "limit must be an integer"}, 400

    if limit < 1 or limit > 250:
        return {"ok": False, "error": "limit must be between 1 and 250"}, 400

    if status and status not in VALID_AVA_TASK_STATUSES:
        return {
            "ok": False,
            "error": "invalid status",
            "valid_statuses": sorted(VALID_AVA_TASK_STATUSES),
        }, 400

    where = []
    params = {"limit": limit}

    if status:
        where.append("status = :status")
        params["status"] = status

    if assigned_to:
        where.append("assigned_to = :assigned_to")
        params["assigned_to"] = assigned_to

    if not include_completed:
        where.append("status NOT IN ('completed', 'failed', 'cancelled')")

    where_sql = "WHERE " + " AND ".join(where) if where else ""

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(f"""
                    SELECT *
                    FROM ava_tasks
                    {where_sql}
                    ORDER BY created_at DESC
                    LIMIT :limit
                """),
                params,
            ).mappings().all()

        return {
            "ok": True,
            "tasks": [_ava_task_row_to_dict(row) for row in rows],
            "count": len(rows),
        }, 200

    except Exception as e:
        logging.exception("Failed to poll Ava tasks")
        return {"ok": False, "error": "poll_tasks_failed", "message": str(e)}, 500

def create_task(session_id: str, user_id : str):
   logging.info("Creating task for session_id %s and user_id %s", session_id, user_id)
   try:
      with engine.connect() as conn:
         result = conn.execute(text("INSERT INTO jobs (session_id, user_id, status) VALUES (:session_id, :user_id, 'streaming')"), {"session_id": session_id, "user_id": user_id})
         conn.commit()
         # Get the auto-increment id of the row we just inserted (MySQL)
         task_id = result.lastrowid
      logging.info("Created task with id %d", task_id)
      return str(task_id)
   except Exception as e:
      logging.error(f"Failed to create task: {e}")
      return None

def update_task(task_id: int, content: str, stream: str):
    logging.info("Updating task %s with content and stream", task_id)
    try: 
      with engine.connect() as conn:
        gather_task = conn.execute(
        text("SELECT content, stream FROM jobs WHERE id = :task_id"),
        {"task_id": task_id}
        )
        get_task_row = gather_task.mappings().fetchone()

        new_content = ((get_task_row["content"] or "").rstrip() + ("\n" if get_task_row["content"] else "") + (content or "")).strip()
        new_stream  = ((get_task_row["stream"]  or "").rstrip() + ("\n" if get_task_row["stream"]  else "") + (stream  or "")).strip()

        update_result = conn.execute(
            text("""
                UPDATE jobs
                SET content = :content, stream = :stream
                WHERE id = :task_id
            """),
            {"content": new_content, "stream": new_stream, "task_id": task_id}
        )
        logging.info("Updated task %d rows affected: %d", task_id, update_result.rowcount)
        conn.commit()
      logging.info("Updated task %d", task_id)
      return "success"
    except Exception as e:
      logging.error(f"Failed to update task: {e}")
      return "error"

def close_task(task_id: int):
    logging.info("Closing task %d", task_id)
    try: 
      with engine.connect() as conn:
        result = conn.execute(
          text("UPDATE jobs SET status = 'closed' WHERE id = :task_id"),
          {"task_id": task_id}
        )
        conn.commit()
      logging.info("Closed task %d", task_id)
      return "success"
    except Exception as e:
      logging.error(f"Failed to close task: {e}")
      return "error"

def insight_year_edit(year: str, insight: str):
  """Edit the insight for a given year. Returns (ok, error_message)."""
  year = (year or "")
  insight = (insight or "")
  if not year or not insight:
    return "Bad request"
  logging.info(f"Editing year_insights for year={year}, insight={insight}")
  try:
    with engine.connect() as conn:
      result = conn.execute(
        text("UPDATE year_insights SET Insight = :insight WHERE Year = :year"),
        {"year": year, "insight": insight}
      )
      if result.rowcount == 0:
        return "No insight found for that year"
      conn.commit()
    return "Success"
  except Exception as e:
    logging.exception(f"Failed to edit year_insights: {e}")
    return "Server error"
    
def insight_make_edit(make: str, insight: str):
  """Edit the insight for a given make. Returns (ok, error_message)."""
  make = (make or "")
  insight = (insight or "")
  if not make or not insight:
    return "Bad request"
  logging.info(f"Editing make_insights for make={make}, insight={insight}")
  try:
    with engine.connect() as conn:
      result = conn.execute(
        text("UPDATE make_insights SET Insight = :insight WHERE Make = :make"),
        {"make": make, "insight": insight}
      )
      if result.rowcount == 0:
        return "No insight found for that make"
      conn.commit()
    return "Success"
  except Exception as e:
    logging.exception(f"Failed to edit make_insights: {e}")
    return "Server error"
  
def insight_general_edit(topic: str, insight: str):
    """Edit the insight for a given general topic. Returns (ok, error_message)."""
    topic = (topic or "")
    insight = (insight or "")
    if not topic or not insight:
        return "Bad request"
    logging.info(f"Editing general_insights for topic={topic}, insight={insight}")
    try:
        with engine.connect() as conn:
            result = conn.execute(
              text("UPDATE general_insights SET Insight = :insight WHERE Topic = :topic"),
              {"topic": topic, "insight": insight}
            )
            if result.rowcount == 0:
              return "No insight found for that topic"
            conn.commit()
        return "Success"
    except Exception as e:
        logging.exception(f"Failed to edit general_insights: {e}")
        return "Server error"

def insight_get_uniques():
    """Load all unique years, makes and general topics"""
    logging.info("Loading unique insights from %s", DB_NAME)
    try:
      with engine.connect() as conn:
            result = conn.execute(text("SELECT DISTINCT Year FROM year_insights"))
            year_rows = result.mappings().all()
            result = conn.execute(text("SELECT DISTINCT Make FROM make_insights"))
            make_rows = result.mappings().all()
            result = conn.execute(text("SELECT DISTINCT Topic FROM general_insights"))
            general_rows = result.mappings().all()
      if not year_rows and not make_rows and not general_rows:
          logging.info("No insights found")
          return {"years": [], "makes": [], "generals": []}
      years = [row["Year"] for row in year_rows]
      makes = [row["Make"] for row in make_rows]
      generals = [row["Topic"] for row in general_rows]
      logging.info("Loaded %d unique years, %d makes, and %d general topics", len(years), len(makes), len(generals))
      return {"years": years, "makes": makes, "generals": generals}
    except Exception as e:
      logging.error(f"Failed to load unique insights: {e}")
      return {"years": [], "makes": [], "generals": []}

def insight_get(year: str, make: str):
  """Load all insights for year, make, and general."""
  logging.info("Loading insights for year %s and make %s from %s", year, make, DB_NAME)
  try:
    insights = {"year_insights": [], "make_insights": [], "general_insights": []}
    with engine.connect() as conn:
      # Year insights
      result = conn.execute(
        text("SELECT Insight FROM year_insights WHERE Year = :year"),
        {"year": year}
      )
      rows = result.mappings().all()
      insights["year_insights"] = [row["Insight"] for row in rows]
      # Make insights
      result = conn.execute(
        text("SELECT Insight FROM make_insights WHERE Make = :make"),
        {"make": make}
      )
      rows = result.mappings().all()
      insights["make_insights"] = [row["Insight"] for row in rows]
      # General insights
      result = conn.execute(
        text("SELECT * FROM general_insights")
      )
      rows = result.mappings().all()
      insights["general_insights"] = [row["Insight"] for row in rows]
    logging.info("Loaded insights: %s", insights)
    return insights
  except Exception as e:
    logging.error(f"Failed to load insights: {e}")
    return {"year_insights": [], "make_insights": [], "general_insights": []}
    
def external_sale_add(car: dict):
  "Insert a MMR Data point"
  if not car:
    return "Bad request"
  logging.info("Adding MMR data point: car=%s", car)
  try:
    with engine.connect() as conn:
      conn.execute(text("""
        INSERT INTO external_auction_sales (year, make, model, trim, mileage,source,listed_price,listed_date,days_at_auction) VALUES (:year, :make, :model, :trim, :mileage, :source, :listed_price, :listed_date, :days_at_auction)"""),{
          "year": car.get('year'),
          "make": car.get('make'),
          "model": car.get('model'),
          "trim": car.get('trim'),
          "mileage": car.get('mileage'),
          "source": car.get('source'),
          "listed_price": car.get('listed_price'),
          "listed_date": car.get('listed_date'),
          "days_at_auction": car.get('days_at_auction')
        })
      conn.commit()
    return "Success"
  except Exception as e:
      logging.exception("Failed to insert into external_auction_sales: %s", e)
      return "Server error"
  
def external_sale_update(car: dict):
  "Update a external auction sales Data point"
  if not car or 'id' not in car:
    return "Bad request"
  logging.info("Updating external auction sales data point: car=%s", car)
  try:
    with engine.connect() as conn:
      update_fields = ", ".join([f"{key} = :{key}" for key in car.keys() if key != 'id'])
      query = f"UPDATE external_auction_sales SET {update_fields} WHERE year = :year AND make = :make AND model = :model AND trim = :trim AND mileage = :mileage AND source = :source"
      result = conn.execute(text(query), {
          "year": car.get('year'),
          "make": car.get('make'),
          "model": car.get('model'),
          "trim": car.get('trim'),
          "mileage": car.get('mileage'),
          "source": car.get('source')
      })
      if result.rowcount == 0:
          return "No external auction sales data point found with that information"
      conn.commit()
    return "Success"
  except Exception as e:
    logging.exception("Failed to update external_auction_sales: %s", e)
    return "Server error"

def main_get(object: dict):
  """Load a object. returns object, error_message"""
  if not object:
    return None, "Bad request"
  elif 'table' not in object or 'retrieval_type' not in object:
    return None, "Bad request"
  else:
    if object['retrieval_type'] == 'by_id':
      try:
        with engine.connect() as conn:
          result = conn.execute(
            text(f"SELECT * FROM {object['table']} WHERE {object['field_name']} = :id"),
            {"id": object['field_value']}
          )
          row = result.mappings().first()
        with avaengine.connect() as conn:
          result = conn.execute(
            text(f"SELECT * FROM {object['table']} WHERE {object['field_name']} = :id"),
            {"id": object['field_value']}
          )
          row = result.mappings().first()
        if not row:
            return None, f"No {object['table']} found with that id"
        obj = {key: row[key] for key in row.keys()}
        return obj, None
      except Exception as e:
          logging.exception("Failed to load from %s: %s", object['table'], e)
          return None, "Server error"
    elif object['retrieval_type'] == 'all':
      try:
        with engine.connect() as conn:
          result = conn.execute(
            text(f"SELECT * FROM {object['table']}")
          )
          rows = result.mappings().all()
        with avaengine.connect() as conn:
          result = conn.execute(
            text(f"SELECT * FROM {object['table']}")
          )
          rows = result.mappings().all()
        objs = [{key: row[key] for key in row.keys()} for row in rows]
        return objs, None
      except Exception as e:
          logging.exception("Failed to load all from %s: %s", object['table'], e)
          return None, "Server error"
    elif object['retrieval_type'] == 'by_fields':
      try:
        with engine.connect() as conn:
          conditions = " AND ".join([f"{field} = :{field}" for field in object['fields'].keys()])
          query = f"SELECT * FROM {object['table']} WHERE {conditions}"
          result = conn.execute(
            text(query),
            object['fields']
          )
          rows = result.mappings().all()
        with avaengine.connect() as conn:
          conditions = " AND ".join([f"{field} = :{field}" for field in object['fields'].keys()])
          query = f"SELECT * FROM {object['table']} WHERE {conditions}"
          result = conn.execute(
            text(query),
            object['fields']
          )
          rows = result.mappings().all()
        objs = [{key: row[key] for key in row.keys()} for row in rows]
        return objs, None
      except Exception as e:
          logging.exception("Failed to load by fields from %s: %s", object['table'], e)
          return None, "Server error"
    elif object['retrieval_type'] == 'by_field':
      try:
        with engine.connect() as conn:
          query = f"SELECT * FROM {object['table']} WHERE {object['field_name']} = :field_value"
          result = conn.execute(
            text(query),
            {"field_value": object['field_value']}
          )
          rows = result.mappings().all()
        with avaengine.connect() as conn:
          query = f"SELECT * FROM {object['table']} WHERE {object['field_name']} = :field_value"
          result = conn.execute(
            text(query),
            {"field_value": object['field_value']}
          )
          rows = result.mappings().all()
        objs = [{key: row[key] for key in row.keys()} for row in rows]
        return objs, None
      except Exception as e:
          logging.exception("Failed to load by field from %s: %s", object['table'], e)
          return None, "Server error"
    elif object['retrieval_type'] == 'last_row_id':
      try:
        with engine.connect() as conn:
          result = conn.execute(
            text(f"SELECT {object['field_name']} FROM {object['table']} ORDER BY id DESC LIMIT 1")
          )
          row = result.mappings().first()
        with avaengine.connect() as conn:
          result = conn.execute(
            text(f"SELECT {object['field_name']} FROM {object['table']} ORDER BY id DESC LIMIT 1")
          )
          row = result.mappings().first()
        if not row:
            return None, f"No rows found in {object['table']}"
        return row[object['field_name']], None
      except Exception as e:
        logging.exception("Failed to load last_row_id from %s: %s", object['table'], e)
        return None, "Server error"
    else:
      return None, "Bad request"



def main_add(object: dict):
   """Insert a object. returns ok, error_message"""
   if not object:
      return "Bad request"
   elif 'table' not in object:
      return "Bad request"
   else:
      try:
        with engine.connect() as conn:
          result = conn.execute(
            text(f"""INSERT INTO {object['table']} ({", ".join(object['data'].keys())}) VALUES ({", ".join([f":{key}" for key in object['data'].keys()])})"""),
            object['data']
          )
          conn.commit()
        with avaengine.connect() as conn:
          result = conn.execute(
            text(f"""INSERT INTO {object['table']} ({", ".join(object['data'].keys())}) VALUES ({", ".join([f":{key}" for key in object['data'].keys()])})"""),
            object['data']
          )
          conn.commit()
        return "Success"
      except Exception as e:
          logging.exception("Failed to insert into %s: %s", object['table'], e)
          return "Server error"
      
def main_update(object: dict):
   """Update a object. returns ok, error_message"""
   if not object:
      return "Bad request"
   elif 'table' not in object:
      return "Bad request"
   else:
      try:
        with engine.connect() as conn:
          update_fields = ", ".join([f"{key} = :{key}" for key in object['data'].keys()])
          query = f"UPDATE {object['table']} SET {update_fields} WHERE {object['id']['field_name']} = :id"
          result = conn.execute(text(query), {**object['data'], "id": object['id']['field_value']})
          if result.rowcount == 0:
              return f"No {object['table']} found with that id"
          conn.commit()
        with avaengine.connect() as conn:
          update_fields = ", ".join([f"{key} = :{key}" for key in object['data'].keys()])
          query = f"UPDATE {object['table']} SET {update_fields} WHERE {object['id']['field_name']} = :id"
          result = conn.execute(text(query), {**object['data'], "id": object['id']['field_value']})
          if result.rowcount == 0:
              return f"No {object['table']} found with that id"
          conn.commit()
        return "Success"
      except Exception as e:
          logging.exception("Failed to update %s: %s", object['table'], e)
          return "Server error"

def main_delete(object: dict):
   """Delete a object. returns ok, error_message"""
   if not object:
      return "Bad request"
   elif 'table' not in object:
      return "Bad request"
   else:
      try:
        with engine.connect() as conn:
          result = conn.execute(
            text(f"DELETE FROM {object['table']} WHERE {object['id']['field_name']} = :id"),
            {"id": object['id']['field_value']}
          )
          if result.rowcount == 0:
              return f"No {object['table']} found with that id"
          conn.commit()
        with avaengine.connect() as conn:
          result = conn.execute(
            text(f"DELETE FROM {object['table']} WHERE {object['id']['field_name']} = :id"),
            {"id": object['id']['field_value']}
          )
          if result.rowcount == 0:
              return f"No {object['table']} found with that id"
          conn.commit()
        return "Success"
      except Exception as e:
          logging.exception("Failed to delete from %s: %s", object['table'], e)
          return "Server error"
    
def block_add(car: dict):
  """Insert a block report car. Returns (ok, error_message)."""
  if not car or 'vin' not in car or not car['vin']:
      return "Bad request"
  logging.info("Adding block report car: car=%s", car)
  try:
    
      with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO block_report (TTL, `LANE #`,`RUN #`,EXOTIC,YEAR, MAKE, MODEL,SE, BODY,COLOR,VIN,SOURCE,CB,BUYER,WEEK,MILES,COST,EXP,`COST*`,BCF,AT,`BYR TARGET`,`CAR NOTES`,`ANDREW TARGET`,ANNOUNCEMENTS,run_date,external_id)
              VALUES (:ttl, :lane, :run, :exotic, :year, :make, :model, :se, :body, :color, :vin, :source, :cb, :buyer, :week, :miles, :cost, :exp, :cost_star, :bcf, :at, :byr_target, :car_notes, :andrew_target, :announcements, :run_date, :external_id)"""
        ),        {
              "ttl": car.get('ttl'),
              "lane": car.get('lane'),
              "run": car.get('run'),
              "exotic": car.get('exotic'),
              "year": car.get('year'),
              "make": car.get('make'),
              "model": car.get('model'),
              "se": car.get('se'),
              "body": car.get('body'),
              "color": car.get('color'),
              "vin": car['vin'],
              "source": car.get('source'),
              "cb": car.get('cb'),
              "buyer": car.get('buyer'),
              "week": car.get('week'),
              "miles": car.get('miles'),
              "cost": car.get('cost'),
              "exp": car.get('exp'),
              "cost_star": car.get('cost_star'),
              "bcf": car.get('bcf'),
              "at": car.get('at'),
              "byr_target": car.get('byr_target'),
              "car_notes": car.get('car_notes'),
              "andrew_target": car.get('andrew_target'),
              "announcements" : car.get('announcements'),
              "run_date": car.get('run_date'),
              "external_id": car.get('external_id')
            
            }
          
        )
        conn.commit()
      return "Success"
  except Exception as e:
      logging.exception("Failed to insert into block_reported_cars: %s", e)
      return "Server error"
  
def block_search(object: dict):
  """Search for a block report car. returns cars, error_message"""
  try:
    with engine.connect() as conn:
      conditions = " AND ".join([f"`{field}` = :{field}" for field in object['fields'].keys()])
      query = f"SELECT * FROM block_report WHERE {conditions}"
      result = conn.execute(
        text(query),
        object['fields']
      )
      rows = result.mappings().all()
    if not rows:
      logging.info("No block report cars found")
      return [], None
    cars = [{
        "TTL": row["TTL"],
        "LANE #": row["LANE #"],
        "RUN #": row["RUN #"],
        "EXOTIC": row["EXOTIC"],
        "YEAR": row["YEAR"],
        "MAKE": row["MAKE"],
        "MODEL": row["MODEL"],
        "SE": row["SE"],
        "BODY": row["BODY"],
        "COLOR": row["COLOR"],
        "VIN": row["VIN"],
        "SOURCE": row["SOURCE"],
        "CB": row["CB"],
        "BUYER": row["BUYER"],
        "WEEK": row["WEEK"],
        "MILES": row["MILES"],
        "COST": row["COST"],
        "EXP": row["EXP"],
        "COST*": row["COST*"],
        "BCF": row["BCF"],
        "AT": row["AT"],
        "BYR TARGET": row["BYR TARGET"],
        "BLOCK NOTES": row["BLOCK NOTES"],
        "CAR NOTES": row["CAR NOTES"],
        "ANDREW TARGET": row["ANDREW TARGET"],
        "run_date": row["run_date"],
        "id": row["run_id"],
        "external_id": row["external_id"]
    } for row in rows]
    logging.info("Loaded %d block report cars", len(cars))
    return cars, None
  except Exception as e:
    logging.exception("Failed to search block report cars: %s", e)
    return [], "Server error"

def block_get(lane: str, date):
  """Load all block report cars for lane."""
  try:
    with engine.connect() as conn:
      result = conn.execute(
        text("SELECT * FROM block_report WHERE `LANE #` = :lane AND run_date = :run_date"),
        {"lane": lane, "run_date": date}
      )
      rows = result.mappings().all()
    if not rows:
        logging.info("No block report cars found")
        return []
    cars = [{
        "TTL": row["TTL"],
        "LANE #": row["LANE #"],
        "RUN #": row["RUN #"],
        "EXOTIC": row["EXOTIC"],
        "YEAR": row["YEAR"],
        "MAKE": row["MAKE"],
        "MODEL": row["MODEL"],
        "SE": row["SE"],
        "BODY": row["BODY"],
        "COLOR": row["COLOR"],
        "VIN": row["VIN"],
        "SOURCE": row["SOURCE"],
        "CB": row["CB"],
        "BUYER": row["BUYER"],
        "WEEK": row["WEEK"],
        "MILES": row["MILES"],
        "COST": row["COST"],
        "EXP": row["EXP"],
        "COST*": row["COST*"],
        "BCF": row["BCF"],
        "AT": row["AT"],
        "BYR TARGET": row["BYR TARGET"],
        "BLOCK NOTES": row["BLOCK NOTES"],
        "CAR NOTES": row["CAR NOTES"],
        "ANDREW TARGET": row["ANDREW TARGET"],
        "ANNOUNCEMENTS": row["ANNOUNCEMENTS"],
        "CONDITION_REPORT": row["CONDITION_REPORT"],
        "run_date": row["run_date"],
        "id": row["run_id"],
        "external_id": row["external_id"]
    } for row in rows]
    logging.info("Loaded %d block report cars", len(cars))
    return cars
  except Exception as e:
      logging.error(f"Failed to load block report cars: {e}")
      return []

def block_update(id: int, **kwargs):
  """Update a block report car by id. Returns (ok, error_message)."""
  logging.info("Updating block report car %d with %s", id, kwargs)
  try:
    if not kwargs:
      return "No fields to update"
    with engine.connect() as conn:
      params = {}
      parts = []
      for idx, (col, val) in enumerate(kwargs.items()):
        pname = f"p{idx}"
        parts.append(f"`{col}` = :{pname}")
        params[pname] = val
      update_fields = ", ".join(parts)
      logging.info(update_fields)
      query = f"UPDATE block_report SET {update_fields} WHERE run_id = :id"
      params["id"] = id
      result = conn.execute(text(query), params)
      if result.rowcount == 0:
        return "No block report car found with that id"
      conn.commit()
    return "Success"
  except Exception as e:
    logging.exception(f"Failed to update block report car: {e}")
    return "Server error"



async def perform_autocheck(vin: str):
    result = await get_auto_check(vin, ACCUTRADE_URL, ACCUTRADE_LOGIN, ACCUTRADE_PASSWORD)
    return result


def clean_vin(value) -> str:
    return "" if value is None else str(value).strip().upper()


def clean_odometer(value) -> str:
    if value is None:
      return ""
    text_value = str(value).strip().replace(",", "")
    if not text_value:
      return ""
    try:
      return str(int(float(text_value)))
    except (TypeError, ValueError):
      return text_value


def _parse_accutrade_target_auction(page_text: str):
    amount_pattern = r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{4,7})(?:\.\d{2})?"
    patterns = (
      rf"\b(?:target\s+auction|bring\s+money)\b.{{0,160}}?\$\s*{amount_pattern}",
      rf"\$\s*{amount_pattern}.{{0,160}}?\b(?:target\s+auction|bring\s+money)\b",
    )
    normalized = re.sub(r"\s+", " ", str(page_text or ""))
    for pattern in patterns:
      match = re.search(pattern, normalized, flags=re.IGNORECASE)
      if not match:
        continue
      amount = float(match.group(1).replace(",", ""))
      if 500 <= amount <= 5_000_000:
        return amount
    return None


def _is_authenticated_accutrade_url(url: str) -> bool:
    parsed = urlparse(str(url))
    path = parsed.path.rstrip("/")
    return (
      parsed.hostname == "appraiser3.accu-trade.com"
      and not path.startswith("/auth/get-started")
      and not path.startswith("/auth/oauth")
    )


async def _safe_body_text(page, max_length: int = 3000) -> str:
    try:
      return (await page.locator("body").inner_text(timeout=5000))[:max_length]
    except Exception:
      return "<unable to read page body>"


async def _safe_screenshot(page, path: Path) -> None:
    try:
      path.parent.mkdir(parents=True, exist_ok=True)
      await page.screenshot(path=str(path), full_page=True, timeout=30_000)
    except Exception as error:
      logging.info("Could not save screenshot %s: %s: %s", path, type(error).__name__, error)


async def _open_new_appraisal_page(page) -> bool:
    try:
      if "/appraisal/new" not in page.url:
        await page.goto(
          ACCUTRADE_NEW_APPRAISAL_URL,
          wait_until="domcontentloaded",
          timeout=90_000,
        )
      await page.locator(
        'sidebar-start-appraisal-button[data-qa="start-dealer-appraisal"]'
      ).wait_for(state="visible", timeout=90_000)
      return True
    except Exception as error:
      logging.info("Could not open new appraisal page: %s: %s", type(error).__name__, error)
      return False


async def _login_to_accutrade(page, login: str, password: str) -> bool:
    try:
      await page.goto(ACCUTRADE_LOGIN_URL, wait_until="domcontentloaded", timeout=90_000)

      username = page.locator("#username")
      await username.wait_for(state="visible", timeout=60_000)
      await username.fill(str(login))

      continue_button = page.locator("button._button-login-id")
      await continue_button.wait_for(state="visible", timeout=30_000)
      await continue_button.click()

      password_input = page.locator("#password")
      await password_input.wait_for(state="visible", timeout=60_000)
      await password_input.fill(str(password))
      await password_input.press("Enter")

      await page.wait_for_url(
        _is_authenticated_accutrade_url,
        wait_until="domcontentloaded",
        timeout=120_000,
      )

      if not await _open_new_appraisal_page(page):
        raise RuntimeError("Logged in, but appraisal page did not load")
      return True

    except Exception as error:
      logging.info("AccuTrade login failed: %s: %s", type(error).__name__, error)
      logging.info(await _safe_body_text(page))
      await _safe_screenshot(page, ERROR_OUTPUT_DIR / "accutrade_login_failed.png")
      return False

async def _start_dealer_acquisition(page, vin: str) -> None:
    """Open /appraisal/new and submit the VIN through Dealer Acquisition."""
    if not await _open_new_appraisal_page(page):
      raise RuntimeError(f"{vin}: could not load new appraisal page")

    await page.locator(
      'sidebar-start-appraisal-button[data-qa="start-dealer-appraisal"]'
    ).hover()

    dealer_acquisition = page.locator(
      ".start-appraisal-type:visible", has_text="Dealer Acquisition"
    )
    await dealer_acquisition.wait_for(state="visible", timeout=30_000)
    await dealer_acquisition.click()

    vin_input = page.locator(".vin-input")
    await vin_input.first.wait_for(state="visible", timeout=30_000)
    await vin_input.first.fill(vin)
    await vin_input.first.press("Enter")

async def _select_vehicle_after_vin(page, vin: str, *, allow_restart: bool = True) -> str:
    cards = page.locator("appraisal-card")
    trims = page.locator("new-appraisal-trim-choice")
    odometer_input = page.locator('[data-qa="odometer-input"]')

    try:
      await page.wait_for_function(
        """
        () => document.querySelectorAll('appraisal-card').length > 0
           || document.querySelectorAll('new-appraisal-trim-choice').length > 0
           || document.querySelector('[data-qa="odometer-input"]') !== null
        """,
        timeout=90_000,
      )
    except Exception as error:
      raise RuntimeError(
        f"{vin}: AccuTrade loaded no appraisal, trim list, or odometer input"
      ) from error

    if await odometer_input.count() > 0:
      try:
        await odometer_input.wait_for(state="visible", timeout=10_000)
        return "direct_load"
      except Exception:
        pass

    if await cards.count() > 0:
      live_cards = cards.filter(has_not=page.locator(".expired-footer:visible"))
      live_count = await live_cards.count()

      logging.info(
        "%s: %s appraisal card(s), %s not expired, %s trim block(s)",
        vin, await cards.count(), live_count, await trims.count(),
      )

      if live_count > 0:
        await live_cards.first.wait_for(state="visible", timeout=30_000)
        await live_cards.first.click()
        await odometer_input.wait_for(state="visible", timeout=90_000)
        return "existing_appraisal"

      # All expired. The trim selector on this same page starts a new appraisal.
      logging.info("%s: all appraisals expired, selecting a trim instead", vin)

    if await trims.count() > 0:
      logging.info("%s: %s trim choice(s), selecting the first", vin, await trims.count())
      await trims.first.wait_for(state="visible", timeout=30_000)
      await trims.first.click()
      await odometer_input.wait_for(state="visible", timeout=90_000)
      return "trim_selected"

    if await cards.count() > 0:
      raise RuntimeError(
        f"{vin}: only expired appraisals and no trim selector on the page"
      )

    await odometer_input.wait_for(state="visible", timeout=30_000)
    return "direct_load"


async def _wait_for_enabled_odometer_input(page, timeout_ms: int = 15_000):
    odometer_input = page.locator('[data-qa="odometer-input"]')
    await odometer_input.wait_for(state="visible", timeout=timeout_ms)

    # Wait for editable state. This avoids Locator.fill timeout when the field is visible but disabled.
    await page.wait_for_function(
      """
      () => {
        const input = document.querySelector('[data-qa="odometer-input"]');
        if (!input) return false;
        const disabled = input.disabled || input.hasAttribute('disabled');
        const readonly = input.readOnly || input.hasAttribute('readonly');
        return !disabled && !readonly;
      }
      """,
      timeout=timeout_ms,
    )

    return odometer_input


async def _set_odometer_with_retries(page, vin: str, odometer: str, attempts: int = 3) -> None:
    last_error = None

    for attempt in range(1, attempts + 1):
      try:
        odometer_input = await _wait_for_enabled_odometer_input(page, timeout_ms=15_000)

        await odometer_input.click(timeout=10_000)
        await odometer_input.fill("", timeout=10_000)
        await odometer_input.type(str(odometer), delay=35, timeout=20_000)
        await odometer_input.press("Enter", timeout=10_000)
        return

      except Exception as error:
        last_error = error
        logging.info(
          "AccuTrade odometer entry retry %s/%s for VIN %s failed: %s: %s",
          attempt,
          attempts,
          vin,
          type(error).__name__,
          error,
        )

        # Recovery: try to re-open appraisal page and re-select flow when available.
        if attempt < attempts:
          try:
            await _open_new_appraisal_page(page)
            dealer_acquisition = page.locator(
              ".start-appraisal-type:visible", has_text="Dealer Acquisition"
            )
            if await dealer_acquisition.count() > 0:
              await dealer_acquisition.first.click(timeout=10_000)

            vin_input = page.locator(".vin-input")
            if await vin_input.count() > 0:
              await vin_input.first.fill(str(vin), timeout=15_000)
              await vin_input.first.press("Enter", timeout=10_000)

            await _select_vehicle_after_vin(page, vin)
          except Exception as recovery_error:
            logging.info(
              "AccuTrade odometer recovery for VIN %s failed: %s: %s",
              vin,
              type(recovery_error).__name__,
              recovery_error,
            )

          await page.wait_for_timeout(1_000 * attempt)

    raise RuntimeError(
      f"{vin}: unable to set odometer after {attempts} attempts ({type(last_error).__name__}: {last_error})"
    )


async def _save_carfax_pdf(page, context, vin: str) -> Path:
    badge = page.locator("carfax-badge:visible")
    await badge.wait_for(state="visible", timeout=90_000)
    await badge.click()

    outer_iframe = page.locator('html-iframe > iframe[srcdoc*="connect.carfax.com"]')
    try:
      await outer_iframe.wait_for(state="attached", timeout=45_000)
    except Exception as error:
      await _safe_screenshot(page, CARFAX_OUTPUT_DIR / f"{vin}_carfax_click_failed.png")
      raise RuntimeError("CARFAX badge did not open the report iframe") from error

    outer_handle = await outer_iframe.element_handle()
    outer_frame = await outer_handle.content_frame() if outer_handle else None
    if outer_frame is None:
      raise RuntimeError("Outer CARFAX iframe did not initialize")

    inner_iframe = outer_frame.locator('iframe[src*="connect.carfax.com"]')
    await inner_iframe.wait_for(state="attached", timeout=60_000)
    inner_handle = await inner_iframe.element_handle()
    carfax_frame = await inner_handle.content_frame() if inner_handle else None
    if carfax_frame is None:
      raise RuntimeError("CARFAX report frame did not initialize")

    await carfax_frame.locator("#vehicle-history-report").wait_for(
      state="attached", timeout=90_000
    )

    report_url = carfax_frame.url
    if not report_url or report_url in {"about:blank", "about:srcdoc"}:
      raise RuntimeError(f"CARFAX report URL unusable: {report_url}")

    pdf_path = CARFAX_OUTPUT_DIR / f"{vin}_carfax_report.pdf"
    pdf_path.parent.mkdir(parents=True, exist_ok=True)

    pdf_page = await context.new_page()
    try:
      await pdf_page.goto(report_url, wait_until="domcontentloaded", timeout=90_000)
      await pdf_page.locator("#vehicle-history-report").wait_for(
        state="attached", timeout=90_000
      )
      await pdf_page.emulate_media(media="print")
      await pdf_page.pdf(
        path=str(pdf_path),
        format="Letter",
        print_background=True,
        prefer_css_page_size=True,
        margin={"top": "0.25in", "right": "0.25in", "bottom": "0.25in", "left": "0.25in"},
      )
    finally:
      try:
        await pdf_page.close()
      except Exception:
        pass

    return pdf_path


async def _close_report_modal(page, title: str) -> None:
    modal = page.locator(".herc-modal-container:visible").filter(has_text=title).last
    try:
      if await modal.count() == 0:
        return
      close_button = modal.locator("herc-icon.close-button").first
      await close_button.click(timeout=10_000)
      await modal.wait_for(state="hidden", timeout=15_000)
    except Exception as error:
      logging.info(
        "Could not close %s modal: %s: %s",
        title,
        type(error).__name__,
        error,
      )


async def _close_carfax_modal(page) -> None:
    modal = page.locator(".modal-content.open:visible").filter(
      has=page.locator(
        'html-iframe > iframe[srcdoc*="connect.carfax.com"]'
      )
    ).last
    if await modal.count() == 0:
      return

    close_button = modal.locator(
      'mat-icon[data-qa="modal-close-button"]'
    ).first
    try:
      await close_button.wait_for(state="visible", timeout=10_000)
      await close_button.click(timeout=10_000)
      await modal.wait_for(state="hidden", timeout=15_000)
      return
    except Exception as click_error:
      logging.info(
        "CARFAX close button failed; trying Escape: %s: %s",
        type(click_error).__name__,
        click_error,
      )

    await page.keyboard.press("Escape")
    try:
      await modal.wait_for(state="hidden", timeout=10_000)
    except Exception as error:
      raise RuntimeError(
        "CARFAX modal remained open after close attempts"
      ) from error


async def _save_autocheck_pdf(page, context, vin: str) -> Path:
    badge = page.locator("autocheck-badge:visible")
    await badge.wait_for(state="visible", timeout=90_000)
    await badge.click()

    modal = page.locator(".herc-modal-container:visible").filter(
      has_text="AutoCheck® Report"
    ).last
    try:
      await modal.wait_for(state="visible", timeout=45_000)
      iframe = modal.locator("html-iframe > iframe.visible").first
      await iframe.wait_for(state="attached", timeout=60_000)

      iframe_handle = await iframe.element_handle()
      report_frame = await iframe_handle.content_frame() if iframe_handle else None
      if report_frame is None:
        raise RuntimeError("AutoCheck report iframe did not initialize")

      await report_frame.locator("#full-report").wait_for(
        state="attached", timeout=90_000
      )
      report_text = await report_frame.locator("body").inner_text(timeout=30_000)
      normalized_text = "".join(report_text.upper().split())
      if "EXPERIANAUTOCHECKREPORT" not in normalized_text:
        raise RuntimeError("AutoCheck iframe did not contain a full report")
      if vin not in normalized_text:
        raise RuntimeError(f"AutoCheck report did not match VIN {vin}")
      report_url = report_frame.url
      report_is_embedded = not report_url or report_url in {
        "about:blank", "about:srcdoc"
      }
      embedded_report_html = None
      if report_is_embedded:
        try:
          embedded_report_html = await report_frame.evaluate(
            """() => {
              const clone = document.documentElement.cloneNode(true);
              clone.querySelectorAll('script').forEach((script) => script.remove());
              const head = clone.querySelector('head') || clone.insertBefore(
                document.createElement('head'), clone.firstChild
              );
              const base = document.createElement('base');
              base.href = document.baseURI;
              head.prepend(base);
              return '<!DOCTYPE html>' + clone.outerHTML;
            }"""
          )
          logging.info(
            "AutoCheck report for VIN %s is embedded at %s; captured HTML fallback",
            vin,
            report_url or "<empty>",
          )
        except Exception as snapshot_error:
          logging.info(
            "AutoCheck embedded report snapshot failed for VIN %s: %s: %s",
            vin,
            type(snapshot_error).__name__,
            snapshot_error,
          )

      pdf_path = AUTOCHECK_OUTPUT_DIR / f"{vin}_autocheck_report.pdf"
      pdf_path.parent.mkdir(parents=True, exist_ok=True)

      print_button = report_frame.locator("#singleSummaryButton")
      await print_button.wait_for(state="visible", timeout=30_000)
      popup_task = asyncio.create_task(
        context.wait_for_event("page", timeout=10_000)
      )
      download_task = asyncio.create_task(
        page.wait_for_event("download", timeout=10_000)
      )
      await print_button.click(timeout=30_000)

      done, pending = await asyncio.wait(
        {popup_task, download_task},
        timeout=10,
        return_when=asyncio.FIRST_COMPLETED,
      )
      for task in pending:
        task.cancel()
      if pending:
        await asyncio.gather(*pending, return_exceptions=True)

      download = None
      pdf_page = None
      if download_task in done and not download_task.cancelled():
        try:
          download = download_task.result()
        except Exception:
          download = None
      if popup_task in done and not popup_task.cancelled():
        try:
          pdf_page = popup_task.result()
        except Exception:
          pdf_page = None

      if download is not None:
        await download.save_as(str(pdf_path))
        if not pdf_path.exists() or pdf_path.stat().st_size == 0:
          raise RuntimeError("AutoCheck download did not produce a PDF")
        if pdf_path.read_bytes()[:5] != b"%PDF-":
          raise RuntimeError("AutoCheck download was not a PDF")
        return pdf_path

      if pdf_page is None:
        # Some AutoCheck builds invoke the browser print UI without opening a tab.
        pdf_page = await context.new_page()
      try:
        if pdf_page.url in {"", "about:blank", "about:srcdoc"} and embedded_report_html:
          await pdf_page.set_content(
            embedded_report_html,
            wait_until="domcontentloaded",
            timeout=90_000,
          )
        elif pdf_page.url in {"", "about:blank", "about:srcdoc"} and not report_is_embedded:
          await pdf_page.goto(report_url, wait_until="domcontentloaded", timeout=90_000)
        elif pdf_page.url in {"", "about:blank", "about:srcdoc"}:
          raise RuntimeError("AutoCheck report had no navigable URL or embedded document")
        await pdf_page.wait_for_load_state("domcontentloaded", timeout=90_000)
        await pdf_page.locator("#full-report").wait_for(
          state="attached", timeout=90_000
        )
        printed_text = await pdf_page.locator("body").inner_text(timeout=30_000)
        normalized_printed_text = "".join(printed_text.upper().split())
        if "EXPERIANAUTOCHECKREPORT" not in normalized_printed_text:
          raise RuntimeError("AutoCheck print page did not contain a full report")
        if vin not in normalized_printed_text:
          raise RuntimeError(f"AutoCheck print page did not match VIN {vin}")
        try:
          await pdf_page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
          pass
        await pdf_page.emulate_media(media="print")
        await pdf_page.pdf(
          path=str(pdf_path),
          format="Letter",
          print_background=True,
          prefer_css_page_size=True,
          margin={"top": "0.25in", "right": "0.25in", "bottom": "0.25in", "left": "0.25in"},
        )
      finally:
        try:
          await pdf_page.close()
        except Exception:
          pass

      if not pdf_path.exists() or pdf_path.stat().st_size == 0:
        raise RuntimeError("AutoCheck PDF was not produced")
      return pdf_path
    except Exception:
      await _safe_screenshot(page, AUTOCHECK_OUTPUT_DIR / f"{vin}_autocheck_failed.png")
      raise
    finally:
      await _close_report_modal(page, "AutoCheck® Report")


async def _process_accutrade_vehicle(page, context, vin: str, odometer: str) -> dict:
    vin = clean_vin(vin)
    odometer = clean_odometer(odometer)
    result = {
      "vin": vin,
      "odometer": odometer,
      "success": False,
      "accutrade_screenshot": None,
      "accutrade": {"target_auction": None},
      "carfax_pdf": None,
      "autocheck_pdf": None,
      "stage_status": {
        "accutrade": {
          "navigation": "pending",
          "screenshot": "pending",
          "valuation_parse": "pending",
        },
        "carfax": {"report_pdf": "pending"},
        "autocheck": {"report_pdf": "pending"},
      },
      "error": None,
      "errors": [],
    }

    if not vin:
      result["error"] = "VIN is blank"
      return result
    if not odometer:
      result["error"] = "Odometer is blank"
      return result

    try:
      await _start_dealer_acquisition(page, vin)

      await _select_vehicle_after_vin(page, vin)

      await _set_odometer_with_retries(page, vin, odometer, attempts=3)

      await page.locator("carfax-badge:visible").wait_for(state="visible", timeout=90_000)
      await page.wait_for_timeout(5000)
      result["stage_status"]["accutrade"]["navigation"] = "complete"

      ACCUTRADE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
      screenshot = ACCUTRADE_OUTPUT_DIR / f"{vin}_accutrade.png"
      await page.screenshot(path=str(screenshot), full_page=True, timeout=30_000)
      result["accutrade_screenshot"] = str(screenshot)
      result["stage_status"]["accutrade"]["screenshot"] = "complete"

      try:
        page_text = await page.locator("body").inner_text(timeout=15_000)
        target_auction = _parse_accutrade_target_auction(page_text)
        result["accutrade"]["target_auction"] = target_auction
        result["stage_status"]["accutrade"]["valuation_parse"] = (
          "complete" if target_auction is not None else "not_found"
        )
        if target_auction is None:
          result["errors"].append(
            "AccuTrade target auction was not found in visible page text"
          )
      except Exception as accutrade_parse_error:
        result["stage_status"]["accutrade"]["valuation_parse"] = "error"
        result["errors"].append(
          "AccuTrade valuation parse error: "
          f"{type(accutrade_parse_error).__name__}: {accutrade_parse_error}"
        )

      try:
        result["carfax_pdf"] = str(await _save_carfax_pdf(page, context, vin))
        result["stage_status"]["carfax"]["report_pdf"] = "complete"
      except Exception as carfax_error:
        result["stage_status"]["carfax"]["report_pdf"] = "error"
        result["errors"].append(
          f"CARFAX error: {type(carfax_error).__name__}: {carfax_error}"
        )

      try:
        await _close_carfax_modal(page)
      except Exception as carfax_close_error:
        result["errors"].append(
          "CARFAX close error: "
          f"{type(carfax_close_error).__name__}: {carfax_close_error}"
        )
        result["error"] = "; ".join(result["errors"])
        result["success"] = True
        return result

      try:
        result["autocheck_pdf"] = str(
          await _save_autocheck_pdf(page, context, vin)
        )
        result["stage_status"]["autocheck"]["report_pdf"] = "complete"
      except Exception as autocheck_error:
        result["stage_status"]["autocheck"]["report_pdf"] = "error"
        result["errors"].append(
          f"AutoCheck error: {type(autocheck_error).__name__}: {autocheck_error}"
        )

      if result["errors"]:
        result["error"] = "; ".join(result["errors"])

      result["success"] = True
      return result

    except Exception as error:
      result["error"] = f"AccuTrade error: {type(error).__name__}: {error}"
      await _safe_screenshot(page, ERROR_OUTPUT_DIR / f"{vin}_failed.png")
      return result


def _playwright_import_or_none():
    try:
      from playwright.async_api import async_playwright
      return async_playwright
    except Exception:
      return None

# ---------------------------------------------------------------------
# PERSISTENT ACCUTRADE BROWSER SESSION
# ---------------------------------------------------------------------

ACCUTRADE_KEEPALIVE_SECONDS = int(os.environ.get("ACCUTRADE_KEEPALIVE_SECONDS", "240"))

_accutrade_playwright = None
_accutrade_browser = None
_accutrade_context = None
_accutrade_keeper_page = None
_accutrade_session_lock = asyncio.Lock()
_accutrade_operation_lock = asyncio.Lock()
_accutrade_keepalive_task = None


async def _accutrade_teardown():
    global _accutrade_playwright, _accutrade_browser, _accutrade_context, _accutrade_keeper_page

    for closeable in (_accutrade_keeper_page, _accutrade_context, _accutrade_browser):
      if closeable is not None:
        try:
          await closeable.close()
        except Exception:
          pass

    if _accutrade_playwright is not None:
      try:
        await _accutrade_playwright.stop()
      except Exception:
        pass

    _accutrade_keeper_page = None
    _accutrade_context = None
    _accutrade_browser = None
    _accutrade_playwright = None


async def _accutrade_launch():
    """Start Playwright, launch one browser + context, log in on the keeper tab."""
    global _accutrade_playwright, _accutrade_browser, _accutrade_context, _accutrade_keeper_page

    async_playwright_loader = _playwright_import_or_none()
    if async_playwright_loader is None:
      raise RuntimeError("playwright is not installed on this runtime")

    if not ACCUTRADE_LOGIN or not ACCUTRADE_PASSWORD:
      raise RuntimeError("ACCUTRADE_LOGIN or ACCUTRADE_PASSWORD is not configured")

    for directory in (
      ACCUTRADE_OUTPUT_DIR,
      CARFAX_OUTPUT_DIR,
      AUTOCHECK_OUTPUT_DIR,
      ERROR_OUTPUT_DIR,
    ):
      directory.mkdir(parents=True, exist_ok=True)

    await _accutrade_teardown()

    _accutrade_playwright = await async_playwright_loader().start()
    _accutrade_browser = await _accutrade_playwright.chromium.launch(headless=HEADLESS)
    _accutrade_context = await _accutrade_browser.new_context(
      user_agent=USER_AGENT,
      viewport={"width": 1920, "height": 1080},
      accept_downloads=True,
    )
    _accutrade_keeper_page = await _accutrade_context.new_page()

    if not await _login_to_accutrade(
      _accutrade_keeper_page, ACCUTRADE_LOGIN, ACCUTRADE_PASSWORD
    ):
      await _accutrade_teardown()
      raise RuntimeError("AccuTrade login failed during browser startup")

    logging.info("AccuTrade persistent browser session established")


async def _accutrade_session_is_live() -> bool:
    if _accutrade_browser is None or _accutrade_context is None or _accutrade_keeper_page is None:
      return False
    if not _accutrade_browser.is_connected():
      return False
    if _accutrade_keeper_page.is_closed():
      return False

    # Loads /appraisal/new and waits for the sidebar button. If the session
    # expired, Auth0 redirects and the button never appears.
    return await _open_new_appraisal_page(_accutrade_keeper_page)


async def _accutrade_ensure_session():
    """Return a logged-in context. Re-logs in, or relaunches, when needed."""
    async with _accutrade_session_lock:
      if await _accutrade_session_is_live():
        return _accutrade_context

      logging.info("AccuTrade session not live, re-establishing")

      # Cheap path: browser is alive, just the auth cookie died.
      if (
        _accutrade_browser is not None
        and _accutrade_browser.is_connected()
        and _accutrade_keeper_page is not None
        and not _accutrade_keeper_page.is_closed()
      ):
        if await _login_to_accutrade(
          _accutrade_keeper_page, ACCUTRADE_LOGIN, ACCUTRADE_PASSWORD
        ):
          logging.info("AccuTrade re-login succeeded on existing browser")
          return _accutrade_context

      await _accutrade_launch()
      return _accutrade_context


async def _accutrade_keepalive_loop():
    while True:
      await asyncio.sleep(ACCUTRADE_KEEPALIVE_SECONDS)
      try:
        async with _accutrade_operation_lock:
          await _accutrade_ensure_session()
      except asyncio.CancelledError:
        raise
      except Exception as error:
        logging.error(
          "AccuTrade keepalive failed: %s: %s", type(error).__name__, error
        )


@app.before_serving
async def _accutrade_before_serving():
    global _accutrade_keepalive_task

    try:
      await _accutrade_launch()
    except Exception as error:
      # Do not block boot. _accutrade_ensure_session retries on first request.
      logging.error(
        "AccuTrade startup login failed: %s: %s", type(error).__name__, error
      )

    if _accutrade_keepalive_task is None or _accutrade_keepalive_task.done():
      _accutrade_keepalive_task = asyncio.create_task(_accutrade_keepalive_loop())


@app.after_serving
async def _accutrade_after_serving():
    global _accutrade_keepalive_task

    if _accutrade_keepalive_task is not None:
      _accutrade_keepalive_task.cancel()
      try:
        await _accutrade_keepalive_task
      except BaseException:
        pass
      _accutrade_keepalive_task = None

    await _accutrade_teardown()


async def get_accutrade_details(vin: str, odometer, *, headless: bool = True) -> dict:
    async with _accutrade_operation_lock:
      return await _get_accutrade_details_locked(vin, odometer, headless=headless)


async def _get_accutrade_details_locked(vin: str, odometer, *, headless: bool = True) -> dict:
    # headless is kept for call-site compatibility; the persistent browser is
    # launched once at startup using HEADLESS.
    try:
      context = await _accutrade_ensure_session()
    except Exception as error:
      logging.exception("AccuTrade session unavailable")
      return {
        "vin": clean_vin(vin),
        "success": False,
        "error": f"AccuTrade session unavailable: {type(error).__name__}: {error}",
      }

    page = await context.new_page()
    try:
      return await _process_accutrade_vehicle(page, context, vin, odometer)

    except Exception as fatal:
      logging.exception("Fatal AccuTrade tab error")
      return {
        "vin": clean_vin(vin),
        "success": False,
        "error": f"Fatal AccuTrade tab error: {type(fatal).__name__}: {fatal}",
      }

    finally:
      try:
        await page.close()
      except Exception:
        pass


def _inline(path_value, mime_type: str):
    if not path_value:
      return None
    path_obj = Path(path_value)
    if not path_obj.exists():
      return None
    return {
      "filename": path_obj.name,
      "mime_type": mime_type,
      "size_bytes": path_obj.stat().st_size,
      "data_b64": base64.b64encode(path_obj.read_bytes()).decode("ascii"),
    }


def _extract_evidence_pdf_text(path_value, expected_vin: str):
    path = Path(str(path_value or ""))
    status = {
      "available": bool(path_value),
      "extracted": False,
      "page_count": 0,
      "character_count": 0,
      "vin_matched": None,
    }
    if not path_value or not path.exists():
      return "", status
    if PdfReader is None:
      status["error"] = "pypdf is not installed"
      return "", status

    try:
      reader = PdfReader(str(path))
      pages = [str(page.extract_text() or "").strip() for page in reader.pages]
      report_text = "\n\n".join(page for page in pages if page).strip()
    except Exception as error:
      status["error"] = f"{type(error).__name__}: {error}"
      logging.info("Could not extract evidence PDF text from %s: %s", path, error)
      return "", status

    normalized_text = re.sub(r"\s+", "", report_text).upper()
    vin = clean_vin(expected_vin)
    status.update({
      "page_count": len(reader.pages),
      "character_count": len(report_text),
      "vin_matched": not vin or vin in normalized_text,
    })
    if not report_text:
      status["error"] = "pypdf extracted no text"
      return "", status
    if vin and not status["vin_matched"]:
      status["error"] = f"extracted report did not contain expected VIN {vin}"
      return "", status

    status["extracted"] = True
    return report_text, status


def _check_evidence_auth(authorization: str) -> bool:
    token = (authorization or "").replace("Bearer ", "").strip()
    if SERVICE_TOKEN and token == SERVICE_TOKEN:
      return True
    if SA_AUTHENTICATION and token == SA_AUTHENTICATION:
      return True
    return not SERVICE_TOKEN and not SA_AUTHENTICATION

def sale_add(car: dict):
    """Insert a sale. Returns (ok, error_message)."""
    if not car or 'vin' not in car or not car['vin']:
        return "Bad request"
    
    logging.info("Adding sale: car=%s", car)
    try:
        with engine.connect() as conn:
            conn.execute(
              text("INSERT INTO sales (VIN, Year, Make, Model, Mileage, Color, SalePrice, SaleDate, Notes) VALUES (:vin, :year, :make, :model, :mileage, :color, :sale_price, :sale_date, :notes)"),
              {
                  "vin": car['vin'],
                  "year": car.get('year'),
                  "make": car.get('make'),
                  "model": car.get('model'),
                  "mileage": car.get('mileage'),
                  "color": car.get('color'),
                  "sale_price": car.get('sale_price'),
                  "sale_date": car.get('sale_date'),
                  "notes": car.get('notes'),
              },
            )
            conn.commit()
        return "Success"
    except Exception as e:
        logging.exception("Failed to insert into sales: %s", e)
        return "Server error"

def sales_get(year: int, make: str, model: str):
    """Load all sales for year and make."""
    logging.info("Loading historical_sales for year %s and make %s from %s", year, make, DB_NAME)
    try:
      with engine.connect() as conn:
            result = conn.execute(
              text("SELECT * FROM historical_sales WHERE ModelYear LIKE :year AND Make LIKE :make AND Model LIKE :model AND SoldDate >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)"),
              {"year": f"%{year}%", "make": f"%{make}%", "model": f"%{model}%"}
            )
            rows = result.mappings().all()
      if not rows:
          logging.info("No sales found")
          return []
      sales = [{
          "VIN": row["VIN"],
          "Year": row["Year"],
          "Make": row["Make"],
          "Model": row["Model"],
          "Mileage": row["Mileage"],
          "Color": row["Color"],
          "SalePrice": row["SalePrice"],
          "SaleDate": row["SaleDate"],
          "Notes": row["Notes"]
      } for row in rows]
      logging.info("Loaded %d sales", len(sales))
      return sales
    except Exception as e:
      logging.error(f"Failed to load sales: {e}")
      return []


def sales_full_get():
    """Load all sales matching a query."""
    try:
      with engine.connect() as conn:
            result = conn.execute(
              text("SELECT * FROM sales")
            )
            rows = result.mappings().all()
      if not rows:
          logging.info("No sales found")
          return []
      sales = [{
          "VIN": row["VIN"],
          "Year": row["Year"],
          "Make": row["Make"],
          "Model": row["Model"],
          "Mileage": row["Mileage"],
          "Color": row["Color"],
          "SalePrice": row["SalePrice"],
          "SaleDate": row["SaleDate"],
          "Notes": row["Notes"]
      } for row in rows]
      logging.info("Loaded %d sales", len(sales))
      return sales
    except Exception as e:
      logging.error(f"Failed to load sales: {e}")
      return []
    
def sale_query(query: str):
    """Load all sales matching a query."""
    logging.info("Loading sales matching query %s from %s", query, DB_NAME)
    try:
      with engine.connect() as conn:
            result = conn.execute(
              text(query)
            )
            rows = result.mappings().all()
            rows = [dict(row) for row in rows]
      if not rows:
          logging.info("No sales found")
          return []
      
      return rows
    except Exception as e:
      logging.error(f"Failed to load sales: {e}")
      return []

def ava_query(query: str):
    """Load all sales matching a query."""
    logging.info("Running query %s on AVA database", query)
    try:

      with pbengine.connect() as conn:
            result = conn.execute(
              text(query)
            )
            rows = result.mappings().all()
            rows = [dict(row) for row in rows]
      if not rows:
          logging.info("No results found")
          return []
      
      return rows
    except Exception as e:
      logging.error(f"Failed to run query on AVA database: {e}")
      return []
def rewrite_query(query: str, access_token: str, table: str) -> str | None:
    """
    Injects an assigned_user_id filter into a SELECT query based on
    the user's visibility scope. Returns None if the user has no access.
    
    USER_VISIBILITY format: { "user_id": ["*"] | ["id1", "id2", ...] }
    "*" = no restriction (admin), None/missing = no access
    """
    logging.info("Rewriting query for access control: query=%s, table=%s", query, table)
    USER_SCOPED_TABLES = {
        'leads',
        'accounts',
        'opportunities'
    }
    #Make this the hardcoded access from the database(PULL THE USER AUTH TOKEN AND ACCESS FROM THE DB USING THE ACCESS TOKEN)
    try:
      with engine.connect() as conn:
          result = conn.execute(
              text("SELECT userId,user_access_md FROM ava_users WHERE authorization = :token"),
              {"token": access_token}
          )
          row = result.mappings().first()
          user_id = row["userId"] if row else None
          USER_VISIBILITY = []
          if row and 'user_access_md' in row and row['user_access_md']:
            USER_VISIBILITY = [v.strip() for v in row['user_access_md'].split(',') if v.strip()]
    except Exception as e:
        logging.error(f"Failed to retrieve user info for access token: {e}")
        return None
    if not user_id:
        logging.info("No user found for access token")
        return None
    if table not in USER_SCOPED_TABLES:
        logging.info("Table %s is not user-scoped, returning original query", table)
        return query

    visible_ids = USER_VISIBILITY

    if visible_ids is None:
        logging.info("User %s has no visibility for table %s", user_id, table)
        return None

    if visible_ids == ["*"]:
        return query

    statement = sqlglot.parse_one(query, read="mysql")

    in_clause = exp.In(
        this=exp.Column(
            this=exp.Identifier(this="assigned_user_id"),
            table=exp.Identifier(this=table),
        ),
        expressions=[exp.Literal.string(uid) for uid in visible_ids]
    )

    return statement.where(in_clause).sql(dialect="mysql")
def ava_flex_query(query: str, instance: str):
    #Pull the users table check for teh ava_token, and pull the user info for the instance. 
    flexengine = sqlalchemy.create_engine("mysql+pymysql://", creator=lambda: getflexconn(instance), pool_pre_ping=True)
    logging.info("Running query %s on AVA database", query)
    try:
      with flexengine.connect() as conn:
            result = conn.execute(
              text(query)
            )
            rows = result.mappings().all()
            rows = [dict(row) for row in rows]
      if not rows:
          logging.info("No results found")
          return []
      
      return rows
    except Exception as e:
      logging.error(f"Failed to run query on AVA database: {e}")
      return []

AVA_SCHEMA_TABLES = {
    "accounts", "block_report", "buyers_teams_managers", "calls", "calls_leads",
    "campaigns", "dma_by_zip_latest", "documents", "documents_leads_1_c",
    "email_addr_bean_rel", "email_addresses", "emails", "emails_text",
    "gmtv_photos_cstm", "gmtv_photos_leads_1", "historical_sales",
    "historical_valuations", "leads", "leads_audit", "leads_cstm",
    "leads_google_data", "notes", "opportunities", "opportunities_audit",
    "opportunities_cstm", "rolus_sms_log", "rolus_sms_log_cstm",
    "rolus_sms_log_gmtv_photos_1", "rolus_sms_log_leads_1_c", "tasks",
    "upsheets", "users",
}
AVA_SALES_SCHEMA_TABLES = {"block_report", "historical_sales", "historical_valuations"}


def database_schemas(tables, instance: str) -> dict:
    """Return authoritative SQLAlchemy metadata for allowlisted database tables."""
    requested = list(dict.fromkeys(str(table).strip() for table in tables if str(table).strip()))
    unknown = sorted(set(requested) - AVA_SCHEMA_TABLES)
    if unknown:
        raise ValueError(f"Unknown schema table(s): {', '.join(unknown)}")

    sales = set(requested) & AVA_SALES_SCHEMA_TABLES
    crm = set(requested) - AVA_SALES_SCHEMA_TABLES
    if sales and crm:
        raise ValueError("CRM and sales schemas must be requested separately")

    selected_engine = engine if sales else sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=lambda: getflexconn(instance),
        pool_pre_ping=True,
    )
    try:
        with selected_engine.connect() as conn:
            inspector = sqlalchemy.inspect(conn)
            available = set(inspector.get_table_names()) | set(inspector.get_view_names())
            missing = sorted(set(requested) - available)
            if missing:
                raise LookupError(f"Table(s) not found: {', '.join(missing)}")

            schemas = {}
            for table in requested:
                primary_keys = set((inspector.get_pk_constraint(table) or {}).get("constrained_columns") or [])
                schemas[table] = {
                    "columns": [
                        {
                            "name": column["name"],
                            "type": str(column.get("type") or ""),
                            "nullable": bool(column.get("nullable", True)),
                            "default": None if column.get("default") is None else str(column.get("default")),
                            "primary_key": column["name"] in primary_keys,
                        }
                        for column in inspector.get_columns(table)
                    ]
                }
            return schemas
    finally:
        if selected_engine is not engine:
            selected_engine.dispose()


    def _safe_schema_error(exc: Exception) -> str:
      detail = str(exc).replace("\n", " ").replace("\r", " ")[:1000]
      for secret in (PB_PASS, DB_PASS, USER_DB_PASS):
        if secret:
          detail = detail.replace(secret, "<redacted>")
      return detail

def insight_general_add(topic: str, insight: str):
    """Insert a buyer. Returns (ok, error_message)."""
    topic = (topic or "")
    insight = (insight or "")
    if not insight:
        return "Bad request"
    
    logging.info("Adding general insight: insight=%s", insight)
    try:
        with engine.connect() as conn:
            conn.execute(
              text("INSERT INTO general_insights (Topic, Insight) VALUES (:topic, :insight)"),
              {"topic": topic, "insight": insight},
            )
            conn.commit()
        return "Success"
    except Exception as e:
        logging.exception("Failed to insert into insights: %s", e)
        return "Server error"

def insight_year_add(year: str, insight: str):
    """Insert a buyer. Returns (ok, error_message)."""
    year = (year or "")
    insight = (insight or "")
    if not year or not insight:
        return "Bad request"
    
    logging.info("Adding insight to year: year=%s, insight=%s", year, insight)
    try:
        with engine.connect() as conn:
            conn.execute(
              text("INSERT INTO year_insights (Year, Insight) VALUES (:year, :insight)"),
              {"year": year, "insight": insight},
            )
            conn.commit()
        return "Success"
    except Exception as e:
        logging.exception("Failed to insert into insights: %s", e)
        return "Server error"
    
def insight_make_add(make: str, insight: str):
    """Insert a buyer. Returns (ok, error_message)."""
    make = (make or "")
    insight = (insight or "")
    if not make or not insight:
        return "Bad request"
    
    logging.info("Adding insight to make: make=%s, insight=%s", make, insight)
    try:
        with engine.connect() as conn:
            conn.execute(
              text("INSERT INTO make_insights (Make, Insight) VALUES (:make, :insight)"),
              {"make": make, "insight": insight},
            )
            conn.commit()
        return "Success"
    except Exception as e:
        logging.exception("Failed to insert into insights: %s", e)
        return "Server error"

def strategy_add(car: str, strategy: str, learning: str):
    """Insert a buyer. Returns (ok, error_message)."""
    car = (car or "")
    strategy = (strategy or "")
    learning = (learning or "")
    if not car or not strategy or not learning:
        return "Bad request"
    
    logging.info("Adding strategy to car: car=%s, strategy=%s, learning=%s", car, strategy, learning)
    try:
        with engine.connect() as conn:
            conn.execute(
              text("INSERT INTO car_strategies (Car, Strategy, Learning) VALUES (:car, :strategy, :learning)"),
              {"car": car, "strategy": strategy, "learning": learning},
            )
            conn.commit()
        return "Success"
    except IntegrityError as e:
        # Likely UNIQUE violation on phone
        logging.warning("Duplicate phone or integrity error: %s", e)
        return "Strategy for this car already exists"
    except Exception as e:
        logging.exception("Failed to insert into strategies: %s", e)
        return "Server error"

def strategy_uniques(year: str, make: str):
    """Load all buyers as [{'name':..., 'phone':...}, ...]."""
    logging.info("Loading unique cars from %s", DB_NAME)
    try:
      with engine.connect() as conn:
        result = conn.execute(
            text("SELECT DISTINCT Car FROM car_strategies WHERE Car LIKE :car_pattern"),
            {"car_pattern": f"{year} {make}%"}
        )
        rows = result.mappings().all()
      if not rows:
          logging.info("No strategies found")
          return []
      group = [row["Car"] for row in rows]
      logging.info("Loaded %d unique cars", len(group))
      return group
    except Exception as e:
      logging.error(f"Failed to load unique cars: {e}")
      return []

def strategy_edit(car: str, learning: str):
    """Insert a buyer. Returns (ok, error_message)."""
    car = (car or "")
    learning = (learning or "")
    if not car or not learning:
        return "Bad request"
    
    logging.info("Editing strategy to car: car=%s, learning=%s", car, learning)
    try:
        with engine.connect() as conn:
            result = conn.execute(
              text("UPDATE car_strategies SET Learning = :learning WHERE Car = :car"),
              {"car": car, "learning": learning}
            )
            if result.rowcount == 0:
              return "No strategy found for that car"
            conn.commit()
        return "Success"
    except Exception as e:
        logging.exception("Failed to edit strategy: %s", e)
        return "Server error"

def andrew_group_add(name: str, phone: str):
    """Insert a buyer. Returns (ok, error_message)."""
    name = (name or "")
    phone = (phone or "").strip()
    if not name or not phone:
        return "Bad request"
    
    logging.info("Adding buyer to group: name=%s, phone=%s", name, phone)
    try:
        with engine.connect() as conn:
            conn.execute(
              text("INSERT INTO andrew_buyers (name, phone) VALUES (:name, :phone)"),
              {"name": name, "phone": phone},
            )
            conn.commit()
        return "Success"
    except IntegrityError as e:
        # Likely UNIQUE violation on phone
        logging.warning("Duplicate phone or integrity error: %s", e)
        return "Buyer with this phone already exists"
    except Exception as e:
        logging.exception("Failed to insert into buyer group: %s", e)
        return "Server error"

def group_remove(phone: str):
    """Delete a buyer by phone. Returns (ok, error_message)."""
    phone = (phone or "").strip()
    if not phone:
        return "Phone is required"

    logging.info("Removing buyer with phone=%s from group", phone)
    try:
        with engine.connect() as conn:
            result = conn.execute(
              text("DELETE FROM buyers WHERE phone = :phone"),
              {"phone": phone}
            )
            if result.rowcount == 0:
              return "No buyer found for that phone"
            conn.commit()
        return "Success"
    except Exception as e:
        logging.exception("Failed to remove buyer: %s", e)
        return "Server error"

def andrew_group_remove(phone: str):
    """Delete a buyer by phone. Returns (ok, error_message)."""
    phone = (phone or "").strip()
    if not phone:
        return "Phone is required"

    logging.info("Removing buyer with phone=%s from group", phone)
    try:
        with engine.connect() as conn:
            result = conn.execute(
              text("DELETE FROM andrew_buyers WHERE phone = :phone"),
              {"phone": phone}
            )
            if result.rowcount == 0:
              return "No buyer found for that phone"
            conn.commit()
        return "Success"
    except Exception as e:
        logging.exception("Failed to remove buyer: %s", e)
        return "Server error"

def modify_send_through_prism(msg):
  if platform == None:
         setuprcsdk()
  platform.login( jwt= os.environ.get('RC_USER_JWT') )
  send_phone = read_extension_phone_number_detect_sms_feature()
  logging.info(f"Sending phone-number {send_phone}")
  sentmsg = f"Unknown:\n{msg['message']}"
  logging.info("Scattering SMS.")
  logging.info("Loaded group: ")
  group = group_init()
  logging.info(group)
  for phone in group:
    logging.info(phone['phone'])
    if phone['phone'] == msg['phone'].replace(" ",""):
      logging.info(f"Prism source: {msg['message']}")
      sentmsg = f"From {phone['name']}:\n{msg['message']}"
      logging.info(f"Prism message: {sentmsg}")
      try:
         with engine.connect() as conn:
            conn.execute(
              text("INSERT INTO chat_log (source, content, timestamp) VALUES (:source, :content, :timestamp)"),
              {"source": phone['name'], "content": msg['message'], "timestamp": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}
            )
            conn.commit()
      except Exception as e:
          logging.error(f"Failed to log chat message: {e}")
      break
  for phone in group:
    if phone['phone'] != msg['phone'].replace(" ",""):
      send_sms(send_phone, phone['phone'], sentmsg, file_paths=msg['file_path'])

def refresh_token():
    if platform.logged_in() == False:
        print("Both tokens expired => Relogin.")
        platform.login(jwt= "RC_USER_JWT")
    else:
        print("Token valid.")

def download_mms_attachments(attachments):
  refresh_token()
  uploads_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
  os.makedirs(uploads_dir, exist_ok=True)
  file_paths = []
  for attachment in attachments:
    fileName = str(attachment['id'])
    if attachment['type'] == "MmsAttachment":
      fileNameExt = attachment['contentType'].split("/")
      fileName = ("%s.%s" % (fileName, fileNameExt[1]))
      file_path = os.path.join(uploads_dir, fileName)
      try:
        res = platform.get(attachment['uri'])
        with open(file_path, 'wb') as file:
          file.write(res.body())
        file_paths.append(file_path)
      except Exception as e:
        logging.error(f"Failed to download MMS attachment {attachment['id']}: {e}")
  return file_paths

def andrew_modify_send_through_prism(msg):
  if platform == None:
         setuprcsdk()
  platform.login( jwt= os.environ.get('RC_USER_JWT') )
  send_phone = read_extension_phone_number_detect_sms_feature()
  logging.info(f"Sending phone-number {send_phone}")
  sentmsg = f"Andrew System:\n{msg['message']}"
  logging.info("Scattering SMS.")
  logging.info("Loaded group: ")
  group = andrew_group_init()
  logging.info(group)

  for phone in group:
    logging.info(phone['phone'])
    if phone['phone'] == msg['phone'].replace(" ",""):
      logging.info(f"Prism source: {msg['message']}")
      sentmsg = f"From Andrew System:\n{msg['message']}"
      logging.info(f"Prism message: {sentmsg}")
      break
  #Eventually this will have to also handle files when andrew gains the capability to parse images.
  for phone in group:
    if phone['phone'] != msg['phone'].replace(" ",""):
      send_sms(send_phone, phone['phone'], sentmsg, file_paths=[])


prism_schema = {
  'schema' : {
    'type' : 'object',
    "properties" : {
      'phone' : {
        'type' : 'string'
      },
      'message' : {
        'type' : 'string'
      }
    }
  }
}

#STATIC PAGE ROUTES

def check_authentication(authtoken: str):
  try:
      with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM ava_users WHERE `authorization` = :auth"), {"auth": authtoken})
        row = result.mappings().first()
        if row is not None:
          return True
  except Exception as e:
      logging.error(f"Failed to check authentication: {e}")
      return False

def check_authentication_user(authtoken: str):
  try:
      with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM ava_users WHERE `authorization` = :auth"), {"auth": authtoken})
        row = result.mappings().first()
        if row is not None:
          return {"user": row.get("userId")}
        return False
  except Exception as e:
      logging.error(f"Failed to check authentication: {e}")
      return False

@app.route('/login', methods=['GET'])
async def login():
  return await send_file('static/login.html')

@app.route('/andrew-chat', methods=['GET'])
async def andrew_chat():
  token = request.args.get("token")
  if not token or not check_authentication(token):
    return await send_file('static/login.html')
  return await send_file('static/andrew_chat_ui.html')

@app.route('/ava-chat', methods=['GET'])
async def ava_chat():
  token = request.args.get("token")
  if not token or not check_authentication(token):
    return await send_file('static/login.html')
  return await send_file('static/ava_chat_ui.html')

@app.route('/users', methods=['GET'])
async def users():
  token = request.args.get("token")
  if not token or not check_authentication(token):
    return await send_file('static/login.html')
  return await send_file('static/users.html')

@app.route('/ava_youtube_login', methods=['GET'])
async def ava_youtube_login():
  token = request.args.get("token")
  if not token or not check_authentication(token):
    return await send_file('static/login.html')
  return await send_file('static/ava_youtube_login.html')

@app.route('/home', methods=['GET'])
async def front_page():
  token = request.args.get("token")
  if not token or not check_authentication(token):
    return await send_file('static/login.html')
  return await send_file('static/users.html')

@app.route('/ava_dashboard', methods=['GET'])
async def ava_dashboard():
  token = request.args.get("token")
  if not token or not check_authentication(token):
    return await send_file('static/login.html')
  return await send_file('static/ava_dashboards.html')

@app.route('/ava_chat_embed', methods=['GET'])
async def ava_chat_embed():
  token = request.args.get("ava_token")
  if not token or not check_authentication(token):
    return "No access", 403
  return await send_file('static/ava_chat_embed.js')

@app.route('/ava_valuation_embed', methods=['GET'])
async def ava_cards_embed():
  token = request.args.get("ava_token")
  if not token or not check_authentication(token):
    return "No access", 403
  return await send_file('static/ava_cards_embed.js')

@app.route('/andrew_chat_embed', methods=['GET'])
async def andrew_chat_embed():
  token = request.args.get("andrew_token")
  if not token or not check_authentication(token):
    return "No access", 403
  return await send_file('static/andrew_chat_embed.js')

@app.route('/liveruns', methods=['GET'])
async def liveruns():
  token = request.args.get("token")
  if not token or not check_authentication(token):
    return await send_file('static/login.html')
  return await send_file('static/andrew_block_report.html')

#AGENT TOOL ENDPOINTS
@app.route('/api/v1/ava/tasks/create', methods=['POST'])
async def create_ava_todo_task_route():
    auth_token = request.headers.get("Authorization")
    if auth_token != SA_AUTHENTICATION:
        return {"ok": False, "error": "Unauthorized"}, 403

    data = await request.get_json(silent=True) or {}

    payload, status_code = ava_create_task_record(
        title=data.get("title"),
        description=data.get("description"),
        assigned_to=data.get("assigned_to"),
        priority=data.get("priority"),
        due_date=data.get("due_date"),
        metadata=data.get("metadata"),
        status=data.get("status", "queued"),
        progress=data.get("progress", 0),
    )

    return payload, status_code


@app.route('/api/v1/ava/tasks/update', methods=['POST'])
async def update_ava_todo_task_route():
    auth_token = request.headers.get("Authorization")
    if auth_token != SA_AUTHENTICATION:
        return {"ok": False, "error": "Unauthorized"}, 403

    data = await request.get_json(silent=True) or {}

    payload, status_code = ava_update_task_record(
        task_id=data.get("task_id"),
        title=data.get("title"),
        description=data.get("description"),
        assigned_to=data.get("assigned_to"),
        priority=data.get("priority"),
        due_date=data.get("due_date"),
        status=data.get("status"),
        progress=data.get("progress"),
        metadata=data.get("metadata"),
    )

    return payload, status_code


@app.route('/api/v1/ava/tasks/progress', methods=['POST'])
async def append_ava_todo_task_progress_route():
    auth_token = request.headers.get("Authorization")
    if auth_token != SA_AUTHENTICATION:
        return {"ok": False, "error": "Unauthorized"}, 403

    data = await request.get_json(silent=True) or {}

    payload, status_code = ava_append_task_progress_record(
        task_id=data.get("task_id"),
        message=data.get("message"),
        progress=data.get("progress"),
        status=data.get("status"),
        metadata=data.get("metadata"),
    )

    return payload, status_code


@app.route('/api/v1/ava/tasks/<task_id>', methods=['GET'])
async def get_ava_todo_task_route(task_id):
    auth_token = request.headers.get("Authorization")
    if auth_token != SA_AUTHENTICATION:
        return {"ok": False, "error": "Unauthorized"}, 403

    include_progress_log = (
        request.args.get("include_progress_log", "true").lower() == "true"
    )

    payload, status_code = ava_get_task_record(
        task_id=task_id,
        include_progress_log=include_progress_log,
    )

    return payload, status_code


@app.route('/api/v1/ava/tasks', methods=['GET'])
async def poll_ava_todo_tasks_route():
    auth_token = request.headers.get("Authorization")
    if auth_token != SA_AUTHENTICATION:
        return {"ok": False, "error": "Unauthorized"}, 403

    payload, status_code = ava_poll_task_records(
        status=request.args.get("status"),
        assigned_to=request.args.get("assigned_to"),
        limit=request.args.get("limit", 25),
        include_completed=request.args.get("include_completed", "false").lower() == "true",
    )

    return payload, status_code

async def _prism_websearch_generate(message: str, *, model: str = "gemini-2.5-flash", temperature: float = 0.0):
    """Prism's shared Google Search runner.

    The public /api/v1/prism/web route and the valuation pipeline both use this exact
    implementation, so valuation web research is not a second custom search stack.
    """
    def _run():
        return webclient.models.generate_content(
            model=model,
            contents=message,
            config=GenerateContentConfig(
                temperature=temperature,
                tools=[Tool(google_search=GoogleSearch())],
                # Keep Google Search grounding, but never let the SDK run a
                # hidden client-side AFC loop around this bounded helper call.
                automatic_function_calling=AutomaticFunctionCallingConfig(disable=True),
            ),
        )

    return await asyncio.to_thread(_run)


@app.route('/api/v1/prism/web', methods=['POST'])
async def prism_web():
  try:
    body = await request.get_json()
    search = body.get('message')
    if not search:
      return {"error": "message is required"}, 400
    response = await _prism_websearch_generate(search)
    logging.info("Web search response generated")
    return {"response": response.text}, 200
  except Exception as e:
      logging.exception("Prism web search failed")
      return {"error": f"{type(e).__name__}: {e}"}, 500


@app.route('/api/v1/prism/sales/add', methods=['POST'])
async def add_sale():
    data = await request.get_json(silent=True) or {}
    result = sale_add(data)
    if result == "Success":
        return ("Sale added", 200)
    else:
        return (result, 400)
    

@app.route('/api/v1/prism/ringcentral/message_store' , methods=['POST'])
async def message_store():
  data = await request.get_json(silent=True) or {}
  phone = data.get("phone")
  extension = data.get("extension")
  if not phone or not extension:
    return "Phone and extension are required", 400
  dateTo = data.get("dateTo","")
  dateFrom = data.get("dateFrom")
  endpoint = f"/restapi/v1.0/account/~/extension/{extension}/message-store?dateFrom={dateFrom}&dateTo={dateTo}&messageType=SMS&direction=Inbound&view=Detailed&phoneNumber={phone}"
  if platform == None:
    setuprcsdk()
  resp = platform.get(endpoint)
  if resp.status_code != 200:
    logging.error(f"Failed to fetch message store from RingCentral: {resp.text}")
    return "Failed to fetch message store", 500
  jsonObj = resp.json()
  return jsonObj, 200
  
  
@app.route('/api/v1/prism/ava/suggestions', methods = ['POST'])
async def ava_suggestions():
    #TEMPORARY REMOVE AUTH CHECK
    #authentication = request.headers.get("Authorization")
    #if not authentication or authentication != SA_AUTHENTICATION:
    #    return "Unauthorized", 401
    data = await request.get_json(silent=True) or {}
    chat_log = data.get("chat_log", [])
    lead_id = data.get("lead_id")

    try:
      with pbengine.connect() as conn:
        result = conn.execute(
          text("SELECT * FROM leads_cstm WHERE id_c = :lead_id"), 
          {"lead_id": lead_id}
        )
        lead = result.mappings().first()
        if not lead:
          logging.error(f"Lead with id {lead_id} not found")
    except Exception as e:
        lead = None
        logging.error(f"Failed to retrieve lead info: {e}")
    if lead:
      logging.info(f"Lead info: {lead}")
    prompt = f"""
    Please help in writing the next short 1-2 sentence response in the chat, provided is the conversation, last few messages, and the lead info. Review the lead, following the standard process of Give me the VIN(the car buying company, the lead already submitted their vin), determine the best possible response to continue the process forward, answer concerns, or gather information to help the customer finish selling their cars(fill out their upsheet). Always try to move the conversation forward, and be proactive in asking for information or offering solutions. Lookup the lead's message store IF not enough info is in the chat_log. Do not say any errors, assume your response is being fed as a direct suggestion like auto-correct.
    chat_log: {json.dumps(chat_log)}
    lead_info: {dict(lead) if lead else "Lead not found"}
    """
    response = await webclient.aio.models.generate_content(
      model=PRISM_SUGGESTIONS_MODEL,
      contents=prompt,
      config=GenerateContentConfig(
        temperature=0.0,
        top_p=1.0,
        max_output_tokens=300,
        automatic_function_calling=AutomaticFunctionCallingConfig(disable=True),
      ),
    )
    message_assembly = (response.text or "").strip()
    logging.info(f"Suggestions model response: {message_assembly}")
    return {"response" : message_assembly}, 200
    

@app.route('/api/v1/prism/temperature', methods = ['POST'])
async def temperature():
    authentication = request.headers.get("Authorization")
    if not authentication or authentication != SA_AUTHENTICATION:
        return "Unauthorized", 401
    data = await request.get_json(silent=True) or {}
    lead_id = data.get("lead_id")
    try:
      with pbengine.connect() as conn:
        result = conn.execute(
          text("SELECT * FROM leads WHERE id = :lead_id")
        )
        lead = result.mappings().first()
        if not lead:
          logging.error(f"Lead with id {lead_id} not found")
    except Exception as e:
        logging.error(f"Failed to retrieve lead info: {e}")
    
    if lead:
      logging.info(f"Lead info: {lead}")
    sms_chat_log = data.get("chat_log", [])
    if sms_chat_log == []:
      try:
        with pbengine.connect() as conn:
          result = conn.execute(
            text("SELECT * FROM rolus_sms_log WHERE lead_id = :lead_id ORDER BY timestamp DESC LIMIT 20"),
            {"lead_id": lead_id}
          )
          sms_chat_log = [dict(row) for row in result.mappings().all()]
      except Exception as e:
          logging.error(f"Failed to retrieve SMS chat log: {e}")
    email_log = data.get("email_log", [])
    buyer_extension = data.get("buyer_extension", "")
    get_session = requests.get(f'https://sale.andrew-chat.com/api/v1/session/prism_temperature', headers = {"Authorization": SA_AUTHENTICATION})
    if get_session.status_code != 200:
      logging.error(f"Failed to get session for temperature: {get_session.text}")
      return "Failed to get session", 500
    session_id = get_session.json()['session_id']
    temperature_schema = {
      'schema' : {
        'type' : 'object',
        "properties" : {
          'temperature' : {
            'type' : 'string',
            'enum': ['Hot', 'Warm', 'Cold']
          },
          'explanation' : {
            'type' : 'string'
          }
        }
    }
    }
    prompt = f"""
    Please review the following lead, and correspondence information. Determine if the lead is Hot, Warm, or Cold. Use the following criteria:
    Hot: Lead has provided most of their car information, is actively engaging in conversation, and is asking questions about selling their car or the process. Has clearly shown interest in selling their car. Showing interest in the offer.
    Warm: Lead has shown some interest in selling their car, but has not provided all car information or is not engaging as actively. May be asking some questions but not showing clear intent to sell yet. May have provided some information but not enough to determine true interest level. 
    Cold: Lead is not engaging, has not provided car information, and is not asking questions. May have only responded with one word answers or not responded at all. Shows little to no interest in selling their car currently. May be unresponsive, has offers from other companies and shows no interest in going lower, or have given up on selling.
    Reply in json format, with only the temperature assessment, and a brief explanation for the rating based on the criteria. Do not include any other information.
    Format schema: {temperature_schema}
    Current time: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}
    sms_chat_log: {json.dumps(sms_chat_log)}
    email_log: {json.dumps(email_log)}
    buyer_extension: {buyer_extension}
    lead_id: {lead_id}
    lead_info: {dict(lead) if lead else "Lead not found"}
    """
    payload = {
       "action" : "create",
       "message" : prompt,
       "session_id": session_id,
       "user_id": "prism_temperature"
    }

    response = requests.post(f'https://sale.andrew-chat.com/api/v1/prism', headers = {"Authorization": SA_AUTHENTICATION}, json=payload)
    if response.status_code != 200:
      logging.error(f"Failed to send message for suggestions: {response.text}")
      return "Failed to send message", 500
    json_ava = response.json()
    message_assembly = ""
    for chunk in json_ava:
          if "text" in chunk:
            message_assembly += " " + chunk['text']
    close_session = requests.post(f'https://ava.andrew-chat.com/api/v1/session/prism_temperature', json={"session_id": session_id}, headers = {"Authorization": SA_AUTHENTICATION})
    return {"response": message_assembly}, 200

@app.route('/api/v1/prism/bug_submission', methods = ['POST'])
async def bug_submission():
    data = await request.get_json(silent=True) or {}
    expected_description = data.get("user_preferred_description", "")
    bug_report = data.get("user_bug_report", "")
    network_logs = data.get("network_logs", [])
    console_logs = data.get("console_logs", [])
    performance_logs = data.get("performance_logs", [])
    sent_files = data.get("sent_files", [])
    user_email = data.get("user_email", "")
    
    user = data.get("user", "Unknown User")
    page = data.get("page", "Unknown Page")
    description = f"User: {user}\n\nUser Description: {expected_description}\n\nBug Report: {bug_report}"
    
    prompt = f"""
    Review the following logs, try and determine where the bug originates. Then, provide a detailed description of the bug, and potential steps to reproduce. If the bug is not clear from the logs, provide a detailed description of what additional information is needed to identify and fix the bug.
    {description}
    Logs:
    {json.dumps({"network_logs": network_logs, "console_logs": console_logs, "performance_logs": performance_logs}, indent=2)}
    """
    try:

      message_assembly = ava_helper([{
                    "role": "user",
                    "content": prompt,
            }],[])  
        
      description += "\n\nAI Analysis:\n" + message_assembly
    except:
      description +='\n\nAI Analysis: Failed to retrieve AI analysis.'
    try:
      with engine.connect() as conn:
        conn.execute(
          text("INSERT INTO bug_reports (description, user_, page, network_log, console_log, performance_log) VALUES (:description, :user, :page, :network_logs, :console_logs, :performance_logs)"),
          {"description": description, "user": user, "page": page, "network_logs": json.dumps(network_logs), "console_logs": json.dumps(console_logs), "performance_logs": json.dumps(performance_logs)},
        )
        conn.commit()
      creds = None
      creds, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/gmail.send'])
      message = EmailMessage()
      message.set_content(description)
      message['To'] = "bugs@givemethevin.com"
      message['Cc'] = user_email
      message['From'] = "andrew@givemethevin.com"
      message['Subject'] = "Bug: " + data.get('subject', 'No subject')

      # Attach log files as text attachments
      if network_logs:
        network_log_content = json.dumps(network_logs, indent=2)
        message.add_attachment(network_log_content, subtype='plain', filename='network_logs.txt')
      
      if console_logs:
        console_log_content = json.dumps(console_logs, indent=2)
        message.add_attachment(console_log_content, subtype='plain', filename='console_logs.txt')
      
      if performance_logs:
        performance_log_content = json.dumps(performance_logs, indent=2)
        message.add_attachment(performance_log_content, subtype='plain', filename='performance_logs.txt')

      # Decode and attach base64 files to the email
      for index, file_data in enumerate(sent_files):
        try:
          # 1. Extract header and base64 data safely BEFORE decoding
          if ',' in file_data:
            mime_header, b64_string = file_data.split(',', 1)
            
            # Extract MIME type from header
            if 'data:' in mime_header:
              mime_type = mime_header.split('data:')[1].split(';')[0]
            else:
              mime_type = "application/octet-stream"
          else:
            # If there's no comma, assume the whole string is the raw base64 data
            b64_string = file_data
            mime_type = "application/octet-stream"

          # 2. Decode ONLY the base64 string
          file_bytes = base64.b64decode(b64_string)

          # 3. Parse MIME type into maintype and subtype
          if '/' in mime_type:
            maintype, subtype = mime_type.split('/', 1)
          else:
            maintype = mime_type
            subtype = "octet-stream"

          # 4. Generate filename with appropriate extension
          ext_map = {
            ("image", "png"): "png",
            ("image", "jpeg"): "jpg",
            ("image", "jpg"): "jpg",
            ("image", "gif"): "gif",
            ("image", "webp"): "webp",
            ("application", "pdf"): "pdf",
          }
          
          ext = ext_map.get((maintype, subtype), subtype or "bin")
          filename = f"attachment_{index + 1}.{ext}"

          # 5. Attach the file
          message.add_attachment(file_bytes, maintype=maintype, subtype=subtype, filename=filename)

        except Exception as e:
          logging.error(f"Failed to process and attach file at index {index}: {e}")

      # Send the email
      try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
          smtp.login("andrew@givemethevin.com", GMAIL_NOTIFICATION_CREDENTIALS)
          smtp.send_message(message)
        logging.info("Email sent successfully to bugs@givemethevin.com")
        logging.info(f"Email Info: To: bugs@givemethevin.com, From: andrew@givemethevin.com, Subject: {data.get('subject')}")
      except Exception as e:
        logging.error(f"Failed to send email: {e}")
      except Exception as e:
        return f"Error: {str(e)}", 500
      return "Bug report submitted successfully", 200
    except Exception as e:
      logging.exception("Failed to insert bug report: %s", e)
      return "Server error", 500

@app.route('/api/v1/prism/autocheck/<vin>', methods=['GET'])
async def autocheck(vin):
    auth_token = request.headers.get('Authorization')
    if auth_token != SA_AUTHENTICATION:
        return "Unauthorized", 403
    try:
      result = await asyncio.wait_for(
        perform_autocheck(clean_vin(vin)),
        timeout=AUTOCHECK_EVIDENCE_TIMEOUT,
      )
      return {"vin": clean_vin(vin), "autocheck": result}, 200
    except asyncio.TimeoutError:
      return {"error": "AutoCheck collection timed out."}, 504
    except Exception as error:
      logging.exception("AutoCheck collection failed for VIN %s", vin)
      return {"error": f"AutoCheck error: {type(error).__name__}: {error}"}, 502


@app.route('/api/v1/prism/evidence/health', methods=['GET'])
async def evidence_health():
    auth_token = request.headers.get('Authorization')
    if not _check_evidence_auth(auth_token):
      return {"error": "Unauthorized"}, 401

    return {
      "ok": True,
      "headless": HEADLESS,
      "max_concurrency": MAX_CONCURRENCY,
      "evidence_dir": str(EVIDENCE_DIR),
      "accutrade_login_configured": bool(ACCUTRADE_LOGIN),
    }, 200


@app.route('/evidence/browser', methods=['POST'])
async def evidence_browser():
    #auth_token = request.headers.get('Authorization')
    #if not _check_evidence_auth(auth_token):
    #  return {"error": "Bad or missing service token"}, 401

    payload = await request.get_json(silent=True) or {}
    vin = clean_vin(payload.get("vin"))
    odometer = clean_odometer(payload.get("odometer"))
    keep_files = bool(payload.get("keep_files", False))

    if not vin:
      return {"error": "vin is required"}, 422
    if not odometer:
      return {"error": "odometer is required"}, 422

    started = time.monotonic()
    async with _evidence_semaphore:
      result = await get_accutrade_details(vin, odometer, headless=HEADLESS)

    errors = [result["error"]] if result.get("error") else []
    artifacts = {
      "accutrade_screenshot": _inline(result.get("accutrade_screenshot"), "image/png"),
      "carfax_pdf": _inline(result.get("carfax_pdf"), "application/pdf"),
      "autocheck_pdf": _inline(result.get("autocheck_pdf"), "application/pdf"),
    }

    for label, artifact in artifacts.items():
      if artifact is None:
        errors.append(f"{label}: not produced")

    report_text = {}
    report_text_status = {}
    stage_status = result.get("stage_status") or {}
    for report_name, path_value in (
      ("carfax", result.get("carfax_pdf")),
      ("autocheck", result.get("autocheck_pdf")),
    ):
      text_value, text_status = await asyncio.to_thread(
        _extract_evidence_pdf_text,
        path_value,
        vin,
      )
      report_text_status[report_name] = text_status
      report_stage = stage_status.setdefault(report_name, {})
      report_stage["text_extraction"] = (
        "complete" if text_status.get("extracted") else
        "error" if text_status.get("error") else
        "not_produced"
      )
      report_stage["vin_match"] = (
        "complete" if text_status.get("vin_matched") is True else
        "mismatch" if text_status.get("vin_matched") is False else
        "not_checked"
      )
      if text_value:
        report_text[report_name] = text_value

    if not keep_files:
      for path_value in (
        result.get("accutrade_screenshot"),
        result.get("carfax_pdf"),
        result.get("autocheck_pdf"),
      ):
        if path_value:
          try:
            Path(path_value).unlink(missing_ok=True)
          except Exception:
            pass

    return {
      "vin": vin,
      "success": bool(result.get("success")) and artifacts["accutrade_screenshot"] is not None,
      "artifacts": {key: value for key, value in artifacts.items() if value},
      "accutrade": result.get("accutrade") or {"target_auction": None},
      "autocheck": {
        "report_collected": artifacts["autocheck_pdf"] is not None,
        "artifact": "autocheck_pdf",
        "sections": {},
      },
      "report_text": report_text,
      "report_text_status": report_text_status,
      "stage_status": stage_status,
      "errors": errors,
      "elapsed_seconds": round(time.monotonic() - started, 1),
    }, 200

@app.route('/api/v1/prism/sales/query', methods=['POST'])
#RED ALERT FIX
async def query_sales():
    auth_token = request.headers.get('Authorization')
    if auth_token != SA_AUTHENTICATION:
      return "Unauthorized", 403
    data = await request.get_json(silent=True) or {}
    query = data.get("query")
    sales = sale_query(query)
    return (sales, 200)

@app.route('/api/v1/prism/ava/get', methods=['POST'])
async def ava_get_call():
    auth_token = request.headers.get('Authorization')
    if auth_token != SA_AUTHENTICATION:
      return "Unauthorized", 403
    
    data = await request.get_json(silent=True) or {}
    ava_q = data.get("query")
    ava = ava_query(ava_q)
    return (ava, 200)

@app.route('/api/v1/prism/ava/get/<environment>', methods=['POST'])
async def ava_get_call_flex(environment):
    auth_token = request.headers.get('Authorization')
    if auth_token != SA_AUTHENTICATION:
      return "Unauthorized", 403
    data = await request.get_json(silent=True) or {}
    ava_q = data.get("query")
    ava_table = data.get("table")
    ava_t = data.get("access_token")
    ava_q = rewrite_query(ava_q,ava_t,ava_table)
    if ava_q == None:
       return "Failed to add authorization, recheck access token and ensure SELECT query only with correct table name is being used", 400
    logging.info(f"Rewritten query: {ava_q}")
    ava = ava_flex_query(ava_q, environment)
    return (ava, 200)

@app.route('/api/v1/prism/ava/schema/<environment>', methods=['POST'])
async def ava_schema_call(environment):
    auth_token = request.headers.get('Authorization')
    if auth_token != SA_AUTHENTICATION:
      return "Unauthorized", 403

    data = await request.get_json(silent=True) or {}
    tables = data.get("tables") or []
    if not isinstance(tables, list) or not tables:
      return {"ok": False, "error": "tables must be a non-empty list"}, 400
    try:
      schemas = await asyncio.to_thread(database_schemas, tables, environment)
    except ValueError as exc:
      return {"ok": False, "error": str(exc)}, 400
    except LookupError as exc:
      return {"ok": False, "error": str(exc)}, 404
    except Exception as exc:
      logging.exception("Failed to load database schemas")
      return {
        "ok": False,
        "error": f"schema lookup failed: {type(exc).__name__}",
        "detail": _safe_schema_error(exc),
      }, 500
    return {"ok": True, "schemas": schemas}, 200

@app.route('/api/v1/prism/youtube/analytics', methods=['POST'])
async def youtube_analytics_v2():
  """
  YouTube Analytics API endpoint using OAuth token from Authorize('youtube_analytics').
  
  Actions:
  - channel_analytics: Get overall channel analytics
  - video_analytics: Get analytics for a specific video
  - top_videos: Get top performing videos
  - audience_demographics: Get audience demographic data
  - traffic_sources: Get traffic source data
  - engagement_metrics: Get engagement metrics (likes, comments, shares)
  """
  auth_token = request.headers.get('Authorization')
  if auth_token != SA_AUTHENTICATION:
    return "Unauthorized", 403
  
  data = await request.get_json(silent=True) or {}
  action = data.get("action")
  
  if not action:
    return {"error": "action is required"}, 400
  
  try:
    token = Authorize('youtube_analytics')
    
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    
    # Get channel ID from YouTube Data API
    channel_response = requests.get(
      "https://www.googleapis.com/youtube/v3/channels",
      params={"part": "id", "mine": "true"},
      headers=headers,
      timeout=30
    )
    
    if channel_response.status_code != 200:
      return {"error": f"Failed to fetch channel info: {channel_response.text}"}, channel_response.status_code
    
    channel_data = channel_response.json()
    if not channel_data.get("items"):
      return {"error": "No channel found for authenticated user"}, 404
    
    channel_id = channel_data["items"][0]["id"]
    
    start_date = data.get("start_date", (datetime.utcnow() - timedelta(days=28)).strftime("%Y-%m-%d"))
    end_date = data.get("end_date", datetime.utcnow().strftime("%Y-%m-%d"))
    
    analytics_base_url = "https://youtubeanalytics.googleapis.com/v2/reports"
    
    if action == "channel_analytics":
      metrics_list = data.get("metrics", ["views", "estimatedMinutesWatched", "subscribersGained", "subscribersLost"])
      params = {
        "ids": f"channel=={channel_id}",
        "startDate": start_date,
        "endDate": end_date,
        "metrics": ",".join(metrics_list)
      }
      response = requests.get(analytics_base_url, params=params, headers=headers, timeout=30)
      if response.status_code == 200:
        return {"channel_analytics": response.json()}, 200
      else:
        return {"error": f"YouTube Analytics API error: {response.text}"}, response.status_code
    
    elif action == "video_analytics":
      video_id = data.get("video_id")
      if not video_id:
        return {"error": "video_id is required for video_analytics action"}, 400
      metrics_list = data.get("metrics", ["views", "likes", "dislikes", "comments", "shares", "estimatedMinutesWatched", "averageViewDuration"])
      params = {
        "ids": f"channel=={channel_id}",
        "startDate": start_date,
        "endDate": end_date,
        "metrics": ",".join(metrics_list),
        "filters": f"video=={video_id}"
      }
      response = requests.get(analytics_base_url, params=params, headers=headers, timeout=30)
      if response.status_code == 200:
        return {"video_analytics": response.json()}, 200
      else:
        return {"error": f"YouTube Analytics API error: {response.text}"}, response.status_code
    
    elif action == "top_videos":
      max_results = data.get("max_results", 10)
      sort_by = data.get("sort_by", "views")
      params = {
        "ids": f"channel=={channel_id}",
        "startDate": start_date,
        "endDate": end_date,
        "metrics": "views,estimatedMinutesWatched,likes,comments",
        "dimensions": "video",
        "maxResults": max_results,
        "sort": f"-{sort_by}"
      }
      response = requests.get(analytics_base_url, params=params, headers=headers, timeout=30)
      if response.status_code == 200:
        analytics_data = response.json()
        # Enrich with video titles from YouTube Data API
        rows = analytics_data.get("rows", [])
        if rows:
          video_ids = [row[0] for row in rows]
          video_details_response = requests.get(
            "https://www.googleapis.com/youtube/v3/videos",
            params={"part": "snippet", "id": ",".join(video_ids)},
            headers=headers,
            timeout=30
          )
          if video_details_response.status_code == 200:
            video_items = video_details_response.json().get("items", [])
            video_titles = {item["id"]: item["snippet"]["title"] for item in video_items}
            analytics_data["video_titles"] = video_titles
        return {"top_videos": analytics_data}, 200
      else:
        return {"error": f"YouTube Analytics API error: {response.text}"}, response.status_code
    
    elif action == "audience_demographics":
      video_id = data.get("video_id")
      params = {
        "ids": f"channel=={channel_id}",
        "startDate": start_date,
        "endDate": end_date,
        "metrics": "viewerPercentage",
        "dimensions": "ageGroup,gender"
      }
      if video_id:
        params["filters"] = f"video=={video_id}"
      response = requests.get(analytics_base_url, params=params, headers=headers, timeout=30)
      if response.status_code == 200:
        return {"audience_demographics": response.json()}, 200
      else:
        return {"error": f"YouTube Analytics API error: {response.text}"}, response.status_code
    
    elif action == "traffic_sources":
      video_id = data.get("video_id")
      params = {
        "ids": f"channel=={channel_id}",
        "startDate": start_date,
        "endDate": end_date,
        "metrics": "views,estimatedMinutesWatched",
        "dimensions": "insightTrafficSourceType"
      }
      if video_id:
        params["filters"] = f"video=={video_id}"
      response = requests.get(analytics_base_url, params=params, headers=headers, timeout=30)
      if response.status_code == 200:
        return {"traffic_sources": response.json()}, 200
      else:
        return {"error": f"YouTube Analytics API error: {response.text}"}, response.status_code
    
    elif action == "engagement_metrics":
      video_id = data.get("video_id")
      params = {
        "ids": f"channel=={channel_id}",
        "startDate": start_date,
        "endDate": end_date,
        "metrics": "likes,dislikes,comments,shares,subscribersGained,subscribersLost"
      }
      if video_id:
        params["filters"] = f"video=={video_id}"
      response = requests.get(analytics_base_url, params=params, headers=headers, timeout=30)
      if response.status_code == 200:
        return {"engagement_metrics": response.json()}, 200
      else:
        return {"error": f"YouTube Analytics API error: {response.text}"}, response.status_code
    elif action == "recent_videos":
      max_results = int(data.get("max_results", 5))
      max_results = max(1, min(max_results, 25))

      response = requests.get(
        "https://www.googleapis.com/youtube/v3/search",
        params={
          "part": "snippet",
          "channelId": channel_id,
          "order": "date",
          "type": "video",
          "maxResults": max_results,
        },
        headers=headers,
        timeout=30
      )

      if response.status_code == 200:
        return {"recent_videos": response.json()}, 200
      return {"error": f"YouTube Data API error: {response.text}"}, response.status_code


    elif action == "channel_timeseries":
      metrics_list = data.get(
        "metrics",
        ["views", "estimatedMinutesWatched", "subscribersGained", "subscribersLost"]
      )

      params = {
        "ids": f"channel=={channel_id}",
        "startDate": start_date,
        "endDate": end_date,
        "metrics": ",".join(metrics_list),
        "dimensions": "day",
        "sort": "day",
      }

      response = requests.get(
        analytics_base_url,
        params=params,
        headers=headers,
        timeout=30
      )

      if response.status_code == 200:
        return {"channel_timeseries": response.json()}, 200
      return {"error": f"YouTube Analytics API error: {response.text}"}, response.status_code


    elif action == "video_timeseries":
      video_id = data.get("video_id")
      if not video_id:
        return {"error": "video_id is required for video_timeseries action"}, 400

      metrics_list = data.get(
        "metrics",
        ["views", "likes", "comments", "shares", "estimatedMinutesWatched", "averageViewDuration"]
      )

      params = {
        "ids": f"channel=={channel_id}",
        "startDate": start_date,
        "endDate": end_date,
        "metrics": ",".join(metrics_list),
        "dimensions": "day",
        "filters": f"video=={video_id}",
        "sort": "day",
      }

      response = requests.get(
        analytics_base_url,
        params=params,
        headers=headers,
        timeout=30
      )

      if response.status_code == 200:
        return {"video_timeseries": response.json()}, 200
      return {"error": f"YouTube Analytics API error: {response.text}"}, response.status_code


    elif action == "compare_videos":
      video_ids = data.get("video_ids")
      if not video_ids or not isinstance(video_ids, list) or len(video_ids) < 2:
        return {"error": "video_ids must be a list with at least 2 video IDs for compare_videos"}, 400

      if len(video_ids) > 10:
        return {"error": "compare_videos supports at most 10 video IDs"}, 400

      metrics_list = data.get(
        "metrics",
        ["views", "likes", "comments", "shares", "estimatedMinutesWatched", "averageViewDuration"]
      )

      results = []

      for video_id in video_ids:
        params = {
          "ids": f"channel=={channel_id}",
          "startDate": start_date,
          "endDate": end_date,
          "metrics": ",".join(metrics_list),
          "filters": f"video=={video_id}",
        }

        response = requests.get(
          analytics_base_url,
          params=params,
          headers=headers,
          timeout=30
        )

        if response.status_code != 200:
          return {
            "error": f"YouTube Analytics API error for video {video_id}: {response.text}"
          }, response.status_code

        results.append({
          "video_id": video_id,
          "analytics": response.json(),
        })

      # Enrich titles
      details_response = requests.get(
        "https://www.googleapis.com/youtube/v3/videos",
        params={
          "part": "snippet",
          "id": ",".join(video_ids),
        },
        headers=headers,
        timeout=30
      )

      video_titles = {}
      if details_response.status_code == 200:
        for item in details_response.json().get("items", []):
          video_titles[item["id"]] = item["snippet"]["title"]

      return {
        "compare_videos": {
          "videos": results,
          "video_titles": video_titles,
          "metrics": metrics_list,
          "start_date": start_date,
          "end_date": end_date,
        }
      }, 200
    else:
      return {
        "error": (
          f"unknown action '{action}', valid options are "
          "['recent_videos', 'channel_analytics', 'channel_timeseries', "
          "'video_analytics', 'video_timeseries', 'top_videos', "
          "'compare_videos', 'audience_demographics', 'traffic_sources', "
          "'engagement_metrics']"
        )
      }, 400
  except Exception as e:
    logging.error(f"YouTube Analytics API error: {e}")
    return {"error": str(e)}, 500

@app.route('/api/v1/prism/facebook/analytics', methods=['POST'])
async def facebook_analytics():
    auth_token = request.headers.get("Authorization")
    if auth_token != SA_AUTHENTICATION:
        return {"ok": False, "error": "Unauthorized"}, 403

    data = await request.get_json(silent=True) or {}
    action = data.get("action")
    action_data = data.get("data", {}) or {}

    VALID_ACTIONS = {
        "get_page_analytics",
        "get_page_timeseries",
        "get_post_analytics",
        "get_top_posts",
        "get_post_engagement",
        "get_page_engagement",
        "search_campaigns",
        "search_adsets",
        "get_campaign_analytics",
        "get_adset_analytics",
    }

    PAGE_METRIC_ALLOWLIST = {
        "page_impressions",
        "page_impressions_paid",
        "page_impressions_organic_v2",
        "page_post_engagements",
        "page_video_views",
        "page_engaged_users",
        "page_fans",
        "page_fan_adds",
        "page_fan_removes",
        "page_reach",
        "page_content_activity",
        "page_actions_post_reactions_total",
    }

    POST_METRIC_ALLOWLIST = {
        "post_impressions",
        "post_impressions_unique",
        "post_impressions_paid",
        "post_impressions_paid_unique",
        "post_impressions_organic",
        "post_impressions_organic_unique",
        "post_impressions_viral",
        "post_impressions_viral_unique",
        "post_engaged_users",
        "post_engagements",
        "post_reactions_by_type_total",
        "post_clicks",
        "post_clicks_by_type",
        "post_video_views",
    }

    ADS_METRIC_ALLOWLIST = {
        "impressions",
        "clicks",
        "spend",
        "cpc",
        "ctr",
        "cpm",
        "reach",
        "frequency",
        "inline_link_clicks",
        "outbound_clicks",
        "conversions",
        "video_p25_watched_actions",
        "video_p50_watched_actions",
        "video_p75_watched_actions",
        "video_p100_watched_actions",
    }

    TOP_POST_SORT_FIELDS = {
        "created_time",
        "shares",
        "reactions",
        "comments",
    }

    def json_error(message: str, status_code: int = 400, **extra):
        payload = {"ok": False, "error": message}
        if extra:
            payload.update(extra)
        return payload, status_code

    def parse_ymd(value: str, field_name: str):
        try:
            return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            raise ValueError(f"{field_name} must be in YYYY-MM-DD format")

    def get_date_range(local_action_data):
        start_date = local_action_data.get(
            "start_date",
            (datetime.utcnow() - timedelta(days=28)).strftime("%Y-%m-%d")
        )
        end_date = local_action_data.get(
            "end_date",
            datetime.utcnow().strftime("%Y-%m-%d")
        )

        start_dt = parse_ymd(start_date, "start_date")
        end_dt = parse_ymd(end_date, "end_date")

        if start_dt > end_dt:
            raise ValueError("start_date must be on or before end_date")

        return {
            "start_date": start_date,
            "end_date": end_date,
            "start_ts": int(start_dt.timestamp()),
            "end_ts": int(end_dt.timestamp()),
        }

    def filter_metrics(requested, allowlist, defaults):
        metrics = requested or defaults
        invalid = [m for m in metrics if m not in allowlist]
        if invalid:
            raise ValueError(f"Invalid metrics requested: {invalid}")
        return metrics

    def normalize_insights_response(response_json):
        rows = []
        for metric in response_json.get("data", []):
            values = metric.get("values", [])
            if not values:
                rows.append({
                    "metric": metric.get("name"),
                    "title": metric.get("title"),
                    "description": metric.get("description"),
                    "period": metric.get("period"),
                    "value": None,
                    "end_time": None,
                })
                continue

            for v in values:
                rows.append({
                    "metric": metric.get("name"),
                    "title": metric.get("title"),
                    "description": metric.get("description"),
                    "period": metric.get("period"),
                    "value": v.get("value"),
                    "end_time": v.get("end_time"),
                })
        return rows

    async def graph_get(path: str, params: dict, timeout: int = 30):
        timeout_cfg = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(timeout=timeout_cfg) as session:
            async with session.get(f"{FB_GRAPH}/{path}", params=params) as response:
                text = await response.text()
                try:
                    body = await response.json()
                except Exception:
                    body = {"raw_text": text}

                if response.status != 200:
                    return None, response.status, body

                return body, 200, None

    async def handle_get_page_analytics():
        metrics = filter_metrics(
            action_data.get("metrics"),
            PAGE_METRIC_ALLOWLIST,
            [
                "page_impressions",
                "page_impressions_paid",
                "page_impressions_organic_v2",
                "page_post_engagements",
                "page_video_views",
            ],
        )
        period = action_data.get("period", "day")

        body, status_code, error_body = await graph_get(
            f"{FB_PAGE_ID}/insights",
            {
                "metric": ",".join(metrics),
                "since": ctx["start_ts"],
                "until": ctx["end_ts"],
                "period": period,
                "access_token": ctx["user_token"],
            },
        )
        if status_code != 200:
            return json_error("Facebook API error", status_code, details=error_body)

        return {
            **ctx["base_meta"],
            "object_type": "page",
            "metrics": metrics,
            "period": period,
            "rows": normalize_insights_response(body),
            "raw": body,
        }, 200

    async def handle_get_page_timeseries():
        metrics = filter_metrics(
            action_data.get("metrics"),
            PAGE_METRIC_ALLOWLIST,
            [
                "page_impressions",
                "page_post_engagements",
                "page_video_views",
            ],
        )

        body, status_code, error_body = await graph_get(
            f"{FB_PAGE_ID}/insights",
            {
                "metric": ",".join(metrics),
                "since": ctx["start_ts"],
                "until": ctx["end_ts"],
                "period": "day",
                "access_token": ctx["user_token"],
            },
        )
        if status_code != 200:
            return json_error("Facebook API error", status_code, details=error_body)

        return {
            **ctx["base_meta"],
            "object_type": "page",
            "metrics": metrics,
            "period": "day",
            "rows": normalize_insights_response(body),
            "raw": body,
        }, 200

    async def handle_get_post_analytics():
        post_id = action_data.get("post_id")
        if not post_id:
            return json_error("post_id is required for get_post_analytics action")

        metrics = filter_metrics(
            action_data.get("metrics"),
            POST_METRIC_ALLOWLIST,
            [
                "post_impressions",
                "post_impressions_unique",
                "post_engagements",
                "post_engaged_users",
                "post_clicks",
            ],
        )
        period = action_data.get("period", "lifetime")

        body, status_code, error_body = await graph_get(
            f"{post_id}/insights",
            {
                "metric": ",".join(metrics),
                "period": period,
                "access_token": ctx["user_token"],
            },
        )
        if status_code != 200:
            return json_error("Facebook API error", status_code, details=error_body)

        return {
            **ctx["base_meta"],
            "object_type": "post",
            "object_id": post_id,
            "metrics": metrics,
            "period": period,
            "rows": normalize_insights_response(body),
            "raw": body,
        }, 200

    async def handle_get_top_posts():
        max_results = int(action_data.get("max_results", 25))
        max_results = max(1, min(max_results, 50))

        sort_by = action_data.get("sort_by", "shares")
        if sort_by not in TOP_POST_SORT_FIELDS:
            return json_error(
                f"invalid sort_by '{sort_by}'",
                400,
                valid_sort_fields=sorted(TOP_POST_SORT_FIELDS),
            )

        body, status_code, error_body = await graph_get(
            f"{FB_PAGE_ID}/posts",
            {
                "fields": ",".join([
                    "id",
                    "message",
                    "created_time",
                    "permalink_url",
                    "shares",
                    "reactions.summary(true)",
                    "comments.summary(true)",
                ]),
                "since": ctx["start_ts"],
                "until": ctx["end_ts"],
                "limit": max_results,
                "access_token": ctx["user_token"],
            },
        )
        if status_code != 200:
            return json_error("Facebook API error", status_code, details=error_body)

        posts = body.get("data", [])

        def post_sort_value(post):
            if sort_by == "shares":
                return post.get("shares", {}).get("count", 0)
            if sort_by == "reactions":
                return post.get("reactions", {}).get("summary", {}).get("total_count", 0)
            if sort_by == "comments":
                return post.get("comments", {}).get("summary", {}).get("total_count", 0)
            if sort_by == "created_time":
                return post.get("created_time", "")
            return 0

        reverse = sort_by != "created_time"
        posts.sort(key=post_sort_value, reverse=reverse)

        rows = []
        for post in posts[:max_results]:
            rows.append({
                "post_id": post.get("id"),
                "message": post.get("message"),
                "created_time": post.get("created_time"),
                "permalink_url": post.get("permalink_url"),
                "shares": post.get("shares", {}).get("count", 0),
                "reactions": post.get("reactions", {}).get("summary", {}).get("total_count", 0),
                "comments": post.get("comments", {}).get("summary", {}).get("total_count", 0),
            })

        return {
            **ctx["base_meta"],
            "object_type": "page_posts",
            "sort_by": sort_by,
            "max_results": max_results,
            "rows": rows,
            "raw": body,
        }, 200

    async def handle_get_post_engagement():
        post_id = action_data.get("post_id")
        if not post_id:
            return json_error("post_id is required for get_post_engagement action")

        metrics = [
            "post_engagements",
            "post_engaged_users",
            "post_clicks",
            "post_impressions",
            "post_impressions_unique",
            "post_reactions_by_type_total",
        ]

        body, status_code, error_body = await graph_get(
            f"{post_id}/insights",
            {
                "metric": ",".join(metrics),
                "period": "lifetime",
                "access_token": ctx["user_token"],
            },
        )
        if status_code != 200:
            return json_error("Facebook API error", status_code, details=error_body)

        return {
            **ctx["base_meta"],
            "object_type": "post",
            "object_id": post_id,
            "metrics": metrics,
            "period": "lifetime",
            "rows": normalize_insights_response(body),
            "raw": body,
        }, 200

    async def handle_get_page_engagement():
        metrics = [
            "page_engaged_users",
            "page_post_engagements",
            "page_actions_post_reactions_total",
        ]

        body, status_code, error_body = await graph_get(
            f"{FB_PAGE_ID}/insights",
            {
                "metric": ",".join(metrics),
                "since": ctx["start_ts"],
                "until": ctx["end_ts"],
                "period": "day",
                "access_token": ctx["user_token"],
            },
        )
        if status_code != 200:
            return json_error("Facebook API error", status_code, details=error_body)

        return {
            **ctx["base_meta"],
            "object_type": "page",
            "metrics": metrics,
            "period": "day",
            "rows": normalize_insights_response(body),
            "raw": body,
        }, 200

    async def handle_search_campaigns():
        query = (action_data.get("query") or "").strip()
        max_results = int(action_data.get("max_results", 25))
        max_results = max(1, min(max_results, 100))

        body, status_code, error_body = await graph_get(
            f"{FB_ACCOUNT_ID}/campaigns",
            {
                "fields": ",".join([
                    "id",
                    "name",
                    "status",
                    "objective",
                    "effective_status",
                    "start_time",
                    "stop_time",
                    "updated_time",
                ]),
                "limit": max_results,
                "access_token": ctx["user_token"],
            },
        )
        if status_code != 200:
            return json_error("Facebook API error", status_code, details=error_body)

        campaigns = body.get("data", [])
        if query:
            q = query.lower()
            campaigns = [c for c in campaigns if q in (c.get("name") or "").lower()]

        rows = [{
            "campaign_id": c.get("id"),
            "name": c.get("name"),
            "status": c.get("status"),
            "effective_status": c.get("effective_status"),
            "objective": c.get("objective"),
            "start_time": c.get("start_time"),
            "stop_time": c.get("stop_time"),
            "updated_time": c.get("updated_time"),
        } for c in campaigns[:max_results]]

        return {
            **ctx["base_meta"],
            "object_type": "campaign_search",
            "query": query,
            "rows": rows,
            "raw": body,
        }, 200

    async def handle_search_adsets():
        query = (action_data.get("query") or "").strip()
        campaign_id = action_data.get("campaign_id")
        max_results = int(action_data.get("max_results", 25))
        max_results = max(1, min(max_results, 100))

        body, status_code, error_body = await graph_get(
            f"{FB_ACCOUNT_ID}/adsets",
            {
                "fields": ",".join([
                    "id",
                    "name",
                    "campaign_id",
                    "status",
                    "effective_status",
                    "optimization_goal",
                    "billing_event",
                    "updated_time",
                ]),
                "limit": max_results,
                "access_token": ctx["user_token"],
            },
        )
        if status_code != 200:
            return json_error("Facebook API error", status_code, details=error_body)

        adsets = body.get("data", [])
        if campaign_id:
            adsets = [a for a in adsets if a.get("campaign_id") == campaign_id]
        if query:
            q = query.lower()
            adsets = [a for a in adsets if q in (a.get("name") or "").lower()]

        rows = [{
            "adset_id": a.get("id"),
            "name": a.get("name"),
            "campaign_id": a.get("campaign_id"),
            "status": a.get("status"),
            "effective_status": a.get("effective_status"),
            "optimization_goal": a.get("optimization_goal"),
            "billing_event": a.get("billing_event"),
            "updated_time": a.get("updated_time"),
        } for a in adsets[:max_results]]

        return {
            **ctx["base_meta"],
            "object_type": "adset_search",
            "query": query,
            "campaign_id": campaign_id,
            "rows": rows,
            "raw": body,
        }, 200

    async def handle_get_campaign_analytics():
        campaign_id = action_data.get("campaign_id")
        if not campaign_id:
            return json_error("campaign_id is required for get_campaign_analytics action")

        metrics = filter_metrics(
            action_data.get("metrics"),
            ADS_METRIC_ALLOWLIST,
            ["impressions", "clicks", "spend", "cpc", "ctr", "cpm", "reach"],
        )

        body, status_code, error_body = await graph_get(
            f"{campaign_id}/insights",
            {
                "fields": ",".join(metrics),
                "time_range": f'{{"since":"{ctx["start_date"]}","until":"{ctx["end_date"]}"}}',
                "access_token": ctx["user_token"],
            },
        )
        if status_code != 200:
            return json_error("Facebook API error", status_code, details=error_body)

        return {
            **ctx["base_meta"],
            "object_type": "campaign",
            "object_id": campaign_id,
            "metrics": metrics,
            "rows": body.get("data", []),
            "raw": body,
        }, 200

    async def handle_get_adset_analytics():
        adset_id = action_data.get("adset_id")
        if not adset_id:
            return json_error("adset_id is required for get_adset_analytics action")

        metrics = filter_metrics(
            action_data.get("metrics"),
            ADS_METRIC_ALLOWLIST,
            ["impressions", "clicks", "spend", "cpc", "ctr", "cpm", "reach"],
        )

        body, status_code, error_body = await graph_get(
            f"{adset_id}/insights",
            {
                "fields": ",".join(metrics),
                "time_range": f'{{"since":"{ctx["start_date"]}","until":"{ctx["end_date"]}"}}',
                "access_token": ctx["user_token"],
            },
        )
        if status_code != 200:
            return json_error("Facebook API error", status_code, details=error_body)

        return {
            **ctx["base_meta"],
            "object_type": "adset",
            "object_id": adset_id,
            "metrics": metrics,
            "rows": body.get("data", []),
            "raw": body,
        }, 200

    try:
        if not FB_AVA_USER_TOKEN:
            return json_error("FB_AVA_USER_TOKEN is not configured", 500)
        if not FB_PAGE_ID:
            return json_error("FB_PAGE_ID is not configured", 500)
        if not FB_ACCOUNT_ID:
            return json_error("FB_ACCOUNT_ID is not configured", 500)

        if not action:
            return json_error("action is required", 400, valid_actions=sorted(VALID_ACTIONS))

        if action not in VALID_ACTIONS:
            return json_error(
                f"unknown action '{action}'",
                400,
                valid_actions=sorted(VALID_ACTIONS),
            )

        date_ctx = get_date_range(action_data)

        ctx = {
            **date_ctx,
            "user_token": FB_AVA_USER_TOKEN,
            "base_meta": {
                "ok": True,
                "action": action,
                "page_id": FB_PAGE_ID,
                "ad_account_id": FB_ACCOUNT_ID,
                "start_date": date_ctx["start_date"],
                "end_date": date_ctx["end_date"],
                "auth_mode": "single_user_token",
            },
        }

        handlers = {
            "get_page_analytics": handle_get_page_analytics,
            "get_page_timeseries": handle_get_page_timeseries,
            "get_post_analytics": handle_get_post_analytics,
            "get_top_posts": handle_get_top_posts,
            "get_post_engagement": handle_get_post_engagement,
            "get_page_engagement": handle_get_page_engagement,
            "search_campaigns": handle_search_campaigns,
            "search_adsets": handle_search_adsets,
            "get_campaign_analytics": handle_get_campaign_analytics,
            "get_adset_analytics": handle_get_adset_analytics,
        }

        handler = handlers.get(action)
        if not handler:
            return json_error(
                f"Unhandled action '{action}'",
                500,
                valid_actions=sorted(VALID_ACTIONS),
            )

        return await handler()

    except ValueError as e:
        return json_error(str(e), 400)

    except asyncio.TimeoutError:
        return json_error("Facebook API request timed out", 504)

    except aiohttp.ClientError as e:
        logging.error(f"Facebook analytics client error: {e}")
        return json_error("Facebook API client error", 502, details=str(e))

    except Exception as e:
        logging.exception("Facebook Analytics API error")
        return json_error("Internal server error", 500, details=str(e))

@app.route('/api/v1/prism/youtube/transcript', methods=['POST'])
async def youtube_transcript():
  """
  Tool for retrieving YouTube video transcripts.
  """
  auth_token = request.headers.get('Authorization')
  if auth_token != SA_AUTHENTICATION:
    return "Unauthorized", 403
  
  data = await request.get_json(silent=True) or {}
  video_id = data.get("video_id")
  
  if not video_id:
    return {"error": "video_id is required"}, 400
  
  try:
    ytt_api = YouTubeTranscriptApi(proxy_config=WebshareProxyConfig(
        proxy_username=os.environ.get("PROXY_USERNAME", "proxyuser"),
        proxy_password=os.environ.get("PROXY_PASSWORD", "proxypass"),
        filter_ip_locations=["de", "us"],
    ))
    transcript = ytt_api.fetch(video_id)
    
    return {"transcript": transcript}, 200
  
  except Exception as e:
    logging.error(f"Error fetching YouTube transcript for video {video_id}: {e}")
    return {"error": f"Failed to fetch transcript: {str(e)}"}, 500


@app.route('/api/v1/prism/youtube/video_context', methods=['POST'])
async def youtube_video_context():
    """
    Tool for retrieving a normalized YouTube video context bundle.

    Body:
    - video_id: required
    - language: optional preferred transcript language code, e.g. "en"
    - include_timestamps: optional bool, default true
    - include_transcript: optional bool, default true
    - include_comments: optional bool, default false
    - max_comments: optional int, default 10, max 50
    - chunk_index: optional int for transcript chunk retrieval
    - chunk_size_chars: optional int, default 25000
    - summarize: optional bool, default false
      (placeholder; returns a lightweight local summary, not an LLM summary)

    Returns:
    - video metadata
    - transcript text + segments
    - optional transcript chunk
    - optional top comments
    """

    auth_token = request.headers.get('Authorization')
    if auth_token != SA_AUTHENTICATION:
        return {"error": "Unauthorized"}, 403

    data = await request.get_json(silent=True) or {}

    video_id = data.get("video_id")
    language = data.get("language")
    include_timestamps = bool(data.get("include_timestamps", True))
    include_transcript = bool(data.get("include_transcript", True))
    include_comments = bool(data.get("include_comments", False))
    summarize = bool(data.get("summarize", False))

    chunk_index = data.get("chunk_index")
    chunk_size_chars = int(data.get("chunk_size_chars", 25000))
    max_comments = int(data.get("max_comments", 10))

    if not video_id:
        return {"error": "video_id is required"}, 400

    if chunk_index is not None:
        try:
            chunk_index = int(chunk_index)
        except Exception:
            return {"error": "chunk_index must be an integer"}, 400
        if chunk_index < 0:
            return {"error": "chunk_index must be >= 0"}, 400

    if chunk_size_chars < 1000:
        return {"error": "chunk_size_chars must be at least 1000"}, 400

    max_comments = max(0, min(max_comments, 50))

    def chunk_text(text: str, size: int):
        return [text[i:i + size] for i in range(0, len(text), size)] if text else []

    def safe_get(dct, *keys, default=None):
        cur = dct
        for key in keys:
            if not isinstance(cur, dict):
                return default
            cur = cur.get(key)
            if cur is None:
                return default
        return cur

    def build_lightweight_summary(title: str, description: str, full_text: str):
        # Very lightweight deterministic summary, not model-generated
        text = (full_text or "").strip()
        first_lines = text.splitlines()
        preview = " ".join([line.strip() for line in first_lines[:8] if line.strip()])[:1500]

        return {
            "title": title,
            "description_preview": (description or "")[:500],
            "transcript_preview": preview,
            "word_count_estimate": len(text.split()) if text else 0,
        }

    try:
        credentials = Credentials(
            token=None,
            refresh_token=YT_REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=YT_OAUTH_CLIENT_ID,
            client_secret=YT_OAUTH_CLIENT_SECRET
        )

        youtube_data_service = google_build('youtube', 'v3', credentials=credentials)

        # --- 1) video metadata ---
        video_response = youtube_data_service.videos().list(
            part="snippet,contentDetails,statistics,status",
            id=video_id,
            maxResults=1
        ).execute()

        items = video_response.get("items", [])
        if not items:
            return {"error": f"No video found for video_id '{video_id}'"}, 404

        item = items[0]
        snippet = item.get("snippet", {})
        content_details = item.get("contentDetails", {})
        statistics = item.get("statistics", {})
        status = item.get("status", {})
        thumbnails = snippet.get("thumbnails", {})

        result = {
            "video": {
                "video_id": video_id,
                "title": snippet.get("title"),
                "description": snippet.get("description"),
                "channel_id": snippet.get("channelId"),
                "channel_title": snippet.get("channelTitle"),
                "published_at": snippet.get("publishedAt"),
                "tags": snippet.get("tags", []),
                "category_id": snippet.get("categoryId"),
                "default_language": snippet.get("defaultLanguage"),
                "default_audio_language": snippet.get("defaultAudioLanguage"),
                "duration": content_details.get("duration"),
                "definition": content_details.get("definition"),
                "caption": content_details.get("caption"),
                "licensed_content": content_details.get("licensedContent"),
                "privacy_status": status.get("privacyStatus"),
                "embeddable": status.get("embeddable"),
                "made_for_kids": status.get("madeForKids"),
                "view_count": statistics.get("viewCount"),
                "like_count": statistics.get("likeCount"),
                "comment_count": statistics.get("commentCount"),
                "thumbnails": {
                    "default": safe_get(thumbnails, "default", "url"),
                    "medium": safe_get(thumbnails, "medium", "url"),
                    "high": safe_get(thumbnails, "high", "url"),
                    "standard": safe_get(thumbnails, "standard", "url"),
                    "maxres": safe_get(thumbnails, "maxres", "url"),
                },
                "url": f"https://www.youtube.com/watch?v={video_id}",
            }
        }

        # --- 2) transcript ---
        full_text = ""
        segments = []
        transcript_language = language
        transcript_meta = {
            "available": False,
            "language": transcript_language,
            "segment_count": 0,
            "chunk_count": 0,
        }

        if include_transcript:
            try:
                ytt_api = YouTubeTranscriptApi(
                    proxy_config=WebshareProxyConfig(
                        proxy_username=os.environ.get("PROXY_USERNAME", "proxyuser"),
                        proxy_password=os.environ.get("PROXY_PASSWORD", "proxypass"),
                        filter_ip_locations=["de", "us"],
                    )
                )

                # Depending on installed library version, fetch may accept languages=
                transcript = ytt_api.fetch(
                    video_id,
                    languages=[language] if language else None
                )

                full_text_parts = []
                normalized_segments = []

                for entry in transcript:
                    if isinstance(entry, dict):
                        text = entry.get("text")
                        start = entry.get("start")
                        duration = entry.get("duration")
                    else:
                        text = getattr(entry, "text", None)
                        start = getattr(entry, "start", None)
                        duration = getattr(entry, "duration", None)

                    if not text:
                        continue

                    full_text_parts.append(text)

                    seg = {"text": text}
                    if include_timestamps:
                        seg["start"] = start
                        seg["duration"] = duration
                    normalized_segments.append(seg)

                full_text = "\n".join(full_text_parts)
                segments = normalized_segments
                transcript_chunks = chunk_text(full_text, chunk_size_chars)

                transcript_meta = {
                    "available": True,
                    "language": transcript_language,
                    "segment_count": len(segments),
                    "chunk_count": len(transcript_chunks),
                    "chunk_size_chars": chunk_size_chars,
                    "char_count": len(full_text),
                    "word_count_estimate": len(full_text.split()) if full_text else 0,
                }

                result["transcript"] = transcript_meta

                if chunk_index is not None:
                    if chunk_index >= len(transcript_chunks):
                        return {
                            "error": "chunk_index out of range",
                            "transcript": transcript_meta,
                        }, 400

                    result["transcript_chunk"] = {
                        "chunk_index": chunk_index,
                        "chunk_count": len(transcript_chunks),
                        "text": transcript_chunks[chunk_index],
                    }
                else:
                    result["transcript_text"] = full_text
                    if include_timestamps:
                        result["transcript_segments"] = segments

            except Exception as transcript_error:
                logging.warning(
                    f"Transcript fetch failed for video {video_id}: {transcript_error}"
                )
                result["transcript"] = {
                    "available": False,
                    "error": f"Failed to fetch transcript: {str(transcript_error)}"
                }

        # --- 3) comments ---
        if include_comments:
            try:
                comments_response = youtube_data_service.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=max_comments,
                    order="relevance",
                    textFormat="plainText"
                ).execute()

                comments = []
                for thread in comments_response.get("items", []):
                    top_comment = safe_get(
                        thread,
                        "snippet",
                        "topLevelComment",
                        "snippet",
                        default={}
                    ) or {}

                    comments.append({
                        "author": top_comment.get("authorDisplayName"),
                        "text": top_comment.get("textDisplay"),
                        "like_count": top_comment.get("likeCount"),
                        "published_at": top_comment.get("publishedAt"),
                        "updated_at": top_comment.get("updatedAt"),
                    })

                result["comments"] = {
                    "count": len(comments),
                    "items": comments,
                }

            except Exception as comments_error:
                logging.warning(
                    f"Comments fetch failed for video {video_id}: {comments_error}"
                )
                result["comments"] = {
                    "count": 0,
                    "error": f"Failed to fetch comments: {str(comments_error)}"
                }

        # --- 4) lightweight summary ---
        if summarize:
            result["summary"] = build_lightweight_summary(
                title=snippet.get("title", ""),
                description=snippet.get("description", ""),
                full_text=full_text,
            )

        return result, 200

    except google.auth.exceptions.TransportError as e:
        logging.error(f"YouTube OAuth transport error: {e}")
        return {"error": "Failed to authenticate with YouTube credentials"}, 401

    except Exception as e:
        logging.exception(f"YouTube video context error for video {video_id}")
        return {"error": f"Failed to build video context: {str(e)}"}, 500

@app.route('/api/v1/prism/sales/get', methods=['POST'])
async def get_sales():
    auth_token = request.headers.get('Authorization')
    if auth_token != SA_AUTHENTICATION:
      return "Unauthorized", 403
    data = await request.get_json(silent=True) or {}
    year = data.get("year")
    make = data.get("make")
    model = data.get("model")
    sales = sales_get(year, make, model)
    return (sales, 200)

@app.route('/api/v1/prism/sales/fullget', methods=['GET'])
async def get_full_sales():
    
    sales = sales_full_get()
    return (sales, 200)


#STYLE ENDPOINTS
@app.route('/kendo/kendo.custom.min.js', methods=['GET'])
async def kendo():
  return await send_file('static/kendo/kendo.custom.min.js')

@app.route('/kendo/style/default-main.css', methods=['GET'])
async def kendo_style():
  return await send_file('static/kendo/style/default-main.css')

@app.route('/kendo/style/font-icons/index.css', methods=['GET'])
async def kendo_font_icons():
  return await send_file('static/kendo/style/font-icons/index.css')

@app.route("/support.js", methods=['GET'])
async def support_js():
    return await send_file('static/support.js')



@app.route('/api/v1/prism/external_auction/add', methods=['POST'])
async def external_auction_add():
  data = await request.get_json(silent=True) or {}
  result = external_sale_add(data)
  if result == "Success":
      return ("External auction sale added", 200)
  else:
      return (result, 400)

@app.route('/api/v1/prism/external_auction/update', methods=['POST'])
async def external_auction_update():
  data = await request.get_json(silent=True) or {}
  result = external_sale_update(data)
  if result == "Success":
      return ("External auction sale updated", 200)
  else:
      return (result, 400)

@app.route('/api/v1/prism/email_notifications', methods=['POST'])
async def email_notifications():
    auth_headers = request.headers.get("Authorization", "")
    if auth_headers != SA_AUTHENTICATION:
        raise HTTPException(status_code=403, detail="Invalid authentication token")

    data = await request.get_json(silent=True) or {}
    # Build Gmail service: prefer service-account JSON (path or JSON string), fall back to ADC
    creds = None
    creds, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/gmail.send'])
    message = EmailMessage()
    message.set_content(data.get('body', ''))
    message['To'] = data.get('to')
    message['From'] = data.get('from')
    message['Subject'] = data.get('subject')

    try:
        # Connect to Gmail's SMTP server
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(data['from'], GMAIL_NOTIFICATION_CREDENTIALS)
            smtp.send_message(message)
        logging.info("Email sent successfully to %s", data.get('to'))
        logging.info(f"Email Info: To: {data.get('to')}, From: {data.get('from')}, Subject: {data.get('subject')}")
        return "Email sent successfully!", 200
    except Exception as e:
        return f"Error: {str(e)}", 500

@app.route('/api/v1/prism/job/create', methods=['GET'])
async def create_job():
  result = create_valuation('')
  if result != "error":
      return (result, 200)
  else:
      return (result, 400)
    
@app.route('/api/v1/prism/job/update', methods=['POST'])
async def update_job():
  data = await request.get_json(silent=True) or {}
  arguments = {}
  for key in data.keys():
    if key != 'id':
      arguments[key] = data[key]
  result = update_valuation(data['id'], **arguments)
  if result == "success":
      return ("Job updated", 200)
  else:
      return (result, 400)

@app.route('/api/v1/prism/job/batch', methods=['POST'])
async def job_batch():
  data = await request.get_json(silent=True) or {}
  start_id = data.get("start_id")
  end_id = data.get("end_id")
  if start_id is None or end_id is None:
      return ("Missing start_id or end_id", 400)
  batch = get_valuation_batch(start_id, end_id)
  if batch == "error":
      return ("Failed to load batch", 500)
  return (batch, 200)


job_search_schema = {
   'schema' : {
      'type' : 'object',
      "properties" : {
        'property' : {
           'type' : 'string',
           'enum' : ['id', 'vin', 'SA_stream', 'RRA_stream', 'Andrew_stream', 'LBA_stream', 'LPA_stream', 'Web_searches','Prompt','job_date']
        },
        'value' : {
           'type' : 'string'
        },
        'batch_size' : {
           'type' : 'integer',
           'default' : 10
        }
      },
      'required' : ['property', 'value']
   }
}

@app.route('/api/v1/prism/job/search', methods=['POST','GET'])
async def job_search():
  if request.method == 'POST':
    data = await request.get_json(silent=True) or {}
    property = data.get("property")
    value = data.get("value")
    batch_size = data.get("batch_size", 10)
    candidate_rows = get_valuation_search(
      property,
      value,
      batch_size,
      select=["id"],
    )
    results = get_valuation_rows_by_ids(
      [row["id"] for row in candidate_rows],
    )
    if not results:
        return ("Failed to load search results", 500)
    return (jsonify(results), 200)
  elif request.method == 'GET':
    index = valuation_get_last_index()
    return index, 204

# ---------------------------------------------------------------------
# TWILIO RELAY MODE - DATABASE BUYERS TABLE VERSION
# ---------------------------------------------------------------------
#
# Deterministic relay:
# - Users text +19843676336
# - App relays labeled messages to every other buyer
# - Andrew responds to everyone when user says Hey Andrew / Hey Ava
# - Images are sent to RRA for parsing, but RRA parse response is not sent
#
# Twilio config:
#   Phone number webhook OR Messaging Service webhook:
#   POST https://prism.andrew-chat.com/api/v1/prism/relay
#
# IMPORTANT:
#   This code forces outbound from +19843676336.
#   It does NOT use messaging_service_sid.
#
# Required imports somewhere above:
# import os, re, uuid, asyncio, logging, requests, time
# from datetime import datetime
# from twilio.rest import Client
# from sqlalchemy import text
#
# Existing globals/helpers used:
# - app
# - request
# - engine
# - encode_image(file_path)
# ---------------------------------------------------------------------


# ---------------------------------------------------------------------
# HARDCODED TEST CONFIG
# ---------------------------------------------------------------------



PRISM_RELAY_USERS_TABLE = "buyers"
PRISM_RELAY_BUFFER_SECONDS = 10

RRA_SESSION_USER_ID = "prism_bot"
RRA_SESSION_LOOKUP_URL = "https://rra.andrew-chat.com/api/v1/session/"
RRA_SESSION_CREATE_URL = "https://rra.andrew-chat.com/api/v1/session/"
RRA_PRISM_URL = "https://rra.andrew-chat.com/api/v1/prism"

# Commented env/prod loading:
TWILIO_ACCOUNT_SID_HARDCODED = os.environ.get("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN_HARDCODED = os.environ.get("TWILIO_AUTH_TOKEN")
TWILIO_RELAY_NUMBER_HARDCODED = os.environ.get("TWILIO_RELAY_NUMBER", "+19843676336")
SA_AUTHENTICATION_HARDCODED = os.environ.get("SA_AUTHENTICATION")
# PRISM_RELAY_USERS_TABLE = os.environ.get("PRISM_RELAY_USERS_TABLE", "buyers")


_prism_relay_buffers = {}
_prism_relay_tasks = {}


# ---------------------------------------------------------------------
# PRISM SMS RELAY - ONE TO ONE ANDREW RESPONSE
# ---------------------------------------------------------------------

PRISM_RELAY_BUFFER_SECONDS = 5


# ---------------------------------------------------------------------
# BASIC HELPERS
# ---------------------------------------------------------------------

def get_twilio_client():
  return Client(
    TWILIO_ACCOUNT_SID_HARDCODED,
    TWILIO_AUTH_TOKEN_HARDCODED,
  )


def normalize_e164_phone(phone: str):
  phone = (phone or "").strip()

  if phone.startswith("+"):
    return "+" + re.sub(r"\D", "", phone)

  digits = re.sub(r"\D", "", phone)

  if len(digits) == 10:
    return "+1" + digits

  if len(digits) == 11 and digits.startswith("1"):
    return "+" + digits

  return None


def _prism_relay_number():
  return normalize_e164_phone(TWILIO_RELAY_NUMBER_HARDCODED)


def _prism_relay_schedule_process_task(delay_seconds=5):
  try:
    logging.info(
      "[prism-relay-db] Scheduling Cloud Task config project=%s location=%s queue=%s url=%s",
      CLOUD_TASKS_PROJECT,
      CLOUD_TASKS_LOCATION,
      CLOUD_TASKS_QUEUE,
      PRISM_RELAY_PROCESS_URL,
    )

    credentials, detected_project = google.auth.default(
      scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    logging.info(
      "[prism-relay-db] google.auth.default detected_project=%s credentials_type=%s quota_project_before=%s",
      detected_project,
      type(credentials).__name__,
      getattr(credentials, "quota_project_id", None),
    )

    if hasattr(credentials, "with_quota_project"):
      credentials = credentials.with_quota_project(CLOUD_TASKS_PROJECT)

    logging.info(
      "[prism-relay-db] Cloud Tasks quota_project_after=%s",
      getattr(credentials, "quota_project_id", None),
    )

    client = tasks_v2.CloudTasksClient(credentials=credentials)

    parent = client.queue_path(
      CLOUD_TASKS_PROJECT,
      CLOUD_TASKS_LOCATION,
      CLOUD_TASKS_QUEUE,
    )

    schedule_time = timestamp_pb2.Timestamp()
    schedule_time.FromDatetime(
      datetime.utcnow() + timedelta(seconds=delay_seconds)
    )

    task = {
      "http_request": {
        "http_method": tasks_v2.HttpMethod.POST,
        "url": PRISM_RELAY_PROCESS_URL,
        "headers": {
          "Authorization": SA_AUTHENTICATION_HARDCODED,
          "Content-Type": "application/json",
        },
        "body": b"{}",
      },
      "schedule_time": schedule_time,
    }

    created_task = client.create_task(
      request={
        "parent": parent,
        "task": task,
      }
    )

    logging.info(
      "[prism-relay-db] Scheduled Cloud Task name=%s delay_seconds=%s",
      created_task.name,
      delay_seconds,
    )

    return created_task.name

  except Exception:
    logging.exception("[prism-relay-db] Failed to schedule Cloud Task")
    return None


def _prism_relay_load_users():
  users = []

  try:
    with engine.connect() as conn:
      rows = conn.execute(
        text("""
          SELECT
            name,
            phone
          FROM buyers
          WHERE phone IS NOT NULL
            AND phone != ''
        """)
      ).mappings().all()

    for row in rows:
      phone = normalize_e164_phone(row.get("phone"))

      if not phone:
        continue

      users.append({
        "name": row.get("name") or phone,
        "phone": phone,
      })

    logging.info(
      "[prism-relay] Loaded %s users from buyers: %s",
      len(users),
      users,
    )

  except Exception:
    logging.exception("[prism-relay] Failed loading users from buyers")
    users = []

  return users


def _prism_relay_user_for_phone(phone):
  normalized = normalize_e164_phone(phone)

  for user in _prism_relay_load_users():
    if normalize_e164_phone(user.get("phone")) == normalized:
      return user

  return None


def _prism_relay_sender_label(phone):
  user = _prism_relay_user_for_phone(phone)

  if not user:
    normalized = normalize_e164_phone(phone) or phone or "unknown"
    return f"Unknown / {normalized}"

  name = user.get("name") or user.get("phone") or "Unknown"
  user_phone = normalize_e164_phone(user.get("phone")) or user.get("phone") or "unknown"

  return f"{name} / {user_phone}"


def _prism_relay_extension_for_mime(content_type):
  content_type = (content_type or "").lower()

  if content_type == "image/jpeg":
    return ".jpg"
  if content_type == "image/png":
    return ".png"
  if content_type == "image/webp":
    return ".webp"
  if content_type == "image/gif":
    return ".gif"
  if content_type == "application/pdf":
    return ".pdf"

  return ".bin"


# ---------------------------------------------------------------------
# MEDIA DOWNLOAD
# ---------------------------------------------------------------------

def _prism_relay_download_mms_attachments(form, request_id=None):
  log_prefix = f"[prism-relay:{request_id}] " if request_id else ""

  uploads_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
  os.makedirs(uploads_dir, exist_ok=True)

  try:
    num_media = int(form.get("NumMedia", 0) or 0)
  except Exception:
    num_media = 0

  logging.info("%sNumMedia=%s", log_prefix, num_media)

  if num_media <= 0:
    return []

  auth = (
    TWILIO_ACCOUNT_SID_HARDCODED,
    TWILIO_AUTH_TOKEN_HARDCODED,
  )

  message_sid = (
    form.get("MessageSid")
    or form.get("SmsMessageSid")
    or uuid.uuid4().hex
  )

  files = []

  for index in range(num_media):
    media_url = form.get(f"MediaUrl{index}")
    content_type = form.get(f"MediaContentType{index}") or "application/octet-stream"

    logging.info(
      "%sMedia index=%s url_present=%s content_type=%s",
      log_prefix,
      index,
      bool(media_url),
      content_type,
    )

    if not media_url:
      continue

    try:
      response = requests.get(
        media_url,
        auth=auth,
        timeout=60,
      )

      logging.info(
        "%sMedia download index=%s status=%s bytes=%s",
        log_prefix,
        index,
        response.status_code,
        len(response.content or b""),
      )

      if response.status_code >= 300:
        logging.error(
          "%sFailed downloading media index=%s status=%s body=%s",
          log_prefix,
          index,
          response.status_code,
          response.text[:500],
        )
        continue

      extension = _prism_relay_extension_for_mime(content_type)

      file_path = os.path.join(
        uploads_dir,
        f"relay_{message_sid}_{index}{extension}",
      )

      with open(file_path, "wb") as file:
        file.write(response.content)

      files.append({
        "path": file_path,
        "mime_type": content_type,
        "source_url": media_url,
      })

      logging.info(
        "%sSaved media index=%s path=%s bytes=%s",
        log_prefix,
        index,
        file_path,
        os.path.getsize(file_path),
      )

    except Exception:
      logging.exception(
        "%sFailed to download media index=%s url=%s",
        log_prefix,
        index,
        media_url,
      )

  return files


def _prism_relay_encode_files_for_db(files):
  encoded_files = []

  for file_info in files or []:
    try:
      file_path = file_info.get("path")
      mime_type = file_info.get("mime_type") or "image/jpeg"

      if not file_path or not os.path.exists(file_path):
        logging.info(
          "[prism-relay-db] Skipping missing media file for DB buffer: %s",
          file_info,
        )
        continue

      encoded_files.append({
        "mime_type": mime_type,
        "data": encode_image(file_path),
        "source_url": file_info.get("source_url"),
      })

    except Exception:
      logging.exception(
        "[prism-relay-db] Failed encoding media file for DB buffer: %s",
        file_info,
      )

  return encoded_files


# ---------------------------------------------------------------------
# TWILIO SEND
# ---------------------------------------------------------------------

def _prism_relay_send_sms(to_phone, body):
  to_phone = normalize_e164_phone(to_phone)

  if not to_phone:
    raise ValueError("Missing to_phone")

  relay_number = _prism_relay_number()

  if not relay_number:
    raise ValueError("Missing relay number")

  twilio_client = get_twilio_client()

  msg = twilio_client.messages.create(
    to=to_phone,
    from_=relay_number,
    body=body,
  )

  logging.info(
    "[prism-relay] Sent one-to-one SMS from=%s to=%s sid=%s",
    relay_number,
    to_phone,
    getattr(msg, "sid", None),
  )

  return msg


# ---------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------

def _prism_relay_log_message(sender_phone, body):
  try:
    sender = _prism_relay_user_for_phone(sender_phone)
    source = (
      sender.get("name")
      if sender
      else normalize_e164_phone(sender_phone) or sender_phone or "Unknown"
    )

    with engine.connect() as conn:
      conn.execute(
        text("""
          INSERT INTO chat_log (source, content, timestamp)
          VALUES (:source, :content, :timestamp)
        """),
        {
          "source": source,
          "content": body or "",
          "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        }
      )
      conn.commit()

  except Exception:
    logging.exception("[prism-relay] Failed to write chat_log")


# ---------------------------------------------------------------------
# DB BUFFER
# ---------------------------------------------------------------------

def _prism_relay_append_to_db_buffer(
  sender_phone,
  sender_label,
  message_sid,
  body,
  files,
  has_media,
):
  now = datetime.utcnow()
  sender_phone = normalize_e164_phone(sender_phone)

  encoded_files = _prism_relay_encode_files_for_db(files)

  new_message = {
    "message_sid": message_sid,
    "body": body or "",
    "files": encoded_files,
    "created_at": now.isoformat(),
  }

  with engine.begin() as conn:
    existing = conn.execute(
      text("""
        SELECT
          id,
          messages_json,
          has_media
        FROM prism_relay_buffer
        WHERE sender_phone = :sender_phone
          AND status = 'pending'
        ORDER BY last_message_at DESC
        LIMIT 1
      """),
      {
        "sender_phone": sender_phone,
      }
    ).mappings().first()

    if existing:
      try:
        messages = json.loads(existing["messages_json"] or "[]")
      except Exception:
        logging.exception(
          "[prism-relay-db] Failed parsing messages_json for buffer=%s",
          existing["id"],
        )
        messages = []

      messages.append(new_message)

      conn.execute(
        text("""
          UPDATE prism_relay_buffer
          SET
            sender_label = :sender_label,
            messages_json = :messages_json,
            has_hey = 1,
            has_media = :has_media,
            last_message_at = :last_message_at,
            updated_at = :updated_at
          WHERE id = :id
        """),
        {
          "id": existing["id"],
          "sender_label": sender_label,
          "messages_json": json.dumps(messages),
          "has_media": 1 if (bool(existing["has_media"]) or has_media) else 0,
          "last_message_at": now,
          "updated_at": now,
        }
      )

      logging.info(
        "[prism-relay-db] Appended to one-to-one buffer id=%s sender=%s message_count=%s",
        existing["id"],
        sender_phone,
        len(messages),
      )

      return existing["id"]

    buffer_id = str(uuid.uuid4())

    conn.execute(
      text("""
        INSERT INTO prism_relay_buffer (
          id,
          sender_phone,
          sender_label,
          messages_json,
          has_hey,
          has_media,
          status,
          first_message_at,
          last_message_at,
          created_at,
          updated_at
        )
        VALUES (
          :id,
          :sender_phone,
          :sender_label,
          :messages_json,
          1,
          :has_media,
          'pending',
          :first_message_at,
          :last_message_at,
          :created_at,
          :updated_at
        )
      """),
      {
        "id": buffer_id,
        "sender_phone": sender_phone,
        "sender_label": sender_label,
        "messages_json": json.dumps([new_message]),
        "has_media": 1 if has_media else 0,
        "first_message_at": now,
        "last_message_at": now,
        "created_at": now,
        "updated_at": now,
      }
    )

    logging.info(
      "[prism-relay-db] Created one-to-one buffer id=%s sender=%s",
      buffer_id,
      sender_phone,
    )

    return buffer_id


def _prism_relay_claim_ready_buffers(limit=10):
  now = datetime.utcnow()

  with engine.begin() as conn:
    rows = conn.execute(
      text("""
        SELECT b.id
        FROM prism_relay_buffer b
        WHERE b.status = 'pending'
          AND b.last_message_at <= DATE_SUB(UTC_TIMESTAMP(), INTERVAL :wait_seconds SECOND)
          AND NOT EXISTS (
            SELECT 1
            FROM prism_relay_buffer p
            WHERE p.sender_phone = b.sender_phone
              AND p.status = 'processing'
          )
        ORDER BY b.last_message_at ASC
        LIMIT :limit
      """),
      {
        "wait_seconds": PRISM_RELAY_BUFFER_SECONDS,
        "limit": limit,
      }
    ).mappings().all()

    claimed = []

    for row in rows:
      try:
        result = conn.execute(
          text("""
            UPDATE prism_relay_buffer
            SET
              status = 'processing',
              processing_started_at = :now,
              updated_at = :now
            WHERE id = :id
              AND status = 'pending'
          """),
          {
            "id": row["id"],
            "now": now,
          }
        )

      except Exception:
        logging.exception(
          "[prism-relay-db] Failed claiming buffer id=%s",
          row["id"],
        )
        continue

      if result.rowcount != 1:
        continue

      buffer = conn.execute(
        text("""
          SELECT
            id,
            sender_phone,
            sender_label,
            messages_json,
            has_hey,
            has_media
          FROM prism_relay_buffer
          WHERE id = :id
          LIMIT 1
        """),
        {
          "id": row["id"],
        }
      ).mappings().first()

      if buffer:
        claimed.append(dict(buffer))

    return claimed


def _prism_relay_mark_buffer_done(buffer_id):
  now = datetime.utcnow()

  with engine.begin() as conn:
    conn.execute(
      text("""
        UPDATE prism_relay_buffer
        SET
          status = 'processed',
          processed_at = :now,
          updated_at = :now
        WHERE id = :id
      """),
      {
        "id": buffer_id,
        "now": now,
      }
    )


def _prism_relay_pending_buffer_count():
  with engine.begin() as conn:
    row = conn.execute(
      text("""
        SELECT COUNT(*) AS pending_count
        FROM prism_relay_buffer
        WHERE status = 'pending'
      """)
    ).mappings().first()

  return int(row["pending_count"] or 0)


def _prism_relay_recover_stuck_processing(max_age_seconds=180):
  with engine.begin() as conn:
    result = conn.execute(
      text("""
        UPDATE prism_relay_buffer
        SET
          status = 'pending',
          processing_started_at = NULL,
          updated_at = UTC_TIMESTAMP()
        WHERE status = 'processing'
          AND processing_started_at <= DATE_SUB(UTC_TIMESTAMP(), INTERVAL :max_age_seconds SECOND)
      """),
      {
        "max_age_seconds": max_age_seconds,
      }
    )

  if result.rowcount:
    logging.warning(
      "[prism-relay-db] Recovered stuck processing buffers count=%s",
      result.rowcount,
    )


def _prism_relay_mark_buffer_retry_or_failed(buffer_id, error):
  try:
    with engine.begin() as conn:
      conn.execute(
        text("""
          UPDATE prism_relay_buffer
          SET
            status = CASE
              WHEN retry_count >= 3 THEN 'failed'
              ELSE 'pending'
            END,
            retry_count = retry_count + 1,
            processing_started_at = NULL,
            last_error = :last_error,
            updated_at = UTC_TIMESTAMP()
          WHERE id = :id
        """),
        {
          "id": buffer_id,
          "last_error": str(error)[:2000],
        }
      )

  except Exception:
    logging.exception(
      "[prism-relay-db] Retry columns missing or retry update failed; marking failed buffer_id=%s",
      buffer_id,
    )

    with engine.begin() as conn:
      conn.execute(
        text("""
          UPDATE prism_relay_buffer
          SET
            status = 'failed',
            processing_started_at = NULL,
            updated_at = UTC_TIMESTAMP()
          WHERE id = :id
        """),
        {
          "id": buffer_id,
        }
      )


# ---------------------------------------------------------------------
# IMAGE PROCESSING
# ---------------------------------------------------------------------

def _prism_relay_process_images_for_car_details(inline_data):
  if not inline_data:
    return ""

  try:
    temp_content = [{
      "type": "input_text",
      "text": (
        "Process the attached image(s). Pull anything that helps fill out the "
        "car_details object. Focus on: year, make, model, trim, VIN, condition, "
        "color, mileage, options, modifications, visible damage, odometer text, "
        "VIN plate text, window sticker details, title/registration text, and "
        "any uncertainty. Return concise plain text with extracted vehicle details. "
        "Do not say you cannot view images."
      )
    }]

    for item in inline_data:
      mime_type = item.get("mime_type") or "image/jpeg"
      image_b64 = item.get("data")

      if not image_b64:
        continue

      if isinstance(image_b64, str) and image_b64.startswith("data:") and "," in image_b64:
        image_b64 = image_b64.split(",", 1)[1]

      temp_content.append({
        "type": "input_image",
        "image_url": f"data:{mime_type};base64,{image_b64}",
      })

    if len(temp_content) <= 1:
      return ""

    image_process = ava_helper([{
      "role": "user",
      "content": temp_content,
    }], [])

    logging.info(
      "[prism-relay-db] Image processed for car_details length=%s text=%s",
      len(image_process or ""),
      (image_process or "")[:1500],
    )

    return image_process or ""

  except Exception:
    logging.exception("[prism-relay-db] Failed processing images for car_details")
    return ""


# ---------------------------------------------------------------------
# ANDREW / RRA
# ---------------------------------------------------------------------

def _prism_relay_parse_andrew_response(response):
  try:
    json_andrew = response.json()

    message_assembly = ""

    if isinstance(json_andrew, list):
      for chunk in json_andrew:
        if isinstance(chunk, dict) and "text" in chunk:
          message_assembly += " " + str(chunk["text"])

    elif isinstance(json_andrew, dict):
      message_assembly = str(
        json_andrew.get("text")
        or json_andrew.get("response")
        or ""
      )

    return message_assembly.strip()

  except Exception:
    logging.exception("[prism-relay] Failed parsing RRA response")
    return ""


def _prism_relay_get_or_create_rra_session(andrew_user_id):
  session_lookup_response = requests.put(
    RRA_SESSION_LOOKUP_URL + andrew_user_id,
    headers={"Authorization": SA_AUTHENTICATION_HARDCODED},
    timeout=240,
  )

  logging.info(
    "[prism-relay] RRA session lookup user_id=%s status=%s body=%s",
    andrew_user_id,
    session_lookup_response.status_code,
    session_lookup_response.text[:500],
  )

  session_payload = session_lookup_response.json()

  if len(session_payload.get("sessions", [])) == 0:
    session_create_response = requests.get(
      RRA_SESSION_LOOKUP_URL + andrew_user_id,
      headers={"Authorization": SA_AUTHENTICATION_HARDCODED},
      timeout=240,
    )

    logging.info(
      "[prism-relay] RRA session create user_id=%s status=%s body=%s",
      andrew_user_id,
      session_create_response.status_code,
      session_create_response.text[:500],
    )

    return {
      "id": session_create_response.json()["session_id"],
      "state": {
        "chat_history": [],
      },
    }

  return session_payload["sessions"][0]


# ---------------------------------------------------------------------
# BUFFER PROCESSOR
# ---------------------------------------------------------------------

def _prism_relay_process_db_buffer(buffer):
  buffer_id = buffer["id"]
  sender_phone = normalize_e164_phone(buffer["sender_phone"])
  sender_label = buffer["sender_label"]

  messages = json.loads(buffer["messages_json"] or "[]")

  combined_text = "\n".join([
    item.get("body") or ""
    for item in messages
    if item.get("body")
  ]).strip()

  inline_data = []

  for item in messages:
    for file_info in item.get("files") or []:
      if file_info.get("data"):
        inline_data.append({
          "mime_type": file_info.get("mime_type") or "image/jpeg",
          "data": file_info["data"],
        })

  logging.info(
    "[prism-relay-db] Processing one-to-one buffer id=%s sender=%s text_len=%s inline_data_count=%s",
    buffer_id,
    sender_label,
    len(combined_text),
    len(inline_data),
  )

  image_context = ""

  if inline_data:
    image_context = _prism_relay_process_images_for_car_details(inline_data)

  if not combined_text and not image_context:
    logging.info(
      "[prism-relay-db] Empty one-to-one buffer after image processing id=%s",
      buffer_id,
    )
    return

  andrew_user_id = sender_phone
  session_id = _prism_relay_get_or_create_rra_session(andrew_user_id)

  message_for_prism = (
    "This is a one-to-one SMS conversation from Prism relay. "
    "The user sent message(s) to the 984 Twilio number. "
    "Reply directly to the user as Andrew. "
    "Treat this like an ongoing Andrew-to-user chat buffer. "
    "Use the sender phone number as the Andrew user identity. "
    "Use any extracted image context to help fill out car_details fields: "
    "year, make, model, trim, VIN, condition, color, mileage, options, and modifications. "
    "If processed image context is provided, do not say you cannot view images; "
    "use that processed image context as the visual evidence.\n\n"
    f"Sender: {sender_label}\n"
    f"Andrew user_id: {andrew_user_id}\n\n"
  )

  if combined_text:
    message_for_prism += f"User message buffer:\n{combined_text}\n\n"

  if image_context:
    message_for_prism += f"Processed image context:\n{image_context}\n\n"

  prism_payload = {
    "action": "create",
    "message": message_for_prism,
    "user_id": andrew_user_id,
    "session_id": str(session_id["id"]),
  }

  logging.info(
    "[prism-relay-db] Sending one-to-one Andrew payload buffer_id=%s user_id=%s message=%s",
    buffer_id,
    andrew_user_id,
    message_for_prism[:2000],
  )

  andrew_response = requests.post(
    RRA_PRISM_URL,
    json=prism_payload,
    headers={"Authorization": SA_AUTHENTICATION_HARDCODED},
    timeout=90,
  )

  logging.info(
    "[prism-relay-db] One-to-one Andrew response buffer_id=%s status=%s body=%s",
    buffer_id,
    andrew_response.status_code,
    andrew_response.text[:1000],
  )

  message_assembly = _prism_relay_parse_andrew_response(andrew_response)

  if not message_assembly:
    logging.info(
      "[prism-relay-db] Empty Andrew response buffer_id=%s",
      buffer_id,
    )

    _prism_relay_send_sms(
      sender_phone,
      "Andrew had trouble responding to that. Try again in a moment.",
    )
    return

  _prism_relay_send_sms(
    sender_phone,
    message_assembly,
  )

  logging.info(
    "[prism-relay-db] Sent one-to-one Andrew response buffer_id=%s to=%s",
    buffer_id,
    sender_phone,
  )


@app.route('/api/v1/prism/relay/process', methods=['POST'])
async def prism_relay_process():
  auth_token = request.headers.get("Authorization")

  if auth_token != SA_AUTHENTICATION_HARDCODED:
    return {"ok": False, "error": "Unauthorized"}, 403

  _prism_relay_recover_stuck_processing(max_age_seconds=180)

  buffers = _prism_relay_claim_ready_buffers(limit=10)

  if not buffers:
    pending_count = _prism_relay_pending_buffer_count()

    if pending_count > 0:
      task_name = _prism_relay_schedule_process_task(
        delay_seconds=PRISM_RELAY_BUFFER_SECONDS
      )

      logging.info(
        "[prism-relay-db] No ready buffers, pending_count=%s, rescheduled task=%s",
        pending_count,
        task_name,
      )

    return {
      "ok": True,
      "claimed": 0,
      "processed": [],
      "failed": [],
      "pending_count": pending_count,
    }, 200

  processed = []
  failed = []

  for buffer in buffers:
    try:
      _prism_relay_process_db_buffer(buffer)
      _prism_relay_mark_buffer_done(buffer["id"])
      processed.append(buffer["id"])

    except Exception as e:
      logging.exception(
        "[prism-relay-db] Failed processing buffer_id=%s",
        buffer["id"],
      )

      failed.append({
        "id": buffer["id"],
        "error": str(e),
      })

      _prism_relay_mark_buffer_retry_or_failed(buffer["id"], e)

  pending_count = _prism_relay_pending_buffer_count()

  if pending_count > 0:
    task_name = _prism_relay_schedule_process_task(
      delay_seconds=PRISM_RELAY_BUFFER_SECONDS
    )

    logging.info(
      "[prism-relay-db] Pending buffers remain after processing pending_count=%s rescheduled task=%s",
      pending_count,
      task_name,
    )

  return {
    "ok": True,
    "claimed": len(buffers),
    "processed": processed,
    "failed": failed,
    "pending_count": pending_count,
  }, 200


# ---------------------------------------------------------------------
# DEBUG ROUTE
# ---------------------------------------------------------------------

@app.route('/api/v1/prism/relay/debug/buffers', methods=['GET'])
async def prism_relay_debug_buffers():
  auth_token = request.headers.get("Authorization") or request.args.get("token")

  if auth_token != SA_AUTHENTICATION_HARDCODED:
    return {"ok": False, "error": "Unauthorized"}, 403

  with engine.begin() as conn:
    rows = conn.execute(
      text("""
        SELECT
          id,
          sender_phone,
          sender_label,
          status,
          has_hey,
          has_media,
          JSON_LENGTH(messages_json) AS message_count,
          first_message_at,
          last_message_at,
          processing_started_at,
          processed_at,
          updated_at,
          TIMESTAMPDIFF(SECOND, last_message_at, UTC_TIMESTAMP()) AS seconds_since_last,
          TIMESTAMPDIFF(SECOND, processing_started_at, UTC_TIMESTAMP()) AS seconds_processing,
          retry_count,
          last_error
        FROM prism_relay_buffer
        WHERE status IN ('pending', 'processing', 'failed')
        ORDER BY updated_at DESC
        LIMIT 25
      """)
    ).mappings().all()

  return {
    "ok": True,
    "buffers": [dict(row) for row in rows],
  }, 200


# ---------------------------------------------------------------------
# WEBHOOK ROUTE
# ---------------------------------------------------------------------

@app.route('/api/v1/prism/relay', methods=['POST'])
async def prism_relay():
  request_id = uuid.uuid4().hex[:8]

  def log_stage(stage, data=None):
    try:
      if data is None:
        logging.info("[prism-relay:%s] %s", request_id, stage)
      else:
        logging.info("[prism-relay:%s] %s | %s", request_id, stage, data)
    except Exception:
      logging.info(
        "[prism-relay:%s] %s | <failed to serialize log data>",
        request_id,
        stage,
      )

  try:
    log_stage("START one-to-one relay webhook received")

    form_data = await request.form
    form = form_data.to_dict(flat=True)

    if not form:
      json_body = await request.get_json(silent=True) or {}
      if "body" in json_body and isinstance(json_body["body"], dict):
        form = json_body["body"]
      else:
        form = json_body

    log_stage("RAW form parsed", {
      "keys": sorted(list(form.keys())),
      "form_preview": {
        k: form.get(k)
        for k in sorted(list(form.keys()))
        if k.lower() not in {"authorization", "password", "token"}
      },
    })

    sender_phone = normalize_e164_phone(
      form.get("From")
      or form.get("Author")
    )

    to_phone = normalize_e164_phone(form.get("To"))

    body = (
      form.get("Body")
      or form.get("body")
      or ""
    ).strip()

    message_sid = (
      form.get("MessageSid")
      or form.get("SmsMessageSid")
      or uuid.uuid4().hex
    )

    relay_number = _prism_relay_number()

    log_stage("Parsed inbound one-to-one relay message", {
      "sender_phone": sender_phone,
      "to_phone": to_phone,
      "relay_number": relay_number,
      "message_sid": message_sid,
      "body": body,
      "NumMedia": form.get("NumMedia"),
    })

    if not sender_phone:
      log_stage("ERROR missing sender_phone")
      return "Missing sender", 400

    if sender_phone == relay_number:
      log_stage("IGNORED self-message from relay number", {
        "sender_phone": sender_phone,
        "relay_number": relay_number,
      })
      return "OK", 200

    sender_user = _prism_relay_user_for_phone(sender_phone)

    if not sender_user:
      log_stage("IGNORED unknown sender not in buyers table", {
        "sender_phone": sender_phone,
      })
      return "OK", 200

    if sender_user.get("name", "").lower() == "system":
      log_stage("IGNORED System sender", sender_user)
      return "OK", 200

    files = _prism_relay_download_mms_attachments(
      form,
      request_id=request_id,
    )

    log_stage("Media download complete", {
      "file_count": len(files),
      "files": files,
    })

    if not body and not files:
      log_stage("IGNORED empty body and no files")
      return "OK", 200

    _prism_relay_log_message(sender_phone, body)

    has_media = len(files) > 0
    sender_label = _prism_relay_sender_label(sender_phone)

    buffer_id = _prism_relay_append_to_db_buffer(
      sender_phone=sender_phone,
      sender_label=sender_label,
      message_sid=message_sid,
      body=body,
      files=files,
      has_media=has_media,
    )

    log_stage("One-to-one relay DB buffer updated", {
      "buffer_id": buffer_id,
      "sender_label": sender_label,
      "has_media": has_media,
      "wait_seconds": PRISM_RELAY_BUFFER_SECONDS,
    })

    task_name = _prism_relay_schedule_process_task(
      delay_seconds=PRISM_RELAY_BUFFER_SECONDS
    )

    if task_name:
      log_stage("Relay process task scheduled", {
        "buffer_id": buffer_id,
        "task_name": task_name,
        "delay_seconds": PRISM_RELAY_BUFFER_SECONDS,
      })
    else:
      log_stage("Relay process task scheduling failed", {
        "buffer_id": buffer_id,
        "delay_seconds": PRISM_RELAY_BUFFER_SECONDS,
      })

    return "OK", 200

  except Exception:
    logging.exception("[prism-relay:%s] FATAL one-to-one relay webhook failed", request_id)
    return "Bad request", 200


# ---------------------------------------------------------------------
# DEBUG ROUTE
# ---------------------------------------------------------------------

@app.route('/api/v1/prism/relay/users', methods=['GET'])
async def prism_relay_users_debug():
  auth_token = request.headers.get("Authorization") or request.args.get("token")

  if auth_token != SA_AUTHENTICATION_HARDCODED:
    return {"ok": False, "error": "Unauthorized"}, 403

  try:
    return {
      "ok": True,
      "relay_number": _prism_relay_number(),
      "table": PRISM_RELAY_USERS_TABLE,
      "users": _prism_relay_load_users(),
      "outbound_mode": "forced_from_number_no_messaging_service_sid",
    }, 200

  except Exception as e:
    logging.exception("Failed loading relay users")

    return {
      "ok": False,
      "error": "Failed loading relay users",
      "message": str(e),
    }, 500

# ---------------------------------------------------------------------------
# Shared Prism-local Manheim implementation.
#
# The HTTP routes and valuation pipeline call the SAME primitives below. Each primitive
# is one real Manheim operation, so whichever piece returns first can be persisted first.
# ---------------------------------------------------------------------------


def _manheim_auth_headers():
  token = Authorize("manheim")
  if not token:
    raise RuntimeError("Manheim authorization returned no token")
  return {'Authorization': f'Bearer {token}'}


async def _prism_manheim_headers():
  return await asyncio.to_thread(_manheim_auth_headers)


def _manheim_json_response(response, label: str):
  if response.status_code < 200 or response.status_code >= 300:
    raise RuntimeError(f"{label} returned HTTP {response.status_code}: {response.text[:500]}")
  return response.json()


def _manheim_common_params(car_details: dict):
  mileage = car_details.get('mileage', None)
  if mileage in ("", None):
    mileage = car_details.get('odometer', None)

  params = {"region": car_details.get('region', 'SW')}
  if mileage not in (None, ""):
    params["odometer"] = mileage
  if car_details.get('color'):
    params["exteriorColor"] = car_details.get('color')
  if car_details.get('grade'):
    params["grade"] = car_details.get('grade')
  return params


async def _prism_manheim_get(url: str, *, headers: dict, params=None, label: str):
  try:
    response = await asyncio.to_thread(
      requests.get,
      url,
      params=params,
      headers=headers,
      timeout=10,
    )
    return _manheim_json_response(response, label)
  except Exception as error:
    return {
      "error": f"{label} failed: {type(error).__name__}: {error}",
      "errors": [f"{label} failed: {type(error).__name__}: {error}"],
    }


async def _prism_manheim_current_mmr(car_details: dict, headers: dict = None):
  headers = headers or await _prism_manheim_headers()
  vin = clean_vin(car_details.get('vin'))
  if not vin:
    return {"error": "Missing VIN", "errors": ["Missing VIN"]}
  return await _prism_manheim_get(
    f"https://api.manheim.com/valuations/vin/{vin}",
    headers=headers,
    params=_manheim_common_params(car_details),
    label="Manheim current MMR",
  )


async def _prism_manheim_historical_mmr(car_details: dict, headers: dict = None):
  headers = headers or await _prism_manheim_headers()
  vin = clean_vin(car_details.get('vin'))
  if not vin:
    return {"error": "Missing VIN", "errors": ["Missing VIN"]}
  params = _manheim_common_params(car_details)
  params["date"] = car_details.get('historical_date') or datetime.utcnow().date().isoformat()
  return await _prism_manheim_get(
    f"https://api.manheim.com/valuations/vin/{vin}",
    headers=headers,
    params=params,
    label="Manheim historical MMR",
  )


async def _prism_manheim_future_mmr(car_details: dict, headers: dict = None):
  headers = headers or await _prism_manheim_headers()
  vin = clean_vin(car_details.get('vin'))
  if not vin:
    return {"error": "Missing VIN", "errors": ["Missing VIN"]}
  params = _manheim_common_params(car_details)
  params["date"] = (datetime.utcnow().date() + timedelta(days=7)).isoformat()
  return await _prism_manheim_get(
    f"https://api.manheim.com/valuations/forecast/vin/{vin}",
    headers=headers,
    params=params,
    label="Manheim future MMR",
  )


async def _prism_manheim_condition_evidence(car_details: dict, headers: dict = None):
  headers = headers or await _prism_manheim_headers()
  vin = clean_vin(car_details.get('vin'))
  if not vin:
    return {"error": "Missing VIN", "errors": ["Missing VIN"]}
  return await _prism_manheim_get(
    f"https://api.manheim.com/condition-reports/search?vin={vin}",
    headers=headers,
    label="Manheim condition report",
  )


def _manheim_execute_search_sync(headers: dict, payload: dict, *, limit: int, label: str):
  create_response = requests.post(
    "https://api.manheim.com/searches",
    json=payload,
    headers=headers,
    timeout=10,
  )
  if create_response.status_code != 201:
    return {
      "error": f"{label} create HTTP {create_response.status_code}",
      "body": create_response.text[:1000],
    }

  search_id = (create_response.json() or {}).get('id')
  if not search_id:
    return {"error": f"{label} create response had no id"}

  execute_response = requests.post(
    f"https://api.manheim.com/searches/execute/id/{search_id}",
    json={"includeAllFields": True, "limit": limit},
    headers=headers,
    timeout=10,
  )
  if execute_response.status_code != 200:
    return {
      "error": f"{label} execute HTTP {execute_response.status_code}",
      "body": execute_response.text[:1000],
    }
  return execute_response.json()


async def _prism_manheim_search(car_details: dict, headers: dict, *, payload: dict, limit: int, label: str):
  try:
    result = await asyncio.to_thread(
      _manheim_execute_search_sync,
      headers,
      payload,
      limit=limit,
      label=label,
    )
    if isinstance(result, dict) and result.get("error"):
      result.setdefault("errors", [str(result.get("error"))])
    return result
  except Exception as error:
    return {
      "error": f"{label} failed: {type(error).__name__}: {error}",
      "errors": [f"{label} failed: {type(error).__name__}: {error}"],
    }


async def _prism_manheim_vin_sales(car_details: dict, headers: dict = None):
  headers = headers or await _prism_manheim_headers()
  vin = clean_vin(car_details.get('vin'))
  if not vin:
    return {"error": "Missing VIN", "errors": ["Missing VIN"]}
  return await _prism_manheim_search(
    car_details,
    headers,
    payload={"vins": [vin], "limit": 500},
    limit=100,
    label="Manheim VIN sales",
  )







async def _prism_manheim_ymm_sales(car_details: dict, headers: dict = None):
  headers = headers or await _prism_manheim_headers()
  year = car_details.get('year')
  make = car_details.get('make')
  model = car_details.get('model')
  three_months_ago = (datetime.utcnow() - timedelta(days=90)).date().isoformat()

  payload={
            "keyword": f"{year} {make} {model}",
            "statusIds": [
                "00000000-0000-1000-0000-000000050085"
            ]
  }
  if not (year and make and model):
    return {"error": "Missing year/make/model", "errors": ["Missing year/make/model"]}
  return await _prism_manheim_search(
    car_details,
    headers,
    payload=payload,
    limit=100,
    label="Manheim YMM sales",
  )


async def _prism_manheim_mm_sales(car_details: dict, headers: dict = None):
  headers = headers or await _prism_manheim_headers()
  make = car_details.get('make')
  model = car_details.get('model')
  if not (make and model):
    return {"error": "Missing make/model", "errors": ["Missing make/model"]}
  return await _prism_manheim_search(
    car_details,
    headers,
    payload={"keyword": f"{make} {model}"},
    limit=1000,
    label="Manheim make/model sales",
  )


@app.route('/api/v1/prism/manheim/condition_report/<vin>', methods=['GET'])
async def prism_manheim_condition_report(vin):
  try:
    auth_token = request.headers.get('Authorization')
    if auth_token != SA_AUTHENTICATION:
      return ("Unauthorized", 403)
    headers = await _prism_manheim_headers()
    return await _prism_manheim_condition_evidence({"vin": vin}, headers), 200
  except Exception as e:
    logging.exception("Manheim condition report route failed")
    return {"error": f"{type(e).__name__}: {e}"}, 500


@app.route('/api/v1/prism/manheim/data/mmr', methods=['POST'])
async def prism_manheim_data():
  try:
    body = await request.get_json()
    car_details = body.get('car_details')
    if not car_details:
      return ("Missing car_details", 400)

    headers = await _prism_manheim_headers()
    current, historical, future = await asyncio.gather(
      _prism_manheim_current_mmr(car_details, headers),
      _prism_manheim_historical_mmr(car_details, headers),
      _prism_manheim_future_mmr(car_details, headers),
    )
    return {
      "historical_mmr": historical,
      "future_mmr": future,
      "current_mmr": current,
    }, 200
  except Exception as e:
    logging.exception("Manheim MMR route failed")
    return {"error": f"{type(e).__name__}: {e}"}, 500


@app.route('/api/v1/prism/manheim/data/sale', methods=['POST'])
async def prism_manheim_sale_data():
  try:
    body = await request.get_json()
    car_details = body.get('car_details')
    if not car_details:
      return ("Missing car_details", 400)

    headers = await _prism_manheim_headers()
    live_sale, comp_sales, all_sales = await asyncio.gather(
      _prism_manheim_vin_sales(car_details, headers),
      _prism_manheim_ymm_sales(car_details, headers),
      _prism_manheim_mm_sales(car_details, headers),
    )
    return {
      "live_sale": live_sale,
      "comp_sales": comp_sales,
      "all_sales": all_sales,
    }, 200
  except Exception as e:
    logging.exception("Manheim sale route failed")
    return {"error": f"{type(e).__name__}: {e}"}, 500


@app.route('/api/v1/prism/uniques/<year>/<make>', methods=['GET'])
async def get_prism_uniques(year: str, make: str):
    try:
        uniques = strategy_uniques(year, make)
        return uniques, 200
    except Exception as e:
        logging.error(f"Failed to get prism uniques, {e}")
        return "Server error", 500

@app.route('/api/v1/prism/insight/get', methods=['POST'])
async def get_prism_insight():
    try:
        message = await request.get_json()
        logging.info(f"Prism get insight call...")
        return insight_get(message['year'],message['make']), 200
    except Exception as e:
        logging.error(f"Failed to get prism insight, {e}")
        return "Server error", 500

@app.route('/api/v1/prism/strategy/get', methods=['POST'])
async def get_prism_strategy():
    try:
        message = await request.get_json()
        logging.info(f"Prism get strategy call...")
        return strategy_group(message['car']), 200
    except Exception as e:
        logging.error(f"Failed to get prism strategy, {e}")
        return "Server error", 500

@app.route('/api/v1/prism/insight/uniques', methods=['GET'])
async def get_prism_insight_uniques():
    try:
        logging.info(f"Prism get insight uniques call...")
        return insight_get_uniques(), 200
    except Exception as e:
        logging.error(f"Failed to get prism insight uniques, {e}")
        return "Server error", 500

@app.route('/api/v1/prism/insight/year/add', methods=['POST'])
async def add_prism_year_insight():
    try:
        message = await request.get_json()
        logging.info(f"Prism add year insight call...")
        return insight_year_add(message['year'],message['insight']), 200
    except Exception as e:
       logging.error(f"Failed to add year insight, {e}")
       return "Server error", 500

@app.route('/api/v1/prism/insight/make/add', methods=['POST'])
async def add_prism_make_insight():
    try:
        message = await request.get_json()
        logging.info(f"Prism add make insight call...")
        return insight_make_add(message['make'],message['insight']), 200
    except Exception as e:
       logging.error(f"Failed to add make insight, {e}")
       return "Server error", 500

@app.route('/api/v1/prism/insight/general/add', methods=['POST'])
async def add_prism_general_insight():
    try:
        message = await request.get_json()
        logging.info(f"Prism add general insight call...")
        return insight_general_add(message['topic'],message['insight']), 200
    except Exception as e:
       logging.error(f"Failed to add general insight, {e}")
       return "Server error", 500
    
@app.route('/api/v1/prism/insight/year/edit', methods=['POST'])
async def edit_prism_year_insight():
    try:
        message = await request.get_json()
        logging.info(f"Prism edit year insight call...")
        return insight_year_edit(message['year'],message['insight']), 200
    except Exception as e:
        logging.error(f"Failed to edit year insight, {e}")
        return "Server error", 500
    
@app.route('/api/v1/prism/insight/make/edit', methods=['POST'])
async def edit_prism_make_insight():
    try:
        message = await request.get_json()
        logging.info(f"Prism edit make insight call...")
        return insight_make_edit(message['make'],message['insight']), 200
    except Exception as e:
        logging.error(f"Failed to edit make insight, {e}")
        return "Server error", 500
    
@app.route('/api/v1/prism/insight/general/edit', methods=['POST'])
async def edit_prism_general_insight():
    try:
        message = await request.get_json()
        logging.info(f"Prism edit general insight call...")
        return insight_general_edit(message['topic'],message['insight']), 200
    except Exception as e:
        logging.error(f"Failed to edit general insight, {e}")
        return "Server error", 500

@app.route('/api/v1/prism/insight/all/edit', methods=['POST'])
async def edit_prism_all_insight():
    try:
        message = await request.get_json()
        logging.info(f"Prism edit all insight call...")
        try:
          year_result = insight_year_edit(message['year'],message['year_insight'])
        except Exception as e:
          logging.error(f"Failed to edit year insight inside all edit, {e}")
          year_result = f"Failed to edit year insight inside all edit, {e}"
        try:
          make_result = insight_make_edit(message['make'],message['make_insight'])
        except Exception as e:
          logging.error(f"Failed to edit make insight inside all edit, {e}")
          make_result = f"Failed to edit make insight inside all edit, {e}"
        try:
          general_result = insight_general_edit(message['topic'],message['general_insight'])
        except Exception as e:
          logging.error(f"Failed to edit general insight inside all edit, {e}")
          general_result = f"Failed to edit general insight inside all edit, {e}"
        return {"year_update_result": year_result, "make_update_result": make_result, "general_update_result": general_result}
    except Exception as e:
        logging.error(f"Failed to edit all insights, {e}")
        return f"Failed to edit all insights, {e}", 500


@app.route('/api/v1/prism/strategy/add', methods=['POST'])
async def add_prism_strategy():
    try:
        message = await request.get_json()
        logging.info(f"Prism add strategy call...")
        return strategy_add(message['car'],message['strategy'],message['learning']), 200
    except Exception as e:
        logging.error(f"Failed to set prism strategy, {e}")
        return "Server error", 500
    
@app.route('/api/v1/prism/strategy/edit', methods=['POST'])
async def edit_prism_strategy():
    try:
        message = await request.get_json()
        logging.info(f"Prism edit strategy call...")
        return strategy_edit(message['car'],message['learning']), 200
    except Exception as e:
        logging.error(f"Failed to set prism strategy, {e}")
        return "Server error", 500


@app.route('/api/v1/prism/task/create', methods=['POST'])
async def create_prism_task():
    try:
        message = await request.get_json()
        logging.info(f"Prism create task call...")
        return create_task(message['user_id'], message['session_id']), 200
    except Exception as e:
        logging.error(f"Failed to create prism task, {e}")
        return "Server error", 500
    
@app.route('/api/v1/prism/task/update', methods=['POST'])
async def update_prism_task():
    try:
        message = await request.get_json()
        logging.info(f"Prism update task call...")
        return update_task(message['job_id'], message['content'], message['stream']), 200
    except Exception as e:
        logging.error(f"Failed to update prism task, {e}")
        return "Server error", 500
    
@app.route('/api/v1/prism/task/close', methods=['POST'])
async def close_prism_task():
    try:
        message = await request.get_json()
        logging.info(f"Prism close task call...")
        return close_task(message['job_id']), 200
    except Exception as e:
        logging.error(f"Failed to close prism task, {e}")
        return "Server error", 500
    

def get_task(task_id: int):
    logging.info("Loading task %d", task_id)
    try: 
      with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM jobs WHERE id = :task_id"), {"task_id": task_id})
        row = result.mappings().first()
      if not row:
          logging.info("No task found")
          return []
      logging.info("Loaded task %d", task_id)
      return [{"id": row["id"], "session_id": row["session_id"], "user_id": row["user_id"], "status": row["status"], "content": row["content"], "stream": row["stream"]}]
    except Exception as e:
      logging.error(f"Failed to load task: {e}")
      return []
    
@app.route('/api/v1/prism/task/get/<int:task_id>', methods=['GET'])
async def get_prism_task(task_id: int):
    try:
        logging.info(f"Prism get task call...")
        return get_task(task_id), 200
    except Exception as e:
        logging.error(f"Failed to get prism task, {e}")
        return "Server error", 500
    

      

      
@app.route('/prism/ava/main/get', methods=['POST'])
async def api_main_get():
  if "Authorization" not in request.headers:
    return "Unauthorized", 401
  authorization = request.headers['Authorization']
  if authorization != os.environ.get("SA_AUTHENTICATION"):
    return "Unauthorized", 403
  else:
    """API endpoint to load a object."""
    object = await request.get_json()
    obj, error = main_get(object)
    return {"object": obj, "error": error}


@app.route('/prism/ava/main/add', methods=['POST'])
async def api_main_add():
  if "Authorization" not in request.headers:
    return "Unauthorized", 401
  authorization = request.headers['Authorization']
  if authorization != os.environ.get("SA_AUTHENTICATION"):
    return "Unauthorized", 403
  else:
    """API endpoint to insert a object."""
    object = await request.get_json()
    return main_add(object)

@app.route('/prism/ava/main/update', methods=['POST'])
async def api_main_update():
  if "Authorization" not in request.headers:
    return "Unauthorized", 401
  authorization = request.headers['Authorization']
  if authorization != os.environ.get("SA_AUTHENTICATION"):
    return "Unauthorized", 403
  else:
    """API endpoint to update a object."""
    object = await request.get_json()
    return main_update(object)
   
@app.route('/prism/ava/main/delete', methods=['POST'])
async def api_main_delete():
  if "Authorization" not in request.headers:
    return "Unauthorized", 401
  authorization = request.headers['Authorization']
  if authorization != os.environ.get("SA_AUTHENTICATION"):
    return "Unauthorized", 403
  else:
    """API endpoint to delete a object."""
    object = await request.get_json()
    return main_delete(object)

#EXTERNAL INTEGRATION ENDPOINTS
@app.route('/api/v1/prism/copart/webhook/events', methods=['POST','GET'])
async def copart_events():
    # ── Health-check ──────────────────────────────────────────────────────
    if request.method == 'GET':
        return {"status": "ok"}, 200

    # ── Ingest ────────────────────────────────────────────────────────────
    data: dict = await request.get_json(silent=True) or {}

    tx_ctx   = _extract_transaction_context(data)
    ev_info  = _extract_event_info(data)
    asn_ctx  = _extract_assignment_context(data)

    log_ctx = {**tx_ctx, **ev_info, **asn_ctx}

    logger.info(
        f"[WEBHOOK] Copart event received | "
        f"event_code={ev_info['event_code']} "
        f"category={ev_info['event_category']} "
        f"stock={tx_ctx['stock_number']} "
        f"claim={asn_ctx['claim_number']}",
        extra=log_ctx,
    )

    # ── Validate minimum required fields ─────────────────────────────────
    errors = []
    if not tx_ctx.get("transaction_id"):
        errors.append({"errorCode": "001", "errorDescription": "Missing transactionID", "severity": "Error"})
    if not tx_ctx.get("stock_number") and not data.get("vehicleDetails", {}).get("vin"):
        errors.append({"errorCode": "002", "errorDescription": "Missing stockNumber or VIN", "severity": "Error"})
    if not ev_info.get("event_code") or ev_info["event_code"] == "UNKNOWN":
        errors.append({"errorCode": "003", "errorDescription": "Missing or unknown eventCode", "severity": "Error"})

    if errors:
        logger.warning(f"[WEBHOOK] Payload validation failed | errors={errors}", extra=log_ctx)
        return _build_response(
            "REJECT",
            "Payload validation failed",
            transaction_id=tx_ctx.get("transaction_id"),
            errors=errors,
        ), 400

    # ── Route to specific handler ─────────────────────────────────────────
    try:
        dispatch_event(
            event_code=ev_info["event_code"],
            category=ev_info["event_category"],
            ctx=log_ctx,
            data=data,
        )
    except Exception as exc:
        logger.exception(
            f"[WEBHOOK] Handler error | event_code={ev_info['event_code']} error={exc}",
            extra=log_ctx,
        )
        return _build_response(
            "REJECT",
            "Internal handler error",
            transaction_id=tx_ctx.get("transaction_id"),
            errors=[{"errorCode": "500", "errorDescription": str(exc), "severity": "Error"}],
        ), 500

    # ── Acknowledge ───────────────────────────────────────────────────────
    return _build_response(
        "ACCEPT",
        "Event received and processed",
        transaction_id=tx_ctx.get("transaction_id"),
    ), 200

# ---------------------------------------------------------------------------
# GRAPH API
# ---------------------------------------------------------------------------
 
_ig_user_id_cache = {"value": None, "fetched_at": 0}
 
 
def _ig_resolve_user_id():
    """IG business account hanging off FB_PAGE_ID. Cached an hour."""
    if _ig_user_id_cache["value"] and time.time() - _ig_user_id_cache["fetched_at"] < 3600:
        return _ig_user_id_cache["value"]
 
    r = requests.get(
        f"{FB_GRAPH}/{FB_PAGE_ID}",
        params={"fields": "instagram_business_account",
                "access_token": FB_LL_USER_TOKEN},
        timeout=IG_GRAPH_TIMEOUT,
    )
    body = r.json() or {}
    if r.status_code >= 400:
        raise RuntimeError(body.get("error", {}).get("message") or r.text)
 
    ig_id = (body.get("instagram_business_account") or {}).get("id")
    if not ig_id:
        raise RuntimeError(
            f"No instagram_business_account on page {FB_PAGE_ID}. The token in "
            "FB_LL_USER_ACCESS_TOKEN needs instagram_basic + pages_show_list."
        )
 
    _ig_user_id_cache.update(value=ig_id, fetched_at=time.time())
    return ig_id
 
 
def _ig_fetch_media_page(limit, after=None, before=None):
    params = {
        "fields": IG_MEDIA_FIELDS,
        "limit": limit,
        "access_token": FB_LL_USER_TOKEN,
    }
    if after:
        params["after"] = after
    if before:
        params["before"] = before
 
    r = requests.get(
        f"{FB_GRAPH}/{_ig_resolve_user_id()}/media",
        params=params,
        timeout=IG_GRAPH_TIMEOUT,
    )
    body = r.json() or {}
    if r.status_code >= 400:
        raise RuntimeError(body.get("error", {}).get("message") or r.text)
    return body
 
 
# ---------------------------------------------------------------------------
# SETTINGS STORAGE
# ---------------------------------------------------------------------------
 
def _ig_row_to_settings(row):
    def _arr(v):
        if v is None or v == "":
            return []
        if isinstance(v, str):
            try:
                v = json.loads(v)
            except ValueError:
                return []
        return [str(x) for x in v] if isinstance(v, list) else []
 
    return {
        "media_id": str(row["media_id"]),
        "auto_reply_enabled": bool(row.get("auto_reply_enabled")),
        "auto_reply_url": row.get("auto_reply_url") or "",
        "auto_reply_options": _arr(row.get("auto_reply_options")),
        "auto_reply_dm_options": _arr(row.get("auto_reply_dm_options")),
        "updated_at": str(row.get("updated_at") or ""),
        "updated_by": row.get("updated_by") or "",
    }
 
 
def _ig_settings_load_main():
    """
    Read the whole table via main_get. Uses retrieval_type 'all' because
    main_get has no IN-list mode -- only by_id, all, by_fields, by_field,
    last_row_id. The table holds one row per *configured* post, so 'all' stays
    small. See IG_USE_MAIN_GET above before switching this on.
    """
    rows, error = main_get({
        "table": IG_SETTINGS_TABLE,
        "retrieval_type": "all",
    })
    if error:
        raise RuntimeError(f"main_get: {error}")
    return {str(r["media_id"]): _ig_row_to_settings(r) for r in (rows or [])}
 
 
def _ig_settings_load_sql(media_ids):
    ids = list(media_ids)
    if not ids:
        return {}
    placeholders = ", ".join(f":m{i}" for i in range(len(ids)))
    params = {f"m{i}": mid for i, mid in enumerate(ids)}
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT media_id, auto_reply_enabled, auto_reply_url, "
                "auto_reply_options, auto_reply_dm_options, updated_at, updated_by "
                f"FROM {IG_SETTINGS_TABLE} WHERE media_id IN ({placeholders})"
            ),
            params,
        ).mappings().all()
    return {str(r["media_id"]): _ig_row_to_settings(r) for r in rows}
 
 
def _ig_settings_load(media_ids):
    """One batched read per page. Never N+1 this."""
    if not media_ids:
        return {}
    try:
        if IG_USE_MAIN_GET:
            everything = _ig_settings_load_main()
            return {mid: everything[mid] for mid in media_ids if mid in everything}
        return _ig_settings_load_sql(media_ids)
    except Exception as e:
        logging.exception("IG settings load failed: %s", e)
        return {}
 
 
def _ig_settings_save(record):
    """
    Upsert. Deliberately not main_add(): main_add inserts into engine AND
    avaengine with no ON DUPLICATE KEY clause, so the second save on a post
    would collide with uq_ig_autoreply_media_id -- and the engine insert has
    already committed by the time the avaengine one fails.
    """
    with engine.connect() as conn:
        conn.execute(
            text(
                f"INSERT INTO {IG_SETTINGS_TABLE} "
                "(media_id, ig_user_id, permalink, auto_reply_enabled, "
                " auto_reply_url, auto_reply_options, auto_reply_dm_options, updated_by) "
                "VALUES (:media_id, :ig_user_id, :permalink, :auto_reply_enabled, "
                " :auto_reply_url, :auto_reply_options, :auto_reply_dm_options, :updated_by) "
                "ON DUPLICATE KEY UPDATE "
                " ig_user_id = VALUES(ig_user_id), "
                " permalink = VALUES(permalink), "
                " auto_reply_enabled = VALUES(auto_reply_enabled), "
                " auto_reply_url = VALUES(auto_reply_url), "
                " auto_reply_options = VALUES(auto_reply_options), "
                " auto_reply_dm_options = VALUES(auto_reply_dm_options), "
                " updated_by = VALUES(updated_by)"
            ),
            record,
        )
        conn.commit()
    _ig_settings_cache.pop(record["media_id"], None)
 
 
# ---------------------------------------------------------------------------
# VALIDATION
# ---------------------------------------------------------------------------
 
class IgSettingsError(Exception):
    """Named this way on purpose -- ValidationError is jsonschema's."""
 
 
def _ig_clean_url(value):
    value = (value or "").strip()
    if not value:
        return None
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise IgSettingsError(
            f"Auto-reply URL must start with http:// or https:// -- got {value!r}"
        )
    if len(value) > 512:
        raise IgSettingsError("Auto-reply URL is longer than 512 characters.")
    return value
 
 
def _ig_placeholders_in(template):
    out, depth, buf = [], 0, ""
    for ch in template:
        if ch == "{":
            depth, buf = depth + 1, ""
        elif ch == "}" and depth:
            depth -= 1
            out.append(buf.strip())
        elif depth:
            buf += ch
    return out
 
 
def _ig_clean_options(value, label):
    if value in (None, ""):
        return []
    if not isinstance(value, list):
        raise IgSettingsError(f"{label} must be a list of message templates.")
    if len(value) > IG_MAX_OPTIONS:
        raise IgSettingsError(f"{label} allows at most {IG_MAX_OPTIONS} messages.")
 
    cleaned = []
    for raw in value:
        item = str(raw).strip()
        if not item:
            continue
        if len(item) > IG_MAX_OPTION_LEN:
            raise IgSettingsError(
                f"{label} message is longer than {IG_MAX_OPTION_LEN} characters."
            )
        for token in _ig_placeholders_in(item):
            if token not in IG_ALLOWED_PLACEHOLDERS:
                raise IgSettingsError(
                    f"{label} uses unknown placeholder {{{token}}}. Available: "
                    f"{', '.join(sorted(IG_ALLOWED_PLACEHOLDERS))}."
                )
        cleaned.append(item)
    return cleaned
 
 
def _ig_build_record(payload, updated_by):
    media_id = str(payload.get("media_id") or "").strip()
    if not media_id:
        raise IgSettingsError("media_id is required.")
 
    enabled = bool(payload.get("auto_reply_enabled"))
    url = _ig_clean_url(payload.get("auto_reply_url"))
    comments = _ig_clean_options(payload.get("auto_reply_options"), "Comment replies")
    dms = _ig_clean_options(payload.get("auto_reply_dm_options"), "DM messages")
 
    if enabled and not comments and not dms:
        raise IgSettingsError(
            "Add at least one comment reply or DM message before turning "
            "auto-reply on."
        )
    if enabled and dms and not url:
        raise IgSettingsError("DM messages need an auto-reply URL for the button.")
 
    return {
        "media_id": media_id,
        "ig_user_id": payload.get("ig_user_id") or None,
        "permalink": payload.get("permalink") or None,
        "auto_reply_enabled": 1 if enabled else 0,
        "auto_reply_url": url,
        "auto_reply_options": json.dumps(comments),
        "auto_reply_dm_options": json.dumps(dms),
        "updated_by": updated_by,
    }

def _ig_normalize_permalink(value: str) -> str:
    """
    Normalize Instagram post/Reel URLs so query strings and trailing slashes
    do not prevent matching.
    """
    value = str(value or "").strip()

    if not value:
        raise ValueError("url is required")

    parsed = urlparse(value)

    if parsed.scheme not in {"http", "https"}:
        raise ValueError("url must begin with http:// or https://")

    hostname = (parsed.hostname or "").lower()
    if hostname not in {"instagram.com", "www.instagram.com"}:
        raise ValueError("url must be an instagram.com URL")

    path = re.sub(r"/+", "/", parsed.path).rstrip("/")

    # Supported Instagram permalink forms:
    # /p/{shortcode}, /reel/{shortcode}, /tv/{shortcode}
    match = re.fullmatch(r"/(p|reel|tv)/([A-Za-z0-9_-]+)", path)
    if not match:
        raise ValueError(
            "Unsupported Instagram URL. Expected /p/{shortcode}, "
            "/reel/{shortcode}, or /tv/{shortcode}."
        )

    content_type, shortcode = match.groups()
    return f"https://www.instagram.com/{content_type}/{shortcode}/"


def _ig_graph_get(path_or_url: str, params=None) -> dict:
    """
    Make one authenticated Instagram Graph API GET request.
    Accepts either a Graph API path or a complete paging URL.
    """
    params = dict(params or {})

    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        url = path_or_url
    else:
        url = f"{FB_GRAPH}/{path_or_url.lstrip('/')}"
        params.setdefault("access_token", FB_LL_USER_TOKEN)

    response = requests.get(
        url,
        params=params,
        timeout=IG_GRAPH_TIMEOUT,
    )

    try:
        body = response.json()
    except ValueError:
        raise RuntimeError(
            f"Instagram returned non-JSON response: HTTP {response.status_code}"
        )

    if response.status_code >= 400 or "error" in body:
        error = body.get("error") or {}
        message = error.get("message") or response.text
        code = error.get("code")
        subcode = error.get("error_subcode")

        raise RuntimeError(
            f"Instagram API error"
            f"{f' {code}' if code else ''}"
            f"{f'/{subcode}' if subcode else ''}: {message}"
        )

    return body


def _ig_find_media_by_permalink(permalink: str, max_pages: int = 100) -> dict:
    """
    Search the connected Instagram professional account's media and match
    the supplied permalink.
    """
    target = _ig_normalize_permalink(permalink)

    body = _ig_graph_get(
        f"{_ig_resolve_user_id()}/media",
        {
            "fields": IG_MEDIA_FIELDS,
            "limit": IG_MAX_LIMIT,
        },
    )

    pages_checked = 0

    while body and pages_checked < max_pages:
        pages_checked += 1

        for media in body.get("data", []):
            media_permalink = media.get("permalink")

            if not media_permalink:
                continue

            try:
                normalized_media_permalink = _ig_normalize_permalink(
                    media_permalink
                )
            except ValueError:
                normalized_media_permalink = media_permalink.split("?")[0]

            if normalized_media_permalink == target:
                return media

        next_url = (body.get("paging") or {}).get("next")
        if not next_url:
            break

        body = _ig_graph_get(next_url)

    raise LookupError(
        "The post was not found among media owned by the connected "
        "Instagram professional account."
    )


def _ig_fetch_paginated_edge(
    object_id: str,
    edge: str,
    fields: str,
    page_limit: int = 50,
    max_items: int | None = None,
) -> list:
    """
    Retrieve every available item from a paginated Graph API edge.
    """
    body = _ig_graph_get(
        f"{object_id}/{edge}",
        {
            "fields": fields,
            "limit": max(1, min(int(page_limit), 100)),
        },
    )

    items = []

    while body:
        page_items = body.get("data", [])
        items.extend(page_items)

        if max_items is not None and len(items) >= max_items:
            return items[:max_items]

        next_url = (body.get("paging") or {}).get("next")
        if not next_url:
            break

        body = _ig_graph_get(next_url)

    return items


def _ig_fetch_comments(
    media_id: str,
    include_replies: bool = True,
    max_comments: int | None = None,
) -> list:
    comment_fields = ",".join([
        "id",
        "text",
        "username",
        "timestamp",
        "like_count",
        "hidden",
    ])

    comments = _ig_fetch_paginated_edge(
        object_id=media_id,
        edge="comments",
        fields=comment_fields,
        max_items=max_comments,
    )

    if not include_replies:
        return comments

    for comment in comments:
        comment_id = comment.get("id")
        if not comment_id:
            comment["replies"] = []
            continue

        try:
            comment["replies"] = _ig_fetch_paginated_edge(
                object_id=comment_id,
                edge="replies",
                fields=comment_fields,
            )
        except Exception as error:
            logging.warning(
                "Could not load Instagram replies for comment %s: %s",
                comment_id,
                error,
            )
            comment["replies"] = []
            comment["replies_error"] = str(error)

    return comments


@app.route("/api/v2/prism/media/ig/comments", methods=["POST"])
async def ig_comments_by_url():
    """
    Retrieve comments from an Instagram post or Reel owned by the connected
    Instagram professional account.

    Request:
    {
        "url": "https://www.instagram.com/p/DbtCL1nJLa8/",
        "include_replies": true,
        "max_comments": null
    }
    """
    auth_token = request.headers.get("Authorization")

    if auth_token != SA_AUTHENTICATION:
        return {"error": "Unauthorized"}, 403

    payload = await request.get_json(silent=True) or {}

    instagram_url = payload.get("url")
    include_replies = bool(payload.get("include_replies", True))
    max_comments = payload.get("max_comments")

    if max_comments is not None:
        try:
            max_comments = int(max_comments)
        except (TypeError, ValueError):
            return {"error": "max_comments must be an integer or null"}, 400

        if max_comments < 1 or max_comments > 10000:
            return {
                "error": "max_comments must be between 1 and 10000"
            }, 400

    try:
        normalized_url = _ig_normalize_permalink(instagram_url)

        media = _ig_find_media_by_permalink(normalized_url)

        comments = _ig_fetch_comments(
            media_id=str(media["id"]),
            include_replies=include_replies,
            max_comments=max_comments,
        )

        reply_count = sum(
            len(comment.get("replies", []))
            for comment in comments
        )

        return {
            "ok": True,
            "url": normalized_url,
            "media": media,
            "media_id": str(media["id"]),
            "top_level_comment_count": len(comments),
            "reply_count": reply_count,
            "total_comment_records": len(comments) + reply_count,
            "comments": comments,
        }, 200

    except ValueError as error:
        return {"ok": False, "error": str(error)}, 400

    except LookupError as error:
        return {"ok": False, "error": str(error)}, 404

    except RuntimeError as error:
        logging.exception("Instagram Graph API request failed")
        return {
            "ok": False,
            "error": "instagram_api_error",
            "message": str(error),
        }, 502

    except Exception as error:
        logging.exception("Failed to retrieve Instagram comments")
        return {
            "ok": False,
            "error": "internal_server_error",
            "message": str(error),
        }, 500

@app.route('/api/v1/prism/media/ig/posts', methods=['GET'])
async def ig_posts_list():
    auth_token = request.headers.get('Authorization')
    if auth_token != SA_AUTHENTICATION:
        return "Unauthorized", 403
 
    try:
        limit = int(request.args.get("limit", IG_DEFAULT_LIMIT))
    except ValueError:
        limit = IG_DEFAULT_LIMIT
    limit = max(1, min(limit, IG_MAX_LIMIT))
 
    after = request.args.get("after") or None
    before = request.args.get("before") or None
 
    try:
        # requests is blocking. Without to_thread this parks the event loop
        # for the whole Graph round trip -- same problem the ig_media webhook
        # already has.
        page = await asyncio.to_thread(_ig_fetch_media_page, limit, after, before)
    except Exception as e:
        logging.exception("IG media list failed")
        return {"posts": [], "paging": {}, "error": str(e)}, 502
 
    media = page.get("data", []) or []
    ids = [str(m["id"]) for m in media if m.get("id")]
    settings = await asyncio.to_thread(_ig_settings_load, ids)
 
    blank = {
        "auto_reply_enabled": False,
        "auto_reply_url": "",
        "auto_reply_options": [],
        "auto_reply_dm_options": [],
        "updated_at": "",
        "updated_by": "",
    }
 
    posts = []
    for m in media:
        mid = str(m.get("id"))
        s = settings.get(mid, blank)
        posts.append({
            "media_id": mid,
            "caption": m.get("caption") or "",
            "media_type": m.get("media_type") or "",
            "media_product_type": m.get("media_product_type") or "",
            "thumbnail_url": m.get("thumbnail_url") or m.get("media_url") or "",
            "permalink": m.get("permalink") or "",
            "timestamp": m.get("timestamp") or "",
            "comments_count": m.get("comments_count") or 0,
            "like_count": m.get("like_count") or 0,
            "configured": mid in settings,
            **{k: v for k, v in s.items() if k != "media_id"},
        })
 
    cursors = (page.get("paging") or {}).get("cursors") or {}
    return {
        "posts": posts,
        "paging": {
            "after": cursors.get("after"),
            "before": cursors.get("before"),
            "has_next": bool((page.get("paging") or {}).get("next")),
            "has_previous": bool((page.get("paging") or {}).get("previous")),
            "limit": limit,
        },
        "error": None,
    }, 200
 
 
@app.route('/api/v1/prism/media/ig/posts/settings', methods=['POST'])
async def ig_posts_settings_save():
    auth_token = request.headers.get('Authorization')
    if auth_token != SA_AUTHENTICATION:
        return "Unauthorized", 403
 
    payload = await request.get_json(silent=True) or {}
    updated_by = request.headers.get("X-Prism-User", "dashboard")
 
    try:
        record = _ig_build_record(payload, updated_by)
    except IgSettingsError as e:
        return {"saved": False, "error": str(e)}, 400
 
    try:
        await asyncio.to_thread(_ig_settings_save, record)
    except Exception as e:
        logging.exception("IG settings save failed for %s", payload.get("media_id"))
        return {"saved": False, "error": str(e)}, 500
 
    return {"saved": True, "media_id": record["media_id"], "error": None}, 200
 
 
@app.route('/api/v1/prism/media/ig/posts/settings/bulk', methods=['POST'])
async def ig_posts_settings_save_bulk():
    auth_token = request.headers.get('Authorization')
    if auth_token != SA_AUTHENTICATION:
        return "Unauthorized", 403
 
    payload = await request.get_json(silent=True) or {}
    items = payload.get("posts") or []
    updated_by = request.headers.get("X-Prism-User", "dashboard")
 
    saved, failed = [], []
    for item in items:
        try:
            record = _ig_build_record(item, updated_by)
            await asyncio.to_thread(_ig_settings_save, record)
            saved.append(record["media_id"])
        except IgSettingsError as e:
            failed.append({"media_id": item.get("media_id"), "error": str(e)})
        except Exception as e:
            logging.exception("IG bulk save failed for %s", item.get("media_id"))
            failed.append({"media_id": item.get("media_id"), "error": str(e)})
 
    return {"saved": saved, "failed": failed, "error": None}, 200
 
 
# ---------------------------------------------------------------------------
# WEBHOOK HELPERS
# ---------------------------------------------------------------------------
 
_ig_settings_cache = {}
 
 
def ig_get_media_settings(media_id):
    """
    Single-post lookup for the comment webhook. Point read on the unique index,
    cached IG_SETTINGS_TTL seconds so a burst of comments on one post doesn't
    hammer the DB. Cache is per-worker, so a dashboard save can take up to a
    minute to reach every Cloud Run instance. Returns None if never configured.
    """
    media_id = str(media_id)
    hit = _ig_settings_cache.get(media_id)
    if hit and time.time() - hit["at"] < IG_SETTINGS_TTL:
        return hit["value"]
 
    value = _ig_settings_load_sql([media_id]).get(media_id)
    _ig_settings_cache[media_id] = {"value": value, "at": time.time()}
    return value
 
 
def ig_render_option(options, offer_data, username="", fallback=None):
    """Pick a variant and fill placeholders. Mirrors the webhook's N/A -> car."""
    pool = options or fallback or []
    if not pool:
        return ""
 
    offer_data = offer_data or {}
    make = offer_data.get("make") or "N/A"
    car = "car" if make in ("N/A", "", None) else make
 
    values = {
        "year": "" if offer_data.get("year") in ("N/A", None) else offer_data.get("year", ""),
        "make": car,
        "model": "" if offer_data.get("model") in ("N/A", None) else offer_data.get("model", ""),
        "car": car,
        "username": username or "",
    }
 
    chosen = random.choice(pool)
    for key, val in values.items():
        chosen = chosen.replace("{" + key + "}", str(val))
    return " ".join(chosen.split()).strip()


# ---------------------------------------------------------------------------
# INSTAGRAM COMMENT LEAD ALERTS
# ---------------------------------------------------------------------------

IG_ALERT_USER_ID = "1b13bab3-3320-f599-24d7-5b16b798befa"
IG_ALERT_DELAY_SECONDS = 300
IG_EXISTING_LEAD_LOOKBACK_DAYS = 30
IG_EXISTING_LEAD_LIMIT = 500

ALERTABLE_IG_LEAD_TYPES = {
    "explicit_sale",
    "possible_sale",
    "vehicle_offered",
    "collection_or_bulk",
    "already_contacted",
    "third_party_referral",
}

# Keep references to background tasks so Python does not garbage-collect them.
_ig_background_tasks = set()


def _start_ig_background_task(coroutine):
    task = asyncio.create_task(coroutine)
    _ig_background_tasks.add(task)
    task.add_done_callback(_ig_background_tasks.discard)
    return task


def _extract_json_object(value):
    """
    Extract one JSON object from an AI response.

    Supports:
    - Python dictionaries
    - Plain JSON
    - JSON inside markdown code fences
    - JSON surrounded by incidental text
    """
    if isinstance(value, dict):
        return value

    raw = str(value or "").strip()

    if raw.startswith("```"):
        raw = re.sub(
            r"^```(?:json)?\s*",
            "",
            raw,
            flags=re.IGNORECASE,
        )
        raw = re.sub(r"\s*```$", "", raw)

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    start = raw.find("{")
    end = raw.rfind("}")

    if start >= 0 and end > start:
        return json.loads(raw[start:end + 1])

    raise ValueError(
        "AI response did not contain a valid JSON object"
    )


def _normalize_ai_response(response_body):
    """
    Extract assistant text from the response returned by
    sale.andrew-chat.com.
    """
    if isinstance(response_body, list) and response_body:
        first = response_body[0]

        if isinstance(first, dict):
            return (
                first.get("text")
                or first.get("content")
                or first.get("response")
                or json.dumps(first)
            )

        return str(first)

    if isinstance(response_body, dict):
        return (
            response_body.get("text")
            or response_body.get("content")
            or response_body.get("response")
            or json.dumps(response_body)
        )

    return str(response_body or "")


def _clean_alert_value(value, default="Not provided"):
    if value in (None, "", [], {}):
        return default

    if isinstance(value, list):
        cleaned = [
            str(item).strip()
            for item in value
            if item not in (None, "")
        ]
        return ", ".join(cleaned) if cleaned else default

    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)

    return str(value).strip() or default


def _create_sale_ai_session(user_id):
    """
    Create or retrieve a session from sale.andrew-chat.com.
    """
    session_response = requests.put(
        f"https://sale.andrew-chat.com/api/v1/session/{user_id}",
        headers={"Authorization": SA_AUTHENTICATION},
        timeout=20,
    )
    session_response.raise_for_status()

    session_body = session_response.json()
    sessions = session_body.get("sessions") or []

    if sessions:
        return str(sessions[0]["id"])

    get_response = requests.get(
        f"https://sale.andrew-chat.com/api/v1/session/{user_id}",
        headers={"Authorization": SA_AUTHENTICATION},
        timeout=20,
    )
    get_response.raise_for_status()

    get_body = get_response.json()
    session_id = get_body.get("session_id")

    if not session_id:
        raise RuntimeError(
            "sale.andrew-chat.com did not return a session_id"
        )

    return str(session_id)


def _close_sale_ai_session(session_id):
    """
    Best-effort cleanup for sale.andrew-chat.com sessions.
    """
    try:
        response = requests.post(
            "https://sale.andrew-chat.com/api/v1/session/"
            "prism_suggestions",
            json={"session_id": session_id},
            headers={"Authorization": SA_AUTHENTICATION},
            timeout=20,
        )

        logging.info(
            "Closed sale AI session %s with status %s",
            session_id,
            response.status_code,
        )

    except Exception:
        logging.exception(
            "Could not close sale AI session %s",
            session_id,
        )


def _call_sale_ai_json(prompt, user_id, timeout=90):
    """
    Send a prompt to sale.andrew-chat.com and require a JSON response.
    """
    session_id = _create_sale_ai_session(user_id)

    try:
        response = requests.post(
            "https://sale.andrew-chat.com/api/v1/prism",
            json={
                "action": "create",
                "message": prompt,
                "session_id": session_id,
                "user_id": user_id,
            },
            headers={"Authorization": SA_AUTHENTICATION},
            timeout=timeout,
        )
        response.raise_for_status()

        response_body = response.json()
        response_text = _normalize_ai_response(response_body)

        return _extract_json_object(response_text)

    finally:
        _close_sale_ai_session(session_id)


def _validate_lead_classification(result):
    """
    Validate and normalize the Instagram comment classification.
    """
    if not isinstance(result, dict):
        raise ValueError(
            "Lead classification must be a JSON object"
        )

    if not isinstance(result.get("is_potential_lead"), bool):
        raise ValueError(
            "is_potential_lead must be true or false"
        )

    confidence = str(
        result.get("confidence") or ""
    ).strip().lower()

    if confidence not in {"high", "medium", "low"}:
        raise ValueError(
            "confidence must be high, medium, or low"
        )

    valid_lead_types = {
        "explicit_sale",
        "possible_sale",
        "vehicle_offered",
        "collection_or_bulk",
        "already_contacted",
        "third_party_referral",
        "restoration_only",
        "employment_request",
        "parts_only",
        "general_engagement",
        "not_a_lead",
    }

    lead_type = str(
        result.get("lead_type") or ""
    ).strip().lower()

    if lead_type not in valid_lead_types:
        raise ValueError(
            f"Invalid lead_type: {lead_type}"
        )

    vehicle = result.get("vehicle")

    if not isinstance(vehicle, dict):
        vehicle = {}

    other_vehicles = result.get("other_vehicles")

    if not isinstance(other_vehicles, list):
        other_vehicles = []

    result["confidence"] = confidence
    result["lead_type"] = lead_type
    result["vehicle"] = vehicle
    result["other_vehicles"] = other_vehicles

    return result


def _validate_duplicate_result(result):
    """
    Validate and normalize the CRM duplicate-check response.
    """
    if not isinstance(result, dict):
        raise ValueError(
            "Duplicate-check result must be a JSON object"
        )

    if not isinstance(result.get("existing_lead_found"), bool):
        raise ValueError(
            "existing_lead_found must be true or false"
        )

    confidence = str(
        result.get("confidence") or ""
    ).strip().lower()

    if confidence not in {"high", "medium", "low"}:
        raise ValueError(
            "Duplicate confidence must be high, medium, or low"
        )

    matching_evidence = result.get("matching_evidence")

    if not isinstance(matching_evidence, list):
        matching_evidence = []

    result["confidence"] = confidence
    result["matching_evidence"] = matching_evidence

    return result


def _is_alertable_instagram_lead(lead_data):
    """
    Deterministic final gate.

    An alert is only allowed when:
    - The model explicitly classified the comment as a potential lead.
    - The lead type represents a possible vehicle acquisition.
    - Confidence is high or medium.
    - The explanation does not clearly describe non-lead engagement.
    """
    if not isinstance(lead_data, dict):
        return False

    if lead_data.get("is_potential_lead") is not True:
        return False

    lead_type = str(
        lead_data.get("lead_type") or ""
    ).strip().lower()

    if lead_type not in ALERTABLE_IG_LEAD_TYPES:
        return False

    confidence = str(
        lead_data.get("confidence") or ""
    ).strip().lower()

    if confidence not in {"high", "medium"}:
        return False

    reason = str(
        lead_data.get("reason") or ""
    ).strip().lower()

    seller_intent = str(
        lead_data.get("seller_intent") or ""
    ).strip().lower()

    blocked_signals = {
        "not for sale",
        "not selling",
        "restoration only",
        "restoration request",
        "employment request",
        "job request",
        "parts only",
        "compliment",
        "general engagement",
        "ordinary engagement",
        "not a lead",
        "unrelated",
        "giveaway request",
        "wants vehicle restored",
        "wants help locating",
        "no longer owns",
    }

    combined_text = f"{reason} {seller_intent}"

    if any(
        signal in combined_text
        for signal in blocked_signals
    ):
        return False

    return True


def _build_instagram_classification_prompt(
    username,
    comment_text,
    comment_id,
    post_data,
):
    """
    Build the first AI prompt.

    This agent only classifies the comment and extracts facts.
    """
    caption = str(post_data.get("caption") or "")
    permalink = str(post_data.get("permalink") or "")
    media_type = str(
        post_data.get("media_product_type")
        or post_data.get("media_type")
        or ""
    )

    return f"""
You are the Instagram vehicle-lead classification agent for an automotive
purchasing company.

YOUR ONLY TASK

Analyze one Instagram comment and determine whether it represents a genuine
potential opportunity to acquire a complete vehicle or vehicle collection.

You are not replying to the Instagram commenter.
You are not writing a public comment.
You are not sending a direct message.
You are not creating an alert.
You are not checking the CRM.
You are not creating or modifying a lead.
You are only classifying the comment and extracting facts.

INSTAGRAM POST CONTEXT

Post caption:
{caption}

Post URL:
{permalink}

Media type:
{media_type}

COMMENT CONTEXT

Instagram username:
@{username}

Instagram comment ID:
{comment_id}

Comment text:
{comment_text}

STRICT POTENTIAL-LEAD STANDARD

Set is_potential_lead to true only when there is a reasonable indication
that the commenter can provide access to a complete vehicle or collection
that may be available for purchase.

Potential leads include:

1. The commenter explicitly says a vehicle is for sale.
2. The commenter says they need, want, might, or are considering selling.
3. The commenter offers a vehicle, project vehicle, collection, or group of
   vehicles.
4. The commenter provides vehicle information in direct response to a post
   asking people to submit vehicles.
5. The commenter asks whether the company is interested in purchasing their
   vehicle.
6. The commenter says they already sent a DM, submitted a form, or made an
   inquiry regarding a vehicle.
7. The commenter refers to a family member, employer, customer, or other
   owner and appears able to connect the company to that owner.
8. The commenter offers pictures, additional vehicle information, or asks
   the company to contact them about the vehicle.
9. The commenter provides a detailed vehicle or collection description that
   reasonably indicates availability, even if they do not explicitly use
   the phrase "for sale."

DO NOT CLASSIFY REGULAR ENGAGEMENT AS A LEAD

Set is_potential_lead to false for:

1. Compliments, emojis, jokes, reactions, or ordinary conversation.
2. Someone merely naming a car they like.
3. Someone mentioning a car they own without any reasonable indication that
   it may be available.
4. Restoration, repair, customization, or build requests when the vehicle is
   not being offered for purchase.
5. Employment, job, networking, partnership, publicity, appearance, or visit
   requests.
6. Stories about former vehicles or vehicles the commenter no longer owns.
7. Requests to find, locate, replace, or recover a vehicle.
8. Parts-only offers.
9. Third-party marketplace sightings when the commenter does not represent
   the seller and cannot connect the owner.
10. General questions about what types of cars the company wants.
11. Questions or complaints about links, forms, bots, or automated replies.
12. Tags, mentions, usernames, or external links without a clear vehicle
    acquisition opportunity.
13. Comments explicitly stating that the vehicle is not for sale.
14. Comments whose primary intent is restoration rather than a sale.
15. Spam or unrelated engagement.

IMPORTANT CONSERVATIVE RULE

When uncertain whether a comment reflects genuine seller intent or ordinary
engagement, set:

"is_potential_lead": false

Do not create a lead classification merely because a vehicle year, make, or
model is mentioned.

EXTRACTION RULES

Extract only information explicitly present in the comment.

Do not invent or guess:

- Year
- Make
- Model
- Trim
- Mileage
- Price
- Location
- Condition
- Ownership history
- Modifications
- Sale intent

Use null when information was not provided.

If multiple vehicles are mentioned:

- Put the most clearly described or primary vehicle in "vehicle".
- Put the remaining vehicle descriptions in "other_vehicles".

Use "contact_action" for statements such as:

- "I sent a DM"
- "I filled out the form"
- "Contact me"
- "Let me know"
- "I can send pictures"
- "Check my profile"
- "I already inquired"

RESPONSE REQUIREMENTS

Return exactly one valid JSON object.

Do not return markdown.
Do not use a code fence.
Do not add introductory text.
Do not add text after the JSON.
Do not return multiple objects.
Do not omit required keys.

Use exactly this structure:

{{
  "is_potential_lead": true,
  "confidence": "high",
  "lead_type": "explicit_sale",
  "seller_intent": "Explicitly offering a vehicle for sale",
  "vehicle": {{
    "year": "1969",
    "make": "Dodge",
    "model": "Coronet R/T",
    "trim": "Convertible",
    "mileage": null,
    "price": null,
    "location": null,
    "condition": null,
    "ownership_history": null,
    "modifications": null
  }},
  "other_vehicles": [],
  "contact_action": null,
  "additional_details": null,
  "reason": "The commenter explicitly states that the vehicle is for sale."
}}

FIELD RULES

- is_potential_lead must be true or false.
- confidence must be exactly "high", "medium", or "low".
- lead_type must be exactly one of:
  - "explicit_sale"
  - "possible_sale"
  - "vehicle_offered"
  - "collection_or_bulk"
  - "already_contacted"
  - "third_party_referral"
  - "restoration_only"
  - "employment_request"
  - "parts_only"
  - "general_engagement"
  - "not_a_lead"
- reason must briefly explain the classification.
- Return JSON only.
""".strip()


def _classify_instagram_comment(
    username,
    comment_text,
    comment_id,
    post_data,
):
    prompt = _build_instagram_classification_prompt(
        username=username,
        comment_text=comment_text,
        comment_id=comment_id,
        post_data=post_data,
    )

    user_id = (
        f"ig_lead_classifier_{comment_id}_{int(time.time())}"
    )

    result = _call_sale_ai_json(
        prompt=prompt,
        user_id=user_id,
        timeout=90,
    )

    return _validate_lead_classification(result)


def _ig_get_recent_pb_leads():
    """
    Retrieve recent active PB leads.

    Adjust this query if your PB schema uses different lead table names.
    """
    cutoff = datetime.utcnow() - timedelta(
        days=IG_EXISTING_LEAD_LOOKBACK_DAYS
    )

    with pbengine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT
                    l.id AS lead_id,
                    l.first_name,
                    l.last_name,
                    l.account_name,
                    l.status,
                    l.lead_source,
                    l.description,
                    l.phone_work,
                    l.phone_mobile,
                    l.date_entered,
                    l.date_modified,
                    lc.*
                FROM leads AS l
                LEFT JOIN leads_cstm AS lc
                    ON lc.id_c = l.id
                WHERE l.deleted = 0
                  AND l.date_entered >= :cutoff
                ORDER BY l.date_entered DESC
                LIMIT 500
            """),
            {"cutoff": cutoff},
        ).mappings().all()

    results = []

    for row in rows:
        item = {}

        for key, value in dict(row).items():
            if value is None:
                continue

            if isinstance(value, datetime):
                value = value.isoformat()

            elif isinstance(value, bytes):
                value = value.decode(
                    "utf-8",
                    errors="replace",
                )

            elif not isinstance(
                value,
                (str, int, float, bool, list, dict),
            ):
                value = str(value)

            item[key] = value

        results.append(item)

    return results


def _build_duplicate_check_prompt(
    username,
    comment_text,
    comment_id,
    comment_timestamp,
    lead_data,
    recent_leads,
):
    """
    Build the CRM duplicate-detection prompt.
    """
    return f"""
You are the CRM duplicate-detection agent for an automotive purchasing
company.

YOUR ONLY TASK

Compare one Instagram vehicle prospect against recent CRM lead records and
determine whether the same prospect already exists in the CRM.

You are not creating a lead.
You are not updating a lead.
You are not deleting a lead.
You are not creating an alert.
You are not sending a message.
You are not replying to Instagram.
You are only returning a duplicate-match decision.

INSTAGRAM PROSPECT

Instagram username:
@{username}

Instagram comment ID:
{comment_id}

Instagram comment timestamp:
{comment_timestamp}

Original Instagram comment:
{comment_text}

Structured information extracted from the comment:
{json.dumps(lead_data, ensure_ascii=False, default=str)}

RECENT CRM LEADS

{json.dumps(recent_leads, ensure_ascii=False, default=str)}

MATCHING STANDARD

Return existing_lead_found as true only when there is convincing evidence
that a CRM record represents the same Instagram commenter, person, vehicle,
or vehicle collection.

Strong evidence includes:

1. The exact Instagram username appears in the CRM lead.
2. The same Instagram comment ID or post URL appears in the CRM.
3. The same phone number or email appears in both sources.
4. The same VIN appears in both sources.
5. A distinctive combination of year, make, model, trim, color, mileage,
   price, location, condition, modifications, ownership history, or
   provenance matches.
6. Multiple vehicles or a collection description match closely.
7. A CRM lead was created shortly after the Instagram comment and contains
   the same distinctive vehicle information.
8. The CRM description explicitly references the Instagram username,
   comment, post, or source.

WEAK EVIDENCE IS NOT ENOUGH

Do not mark a duplicate based only on:

- A common vehicle year
- A common make or model
- A broad vehicle category
- A vague description
- A matching date with no other evidence
- A general location without matching vehicle details

Common vehicles such as these are not enough by themselves:

- 1969 Camaro
- 1972 Chevrolet C10
- Ford Mustang
- Chevrolet Corvette
- Jeep Wrangler
- Ford F-100

CONSERVATIVE DECISION RULE

When uncertain, return:

"existing_lead_found": false

Only suppress the alert when the evidence reasonably identifies the same
prospect.

RESPONSE REQUIREMENTS

Return exactly one valid JSON object.

Do not return markdown.
Do not use a code fence.
Do not add introductory text.
Do not add text after the JSON.
Do not return multiple objects.
Do not invent a CRM lead ID.

When a match exists, matched_lead_id must exactly equal a lead_id from the
provided CRM records.

Use exactly this structure:

{{
  "existing_lead_found": false,
  "confidence": "high",
  "matched_lead_id": null,
  "matched_lead_name": null,
  "matching_evidence": [],
  "reason": "No CRM lead had sufficiently strong matching evidence."
}}

FIELD RULES

- existing_lead_found must be true or false.
- confidence must be exactly "high", "medium", or "low".
- matched_lead_id must be null or an exact CRM lead_id.
- matched_lead_name must be null or the matching CRM name.
- matching_evidence must be a JSON array.
- reason must briefly explain the decision.
- Return JSON only.
""".strip()


def _ig_ai_check_existing_lead(
    username,
    comment_text,
    comment_id,
    comment_timestamp,
    lead_data,
    recent_leads,
):
    prompt = _build_duplicate_check_prompt(
        username=username,
        comment_text=comment_text,
        comment_id=comment_id,
        comment_timestamp=comment_timestamp,
        lead_data=lead_data,
        recent_leads=recent_leads,
    )

    user_id = (
        f"ig_duplicate_checker_{comment_id}_{int(time.time())}"
    )

    result = _call_sale_ai_json(
        prompt=prompt,
        user_id=user_id,
        timeout=120,
    )

    return _validate_duplicate_result(result)


def _build_instagram_alert_description(
    username,
    comment_text,
    comment_id,
    comment_timestamp,
    media_permalink,
    lead_data,
    duplicate_result,
):
    vehicle = lead_data.get("vehicle") or {}

    lines = [
        f"Instagram user: @{username}",
        f"Comment ID: {comment_id}",
        f"Comment timestamp: {_clean_alert_value(comment_timestamp)}",
        f"Lead confidence: {_clean_alert_value(lead_data.get('confidence'))}",
        f"Lead type: {_clean_alert_value(lead_data.get('lead_type'))}",
        f"Seller intent: {_clean_alert_value(lead_data.get('seller_intent'))}",
        "",
        "Original comment:",
        comment_text or "(empty comment)",
        "",
        "Information already provided:",
        f"Year: {_clean_alert_value(vehicle.get('year'))}",
        f"Make: {_clean_alert_value(vehicle.get('make'))}",
        f"Model: {_clean_alert_value(vehicle.get('model'))}",
        f"Trim: {_clean_alert_value(vehicle.get('trim'))}",
        f"Mileage: {_clean_alert_value(vehicle.get('mileage'))}",
        f"Price: {_clean_alert_value(vehicle.get('price'))}",
        f"Location: {_clean_alert_value(vehicle.get('location'))}",
        f"Condition: {_clean_alert_value(vehicle.get('condition'))}",
        (
            "Ownership/history: "
            f"{_clean_alert_value(vehicle.get('ownership_history'))}"
        ),
        (
            "Modifications: "
            f"{_clean_alert_value(vehicle.get('modifications'))}"
        ),
        (
            "Other vehicles: "
            f"{_clean_alert_value(lead_data.get('other_vehicles'))}"
        ),
        (
            "Contact action: "
            f"{_clean_alert_value(lead_data.get('contact_action'))}"
        ),
        (
            "Additional details: "
            f"{_clean_alert_value(lead_data.get('additional_details'))}"
        ),
        "",
        "CRM duplicate check:",
        "Existing lead found: False",
        (
            "Duplicate-check confidence: "
            f"{_clean_alert_value(duplicate_result.get('confidence'))}"
        ),
        (
            "Duplicate-check reason: "
            f"{_clean_alert_value(duplicate_result.get('reason'))}"
        ),
        "",
        f"Instagram post: {media_permalink}",
    ]

    return "\n".join(lines)


def _create_instagram_lead_alert(
    username,
    comment_text,
    comment_id,
    comment_timestamp,
    media_permalink,
    lead_data,
    duplicate_result,
):
    """
    Create one PB alert for an alertable potential lead.
    """
    if not _is_alertable_instagram_lead(lead_data):
        logging.info(
            "Alert creation blocked for comment %s because it is not "
            "an alertable vehicle lead",
            comment_id,
        )

        return {
            "status": "blocked",
            "reason": "not_an_alertable_vehicle_lead",
        }

    media_permalink = str(
        media_permalink
        or "https://www.instagram.com"
    ).rstrip("/")

    comment_url = (
        f"{media_permalink}/c/{comment_id}/"
    )

    alert_title = (
        f"Instagram Post Potential Lead - @{username}"
    )

    alert_description = _build_instagram_alert_description(
        username=username,
        comment_text=comment_text,
        comment_id=comment_id,
        comment_timestamp=comment_timestamp,
        media_permalink=media_permalink,
        lead_data=lead_data,
        duplicate_result=duplicate_result,
    )

    alert_id = str(uuid.uuid4())

    with pbengine.connect() as conn:
        existing_alert = conn.execute(
            text("""
                SELECT id
                FROM alerts
                WHERE assigned_user_id = :assigned_user_id
                  AND url_redirect = :url_redirect
                  AND deleted = '0'
                LIMIT 1
            """),
            {
                "assigned_user_id": IG_ALERT_USER_ID,
                "url_redirect": comment_url,
            },
        ).mappings().first()

        if existing_alert:
            logging.info(
                "Instagram alert already exists for comment %s: %s",
                comment_id,
                existing_alert["id"],
            )

            return {
                "status": "duplicate_alert",
                "alert_id": str(existing_alert["id"]),
            }

        conn.execute(
            text("""
                INSERT INTO alerts (
                    id,
                    name,
                    description,
                    target_module,
                    url_redirect,
                    date_entered,
                    assigned_user_id,
                    is_read,
                    deleted
                )
                VALUES (
                    :id,
                    :name,
                    :description,
                    :target_module,
                    :url_redirect,
                    NOW(),
                    :assigned_user_id,
                    '0',
                    '0'
                )
            """),
            {
                "id": alert_id,
                "name": alert_title,
                "description": alert_description,
                "target_module": "Ava Alerts",
                "url_redirect": comment_url,
                "assigned_user_id": IG_ALERT_USER_ID,
            },
        )

        conn.commit()

    logging.info(
        "Created Instagram lead alert %s for comment %s by @%s",
        alert_id,
        comment_id,
        username,
    )

    return {
        "status": "created",
        "alert_id": alert_id,
    }


async def _ig_delayed_lead_check_and_alert(
    username,
    comment_text,
    comment_id,
    comment_timestamp,
    media_permalink,
    lead_data,
):
    """
    Wait five minutes, check the PB database for an existing lead, and only
    create an alert when no matching lead exists.
    """
    try:
        # Second deterministic gate before waiting or checking the CRM.
        if not _is_alertable_instagram_lead(lead_data):
            logging.info(
                "Delayed processing blocked for Instagram comment %s "
                "because it is not an alertable potential lead",
                comment_id,
            )

            return {
                "status": "ignored",
                "reason": "not_an_alertable_vehicle_lead",
                "comment_id": comment_id,
            }

        logging.info(
            "Instagram potential lead %s queued for a %s-second delay",
            comment_id,
            IG_ALERT_DELAY_SECONDS,
        )

        await asyncio.sleep(IG_ALERT_DELAY_SECONDS)

        # Third deterministic gate after the delay.
        if not _is_alertable_instagram_lead(lead_data):
            return {
                "status": "ignored",
                "reason": "not_an_alertable_vehicle_lead",
                "comment_id": comment_id,
            }

        recent_leads = await asyncio.to_thread(
            _ig_get_recent_pb_leads
        )

        logging.info(
            "Comparing Instagram comment %s against %s recent PB leads",
            comment_id,
            len(recent_leads),
        )

        duplicate_result = await asyncio.to_thread(
            _ig_ai_check_existing_lead,
            username,
            comment_text,
            comment_id,
            comment_timestamp,
            lead_data,
            recent_leads,
        )

        logging.info(
            "Instagram duplicate-check result for comment %s: %s",
            comment_id,
            duplicate_result,
        )

        if duplicate_result.get("existing_lead_found") is True:
            logging.info(
                "No alert created for Instagram comment %s. "
                "Existing PB lead matched: %s",
                comment_id,
                duplicate_result.get("matched_lead_id"),
            )

            return {
                "status": "existing_lead_found",
                "comment_id": comment_id,
                "matched_lead_id": duplicate_result.get(
                    "matched_lead_id"
                ),
                "duplicate_check": duplicate_result,
            }

        alert_result = await asyncio.to_thread(
            _create_instagram_lead_alert,
            username,
            comment_text,
            comment_id,
            comment_timestamp,
            media_permalink,
            lead_data,
            duplicate_result,
        )

        logging.info(
            "Instagram delayed processing completed for comment %s: %s",
            comment_id,
            alert_result,
        )

        return {
            "status": "alert_processed",
            "comment_id": comment_id,
            "alert": alert_result,
            "duplicate_check": duplicate_result,
        }

    except Exception as error:
        logging.exception(
            "Delayed Instagram processing failed for comment %s",
            comment_id,
        )

        return {
            "status": "error",
            "comment_id": comment_id,
            "message": str(error),
        }


@app.route(
    "/api/v1/prism/media/ig",
    methods=["POST", "GET"],
)
async def ig_media():
    logging.info(
        "Received request at /api/v1/prism/media/ig method=%s",
        request.method,
    )

    # -----------------------------------------------------------------------
    # META WEBHOOK VERIFICATION
    # -----------------------------------------------------------------------

    if request.method == "GET":
        hub_mode = request.args.get("hub.mode", "")
        hub_verify_token = request.args.get(
            "hub.verify_token",
            "",
        )
        hub_challenge = request.args.get(
            "hub.challenge",
            "",
        )

        if (
            hub_mode == "subscribe"
            and hub_verify_token == FB_VERIFY_TOKEN
        ):
            return hub_challenge, 200

        return {
            "error": "Invalid verification token"
        }, 403

    # -----------------------------------------------------------------------
    # INSTAGRAM WEBHOOK EVENT
    # -----------------------------------------------------------------------

    data = await request.get_json(silent=True) or {}

    logging.info(
        "Instagram webhook payload: %s",
        data,
    )

    try:
        entries = data.get("entry") or []

        if not entries:
            return {
                "status": "ignored",
                "reason": "no_entries",
            }, 200

        queued_comments = []
        ignored_events = []

        for entry in entries:
            changes = entry.get("changes") or []

            if not changes:
                ignored_events.append({
                    "reason": "no_changes",
                })
                continue

            for change in changes:
                field = str(
                    change.get("field") or ""
                ).strip().lower()

                if field == "messages":
                    ignored_events.append({
                        "reason": "message_event",
                    })
                    continue

                change_value = change.get("value") or {}

                comment_id = str(
                    change_value.get("id") or ""
                ).strip()

                comment_text = str(
                    change_value.get("text") or ""
                ).strip()

                comment_timestamp = str(
                    change_value.get("timestamp") or ""
                ).strip()

                commenter = change_value.get("from") or {}

                username = str(
                    commenter.get("username")
                    or commenter.get("id")
                    or "unknown"
                ).strip()

                media = change_value.get("media") or {}

                media_id = str(
                    media.get("id") or ""
                ).strip()

                if not comment_id:
                    ignored_events.append({
                        "reason": "missing_comment_id",
                    })
                    continue

                if not media_id:
                    ignored_events.append({
                        "comment_id": comment_id,
                        "reason": "missing_media_id",
                    })
                    continue

                if not comment_text:
                    ignored_events.append({
                        "comment_id": comment_id,
                        "reason": "empty_comment",
                    })
                    continue

                # Ignore comments written by the owned account.
                if username.lower() in {
                    "johnclaywolfeshow",
                    "johnclaywolfshow",
                }:
                    ignored_events.append({
                        "comment_id": comment_id,
                        "reason": "account_comment",
                    })
                    continue

                media_response = await asyncio.to_thread(
                    requests.get,
                    f"{FB_GRAPH}/{media_id}",
                    params={
                        "fields": (
                            "id,caption,permalink,media_type,"
                            "media_product_type,timestamp"
                        ),
                        "access_token": FB_LL_USER_TOKEN,
                    },
                    timeout=IG_GRAPH_TIMEOUT,
                )

                try:
                    post_data = media_response.json()
                except ValueError:
                    post_data = {}

                if (
                    media_response.status_code >= 400
                    or "error" in post_data
                ):
                    logging.error(
                        "Could not retrieve Instagram media %s: %s",
                        media_id,
                        media_response.text,
                    )

                    ignored_events.append({
                        "comment_id": comment_id,
                        "reason": "media_lookup_failed",
                    })
                    continue

                lead_data = await asyncio.to_thread(
                    _classify_instagram_comment,
                    username,
                    comment_text,
                    comment_id,
                    post_data,
                )

                logging.info(
                    "Instagram classification for comment %s: %s",
                    comment_id,
                    lead_data,
                )

                # Hard gate: ordinary engagement never reaches delayed logic.
                if not _is_alertable_instagram_lead(lead_data):
                    ignored_events.append({
                        "comment_id": comment_id,
                        "username": username,
                        "reason": "not_an_alertable_vehicle_lead",
                        "classification": lead_data,
                    })
                    continue

                media_permalink = str(
                    post_data.get("permalink")
                    or "https://www.instagram.com"
                )

                _start_ig_background_task(
                    _ig_delayed_lead_check_and_alert(
                        username=username,
                        comment_text=comment_text,
                        comment_id=comment_id,
                        comment_timestamp=comment_timestamp,
                        media_permalink=media_permalink,
                        lead_data=lead_data,
                    )
                )

                queued_comments.append({
                    "comment_id": comment_id,
                    "username": username,
                    "delay_seconds": IG_ALERT_DELAY_SECONDS,
                    "classification": lead_data,
                })

        return {
            "status": "accepted",
            "queued_count": len(queued_comments),
            "queued_comments": queued_comments,
            "ignored_count": len(ignored_events),
            "ignored_events": ignored_events,
        }, 200

    except requests.RequestException as error:
        logging.exception(
            "Instagram webhook external request failed"
        )

        return {
            "status": "error",
            "error": "external_request_failed",
            "message": str(error),
        }, 502

    except (
        ValueError,
        KeyError,
        TypeError,
        json.JSONDecodeError,
    ) as error:
        logging.exception(
            "Instagram AI response could not be parsed"
        )

        return {
            "status": "error",
            "error": "ai_response_parse_failed",
            "message": str(error),
        }, 500

    except Exception as error:
        logging.exception(
            "Instagram webhook processing failed"
        )

        return {
            "status": "error",
            "error": "internal_server_error",
            "message": str(error),
        }, 500

@app.route("/api/v2/prism/media/ig", methods=['POST'])
async def ig_media_v2():
    """
    Get post context from Instagram by post ID.
    Expected JSON: {"post_id": "<media_id>"}
    Returns: {"caption": "<post_caption>", "post_id": "<media_id>"}
    """
    try:
        data = await request.get_json(silent=True) or {}
        post_id = data.get('post_id')
        
        if not post_id:
            logging.error("No post_id provided in request")
            return {"error": "post_id is required"}, 400
        
        logging.info(f"Fetching post context for post_id: {post_id}")
        
        post_data = requests.get(
            f"{FB_GRAPH}/{post_id}?fields=caption&access_token={FB_LL_USER_TOKEN}"
        ).json()
        
        if 'error' in post_data:
            logging.error(f"Error fetching post data: {post_data['error']}")
            return {"error": post_data['error'].get('message', 'Failed to fetch post')}, 400
        
        logging.info(f"Successfully fetched post context: {post_data}")
        
        return {
            "post_id": post_id,
            "caption": post_data.get('caption', ''),
            "post_data": post_data
        }, 200
    
    except Exception as e:
        logging.error(f"Exception in ig_media_v2: {e}")
        return {"error": f"Internal server error: {str(e)}"}, 500

#HEALTHCHECK
@app.route('/api/v1/prism', methods=['GET'])
@app.route('/api/v1/prism/healthcheck', methods=['GET'])
async def health():
  return "OK", 200


#PRISM-AVA API ENDPOINTS
from tools.ava_image_agents import (
    build_image_parts,
    classify_ava_image,
    collect_ava_photo_evidence,
    describe_prism_images,
)


def _ava_uploaded_files(form_files):

    if "images" in form_files:
        return form_files.getlist(
            "images"
        )

    single = (
        form_files.get("image")
        or form_files.get("file")
        or form_files.get("images")
    )

    return (
        [single]
        if single
        else []
    )


# ============================================================
# AVA IMAGE CLASSIFICATION
# ============================================================

@app.route(
    '/api/v1/prism/ava/image_upload',
    methods=['POST'],
)
async def ava_image_upload():

    try:

        form = await request.form

        image_urls = form.getlist(
            'image_urls'
        )

        try:

            form_files = await request.files

            files = _ava_uploaded_files(
                form_files
            )

        except Exception as e:

            logging.error(
                "Failed to load files from "
                "AVA image upload request: %s",
                e,
            )

            files = []


        image_parts = await build_image_parts(
            files,
            image_urls,
        )


        if not image_parts:

            return {
                "error":
                    "No image provided"
            }, 400


        image_process = (
            await classify_ava_image(
                image_parts
            )
        )


        return (
            image_process,
            200,
        )


    except ValidationError as e:

        logging.error(
            "AVA Gemini image classifier "
            "returned invalid JSON schema: %s",
            e,
        )

        return {
            "error":
                "Invalid model response"
        }, 502


    except json.JSONDecodeError as e:

        logging.error(
            "AVA Gemini image classifier "
            "returned non-JSON response: %s",
            e,
        )

        return {
            "error":
                "Invalid model response"
        }, 502


    except asyncio.TimeoutError:

        logging.error(
            "AVA Gemini image classifier timed out"
        )

        return {
            "error":
                "Image model timed out"
        }, 504


    except ValueError as e:

        logging.warning(
            "AVA image upload rejected: %s",
            e,
        )

        return {
            "error":
                str(e)
        }, 400


    except Exception as e:

        logging.exception(
            "Failed to process "
            "AVA image upload request: %s",
            e,
        )

        return (
            "Server error",
            500,
        )


# ============================================================
# GENERAL PRISM IMAGE DESCRIPTION
# ============================================================

@app.route(
    '/api/v1/prism/image',
    methods=['POST'],
)
async def prism_image():

    try:

        form = await request.form

        message = (
            form.get('message')
            or form.get('text')
            or ''
        )

        image_urls = form.getlist(
            'image_urls'
        )


        try:

            form_files = await request.files

            files = _ava_uploaded_files(
                form_files
            )

        except Exception as e:

            logging.error(
                "Failed to load files from "
                "Prism image request: %s",
                e,
            )

            files = []


        image_parts = await build_image_parts(
            files,
            image_urls,
        )


        if not image_parts:

            return {
                "error":
                    "No image provided"
            }, 400


        image_process = (
            await describe_prism_images(
                image_parts,
                message=message,
            )
        )


        # Preserve old API contract.
        return {
            "Image Processed Text":
                image_process
        }, 200


    except asyncio.TimeoutError:

        logging.error(
            "Prism Gemini image description timed out"
        )

        return {
            "error":
                "Image model timed out"
        }, 504


    except ValueError as e:

        logging.warning(
            "Prism image request rejected: %s",
            e,
        )

        return {
            "error":
                str(e)
        }, 400


    except Exception as e:

        logging.exception(
            "Failed to process Prism "
            "image request: %s",
            e,
        )

        return (
            "Server error",
            500,
        )
  
@app.route('/api/v1/prism/block/run_summary/<date>', methods=['GET'])
async def lane_summary(date):
  try:
    logging.info("Block run summary request received for date: %s", date)
    output = io.BytesIO()
    with pandas.ExcelWriter(output, engine='openpyxl') as writer:
      lanes = ['13', '20', '21', '22', '23', '24']
      for lane in lanes:
        block = block_get(lane, date)
        if len(block) ==  0:
          continue
        df = pandas.DataFrame(block)
        # Write the raw data
        sheet_name = f"Lane {lane} Report"
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.sheets[sheet_name]


        

        # Mapping Column Letters (Excel is 1-based index)
        col_map = {col: ws.cell(row=1, column=i + 1).column_letter for i, col in enumerate(df.columns)}

        # Find the indices for our logic
        cb_col = col_map.get('CB', 'K')
        cost_star_col = col_map.get('COST*', 'M')
        target_col = col_map.get('BYR TARGET', 'N')

        # 1. ADD ROW-LEVEL FORMULAS (Profit/Loss Logic)
        profit_col_idx = len(df.columns) + 1
        ws.cell(row=1, column=profit_col_idx, value="NET PROFIT/LOSS").font = Font(bold=True)

        for row_num in range(2, len(df) + 2):
          ws.cell(row=row_num, column=profit_col_idx, value=f"={cb_col}{row_num}-{cost_star_col}{row_num}")

        # 2. ADD SUMMARY FORMULAS (The footer from Lane 13)
        summary_row = len(df) + 3
        ws.cell(row=summary_row, column=1, value="LANE TOTALS:").font = Font(bold=True)

        # Sum of Hammer Prices (CB)
        ws.cell(
          row=summary_row,
          column=df.columns.get_loc('CB') + 1,
          value=f"=SUM({cb_col}2:{cb_col}{len(df) + 1})"
        ).font = Font(bold=True)

        # Sum of Targets
        ws.cell(
          row=summary_row,
          column=df.columns.get_loc('BYR TARGET') + 1,
          value=f"=SUM({target_col}2:{target_col}{len(df) + 1})"
        ).font = Font(bold=True)

        # 3. STYLING & FORMATTING
        # Bold headers
        for cell in ws[1]:
          cell.font = Font(bold=True)
          cell.fill = PatternFill(start_color="D3D3D3", end_color="D3D3D3", fill_type="solid")

        # Auto-adjust column width
        for col in ws.columns:
          ws.column_dimensions[col[0].column_letter].width = 22

    output.seek(0)

    # 4. Send the file back to the client
    return await send_file(
      output,
      mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      as_attachment=True,
      attachment_filename=f'Auction_Report_{date}.xlsx'
    )
  except Exception as e:
    logging.error("Failed to get block run summary, %s", e)
    return "Server error", 500

@app.route('/api/v1/prism/get_session/<user>/<agent_type>', methods=['GET'])
async def andrew_prism(user, agent_type):
  session_id = requests.put(f'https://{agent_type}.andrew-chat.com/api/v1/session/{user}', headers = {"Authorization": SA_AUTHENTICATION}).json()
  if(len(session_id['sessions']) == 0):
      session_id = requests.get(f'https://{agent_type}.andrew-chat.com/api/v1/session/{user}', headers = {"Authorization": SA_AUTHENTICATION}).json()['session_id']
      session_id = {
          'id': session_id,
          'chat_history': []
      }
  else:
    session_id = {'id' : session_id['sessions'][0]['id']}
    session_history = requests.get(f'https://{agent_type}.andrew-chat.com/api/v1/session/events/{session_id["id"]}', headers = {"Authorization": SA_AUTHENTICATION})
    if session_history.status_code == 200:
        session_history = session_history.json()
    else:
      session_history = {'events' : []}
    session_id['chat_history'] = session_history.get('events', [])
  return session_id, 200
   
@app.route('/api/v1/prism/get_token', methods=['GET'])
async def get_prism_token():
    token = request.args.get("token")
    if not token or not check_authentication(token):
      return "Unauthorized",403
    return SA_AUTHENTICATION, 200

@app.route('/api/v1/prism/get_user/<token>', methods=['GET'])
async def get_prism_user(token):
  user = check_authentication_user(token) if token else False
  if not user:
        return "Unauthorized", 403
  return user, 200

@app.route('/api/v1/prism/andrew/valuations', methods=['POST'])
async def andrew_valuations():
  authorization = request.headers['Authorization']
  if authorization != os.environ.get("SA_AUTHENTICATION"):
    return "Unauthorized", 403
  
  data = await request.get_json(silent=True)
  if data == None:
    return "Bad request", 400
  
  def andrew_helper(message: list, tools: list = []):

    response = gptclient.responses.create(
      model="gpt-5.4-mini",
      input=message,
      temperature=0.0,             # deterministic, direct replies
      top_p=1.0,
      tools=tools,
      store=False,
    )
    print(response)
    return response.output_text
  def parse_andrew_valuation(raw: str) -> dict:
      raw = (raw or "").strip()

      # Remove markdown fences like ```json ... ```
      raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
      raw = re.sub(r"\s*```$", "", raw)

      # Fallback: extract first JSON object
      if not raw.startswith("{"):
          match = re.search(r"\{.*\}", raw, flags=re.DOTALL)
          if match:
              raw = match.group(0)

      data = json.loads(raw)

      valuation = data.get("valuation")

      # Supports:
      # {"valuation": 24000, "text": "..."}
      # {"valuation": {"amount": 24000, ...}, "text": "..."}
      if isinstance(valuation, dict):
          valuation_number = valuation.get("amount")
      else:
          valuation_number = valuation

      return {
          "raw": data,
          "valuation_number": valuation_number,
          "valuation_text": data.get("text"),
      }


  async def _fetch_and_store_valuation(car_data: dict, car_id: int, token: str):
      print("Starting valuation")

      if "vin" in car_data:
        candidate_rows = get_valuation_search(
          "vin",
          car_data['vin'],
          100,
          select=["id"],
        )
        vin_batch = get_valuation_rows_by_ids(
          [row["id"] for row in candidate_rows],
        )
        today = datetime.utcnow().date().isoformat()
        todays_valuations = []
        for vin in vin_batch:
          if vin['job_date'].date() == datetime.fromisoformat(today).date() and vin['Andrew_stream'] is not None:
            logging.info(f"Valuation for VIN {car_data['vin']} already exists for today, skipping...")
            todays_valuations.append(vin)
        if todays_valuations:
          try:
            latest = max(todays_valuations, key=lambda v: datetime.fromisoformat(v.get('job_date')))
          except Exception:
            latest = max(todays_valuations, key=lambda v: v.get('job_date') or "")
          andrew_stream = json.loads(latest.get('Andrew_stream'))
          latest_summary = andrew_stream.get('final_valuation_summary','No summary')
          final = andrew_stream.get('final_valuation_number','No valuation')
          if final is not None:
            logging.info(f"Returning latest valuation for today: {final}")
            return {"valuation": final, "text": latest_summary}, 200

      try:
          async with aiohttp.ClientSession() as session:
              headers = {"Authorization": token}

              vin = car_data.get("VIN") or car_data.get("vin")
              if not vin:
                  print(f"No VIN found for car_id {car_id}")
                  return

              car_data["vin"] = vin

              async with session.post(
                  "https://prism.andrew-chat.com/api/v1/prism/manheim/data/mmr",
                  json={"car_details": car_data},
                  headers=headers,
                  timeout=aiohttp.ClientTimeout(total=600),
              ) as mmr_resp:
                  if mmr_resp.status != 200:
                      print(f"MMR fetch failed: {mmr_resp.status}")
                      return

                  mmr_data = await mmr_resp.json()

              current_mmr = mmr_data.get("current_mmr")['items']

              for item in current_mmr:
                 if item['bestMatch'] == True:
                    current_mmr = {"MMR With condition Mileage taken into account" : item["adjustedPricing"], "Average MMR no conditions" : item["wholesale"], "sampleSize" : item["sampleSize"]}
                    break
              

              async with session.post(
                  "https://prism.andrew-chat.com/api/v1/prism/sales/get",
                  json={
                        "make" : car_data['make'],
                        "model" : car_data['model']},
                  headers=headers,
                  timeout=aiohttp.ClientTimeout(total=600)
              ) as past_sales:
                  if past_sales.status != 200:
                      print(f"Past sales fetch failed: {past_sales.status}")
                      return

                  past_sales_data = await past_sales.json()

              

              ava_valuation_schema = {
                  "vehicle_summary": {
                      "vin": None,
                      "year": None,
                      "make": None,
                      "model": None,
                      "trim": None,
                      "mileage": None,
                      "drivetrain": None,
                      "engine": None,
                      "transmission": None,
                      "body_style": None,
                      "fuel_type": None,
                      "exterior_color": None,
                      "interior_color": None,
                      "title_status": None,
                      "condition_grade": None,
                      "seller_type": None,
                      "auction_lane": "GMTV Lane",
                      "location": None,
                      "sale_date": None
                  },
                  "valuation": {
                      "amount": 24000,
                      "currency": "USD",
                      "source": "GMTV Lane"
                  },
                  "valuation_range": {
                      "conservative_wholesale_value": None,
                      "base_wholesale_value": None,
                      "aggressive_wholesale_value": None,
                      "target_hammer_price": 24000,
                      "predicted_hammer_price": 24000,
                      "recommended_max_bid": None
                  },
                  "valuation_anchors": {
                      "mmr": None,
                      "accutrade": None,
                      "at": None,
                      "msrp": None,
                      "recent_lane_comp_low": None,
                      "recent_lane_comp_high": None,
                      "recent_lane_comp_average": None,
                      "public_market_low": None,
                      "public_market_high": None
                  },
                  "anchor_weighting": {
                      "primary_anchor": None,
                      "mmr_weight": None,
                      "accutrade_weight": None,
                      "lane_sales_weight": None,
                      "public_market_weight": None,
                      "weighting_explanation": None
                  },
                  "key_ratios": {
                      "price_to_mmr": None,
                      "price_to_accutrade": None,
                      "price_to_lane_average": None,
                      "depreciation_from_msrp": None,
                      "lane_realization_vs_mmr": None
                  },
                  "equipment_assessment": {
                      "equipment_level": None,
                      "is_loaded": None,
                      "important_options": [],
                      "missing_or_basic_features": [],
                      "dealer_bid_impact": None
                  },
                  "comparable_sales_last_3_months": [
                      {
                          "sale_date": None,
                          "source": None,
                          "year": None,
                          "make": None,
                          "model": None,
                          "trim": None,
                          "mileage": None,
                          "condition": None,
                          "title_or_history_notes": None,
                          "sale_price": None,
                          "similarity_score": None,
                          "why_relevant": None
                      }
                  ],
                  "public_market_comps": {
                      "kbb": {
                          "value_or_range": None,
                          "notes": None
                      },
                      "cargurus": {
                          "value_or_range": None,
                          "notes": None
                      },
                      "mecum": {
                          "value_or_range": None,
                          "notes": None
                      },
                      "bring_a_trailer": {
                          "value_or_range": None,
                          "notes": None
                      },
                      "public_information_found": None,
                      "public_market_explanation": None
                  },
                  "carfax_history_assessment": {
                      "risk_level": None,
                      "accident_history": None,
                      "damage_or_structural_notes": None,
                      "title_brand_or_odometer_issues": None,
                      "owner_use_type": None,
                      "service_history_quality": None,
                      "open_recalls": None,
                      "estimated_value_adjustment": None,
                      "dealer_bid_impact": None
                  },
                  "condition_recon_assessment": {
                      "appearance_assessment": None,
                      "condition_notes": [],
                      "estimated_reconditioning_cost": None,
                      "condition_risk": None,
                      "dealer_bid_impact": None
                  },
                  "demand_supply_assessment": {
                      "demand_level": None,
                      "supply_level": None,
                      "market_position": None,
                      "segment_strength": None,
                      "dealer_demand": None,
                      "retail_demand": None,
                      "auction_liquidity": None,
                      "seasonality": None,
                      "demand_supply_explanation": None
                  },
                  "market_conditions": {
                      "as_of_date": datetime.now().isoformat(),
                      "market_state": None,
                      "pricing_trend": None,
                      "volatility": None,
                      "market_adjustment_amount": None,
                      "market_explanation": None
                  },
                  "score": {
                      "total": None,
                      "market_value_vs_benchmarks": None,
                      "historic_lane_performance": None,
                      "carfax_history_quality": None,
                      "condition_recon_risk": None,
                      "demand_public_signal": None,
                      "data_confidence": None
                  },
                  "decision": {
                      "rating": None,
                      "confidence": None,
                      "buy_signal": None,
                      "reason": None
                  },
                  "risk_flags": {
                      "branded_title": None,
                      "structural_damage": None,
                      "odometer_issue": None,
                      "severe_accident": None,
                      "rental_or_fleet_history": None,
                      "poor_condition": None,
                      "weak_comps": None,
                      "high_market_volatility": None,
                      "other_flags": []
                  },
                  "data_gaps": [],
                  "text": "Explain the Predicted Hammer Price / Target Hammer in plain English. Include MMR vs AT weighting, why the valuation.amount is appropriate for a GMTV dealer hammer, whether the car is loaded or basic, current market conditions, demand and supply, Carfax impact, condition and recon impact, key comparable sales from the last 3 months, and public web-search comparison from KBB, CarGurus, Mecum, and Bring a Trailer."
              }

              ava_input = [
                  {
                      "role": "user",
                      "content": (
                          f"Return a vehicle valuation. Adjust for current market conditions. Always put first your hammer target, then the condition and car details used in your valuation. "
                          f"Assume the valuation is what a rational dealer in a GMTV lane would pay as Target Hammer. Ignore the Cost and Buyer target values, they provide no insight to a dealer, so you should not use them in predicting what they would pay."
                          f"The valuation.amount must represent the GMTV Lane Target Hammer / Predicted Hammer Price, not retail value, private-party value, or book value. "
                          f"Take on a dealer's mindset looking to buy a car GMTV is selling at auction. "
                          f"Use the current market state as of date {datetime.now().isoformat()} to predict dealer behavior. "
                          f"Assume the dealer wants margin after auction fees, transport, reconditioning, holding cost, market risk, and resale profit. "
                          f"Consider both AT / AccuTrade and MMR. For MMR make sure you are reading the ADJUSTED PRICING if it is there, it's usually the first item, not just the default wholesale value, not just the base wholesale valuation. It should say adjustedBy. AT valuation might be closer to the truth if the options are really good, if the car is newer/recent-year, or if the condition is rougher. "
                          f"MMR is usually closer if it is a normal mainstream market car. "
                          f"If the car is loaded with desirable options, AT may deserve more weight. "
                          f"If the car is basic, average, or common, MMR may deserve more weight. "
                          f"If MMR and AT disagree, explain which anchor you trusted more and why. "
                          f"Consider the car's appearance from car_details. Does it have options, trim, packages, color, wheels, drivetrain, interior, technology, luxury, performance, towing, safety, or comfort features that make it a better deal for dealers? "
                          f"Carefully determine whether the car is loaded, better-than-average, average, basic, or undesirable/poorly equipped. "
                          f"In the text valuation explanation, include the key comparable sales you saw in sales_last_3_months_for_market_trend for similar cars. "
                          f"Prioritize comps with the same year/make/model/trim, similar mileage, similar condition, similar title/Carfax quality, same or similar auction lane, recent sale date, and similar options/drivetrain. "
                          f"Perform a websearch for KBB, CarGurus, Mecum, and Bring a Trailer to find other comparable cars and market context. "
                          f"Use public web results only as supporting context for demand, desirability, scarcity, or pricing direction. Do not let retail asking prices directly override wholesale auction evidence. "
                          f"Perform a demand and supply check. Consider segment strength, dealer demand, retail demand, auction liquidity, sale-through behavior, scarcity, mileage sensitivity, seasonality, market softness or strength, and whether supply is high or limited. "
                          f"Use Carfax and vehicle history to adjust for accident history, damage severity, structural damage, airbag deployment, branded title, odometer issues, number of owners, fleet/rental/lease/commercial/personal use, service history, registration geography, and open recalls. "
                          f"Bad history should reduce the Target Hammer. Severe title, odometer, structural, or flood issues should cause dealers to bid cautiously or avoid the unit. "
                          f"Use car_details and any images/descriptions to assess whether the car presents well. Adjust for clean appearance, desirable color, good wheels/options, poor cosmetics, interior wear, mechanical concerns, reconditioning risk, tire condition, warning lights, and prior repairs. "
                          f"Follow any user input regarding valuation strategy given with car_details, as these are learned behaviors and are very important. If user-provided strategy conflicts with generic rules, prefer the user's learned behavior unless it is clearly impossible or unsupported by data. "
                          f"See if the car, VIN, model, trim, or sale has any public information that could affect value. "
                          f"Avoid false precision. If data is limited, conflicting, stale, or low quality, still return a single predicted hammer amount, but reduce confidence and widen the supporting valuation range. "
                          f"Return valid JSON only. Do not include markdown. Do not include commentary outside the JSON. Do not include trailing commas. "
                          f"All dollar amounts must be numbers, not strings. Use null when a value is unavailable. "
                          f"Use this schema exactly:\n"
                          f"{json.dumps(ava_valuation_schema, indent=2, default=str)}\n\n"
                          f"current_mmr MAKE SURE TO USE ADJUSTED PRICING NOT JUST THE BASE WHOLESALE VALUE:\n{json.dumps(current_mmr, default=str)}\n\n"
                          f"car_details:\n{json.dumps(car_data, default=str)}\n\n"
                          f"sales_last_3_months_for_market_trend on all same Make Models(SORT THIS BY SIMILAR YEARS):\n{json.dumps(past_sales_data, default=str)}"
                      ),
                  }
              ]

              ava_response_text = andrew_helper(ava_input,tools = [{ "type": "web_search" }])

              try:
                  parsed_valuation = parse_andrew_valuation(ava_response_text)
                  valuation_data = parsed_valuation["raw"]
                  valuation_number = parsed_valuation["valuation_number"]
                  valuation_logic = parsed_valuation["valuation_text"]
              except Exception as e:
                  print(f"Failed to parse Ava valuation response: {e}")
                  valuation_data = {
                      "valuation": None,
                      "text": ava_response_text,
                  }
                  valuation_number = None
                  valuation_logic = ava_response_text

              print(f"Valuation received: {valuation_number}")

              if valuation_number is None:
                  print(f"No valuation returned for car_id {car_id}")
                  return

              async with session.get(
                  "https://prism.andrew-chat.com/api/v1/prism/job/create",
                  headers=headers,
                  timeout=aiohttp.ClientTimeout(total=600),
              ) as create_job_resp:
                  if create_job_resp.status != 200:
                      print(f"Job creation failed: {create_job_resp.status}")
                      return

                  job_create_result = await create_job_resp.text()

              try:
                  job_data = json.loads(job_create_result)

                  if isinstance(job_data, dict):
                      job_id = job_data.get("id") or job_data.get("job_id")
                  else:
                      job_id = job_data

              except json.JSONDecodeError:
                  job_id = job_create_result.strip()

              if not job_id:
                  print(f"No job id returned from job/create: {job_create_result}")
                  return
              car_data = {"current_mmr" : json.dumps(current_mmr, default=str), "car_data" : car_data}
              job_update_payload = {
                  "id": int(job_id),
                  "Andrew_stream": json.dumps({"final_valuation_number" : valuation_number, "final_valuation_summary": valuation_logic}, default=str),
                  "vin": vin,
                  "job_date": datetime.now().isoformat(),
                  "car_details": json.dumps(car_data, default=str),
                  "SA_stream": json.dumps(past_sales_data, default=str)
              }

              async with session.post(
                  "https://prism.andrew-chat.com/api/v1/prism/job/update",
                  json=job_update_payload,
                  headers=headers,
                  timeout=aiohttp.ClientTimeout(total=600),
              ) as update_job_resp:
                  print(f"Job updated: {update_job_resp.status}")

              

      except asyncio.CancelledError:
          pass
      except Exception as e:
          print(f"Valuation task error for car_id {car_id}: {e}")
  
  await _fetch_and_store_valuation(data, data.get("vin"), authorization)

  if "vin" in data:
    candidate_rows = get_valuation_search(
      "vin",
      data['vin'],
      100,
      select=["id"],
    )
    vin_batch = get_valuation_rows_by_ids(
      [row["id"] for row in candidate_rows],
    )
    today = datetime.utcnow().date().isoformat()
    todays_valuations = []
    for vin in vin_batch:
      if vin['job_date'].date() == datetime.fromisoformat(today).date() and vin['Andrew_stream'] is not None:
        logging.info(f"Valuation for VIN {data['vin']} already exists for today, skipping...")
        todays_valuations.append(vin)
    if todays_valuations:
      try:
        latest = max(todays_valuations, key=lambda v: datetime.fromisoformat(v.get('job_date')))
      except Exception:
        latest = max(todays_valuations, key=lambda v: v.get('job_date') or "")
      andrew_stream = json.loads(latest.get('Andrew_stream'))
      latest_summary = andrew_stream.get('final_valuation_summary','No summary')
      final = andrew_stream.get('final_valuation_number',0)
      if final is not None:
        logging.info(f"Returning latest valuation for today: {final}")
        return {"valuation": final, "text": latest_summary}, 200
  else:
     return {"valuation" : 0, "text": "No VIN provided"}, 200
  return "OK", 200

# Drop-in replacement for /api/v1/prism/ava/filterurl
# Assumes these globals already exist in your server:
#   app, request, webclient, engine, pbengine
# engine must have access to `prism-prime`.`ava_filter_history`.

import uuid
from tools.filterurl_one_shot import execute_filterurl_one_shot


@app.route('/api/v1/prism/ava/filterurl', methods=['POST'])
async def filterurlv2():
    request_id = uuid.uuid4().hex[:12]
    data = await request.get_json(silent=True)
    if data is None:
        return [{"url": "", "error": "Bad request."}], 400

    return await execute_filterurl_one_shot(
        data=data,
        webclient=webclient,
        engine=engine,       # Ava/main DB: ava_users token compatibility
        crm_engine=pbengine, # Powerbuyer/SuiteCRM DB: schema + activity query
        request_id=request_id,
      valuation_search=get_valuation_search,
      valuation_rows_by_ids=get_valuation_rows_by_ids,
    )



@app.route('/api/v0/prism/ava/filterurl', methods=['POST'])
async def filterurl():
    request_id = uuid.uuid4().hex[:12]

    def log_stage(stage, **fields):
      safe_fields = {}
      for key, value in fields.items():
        if key in {"authorization", "admin_token", "token", "access_token"}:
          continue
        safe_fields[key] = value
      logging.info("[filterurl:%s] %s | %s", request_id, stage, safe_fields)

    async def read_response_safely(resp):
      body_text = ""
      body_json = None
      content_type = resp.headers.get("Content-Type", "")

      try:
        body_text = await resp.text()
      except Exception as e:
        body_text = f"<failed to read body: {e}>"

      try:
        if "application/json" in content_type.lower():
          body_json = json.loads(body_text) if body_text else None
      except Exception as e:
        log_stage("json_parse_failed", status=resp.status, content_type=content_type, error=str(e), body_preview=body_text[:500])

      return {
        "status": resp.status,
        "content_type": content_type,
        "body_text": body_text,
        "body_json": body_json,
      }

    try:
      """authorization = request.headers.get('Authorization')
      if authorization != os.environ.get("SA_AUTHENTICATION"):
        log_stage("unauthorized", has_auth_header=bool(authorization))
        return "Unauthorized", 403"""

      data = await request.get_json(silent=True)
      if data is None:
        log_stage("bad_request_missing_json")
        return "Bad request", 400

      if "uuid" not in data or "filter_query" not in data:
        log_stage("bad_request_missing_required_fields", has_uuid=("uuid" in data), has_filter_query=("filter_query" in data))
        return "Bad request", 400
      elif "uuid" not in data:
        log_stage("bad_request_missing_uuid")
        return "Bad request, missing uuid", 400
      elif "filter_query" not in data:
        log_stage("bad_request_missing_query")
        return "Bad request, missing query", 400

      user = data['uuid']
      log_stage("request_validated", user=user, filter_query_len=len(data.get("filter_query", "")))

      # Insert random authorization token for admin user
      try:
          admin_token = secrets.token_urlsafe(32)
          with engine.begin() as conn:
              conn.execute(
                  text("INSERT INTO ava_users (authorization, user_access_md, userId) VALUES (:auth_token, :user_access_md, :user_id) ON DUPLICATE KEY UPDATE authorization = :auth_token, user_access_md = :user_access_md"),
                  {"user_id": f"admin_{data['uuid']}", "auth_token": admin_token, "user_access_md": "*"}
              )
          log_stage("admin_token_upserted", admin_user="admin")
      except Exception as e:
          logging.exception("[filterurl:%s] admin_token_upsert_failed", request_id)
          return {"error": "Failed to set up admin authorization", "stage": "admin_token_upsert", "request_id": request_id}, 500

      async with aiohttp.ClientSession() as session:
        # Get or create session ID
        session_url = f'https://sale.andrew-chat.com/api/v1/session/{user}'
        log_stage("session_lookup_start", url=session_url, user=user)
        async with session.get(
          session_url,
          headers={"Authorization": SA_AUTHENTICATION},
          timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
          session_resp = await read_response_safely(resp)

        log_stage(
          "session_lookup_done",
          status=session_resp["status"],
          content_type=session_resp["content_type"],
          body_preview=session_resp["body_text"][:500],
        )

        if session_resp["status"] != 200:
          return {
            "error": "Failed to get sale session",
            "stage": "session_lookup",
            "upstream_status": session_resp["status"],
            "request_id": request_id,
          }, 502

        session_data = session_resp["body_json"]
        if not isinstance(session_data, dict):
          return {
            "error": "Session endpoint returned non-JSON",
            "stage": "session_lookup_parse",
            "request_id": request_id,
          }, 502

        session_id = session_data.get('session_id')
        if not session_id:
          log_stage("session_lookup_missing_session_id", payload=session_data)
          return {
            "error": "Session endpoint missing session_id",
            "stage": "session_lookup_parse",
            "request_id": request_id,
          }, 502

        log_stage("session_lookup_success", session_id=session_id)

        filter_prompt = f"""My Powerbuyer CRM user UUID is {data['uuid']}.

        Build a persistent Leads saved-search filter that fulfills this request:
        {data['filter_query']}

        User access token:
        {admin_token}

        Return only one of these JSON forms:

        Success:
        [{{"url":"<tool_returned_url>","error":""}}]

        No matching Leads after a required database search:
        []

        Invalid or unrepresentable request:
        [{{"url":"","error":"<clear error message>"}}]

        RULES

        1. This is automated. Do not ask questions. Use the most literal reasonable
          interpretation and complete the request to the best of your ability.

        2. Use `build_leads_filter_url` when the request needs a clickable filtered
          Leads list. Do not use it for counts, totals, reports, exports, or raw data.

        3. Never add `assigned_user_id` merely because my UUID is provided.

          There are three ownership cases:

          A. No user or ownership condition requested:
              Do not add an assigned-user filter. Allow the normal access-control layer
              to restrict visibility.

          B. The request requires a database query but does not name a specific buyer, user, owner, manager, or team member:
              Apply the requesting-user plus managed-user ownership scope in the
              database query when a database query is required. Do not use
              `current_user_only`.

          C. The request explicitly names one buyer, user, owner, manager, or team
              member, or requests a certain buyer to not be counted:
              Resolve that person to their exact CRM user UUID and restrict the result
              to that specific user only. Do not expand the result to the requesting
              manager's entire team.

        4. Never use `current_user_only` or "My Items." It excludes Leads assigned to
          users I manage.

        5. Preserve the comparison meaning for every range-capable field, including
          year, mileage, current bid, payoff cost, repair cost, and photo count.

          Use the formats supported by `build_leads_filter_url`:

          - between/from/through X and Y -> {{"min": X, "max": Y}}
          - under/below/less than X -> {{"max": X}}
          - over/above/more than X -> {{"min": X}}
          - exactly/equal to X -> scalar X

          Example: "under 150,000 miles" must be:

          {{"miles": {{"max": 150000}}}}

          Do not put comparison symbols inside values. Do not send values such as
          "<150000" or ">5000".

        6. For relative dates, pass the supported shortcut to the tool rather than
          inventing a date in the tool call. Supported values are:

          today, yesterday, this_week, last_week, this_month, last_month,
          last_7_days, last_30_days, and last_90_days.

        7. For make, model, trim, year values, status, sub-status, Rolus SMS direction,
          email direction, and other database-backed categorical values, query
          distinct stored values first when necessary so spelling and capitalization
          match the database.

          When multiple makes or models are used in a direct filter, pass them as one
          comma-separated string. Do not pass make or model as a JSON array.

        8. Resolve buyer, assigned-user, created-by, owner, manager, or team-member
          names to CRM user UUIDs before calling the filter tool or building a
          user-specific database query.

          Never pass a person's name into a UUID field.

          When resolving a user:

          - Search for the exact or closest matching active CRM user.
          - Use the resolved CRM UUID, not the requesting manager's UUID.
          - If no user matches, return an invalid-request error.
          - If multiple users match and one cannot be selected confidently, return an
            ambiguity error rather than combining them.

        9. SPECIFIC BUYER OR USER REQUESTS

          When the request explicitly asks for Leads belonging to one particular
          buyer, user, owner, manager, or team member, restrict the result to that
          person only.

          Examples:

          - "Show me John's Leads"
          - "Show me Sarah's inbound text Leads"
          - "Pull up Mike's hot Leads"
          - "Show buyer Daniel's vehicles from last month"
          - "Only show results assigned to Angela"

          Required behavior:

          A. Direct Leads filter:

              If the request can be represented entirely by direct Leads fields, pass
              the resolved user UUID through:

              {{
                "assigned_user_id": "<resolved_user_uuid>"
              }}

              Example:

              {{
                "filters": {{
                  "status": "Hot",
                  "assigned_user_id": "<resolved_user_uuid>"
                }},
                "lead_ids": "",
                "user_id": "{data['uuid']}",
                "user_access_token": "{admin_token}",
                "markdown_key": "leads_filter_url_key_442"
              }}

          B. Database or cross-table filter:

              If the request requires `database_query`, put the specific-user
              restriction directly in the SQL:

              leads.assigned_user_id = '<resolved_user_uuid>'

              The database access-control rewriter may also append a visibility
              condition such as:

              leads.assigned_user_id IN (<authorized_user_uuids>)

              This is expected. The equality condition limits the query to the
              specifically requested user, while the IN condition verifies that the
              requesting user is authorized to see that person's Leads.

          C. Do not broaden a named-user request:

              A request for one named buyer must not return:

              - the requesting manager's own Leads;
              - Leads assigned to other managed users;
              - the full manager-visible team;
              - Leads with no assigned user.

              It must return only Leads assigned to the resolved requested user.

          D. If the requested user is outside the requester's authorized visibility,
              return no matching Leads. Never remove the specific-user condition merely
              to produce results.

        10. Use `database_query` before the filter tool whenever the request depends on
            SMS, calls, emails, photos, related records, unanswered activity, aggregate
            logic, existence or non-existence logic, or any condition unavailable in
            the Leads search form. ALWAYS add assigned_user_id logic to the query when pulling lead IDs, to ensure the user only sees Leads they are authorized to see. For managers include leads they manage in the query.

            General manager/requester visibility must be enforced through the database
            query's access-control layer rather than through the saved-search filter.

            Do not manually add a general requesting-user ownership condition unless
            the query specifically asks for one user.

            When the request names one specific buyer or user, include that resolved
            user's UUID as an explicit equality condition in SQL.

        11. EXACT-ID MODE — HIGHEST PRIORITY

            When a database pre-query returns one or more matching Lead UUIDs, the
            returned UUID list is the complete and authoritative saved-search filter.

            Before running the database query, include every condition from the user's
            request in that SQL query, including:

            - all cross-table activity conditions;
            - all direct Lead conditions needed by the request;
            - all date conditions;
            - all make, model, year, mileage, status, and ownership conditions;
            - a specifically requested buyer or user condition, when applicable;
            - the requesting-user or manager visibility scope supplied by the
              database access-control layer.

            The database query must:

            - return exactly one Lead UUID column;
            - use `SELECT DISTINCT`;
            - alias that column as `lead_id`;
            - exclude deleted Leads, activities, and relationship rows where applicable;
            - order deterministically, normally by newest matching activity;
            - return no more than 50 distinct Lead UUIDs.

            Recommended result shape:

            SELECT DISTINCT leads.id AS lead_id
            FROM ...
            WHERE ...
            ORDER BY ...
            LIMIT 50

        12. After a database pre-query returns matching Lead IDs:

            - Extract only the `lead_id` UUID value from each row.
            - Ignore every other returned value or column.
            - Do not pass first names, last names, display names, labels, statuses,
              dates, activity values, or complete database rows to the filter tool.
            - Deduplicate the UUIDs.
            - Join the UUIDs into one comma-separated string because the current
              `lead_ids` tool parameter is a string.
            - Pass that string through `lead_ids`.
            - Set `filters` to exactly an empty object: {{}}.

        13. In exact-ID mode, call `build_leads_filter_url` using this shape:

            {{
              "filters": {{}},
              "lead_ids": "uuid1,uuid2,uuid3",
              "user_id": "{data['uuid']}",
              "user_access_token": "{admin_token}",
              "markdown_key": "leads_filter_url_key_442"
            }}

        14. When `lead_ids` contains one or more UUIDs, absolutely no direct filters may
            be sent with it.

            In particular, do not send any of the following with `lead_ids`:

            - `name`
            - `first_name`
            - `last_name`
            - `assigned_user_id`
            - `current_user_only`
            - `status`
            - `sub_status`
            - `date_entered`
            - `date_modified`
            - `make`
            - `model`
            - `year`
            - `miles`
            - any other field inside `filters`

            The `filters` argument must remain exactly: {{}}

            This rule also applies when the original request named a specific buyer.
            The buyer restriction must already have been applied in the database query.

        15. Never use `name` to name the saved search. Never place a saved-search title
            inside `filters`.

            This tool always updates the fixed saved search named `Ava_Filter`.
            It does not accept a `search_name` parameter.

            The alias `name` means the Lead's `last_name`; therefore, using `name` for a
            saved-search title would incorrectly create a last-name filter.

        16. When ownership scope was already enforced in the database pre-query, do not
            also add `assigned_user_id` to the saved search.

            Ownership must be applied exactly once in the SQL query.

            A specifically requested buyer restriction must also be applied in SQL and
            must not be repeated in the saved-search filters after exact Lead IDs have
            been returned.

        17. If the database pre-query returns zero matching Lead UUIDs:

            - Do not call `build_leads_filter_url`.
            - Do not pass an empty `lead_ids` value.
            - Do not create a broad or unscoped saved search.
            - Return exactly:

            []

        18. If more than 50 Leads match:

            - Apply all requested conditions in SQL.
            - Preserve any specifically requested buyer restriction.
            - Order by the newest relevant activity.
            - Return only the newest 50 distinct Lead UUIDs.
            - Pass only those UUIDs through `lead_ids`.
            - Keep `filters` exactly {{}}.

        19. For a direct Leads filter that does not require a database pre-query, use
            the ordinary `filters` object and leave `lead_ids` blank.

            A direct filter may include `assigned_user_id` when the request explicitly
            asks for one resolved buyer or user.

            Do not use exact-ID mode unless a database query actually returned matching
            Lead UUIDs.

        20. Every `build_leads_filter_url` call must include:

            "markdown_key": "leads_filter_url_key_442"

        21. Before calling the filter tool after a database search, verify all of the
            following:

            - the database query returned only or clearly aliased `lead_id`;
            - every extracted value is a Lead UUID;
            - no names or other database columns were included;
            - the UUIDs are distinct;
            - there are no more than 50 UUIDs;
            - `filters` is exactly {{}};
            - `lead_ids` contains only comma-separated UUIDs;
            - no `name`, `last_name`, or saved-search title was supplied;
            - general ownership visibility was applied in SQL;
            - if one buyer was requested, that buyer's resolved UUID was applied in SQL;
            - the named-buyer restriction was not broadened to the whole managed team.

        22. On tool success, return exactly:

            [{{"url":"<tool returned URL>","error":""}}]

        23. On an invalid filter query or tool failure, return exactly:

            [{{"url":"","error":"<clear error message>"}}]

        Do not include markdown, explanations, commentary, or any text outside the
        required JSON response."""

        # Send filter prompt to chat
        prism_url = 'https://sale.andrew-chat.com/api/v1/prism'
        prism_payload = {
          'session_id': session_id,
          'message': filter_prompt,
          "user_id": user,
          "action": "create",
        }
        prism_resp = None
        prism_timeout_seconds = 60
        prism_attempts = 2

        for attempt in range(1, prism_attempts + 1):
          log_stage(
            "prism_create_start",
            url=prism_url,
            session_id=session_id,
            user=user,
            attempt=attempt,
            timeout_seconds=prism_timeout_seconds,
          )

          try:
            async with session.post(
              prism_url,
              json=prism_payload,
              headers={"Authorization": SA_AUTHENTICATION},
              timeout=aiohttp.ClientTimeout(total=prism_timeout_seconds),
            ) as resp:
              prism_resp = await read_response_safely(resp)

            break

          except asyncio.TimeoutError:
            log_stage(
              "prism_create_timeout",
              attempt=attempt,
              timeout_seconds=prism_timeout_seconds,
              session_id=session_id,
            )

            if attempt == prism_attempts:
              return {
                "error": "Timed out while creating prism message",
                "stage": "prism_create_timeout",
                "timeout_seconds": prism_timeout_seconds,
                "attempts": prism_attempts,
                "request_id": request_id,
              }, 504

            # Short backoff before retrying the outbound call.
            await asyncio.sleep(1)

          except aiohttp.ClientError as e:
            log_stage(
              "prism_create_client_error",
              attempt=attempt,
              error=str(e),
              session_id=session_id,
            )

            if attempt == prism_attempts:
              return {
                "error": "Network error while creating prism message",
                "stage": "prism_create_client_error",
                "attempts": prism_attempts,
                "request_id": request_id,
              }, 502

            await asyncio.sleep(1)

        if prism_resp is None:
          return {
            "error": "No response from prism create call",
            "stage": "prism_create_empty_response",
            "request_id": request_id,
          }, 502

        log_stage(
          "prism_create_done",
          status=prism_resp["status"],
          status_type=type(prism_resp["status"]).__name__,
          content_type=prism_resp["content_type"],
          body_preview=prism_resp["body_text"][:500],
        )

        logging.info("prism_resp: %s", prism_resp)
        status_code = prism_resp["status"]

        if status_code not in (200, 204):
          log_stage("prism_create_rejected", status=status_code, raw_status=prism_resp["body_text"][:500], accepted_statuses=[200, 204])
          return {
            "error": "Failed to create prism message",
            "stage": "prism_create",
            "upstream_status": status_code,
            "request_id": request_id,
          }, 502

        log_stage("prism_create_accepted_status", status=status_code, raw_status=prism_resp["body_text"][:500], accepted_statuses=[200, 204])
        log_stage("prism_create_accepted", session_id=session_id, user=user)
        return "", 204

    except Exception:
      logging.exception("[filterurl:%s] unhandled_exception", request_id)
      return {"error": "Internal server error", "request_id": request_id}, 500
        



# Tool names that carry a finished valuation, newest first.
VALUATION_TOOLS = ("run_valuation", "finish_valuation")


def _num(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace("$", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def normalize_valuation(response_obj):
    """Map either tool's response onto one shape.

    run_valuation      -> predicted_hammer / summary / card keys at top level
    finish_valuation   -> final_valuation_number / final_valuation_summary
    """
    if not isinstance(response_obj, dict):
        return None

    nested = response_obj.get("valuation")
    nested_amount = nested.get("amount") if isinstance(nested, dict) else nested
    number = _num(
        response_obj.get("predicted_hammer")
        or response_obj.get("final_valuation_number")
        or nested_amount
    )
    text = (
        response_obj.get("summary")
        or response_obj.get("final_valuation_summary")
        or response_obj.get("text")
        or ""
    )

    if number is None and not text:
        return None

    return {"valuation": number, "text": text, "cards": response_obj.get("cards")}


def scan_agent_response(andrew_response):
    """Walk the agent's event list for a valuation tool response.

    Returns (payload_or_None, assembled_text).
    """
    assembled = ""
    payload = None

    for chunk in andrew_response or []:
        if not isinstance(chunk, dict):
            continue
        if "text" in chunk and chunk["text"]:
            assembled += " " + str(chunk["text"])

        fn = chunk.get("function_response")
        if not fn:
            continue
        if fn.get("name") not in VALUATION_TOOLS:
            continue

        candidate = normalize_valuation(fn.get("response") or {})
        if candidate:
            payload = candidate
            break

    return payload, assembled.strip()


def cached_valuation_for_recent_days(vin_batch, days=10):
    """Reuse the newest valuation from within the last N days."""

    recent = []
    cutoff = datetime.utcnow() - timedelta(days=days)

    for row in vin_batch or []:

        job_date = row.get("job_date")

        if not job_date:
            continue

        if not (
            row.get("LBA_stream")
            or row.get("Andrew_stream")
            or row.get("SA_stream")
        ):
            continue

        row_date = (
            job_date
            if isinstance(job_date, datetime)
            else datetime.fromisoformat(str(job_date))
        )

        if row_date >= cutoff:
            recent.append(row)

    if not recent:
        return None

    def sort_key(row):
        value = row.get("job_date")

        if hasattr(value, "isoformat"):
            return value.isoformat()

        return str(value or "")

    latest = max(recent, key=sort_key)

    def _load(col):
        try:
            return json.loads(latest.get(col) or "{}")
        except json.JSONDecodeError:
            return {}

    andrew = _load("Andrew_stream")
    sa = _load("SA_stream")
    ladder = _load("LPA_stream")

    # Andrew first so Ana override wins.
    payload = (
        normalize_valuation(andrew)
        or normalize_valuation(_load("LBA_stream"))
        or {}
    )

    number = payload.get("valuation")

    if number is None:
        return None

    stages = ladder.get("stages") or (sa.get("cards") or {}).get("Value_ladder") or []

    return {
        "job_id": str(latest.get("id") or ""),
        "valuation": number,
        "text": payload.get("text") or "No summary",
        "likely_range": andrew.get("likely_range"),
        "heat": andrew.get("heat"),
        "confidence": andrew.get("confidence"),
        "anchors": andrew.get("anchors"),
        "cards": sa.get("cards"),
        "Value_ladder": stages,
        "stage_count": len(stages),
    }

def running_valuation_for_today(vin_batch, today_iso):
    """Return the most recent running valuation for today, if any."""
    todays_running = []
    for row in vin_batch or []:
        job_date = row.get("job_date")
        if not job_date:
            continue

        sa_raw = row.get("SA_stream")
        if not sa_raw:
            continue

        row_date = job_date.date() if hasattr(job_date, "date") else datetime.fromisoformat(str(job_date)).date()
        if row_date != datetime.fromisoformat(today_iso).date():
            continue

        try:
            sa = json.loads(sa_raw)
        except json.JSONDecodeError:
            continue

        status = str(sa.get("status") or "").lower()
        if status in ("running", "starting"):
            todays_running.append(row)

    if not todays_running:
        return None

    def sort_key(row):
        value = row.get("job_date")
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value or "")

    latest = max(todays_running, key=sort_key)

    try:
        sa = json.loads(latest.get("SA_stream") or "{}")
    except json.JSONDecodeError:
        sa = {}

    return {
        "job_id": str(latest.get("id") or ""),
        "stage": sa.get("stage", "starting"),
    }


# ----------------------------------------------------------------Valuation route

from tools.prism_valuation_pipeline import (
    configure_prism_valuation,
    run_prism_valuation,
)


STALE_AFTER_SECONDS = int(os.environ.get("VALUATION_STALE_AFTER", "1800"))

CARD_KEYS = [
    "Accutrade_comps",
    "Manheim_comps",
    "GMTV_comps",
    "Market_trend",
    "Carfax_scan",
    "Image_scans",
    "Logic_path",
    "Market_scan",
    "Value_ladder",
]


def _empty_cards():
    return {key: ("" if key == "Carfax_scan" else []) for key in CARD_KEYS}


# ----------------------------------------------------------------
# Cloud Tasks valuation worker
#
# IMPORTANT: the POST route does NOT detach an asyncio task anymore.
# It creates the job, enqueues a Cloud Task, and returns. The Cloud Task calls the
# SAME POST route with X-Prism-Valuation-Worker: 1 and keeps that HTTP request open
# until the full valuation finishes. This keeps Cloud Run CPU/request ownership for
# the entire evidence + 3-pass workflow.
# ----------------------------------------------------------------

VALUATION_TASKS_PROJECT = os.environ.get(
    "VALUATION_TASKS_PROJECT",
    CLOUD_TASKS_PROJECT,
)
VALUATION_TASKS_LOCATION = os.environ.get(
    "VALUATION_TASKS_LOCATION",
    CLOUD_TASKS_LOCATION,
)
VALUATION_TASKS_QUEUE = os.environ.get(
    "VALUATION_TASKS_QUEUE",
    CLOUD_TASKS_QUEUE,
)
VALUATION_WORKER_URL = os.environ.get(
    "PRISM_VALUATION_WORKER_URL",
    f"{os.environ.get('PRISM_URL', 'https://prism.andrew-chat.com').rstrip('/')}/api/v2/prism/andrew/valuations",
)
VALUATION_TASK_DISPATCH_DEADLINE = min(
  1800,
  max(15, int(os.environ.get("VALUATION_TASK_DISPATCH_DEADLINE", "1800"))),
)
VALUATION_WORKER_HEADER = "X-Prism-Valuation-Worker"


def _schedule_prism_valuation_task(job_id: str, car_details: dict, job_date: str):
    """Enqueue one durable valuation worker request and return the Cloud Task name."""

    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    if hasattr(credentials, "with_quota_project"):
        credentials = credentials.with_quota_project(VALUATION_TASKS_PROJECT)

    client = tasks_v2.CloudTasksClient(credentials=credentials)
    parent = client.queue_path(
        VALUATION_TASKS_PROJECT,
        VALUATION_TASKS_LOCATION,
        VALUATION_TASKS_QUEUE,
    )

    payload = json.dumps(
        {
            "job_id": str(job_id),
            "job_date": str(job_date),
            "car_details": car_details,
        },
        default=str,
    ).encode("utf-8")

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": VALUATION_WORKER_URL,
            "headers": {
                "Authorization": SA_AUTHENTICATION,
                "Content-Type": "application/json",
                VALUATION_WORKER_HEADER: "1",
            },
            "body": payload,
        },
          "dispatch_deadline": duration_pb2.Duration(
            seconds=VALUATION_TASK_DISPATCH_DEADLINE
          ),
    }

    created = client.create_task(
        request={
            "parent": parent,
            "task": task,
        }
    )
    logging.info(
        "Scheduled valuation Cloud Task job=%s task=%s url=%s",
        job_id,
        created.name,
        VALUATION_WORKER_URL,
    )
    return created.name


async def _valuation_job_updater(job_id: str, fields: dict) -> bool:
    """Directly update the existing valuation row; no Prism self-HTTP."""
    result = await asyncio.to_thread(update_valuation, job_id, **fields)
    return str(result).strip().lower() == "success"


def _valuation_exact_job(rows, job_id: str):
    target = str(job_id)
    for row in rows or []:
        if str(row.get("id") or "") == target:
            return row
    return None


def _valuation_load_json(raw):
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError):
        return {}


def _prism_crm_photo_evidence(car_details: dict, max_images: int = 50) -> dict:
    """Photo URLs + Ava descriptions from the Powerbuyer lead matching the VIN."""
    return collect_ava_photo_evidence(
        pbengine,
        car_details,
        max_images=max_images,
    )


# The valuation pipeline orchestrates/reasons and reuses Prism's Manheim + websearch implementations.
# GMTV comparable sales are loaded from one three-month Storm Sync HeadStart export.
# Browser evidence is intentionally NOT injected: the pipeline calls /evidence/browser
# over HTTP so that route owns the persistent Playwright session and semaphore.
# That route collects CARFAX and then AutoCheck in the active AccuTrade appraisal.
configure_prism_valuation(
    manheim_headers_provider=_prism_manheim_headers,
    manheim_current_mmr_collector=_prism_manheim_current_mmr,
    manheim_historical_mmr_collector=_prism_manheim_historical_mmr,
    manheim_future_mmr_collector=_prism_manheim_future_mmr,
    manheim_condition_collector=_prism_manheim_condition_evidence,
    manheim_vin_sales_collector=_prism_manheim_vin_sales,
    manheim_ymm_sales_collector=_prism_manheim_ymm_sales,
    manheim_mm_sales_collector=_prism_manheim_mm_sales,
    crm_photos_collector=_prism_crm_photo_evidence,
    job_updater=_valuation_job_updater,
    websearch_runner=_prism_websearch_generate,
    web_client=webclient,
)


@app.route("/api/v2/prism/andrew/valuations", methods=["POST"])
async def valuations():
    """
    Start a valuation, or execute it when invoked by the Cloud Tasks worker.

    Frontend flow:
      1. Validate VIN.
      2. Check newest cached valuation from last 10 days.
      3. If cached -> return immediately.
      4. If NOT cached -> find newest Powerbuyer Lead for VIN.
      5. Pull Ava's saved photo descriptions and photo URLs from that Lead.
      6. Append those descriptions to the vehicle description and carry the URLs
         through on car_details so the worker can attach the photos themselves.
      7. Create/schedule the normal valuation job.

    Cloud Tasks worker mode is unchanged.
    """

    if request.headers.get("Authorization") != SA_AUTHENTICATION:
        return "Unauthorized", 403

    data = await request.get_json(silent=True) or {}

    # ========================================================
    # CLOUD TASK WORKER MODE
    # ========================================================

    if request.headers.get(VALUATION_WORKER_HEADER) == "1":

        job_id = str(
            data.get("job_id") or ""
        ).strip()

        car_details = (
            data.get("car_details")
            or {}
        )

        job_date = (
            str(
                data.get("job_date")
                or ""
            ).strip()
            or None
        )

        if not job_id:
            return {
                "status": "error",
                "error": "job_id is required",
            }, 400

        if not isinstance(
            car_details,
            dict,
        ):
            return {
                "status": "error",
                "error": "car_details must be an object",
            }, 400

        # Cloud Tasks is at-least-once.
        # If this job is already terminal, acknowledge it
        # instead of launching a duplicate valuation.
        try:

            rows = await asyncio.to_thread(
                get_valuation_search,
                "id",
                job_id,
                5,
            )

            row = _valuation_exact_job(
                rows,
                job_id,
            )

        except Exception:

            logging.exception(
                "Failed reading valuation worker job=%s",
                job_id,
            )

            return {
                "status": "error",
                "error": "Could not load valuation job",
            }, 500

        if not row:

            return {
                "status": "error",
                "error": f"Unknown valuation job {job_id}",
            }, 404

        stored = _valuation_load_json(
            row.get("SA_stream")
        )

        stored_status = str(
            stored.get("status")
            or ""
        ).lower()

        if stored_status in {
            "complete",
            "error",
        }:

            return {
                "status": stored_status,
                "job_id": job_id,
                "already_terminal": True,
            }, 200

        if not job_date:

            job_date = (
                str(
                    row.get("job_date")
                    or ""
                )
                or None
            )

        logging.info(
            "Valuation Cloud Task worker starting "
            "job=%s vin=%s task=%s",
            job_id,
            clean_vin(
                car_details.get("vin")
            ),
            request.headers.get(
                "X-CloudTasks-TaskName",
                "",
            ),
        )

        # IMPORTANT:
        # Do not create_task() here.
        # The Cloud Tasks HTTP request owns the valuation
        # until run_prism_valuation finishes.
        result = await run_prism_valuation(
            car_details=car_details,
            job_id=job_id,
            job_date=job_date,
        )

        # run_prism_valuation already persists terminal errors.
        # Acknowledge them so Cloud Tasks does not retry the
        # valuation and accidentally duplicate it.
        if (
            isinstance(result, dict)
            and result.get("error")
        ):

            return {
                "status": "error",
                "job_id": job_id,
                "error": result.get("error"),
            }, 200

        return {
            "status": "complete",
            "job_id": job_id,
            "vin": (
                result.get("vin")
                if isinstance(result, dict)
                else None
            ),
        }, 200


    # ========================================================
    # NORMAL FRONTEND START MODE
    # ========================================================

    vin = clean_vin(
        data.get("vin")
    )

    if not vin:

        return {
            "error": "vin is required"
        }, 400

    if (
        not vin.isalnum()
        or not 11 <= len(vin) <= 17
    ):

        return {
            "error": "malformed vin"
        }, 400

    data["vin"] = vin


    # ========================================================
    # 1. CHECK 10-DAY CACHE FIRST
    #
    # Do NOT look up CRM photos before this.
    # A valid cache should return as cheaply/quickly as possible.
    # ========================================================

    try:

        cutoff = datetime.utcnow() - timedelta(days=10)
        candidate_rows = await asyncio.to_thread(
          get_valuation_search,
          "vin",
          vin,
          100,
          cutoff,
          ["id", "job_date"],
        )
        existing_rows = await asyncio.to_thread(
          get_valuation_rows_by_ids,
          [row["id"] for row in candidate_rows],
          ["id", "vin", "job_date", "Andrew_stream", "LBA_stream", "SA_stream", "LPA_stream"],
        )

    except Exception:

        logging.exception(
            "Failed checking valuation cache vin=%s",
            vin,
        )

        existing_rows = []


    cached = cached_valuation_for_recent_days(
        existing_rows or [],
        days=10,
    )
    force_new = False
    if data.get("force_new") is True:
       force_new = True
    
    if cached and not force_new:

        logging.info(
            "Returning recent cached valuation vin=%s job=%s",
            vin,
            cached.get("job_id"),
        )

        return {
            **cached,
            "vin": vin,
            "status": "complete",
            "cached": True,
        }, 200


    # ========================================================
    # 2. CACHE MISS:
    # LOAD AVA'S SAVED PHOTO DESCRIPTIONS
    #
    # collect_ava_photo_evidence() should:
    #
    #   VIN
    #    ↓
    #   newest Powerbuyer Lead with that VIN
    #    ↓
    #   photos attached to that Lead
    #    ↓
    #   Ava descriptions stored on those photos
    #
    # This is enrichment only.
    # Failure here must NEVER stop valuation.
    # ========================================================

    try:

        ava_photo_evidence = await asyncio.to_thread(
            collect_ava_photo_evidence,
            pbengine,
            data,
            max_images=50,
        )

        images = (
            ava_photo_evidence.get("images")
            or []
        )

        # Carried on car_details so the Cloud Task worker reuses these photo URLs
        # instead of re-querying Powerbuyer and re-running the classifier.
        if images:
            data["crm_photos"] = {
                "lead": ava_photo_evidence.get("lead"),
                "images": images,
                "count": len(images),
                "errors": ava_photo_evidence.get("errors") or [],
            }

        image_descriptions = []


        for image in images:

            if not isinstance(
                image,
                dict,
            ):
                continue


            description = str(
                image.get("description")
                or ""
            ).strip()


            if not description:
                continue


            photo_type = str(
                image.get("type")
                or ""
            ).strip()


            damaged = bool(
                image.get("damaged")
            )


            prefix_parts = []


            if photo_type:

                prefix_parts.append(
                    photo_type
                )


            if damaged:

                prefix_parts.append(
                    "AVA FLAGGED DAMAGE"
                )


            prefix = ""

            if prefix_parts:

                prefix = (
                    "["
                    + " | ".join(
                        prefix_parts
                    )
                    + "] "
                )


            image_descriptions.append(
                prefix
                + description
            )


        # ----------------------------------------------------
        # APPEND PHOTO OBSERVATIONS TO VEHICLE DESCRIPTION
        # ----------------------------------------------------

        if image_descriptions:

            existing_description = str(
                data.get("description")
                or ""
            ).strip()


            ava_photo_description = (
                "AVA VEHICLE PHOTO OBSERVATIONS\n"
                "These observations were produced from photos "
                "attached to the newest CRM Lead matching this VIN.\n"
                + "\n".join(
                    "- " + description
                    for description
                    in image_descriptions
                )
            )


            if existing_description:

                data["description"] = (
                    existing_description
                    + "\n\n"
                    + ava_photo_description
                )

            else:

                data["description"] = (
                    ava_photo_description
                )


            logging.info(
                "Added %s Ava photo descriptions "
                "to valuation vin=%s lead=%s damaged=%s",
                len(image_descriptions),
                vin,
                (
                    ava_photo_evidence.get("lead")
                    or {}
                ).get("id"),
                sum(
                    1
                    for image
                    in images
                    if (
                        isinstance(image, dict)
                        and image.get("damaged")
                    )
                ),
            )

        else:

            logging.info(
                "No Ava-described CRM photos found "
                "for fresh valuation vin=%s",
                vin,
            )


    except Exception:

        # Photo descriptions are useful context,
        # but must NEVER stop Andrew from valuing the car.
        logging.exception(
            "Failed loading Ava photo descriptions "
            "for fresh valuation vin=%s",
            vin,
        )


    # ========================================================
    # 3. CREATE NEW VALUATION JOB
    # ========================================================

    try:

        job_id = await asyncio.to_thread(
            create_valuation,
            "",
        )

    except Exception:

        logging.exception(
            "Failed creating valuation job vin=%s",
            vin,
        )

        return {
            "vin": vin,
            "status": "error",
            "text": "Could not open a valuation job.",
        }, 503


    if (
        not job_id
        or str(job_id).strip().lower()
        == "error"
    ):

        return {
            "vin": vin,
            "status": "error",
            "text": "Could not open a valuation job.",
        }, 503


    job_id = str(
        job_id
    ).strip()


    now = datetime.utcnow()

    job_date = now.strftime(
        "%Y-%m-%d %H:%M:%S"
    )


    # ========================================================
    # 4. SEED EMPTY LIVE VALUATION STATE
    # ========================================================

    initial_ladder = {
        "vin": vin,
        "baseline_source": "",
        "stages": [],
        "final_value": 0,
        "rounded_value": None,
        "complete": False,
        "implausible": False,
        "implausible_reason": "",
    }


    initial_stream = {
        "status": "running",
        "stage": "starting",
        "updated_at": (
            now.isoformat(
                timespec="seconds"
            )
            + "Z"
        ),
        "evidence": {},
        "collector_status": {},
        "last_collector_event": None,
        "cards": _empty_cards(),
        "errors": [],
        "workflow": "prism_cloudtask_three_pass_v3",
    }


    initial_andrew = {
        "valuation": {
            "amount": None,
            "source": "pending",
        },

        "final_valuation_number": None,

        "text": "",

        "likely_range": {
            "low": None,
            "high": None,
        },

        "confidence": None,

        "heat": None,

        "anchors": {
            "mmr": None,
            "accutrade": None,
            "same_vin_public_ask": None,
        },
    }


    try:

        update_result = await asyncio.to_thread(
            update_valuation,
            job_id,

            vin=vin,

            job_date=job_date,

            # IMPORTANT:
            # This now contains the Ava photo descriptions.
            car_details=json.dumps(
                data,
                default=str,
            ),

            SA_stream=json.dumps(
                initial_stream,
                default=str,
            ),

            Andrew_stream=json.dumps(
                initial_andrew,
                default=str,
            ),

            LPA_stream=json.dumps(
                initial_ladder,
                default=str,
            ),
        )

    except Exception:

        logging.exception(
            "Failed seeding valuation job=%s vin=%s",
            job_id,
            vin,
        )

        return {
            "vin": vin,
            "status": "error",
            "text": "Could not initialize valuation job.",
        }, 503


    if (
        str(update_result)
        .strip()
        .lower()
        != "success"
    ):

        return {
            "vin": vin,
            "status": "error",
            "text": "Could not initialize valuation job.",
        }, 503


    # ========================================================
    # 5. SCHEDULE CLOUD TASK
    #
    # The enriched `data` object is sent into the worker,
    # so the photo descriptions survive into Pass 2.
    # ========================================================

    try:

        task_name = await asyncio.to_thread(
            _schedule_prism_valuation_task,
            job_id,
            data,
            job_date,
        )

    except Exception as error:

        logging.exception(
            "Failed scheduling valuation Cloud Task "
            "job=%s vin=%s",
            job_id,
            vin,
        )


        error_stream = {
            **initial_stream,

            "status": "error",

            "stage": "error",

            "error": (
                "Could not schedule valuation worker: "
                f"{type(error).__name__}: {error}"
            ),

            "updated_at": (
                datetime.utcnow()
                .isoformat(
                    timespec="seconds"
                )
                + "Z"
            ),
        }


        await asyncio.to_thread(
            update_valuation,
            job_id,
            SA_stream=json.dumps(
                error_stream,
                default=str,
            ),
        )


        return {
            "vin": vin,
            "job_id": job_id,
            "status": "error",
            "stage": "error",
            "text": "Could not schedule valuation worker.",
        }, 503


    # ========================================================
    # 6. STORE CLOUD TASK NAME
    # ========================================================

    initial_stream[
        "cloud_task"
    ] = task_name


    await asyncio.to_thread(
        update_valuation,
        job_id,
        SA_stream=json.dumps(
            initial_stream,
            default=str,
        ),
    )


    logging.info(
        "Queued Prism valuation "
        "job=%s vin=%s task=%s",
        job_id,
        vin,
        task_name,
    )


    # ========================================================
    # EXISTING FRONTEND START CONTRACT
    # ========================================================

    return {
        "vin": vin,
        "job_id": job_id,
        "status": "running",
        "stage": "starting",

        "poll_url": (
            "/api/v2/prism/andrew/valuations/"
            + vin
        ),

        "valuation": None,

        "text": "",

        "cards": _empty_cards(),

        "Value_ladder": [],

        "stage_count": 0,
    }, 200


@app.route("/api/v2/prism/andrew/valuations/<vin>", methods=["GET"])
async def valuation_status(vin):
    """Poll the newest valuation. complete/error/stalled are terminal statuses."""

    if request.headers.get("Authorization") != SA_AUTHENTICATION:
        return "Unauthorized", 403

    vin = clean_vin(vin)
    if not vin.isalnum() or not 11 <= len(vin) <= 17:
        return {"error": "malformed vin"}, 400

    try:
        candidate_rows = await asyncio.to_thread(
          get_valuation_search,
          "vin",
          vin,
          1,
          None,
          ["id"],
        )
        rows = await asyncio.to_thread(
          get_valuation_rows_by_ids,
          [row["id"] for row in candidate_rows],
          ["id", "vin", "job_date", "Andrew_stream", "SA_stream", "LPA_stream"],
        )
    except Exception as error:
        logging.exception("Failed loading valuation status vin=%s", vin)
        return {
            "vin": vin,
            "status": "error",
            "stage": "error",
            "valuation": None,
            "text": f"Failed loading valuation: {type(error).__name__}: {error}",
            "cards": _empty_cards(),
            "Value_ladder": [],
            "stage_count": 0,
            "collector_errors": [],
        }, 500

    rows = rows or []
    if not rows:
        return {
            "vin": vin,
            "status": "unknown",
            "stage": "unknown",
            "cards": _empty_cards(),
            "Value_ladder": [],
            "valuation": None,
            "text": "",
            "stage_count": 0,
            "collector_status": {},
            "last_collector_event": None,
            "collector_errors": [],
        }, 404

    row = sorted(rows, key=lambda r: str(r.get("job_date") or ""), reverse=True)[0]

    andrew = _valuation_load_json(row.get("Andrew_stream"))
    stored = _valuation_load_json(row.get("SA_stream"))
    ladder = _valuation_load_json(row.get("LPA_stream"))

    cards = dict(_empty_cards())
    cards.update(stored.get("cards") or {})

    # LPA_stream is authoritative for both partial and final ladder state.
    stages = ladder.get("stages") or cards.get("Value_ladder") or []
    cards["Value_ladder"] = stages

    valuation = None
    valuation_obj = andrew.get("valuation")
    if isinstance(valuation_obj, dict):
        valuation = valuation_obj.get("amount")
    elif valuation_obj is not None:
        valuation = valuation_obj
    if valuation is None:
        valuation = andrew.get("final_valuation_number")
    if valuation is None and ladder.get("complete") and ladder.get("rounded_value") is not None:
        valuation = ladder.get("rounded_value")

    stored_status = str(stored.get("status") or "").lower()
    stored_stage = str(stored.get("stage") or "").lower()
    failed = stored_status == "error" or stored_stage == "error"
    complete = (
        not failed
        and ladder.get("complete") is True
        and valuation is not None
    )

    stalled = False
    if not complete and not failed:
        try:
            started = datetime.strptime(str(row.get("job_date"))[:19], "%Y-%m-%d %H:%M:%S")
            stalled = (datetime.utcnow() - started).total_seconds() > STALE_AFTER_SECONDS
        except (TypeError, ValueError):
            stalled = False

    if complete:
        status = "complete"
    elif failed:
        status = "error"
    elif stalled:
        status = "stalled"
    else:
        status = "running"

    stage = stored.get("stage") or ("complete" if complete else "running")
    text_value = andrew.get("text") or ""
    if failed and not text_value:
        text_value = str(stored.get("error") or "Valuation failed")

    return {
        "vin": vin,
        "job_id": str(row.get("id") or ""),
        "job_date": str(row.get("job_date") or ""),
        "status": status,
        "stage": stage,
        "valuation": valuation,
        "text": text_value,
        "likely_range": andrew.get("likely_range"),
        "heat": andrew.get("heat"),
        "confidence": andrew.get("confidence"),
        "anchors": andrew.get("anchors"),
        "cards": cards,
        "Value_ladder": stages,
        "stage_count": len(stages),
        "collector_status": stored.get("collector_status") or {},
        "last_collector_event": stored.get("last_collector_event"),
        "collector_errors": list(stored.get("errors") or []),
        "implausible": ladder.get("implausible", False),
        "implausible_reason": ladder.get("implausible_reason", ""),
    }, 200


#DASHBOARD ENDPOINTS
@app.route('/api/v1/prism/block/add', methods=['POST'])
async def add_prism_block():
  data = await request.get_json(silent=True) or {}
  result = block_add(data)
  if result == "Success":
      return ("Block added", 200)
  else:
      return (result, 400)

@app.route('/api/v1/prism/agents/task/schedule', methods=['POST'])
async def schedule_prism_task():
  return "Task scheduled", 200

@app.route('/api/v1/prism/block/get', methods=['POST'])
async def get_prism_blocks():
  auth_token = request.headers.get('Authorization')
  if auth_token != SA_AUTHENTICATION:
    return "Unauthorized", 403
  data = await request.get_json(silent=True) or {}
  lane = data.get("lane")
  date = data.get("date")
  blocks = block_get(lane, date)
  return (blocks, 200)

@app.route('/api/v1/prism/block/search', methods=['POST'])
async def search_prism_blocks():
  data = await request.get_json(silent=True) or {}
  object = data.get("object")
  if not object or "fields" not in object:
      return ("Missing object or fields", 400)
  cars, error = block_search(object)
  if error:
      return (error, 500)
  return (cars, 200)

@app.route("/api/v1/prism/block/update", methods=['POST'])
async def update_prism_block():
  data = await request.get_json(silent=True) or {}
  id = data.get("id")
  data = {k: v for k, v in data.items() if k != "id"}
  if not id:
      return ("Missing id", 400)
  result = block_update(id, **data)
  if result == "Success":
      return ("Block updated", 200)
  else:
      return (result, 400)
@app.route('/instagram_dashboard'  , methods=['GET'])
async def media_dashboard():
  return await send_file('static/instagram_dashboard.html')



def is_user_authorized(email: str) -> bool:
  with engine.connect() as conn:
    result = conn.execute(text("SELECT userId,authorization FROM ava_users"))
    rows = result.mappings().all()
    authorized_emails = [row["userId"] for row in rows]
    authorization = [row["authorization"] for row in rows]

    if email in authorized_emails:
      idx = authorized_emails.index(email)
      auth_token = authorization[idx]
      if not auth_token:
        # Generate a new random token and update the DB
        auth_token = secrets.token_urlsafe(32)
        conn.execute(
          text("""
            UPDATE ava_users
            SET
              authorization = :auth,
              last_authorization_update = NOW()
            WHERE userId = :email
          """),
          {"auth": auth_token, "email": email}
        )
        conn.commit()
      return {
        "authorized": True,
        "authorization": auth_token
      }
    else:
      return {
        "authorized": False,
        "authorization": None
      }

@app.route('/api/v1/prism/verify_user', methods=['POST'])
async def verify_user():
  data = await request.get_json(silent=True) or {}
  token = data.get("id_token")
  if not token:
      return ("Missing id_token", 400)
  try:
      idinfo = id_token.verify_oauth2_token(token, grequests.Request(), OAUTH_CLIENT_ID)

      user_id = idinfo["sub"]
      email = idinfo.get("email")
      logging.info(f"Returned idinfo from Google token verification: {idinfo}")
      if email is None or not is_user_authorized(email)['authorized']:
          return ("Forbidden", 403)
      logging.info(f"User {email} authenticated successfully with Google ID {user_id}")
      
      # create your app session cookie / token here
      resp = {'token' : is_user_authorized(email)['authorization']}, 200
      # resp.set_cookie("sid", sign_session({"uid": user_id, "email": email}), httponly=True, secure=True, samesite="Strict")
      return resp
  except Exception:
      return ("Invalid ID token", 401)


@app.route('/api/v1/prism/add', methods=['POST'])
async def add_prism():
    try:
        message = await request.get_json()
        logging.info(f"Prism add user call...")
        return group_add(message['name'],message['phone']), 200
    except Exception as e:
       logging.error(f"Failed to add buyer, {e}")
       return "Server error", 500
    
@app.route('/api/v1/prism/add/andrew', methods=['POST'])
async def add_andrew_prism():
    try:
        message = await request.get_json()
        logging.info(f"Prism add user call...")
        return andrew_group_add(message['name'],message['phone']), 200
    except Exception as e:
       logging.error(f"Failed to add buyer, {e}")
       return "Server error", 500

@app.route('/api/v1/prism/remove', methods=['POST'])
async def remove_prism():
    try:
        message = await request.get_json()
        logging.info(f"Removing user from buyers...")
        return group_remove(message['phone']), 200
    except Exception as e:
        logging.error(f"Failed to add buyer, {e}")
        return "Server error", 500
    
@app.route('/api/v1/prism/remove/andrew', methods=['POST'])
async def remove_andrew_prism():
    try:
        message = await request.get_json()
        logging.info(f"Removing user from buyers...")
        return andrew_group_remove(message['phone']), 200
    except Exception as e:
        logging.error(f"Failed to add buyer, {e}")
        return "Server error", 500

@app.route('/api/v1/prism/get_group', methods=['GET'])
async def get_prism_group():
    """Return the current buyer group."""
    try:
        logging.info("Sending current group...")
        group = group_init()
        return group, 200
    except Exception as e:
        logging.error(e)
        return "Server error", 500
    
@app.route('/api/v1/prism/andrew/get_group', methods=['GET'])
async def get_andrew_prism_group():
    """Return the current buyer group that have andrew access."""
    try:
        logging.info("Sending current andrew group...")
        group = andrew_group_init()
        return group, 200
    except Exception as e:
        logging.error(e)
        return "Server error", 500