"""
JWT Token Generator for NetApp Proxy API

This script generates JWT tokens for authenticating with the NetApp Proxy endpoint.
Unlike CCFE which uses Bearer tokens from `gcloud auth print-access-token`,
the Proxy endpoint requires JWT tokens signed by a service account.

Usage:
    python proxy-jwt-token.py --service-account /path/to/service-account.json --project-number 123456789

Requirements:
    pip install google-auth google-api-python-client
"""

import json
import time
import argparse
import logging
from google.oauth2 import service_account
from googleapiclient import discovery

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Audience URL for NetApp Proxy
AUDIENCE_URL = "https://cloudvolumesgcp-api.netapp.com"


def generate_jwt_token(service_account_file, audience_url, project_number, token_expiry=3600):
    """
    Generate JWT token for NetApp Proxy API authentication.
    
    Args:
        service_account_file: Path to the service account JSON file
        audience_url: The audience URL (default: https://cloudvolumesgcp-api.netapp.com)
        project_number: GCP project number (numeric)
        token_expiry: Token expiry time in seconds (default: 3600 = 1 hour)
    
    Returns:
        Signed JWT token string
    """
    logger.info("Generating JWT token for Proxy API")
    
    iat = int(time.time())
    exp = iat + token_expiry
    
    logger.debug(f"Reading service account file: {service_account_file}")
    with open(service_account_file, 'r') as f:
        data = f.read()
    
    json_data = json.loads(data)
    client_email = json_data["client_email"]
    
    # Build JWT payload
    payload = json.dumps({
        "sub": client_email,
        "aud": audience_url,
        "iss": client_email,
        "exp": exp,
        "iat": iat,
        "Google": {
            "project_number": int(project_number)
        }
    })
    
    logger.debug(f"JWT Payload: {payload}")

    # Sign the JWT using IAM API
    credentials = service_account.Credentials.from_service_account_file(service_account_file)
    iam = discovery.build('iam', "v1", credentials=credentials)
    
    req = iam.projects().serviceAccounts().signJwt(
        name=f"projects/-/serviceAccounts/{client_email}",
        body={"payload": payload}
    )
    
    signed_jwt = req.execute().get('signedJwt')
    logger.info("JWT token generated successfully")
    
    return signed_jwt


def main():
    parser = argparse.ArgumentParser(description='Generate JWT token for NetApp Proxy API')
    parser.add_argument('--service-account', '-s', required=True, 
                        help='Path to service account JSON file')
    parser.add_argument('--project-number', '-p', required=True,
                        help='GCP project number (numeric)')
    parser.add_argument('--expiry', '-e', type=int, default=3600,
                        help='Token expiry in seconds (default: 3600)')
    parser.add_argument('--audience', '-a', default=AUDIENCE_URL,
                        help=f'Audience URL (default: {AUDIENCE_URL})')
    
    args = parser.parse_args()
    
    token = generate_jwt_token(
        service_account_file=args.service_account,
        audience_url=args.audience,
        project_number=args.project_number,
        token_expiry=args.expiry
    )
    
    print("\n" + "="*60)
    print("JWT Token (use in Authorization header):")
    print("="*60)
    print(token)
    print("="*60)
    print(f"\nExpires in: {args.expiry} seconds")
    print(f"\nUsage in curl:")
    print(f'  -H "Authorization: {token}"')


if __name__ == "__main__":
    main()
